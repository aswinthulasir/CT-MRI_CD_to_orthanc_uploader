"""
DICOM CD Importer — Optimised for 1000+ slices in <60 s on localhost
====================================================================

Pipeline:
  1. Detect CD/removable drives.
  2. Scan DICOM headers directly from CD:
       - Fast path  → parse DICOMDIR index file (ms-level, zero per-file I/O)
       - Fallback   → parallel per-file header scan with PRELOAD_WORKERS threads
  3. Upload to Orthanc individually via httpx with high concurrency
     (MAX_UPLOAD_WORKERS concurrent async coroutines, one POST per file).

Key speed improvements:
  - DICOMDIR fast-path avoids reading every file header individually (ms-level).
  - Only essential DICOM tags decoded in fallback scan (3-5× faster than full).
  - Individual file uploads with massive httpx concurrency (no ZIP overhead).
  - httpx AsyncClient with tuned connection pool & keep-alive.
  - Semaphore-controlled concurrent uploads (MAX_UPLOAD_WORKERS).
  - Preloading files into memory before upload eliminates disk I/O bottleneck.
"""

import json
import os
import platform
import time
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import httpx
import pydicom
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ORTHANC_BASE = "http://localhost:8042"       # Orthanc base URL (no trailing slash)
ORTHANC_URL  = ORTHANC_BASE + "/instances"   # Upload endpoint
ORTHANC_USER = "admin"
ORTHANC_PASS = "password"

MAX_UPLOAD_WORKERS = 64          # concurrent async upload coroutines
PRELOAD_WORKERS    = 16          # threads for parallel disk reads
UPLOAD_TIMEOUT     = 30          # seconds per individual file upload

# DICOM tags needed for fallback header scan (avoids decoding the whole header)
_DICOM_TAGS = [
    0x00100010,  # PatientName
    0x0020000D,  # StudyInstanceUID
    0x00081030,  # StudyDescription
    0x00080020,  # StudyDate
    0x00080060,  # Modality
    0x00100020,  # PatientID
    0x00100040,  # PatientSex
    0x00101010,  # PatientAge
    0x00080030,  # StudyTime
    0x00200011,  # SeriesNumber
    0x0020000E,  # SeriesInstanceUID
]

app       = FastAPI()
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
scan_cache: dict = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def _norm_str(val) -> str:
    """Convert any pydicom value to a plain stripped string."""
    return "" if val is None else str(val).strip()


def _print_scan_timing(method: str, elapsed: float, file_count: int,
                       patient_count: int = 0, study_count: int = 0,
                       series_count: int = 0) -> None:
    """Print a detailed timing summary after scanning."""
    border = "=" * 60
    print(f"\n+{border}+")
    print(f"|  SCAN TIMING REPORT  ({method})")
    print(f"+{border}+")
    print(f"|  Total time        : {_fmt_duration(elapsed)}  ({elapsed * 1000:.1f} ms)")
    print(f"|  Files found       : {file_count}")
    if patient_count:
        print(f"|  Patients          : {patient_count}")
    if study_count:
        print(f"|  Studies           : {study_count}")
    if series_count:
        print(f"|  Series            : {series_count}")
    if elapsed > 0 and file_count > 0:
        rate = file_count / elapsed
        print(f"|  Scan rate         : {rate:.0f} files/sec")
    print(f"+{border}+\n")


# ---------------------------------------------------------------------------
# Step 1 — CD / removable drive detection
# ---------------------------------------------------------------------------

def detect_cd_drives() -> list:
    system, drives = platform.system(), []
    if system == "Windows":
        import ctypes
        bitmask = ctypes.windll.kernel32.GetLogicalDrives()
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            if bitmask & 1:
                path = f"{letter}:\\"
                if ctypes.windll.kernel32.GetDriveTypeW(path) in (5, 2):
                    drives.append(path)
            bitmask >>= 1
    elif system == "Linux":
        for base in ("/media", "/mnt", "/run/media"):
            if os.path.isdir(base):
                for entry in os.scandir(base):
                    if entry.is_dir():
                        drives.append(entry.path)
                        try:
                            for sub in os.scandir(entry.path):
                                if sub.is_dir():
                                    drives.append(sub.path)
                        except PermissionError:
                            pass
        try:
            with open("/proc/mounts") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2 and parts[0].startswith("/dev/sr"):
                        if parts[1] not in drives:
                            drives.append(parts[1])
        except Exception:
            pass
    elif system == "Darwin":
        if os.path.isdir("/Volumes"):
            for entry in os.scandir("/Volumes"):
                if entry.is_dir():
                    drives.append(entry.path)
    return drives


# ---------------------------------------------------------------------------
# Step 2a — Fast path: parse DICOMDIR index (when available)
# ---------------------------------------------------------------------------

def _parse_dicomdir(dicomdir_path: str) -> dict:
    """
    Parse a DICOMDIR file and return the same dict structure as scan_drive().
    This is orders-of-magnitude faster than reading every DICOM file header.
    """
    dicomdir_path = os.path.abspath(dicomdir_path)
    dicomdir_dir  = os.path.dirname(dicomdir_path)
    t0 = time.monotonic()
    print(f"[DICOMDIR] Parsing: {dicomdir_path}")
    print(f"[DICOMDIR] File-set root: {dicomdir_dir}")

    dicomdir = pydicom.dcmread(dicomdir_path)
    if not hasattr(dicomdir, "DirectoryRecordSequence"):
        print("[DICOMDIR] No DirectoryRecordSequence — cannot parse.")
        return {}

    studies: dict = defaultdict(lambda: defaultdict(list))
    cur_patient = cur_patient_id = cur_patient_sex = cur_patient_age = ""
    cur_study_uid = cur_desc = cur_date = cur_study_time = ""
    cur_study_id = cur_accession = cur_modality = ""
    cur_series_desc = cur_series_num = cur_series_uid = ""
    cur_manufacturer = cur_model = cur_institution = ""
    image_count = patient_count = study_count = series_count = 0
    first_path_shown = False

    for record in dicomdir.DirectoryRecordSequence:
        rtype = _norm_str(getattr(record, "DirectoryRecordType", "")).upper()

        if rtype == "PATIENT":
            patient_count  += 1
            cur_patient_id  = _norm_str(getattr(record, "PatientID",  ""))
            cur_patient_sex = _norm_str(getattr(record, "PatientSex", ""))
            cur_patient_age = _norm_str(getattr(record, "PatientAge", ""))
            cur_patient     = _norm_str(getattr(record, "PatientName", "Unknown")) or "Unknown"

        elif rtype == "STUDY":
            study_count   += 1
            cur_study_uid  = _norm_str(getattr(record, "StudyInstanceUID", "Unknown")) or "Unknown"
            cur_desc       = _norm_str(getattr(record, "StudyDescription", ""))
            cur_date       = _norm_str(getattr(record, "StudyDate",        ""))
            cur_study_time = _norm_str(getattr(record, "StudyTime",        ""))
            cur_study_id   = _norm_str(getattr(record, "StudyID",          ""))
            cur_accession  = _norm_str(getattr(record, "AccessionNumber",  ""))

        elif rtype == "SERIES":
            series_count    += 1
            cur_modality     = _norm_str(getattr(record, "Modality",              ""))
            cur_series_desc  = _norm_str(getattr(record, "SeriesDescription",     ""))
            cur_series_num   = _norm_str(getattr(record, "SeriesNumber",          ""))
            cur_series_uid   = _norm_str(getattr(record, "SeriesInstanceUID",     ""))
            cur_manufacturer = _norm_str(getattr(record, "Manufacturer",          ""))
            cur_model        = _norm_str(getattr(record, "ManufacturerModelName", ""))
            cur_institution  = _norm_str(getattr(record, "InstitutionName",       ""))

        elif rtype == "IMAGE":
            ref_file_id = getattr(record, "ReferencedFileID", None)
            if ref_file_id is None:
                continue

            if hasattr(ref_file_id, "__iter__") and not isinstance(ref_file_id, str):
                parts = [str(p).strip() for p in ref_file_id if str(p).strip()]
            else:
                raw = str(ref_file_id).strip()
                if "\\" in raw:
                    parts = [p for p in raw.split("\\") if p]
                elif "/" in raw:
                    parts = [p for p in raw.split("/") if p]
                else:
                    parts = [raw]

            if not parts:
                continue

            file_path = os.path.join(dicomdir_dir, *parts)

            if not first_path_shown:
                first_path_shown = True
                exists = os.path.isfile(file_path)
                print(f"[DICOMDIR] Sample path: {file_path!r}  exists={exists}")
                if not exists:
                    print("[DICOMDIR] WARNING: file not found — check path construction!")
                    print(f"[DICOMDIR]   dicomdir_dir={dicomdir_dir!r}  parts={parts}")

            studies[cur_patient][cur_study_uid].append({
                "path":            file_path,
                "patient":         cur_patient,
                "patient_id":      cur_patient_id,
                "patient_sex":     cur_patient_sex,
                "patient_age":     cur_patient_age,
                "study_uid":       cur_study_uid,
                "desc":            cur_desc,
                "date":            cur_date,
                "study_time":      cur_study_time,
                "study_id":        cur_study_id,
                "accession":       cur_accession,
                "modality":        cur_modality,
                "series_desc":     cur_series_desc,
                "series_num":      cur_series_num,
                "series_uid":      cur_series_uid,
                "manufacturer":    cur_manufacturer,
                "model":           cur_model,
                "institution":     cur_institution,
                "slice_thickness": _norm_str(getattr(record, "SliceThickness", "")),
                "rows":            _norm_str(getattr(record, "Rows",           "")),
                "columns":         _norm_str(getattr(record, "Columns",        "")),
                "instance_number": _norm_str(getattr(record, "InstanceNumber", "")),
            })
            image_count += 1

    elapsed = time.monotonic() - t0
    print(
        f"[DICOMDIR] Parsed in {elapsed * 1000:.0f}ms — "
        f"{patient_count} patient(s), {study_count} study(ies), "
        f"{series_count} series, {image_count} images"
    )
    _print_scan_timing(
        method="DICOMDIR",
        elapsed=elapsed,
        file_count=image_count,
        patient_count=patient_count,
        study_count=study_count,
        series_count=series_count,
    )
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
# Step 2b — Fallback: parallel DICOM header scan (no DICOMDIR)
# ---------------------------------------------------------------------------

def _read_dicom_header(fpath: str) -> Optional[dict]:
    """Read only the tags we need — 3-5× faster than full header decode."""
    try:
        ds = pydicom.dcmread(fpath, specific_tags=_DICOM_TAGS)
        return {
            "path":            fpath,
            "patient":         _norm_str(getattr(ds, "PatientName",      "Unknown")) or "Unknown",
            "patient_id":      _norm_str(getattr(ds, "PatientID",        "")),
            "patient_sex":     _norm_str(getattr(ds, "PatientSex",       "")),
            "patient_age":     _norm_str(getattr(ds, "PatientAge",       "")),
            "study_uid":       _norm_str(getattr(ds, "StudyInstanceUID", "Unknown")) or "Unknown",
            "desc":            _norm_str(getattr(ds, "StudyDescription", "")),
            "date":            _norm_str(getattr(ds, "StudyDate",        "")),
            "study_time":      _norm_str(getattr(ds, "StudyTime",        "")),
            "study_id":        "",
            "accession":       "",
            "modality":        _norm_str(getattr(ds, "Modality",         "")),
            "series_desc":     "",
            "series_num":      _norm_str(getattr(ds, "SeriesNumber",     "")),
            "series_uid":      _norm_str(getattr(ds, "SeriesInstanceUID","")),
            "manufacturer":    "",
            "model":           "",
            "institution":     "",
            "slice_thickness": "",
            "rows":            "",
            "columns":         "",
            "instance_number": "",
        }
    except Exception:
        return None


def scan_drive(drive_path: str) -> dict:
    """
    Scan a directory for DICOM metadata.

    Fast path  → try DICOMDIR (milliseconds, no per-file I/O).
    Fallback   → parallel per-file header scan with PRELOAD_WORKERS threads.
    """
    # Fast path — DICOMDIR (case-insensitive check)
    for name in ("DICOMDIR", "dicomdir"):
        dicomdir_path = os.path.join(drive_path, name)
        if os.path.isfile(dicomdir_path):
            print("[SCAN] DICOMDIR found — using fast path")
            result = _parse_dicomdir(dicomdir_path)
            if result:
                return result
            print("[SCAN] DICOMDIR parse returned empty — falling back to full scan")
            break

    # Fallback — walk + parallel header scan
    print(f"[SCAN] Walking directory: {drive_path}")
    t0 = time.monotonic()
    all_paths = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(drive_path)
        for fname in files
    ]
    print(f"[SCAN] Found {len(all_paths)} files. Scanning DICOM headers with "
          f"{PRELOAD_WORKERS} threads ...")

    studies: dict = defaultdict(lambda: defaultdict(list))
    scanned = valid = 0
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        for result in pool.map(_read_dicom_header, all_paths):
            scanned += 1
            if scanned % 200 == 0:
                print(f"[SCAN]   scanned {scanned}/{len(all_paths)} headers ...")
            if result:
                valid += 1
                studies[result["patient"]][result["study_uid"]].append(result)

    elapsed = time.monotonic() - t0
    print(f"[SCAN] Scan complete in {_fmt_duration(elapsed)}: "
          f"{valid} DICOM files, {len(studies)} patients, "
          f"{sum(len(s) for s in studies.values())} studies")
    _print_scan_timing(
        method="File Scan (fallback)",
        elapsed=elapsed,
        file_count=valid,
        patient_count=len(studies),
        study_count=sum(len(s) for s in studies.values()),
    )
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
# Step 3a — Preload files into memory
# ---------------------------------------------------------------------------

def _load_file(info: dict) -> dict:
    try:
        with open(info["path"], "rb") as fh:
            return {**info, "data": fh.read()}
    except Exception as exc:
        return {**info, "data": None, "load_error": str(exc)}


def preload_files(files: list) -> list:
    print(f"[PRELOAD] Loading {len(files)} files into memory with "
          f"{PRELOAD_WORKERS} threads ...")
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        result = list(pool.map(_load_file, files))
    elapsed = time.monotonic() - t0
    ok = sum(1 for r in result if r.get("data"))
    print(f"[PRELOAD] Loaded {ok}/{len(files)} files in {_fmt_duration(elapsed)}")
    return result


# ---------------------------------------------------------------------------
# Step 3b — Upload a single file via httpx (replaces batched ZIP)
# ---------------------------------------------------------------------------

async def _upload_single(file_info: dict, client: httpx.AsyncClient) -> dict:
    """Upload a single DICOM file to Orthanc using httpx."""
    study_uid = file_info.get("study_uid", "__unknown__")

    if not file_info.get("data"):
        return {
            "path":      file_info["path"],
            "ok":        False,
            "error":     file_info.get("load_error", "read error"),
            "study_uid": study_uid,
        }

    try:
        r = await client.post(
            ORTHANC_URL,
            content=file_info["data"],
            headers={"Content-Type": "application/dicom"},
            timeout=UPLOAD_TIMEOUT,
        )
        # Extract Orthanc's internal ID for the parent study (for C-STORE)
        orthanc_study_id = ""
        if r.status_code == 200:
            try:
                body = r.json()
                orthanc_study_id = body.get("ParentStudy", "")
            except Exception:
                pass
        return {
            "path":            file_info["path"],
            "ok":              r.status_code in (200, 409),
            "status":          r.status_code,
            "study_uid":       study_uid,
            "orthanc_study_id": orthanc_study_id,
        }
    except Exception as exc:
        return {
            "path":      file_info["path"],
            "ok":        False,
            "error":     str(exc),
            "study_uid": study_uid,
        }


# ---------------------------------------------------------------------------
# Step 3c — Collect files from scan cache
# ---------------------------------------------------------------------------

def collect_files(patient: Optional[str], study: Optional[str]) -> list:
    files    = []
    patients = [patient] if patient else list(scan_cache.keys())
    for p in patients:
        studies = [study] if (study and patient) else list(scan_cache.get(p, {}).keys())
        for s in studies:
            files.extend(scan_cache.get(p, {}).get(s, []))
    return files


# ---------------------------------------------------------------------------
# Step 3d — SSE upload stream
# ---------------------------------------------------------------------------

async def upload_stream(files: list):
    """
    SSE generator: preload all files -> concurrent individual httpx uploads ->
    emit per-file and per-study progress events.
    """
    total      = len(files)
    done_count = 0
    failed     = 0
    wall_start = time.monotonic()
    # Collect unique Orthanc study IDs for post-upload C-STORE
    orthanc_study_ids: set = set()

    print(f"[UPLOAD] Starting upload of {total} files to Orthanc ...")
    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

    study_totals: dict = defaultdict(int)
    study_counts: dict = defaultdict(int)
    study_failed: dict = defaultdict(int)
    study_starts: dict = {}
    for f in files:
        study_totals[f.get("study_uid", "__unknown__")] += 1

    # Preload all files from CD into memory before uploading
    preload_msg = {
        'type': 'progress',
        'done': 0,
        'total': total,
        'failed': 0,
        'file': 'Pre-loading files into memory...',
        'ok': True,
        'elapsed': 0.0,
        'study_uid': ''
    }
    yield f"data: {json.dumps(preload_msg)}\n\n"
    loaded = await asyncio.get_event_loop().run_in_executor(None, preload_files, files)

    # httpx connection pool sized for maximum concurrency
    limits = httpx.Limits(
        max_connections=MAX_UPLOAD_WORKERS + 4,
        max_keepalive_connections=MAX_UPLOAD_WORKERS,
    )
    sem = asyncio.Semaphore(MAX_UPLOAD_WORKERS)
    # Queue for collecting results as they complete
    result_queue: asyncio.Queue = asyncio.Queue()

    print(f"[UPLOAD] Uploading {total} files individually "
          f"with {MAX_UPLOAD_WORKERS} concurrent workers ...")

    async def upload_single_sem(file_info, client):
        uid = file_info.get("study_uid", "__unknown__")
        if uid not in study_starts:
            study_starts[uid] = time.monotonic()
        async with sem:
            result = await _upload_single(file_info, client)
        await result_queue.put(result)

    async with httpx.AsyncClient(
        auth=(ORTHANC_USER, ORTHANC_PASS), limits=limits
    ) as client:
        # Fire off all upload tasks
        tasks = [
            asyncio.create_task(upload_single_sem(f, client))
            for f in loaded
        ]

        # Consume results as they arrive
        for _ in range(total):
            result = await result_queue.get()
            done_count += 1
            uid = result.get("study_uid", "__unknown__")
            study_counts[uid] += 1
            # Track Orthanc internal study ID for C-STORE
            oid = result.get("orthanc_study_id", "")
            if oid:
                orthanc_study_ids.add(oid)
            if not result["ok"]:
                failed += 1
                study_failed[uid] += 1

            elapsed = time.monotonic() - wall_start
            progress_data = {
                'type': 'progress',
                'done': done_count,
                'total': total,
                'failed': failed,
                'file': os.path.basename(result['path']),
                'ok': result['ok'],
                'elapsed': round(elapsed, 1),
                'study_uid': uid
            }
            yield f"data: {json.dumps(progress_data)}\n\n"

            if study_counts[uid] == study_totals[uid]:
                study_elapsed = time.monotonic() - study_starts.get(uid, wall_start)
                s_ok = study_totals[uid] - study_failed[uid]
                print(f"[UPLOAD] Study {uid}: {s_ok} succeeded, "
                      f"{study_failed[uid]} failed in {_fmt_duration(study_elapsed)}")
                study_data = {
                    'type': 'study_done',
                    'study_uid': uid,
                    'elapsed': round(study_elapsed, 1),
                    'elapsed_str': _fmt_duration(study_elapsed),
                    'succeeded': s_ok,
                    'failed': study_failed[uid]
                }
                yield f"data: {json.dumps(study_data)}\n\n"

        # Ensure all tasks are done (should be, since we consumed all results)
        await asyncio.gather(*tasks, return_exceptions=True)

    total_elapsed = time.monotonic() - wall_start
    upload_rate = total / total_elapsed if total_elapsed > 0 else 0
    print(f"[UPLOAD] Upload complete: {total - failed} succeeded, "
          f"{failed} failed in {_fmt_duration(total_elapsed)} "
          f"({upload_rate:.1f} files/sec)")

    done_data = {
        'type': 'done',
        'total': total,
        'succeeded': total - failed,
        'failed': failed,
        'elapsed': round(total_elapsed, 1),
        'elapsed_str': _fmt_duration(total_elapsed),
        'orthanc_study_ids': list(orthanc_study_ids),
    }
    yield f"data: {json.dumps(done_data)}\n\n"


# ---------------------------------------------------------------------------
# Routes  (all preserved — no frontend changes)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html", {"request": request, "cd_drives": detect_cd_drives()}
    )


@app.post("/scan", response_class=HTMLResponse)
async def scan(request: Request, drive: str = Form(...)):
    loop = asyncio.get_event_loop()

    print(f"\n{'='*60}")
    print(f"[PIPELINE] Starting DICOM scan for drive: {drive}")
    print(f"{'='*60}")

    t_pipeline = time.monotonic()

    # Scan DICOM headers directly from drive (fast DICOMDIR fast-path when available)
    print(f"\n[PIPELINE] Scanning DICOM headers from drive ...")
    studies_raw = await loop.run_in_executor(None, scan_drive, drive)

    scan_cache.clear()
    scan_cache.update(studies_raw)

    pipeline_elapsed = time.monotonic() - t_pipeline
    total_dicoms = sum(
        len(fl) for stds in studies_raw.values() for fl in stds.values()
    )
    print(f"\n[PIPELINE] Scan done in {_fmt_duration(pipeline_elapsed)}: "
          f"{total_dicoms} DICOM files found")
    print("[PIPELINE] Upload will begin when user confirms.\n")

    flat = []
    for patient, studies_dict in studies_raw.items():
        for uid, file_list in studies_dict.items():
            first = file_list[0] if file_list else {}
            flat.append({
                "patient":   patient,
                "study_uid": uid,
                "desc":      first.get("desc",     ""),
                "date":      first.get("date",     ""),
                "modality":  first.get("modality", ""),
                "count":     len(file_list),
            })
    return templates.TemplateResponse(
        "results.html", {"request": request, "studies": flat, "drive": drive}
    )


@app.get("/upload-stream")
async def upload_stream_route(patient: str = "", study: str = ""):
    files = collect_files(patient or None, study or None)
    print(f"\n[PIPELINE] Starting upload of {len(files)} files to Orthanc")
    return StreamingResponse(
        upload_stream(files),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/detect-drives")
def detect_drives_route():
    return {"drives": detect_cd_drives()}


# ---------------------------------------------------------------------------
# DICOM Modality endpoints
# ---------------------------------------------------------------------------

@app.get("/modalities")
async def list_modalities():
    """List DICOM modalities configured in Orthanc."""
    try:
        async with httpx.AsyncClient(
            auth=(ORTHANC_USER, ORTHANC_PASS), timeout=10
        ) as client:
            r = await client.get(f"{ORTHANC_BASE}/modalities")
            if r.status_code == 200:
                modalities = r.json()  # returns a list of modality names
                print(f"[MODALITY] Found {len(modalities)} configured modality(ies): {modalities}")
                return {"modalities": modalities}
            return JSONResponse(
                {"error": f"Orthanc returned {r.status_code}"},
                status_code=r.status_code,
            )
    except Exception as exc:
        print(f"[MODALITY] Error listing modalities: {exc}")
        return JSONResponse({"error": str(exc)}, status_code=502)


class SendToModalityRequest(BaseModel):
    modality: str
    orthanc_study_ids: list[str]


@app.post("/send-to-modality")
async def send_to_modality(req: SendToModalityRequest):
    """
    Send uploaded studies to a DICOM modality via Orthanc C-STORE.
    Uses POST /modalities/{name}/store with a list of Orthanc study IDs.
    """
    modality = req.modality
    study_ids = req.orthanc_study_ids

    if not study_ids:
        return JSONResponse({"error": "No study IDs provided"}, status_code=400)

    print(f"[MODALITY] Sending {len(study_ids)} study(ies) to modality '{modality}'")
    print(f"[MODALITY] Study IDs: {study_ids}")

    try:
        async with httpx.AsyncClient(
            auth=(ORTHANC_USER, ORTHANC_PASS), timeout=300
        ) as client:
            r = await client.post(
                f"{ORTHANC_BASE}/modalities/{modality}/store",
                json=study_ids,
            )
            if r.status_code == 200:
                print(f"[MODALITY] C-STORE to '{modality}' succeeded")
                return {"ok": True, "modality": modality, "studies_sent": len(study_ids)}
            else:
                body = r.text
                print(f"[MODALITY] C-STORE failed: {r.status_code} — {body}")
                return JSONResponse(
                    {"ok": False, "error": f"Orthanc returned {r.status_code}: {body}"},
                    status_code=r.status_code,
                )
    except Exception as exc:
        print(f"[MODALITY] C-STORE error: {exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)