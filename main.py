"""
DICOM CD Importer — DICOMDIR-first, no temp-copy pipeline
==========================================================

Pipeline:
  1. Detect CD/removable drives.
  2. FAST SCAN: Search for DICOMDIR at the root or one level deep.
     If found, parse it directly to extract all file paths + metadata
     without touching any individual DICOM file.
  3. If no DICOMDIR found, fall back to parallel DICOM header scan.
  4. Upload to Orthanc in batched ZIPs directly from the CD/drive.

Key speed improvements over the old mirror-copy pipeline:
  - No temp folder creation — files are read directly from the source.
  - DICOMDIR parse is near-instant (single small file read).
  - Full patient metadata (Name, ID, Age, Sex) available without
    scanning any individual DICOM file.
  - Batched ZIP uploads (BATCH_SIZE files per POST).
  - Async semaphore-controlled concurrent uploads (MAX_UPLOAD_WORKERS).
  - httpx connection pool explicitly sized.
"""

import io
import json
import os
import platform
import time
import zipfile
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import httpx
import pydicom
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ORTHANC_URL  = "http://localhost:8041/instances"
ORTHANC_USER = "admin"
ORTHANC_PASS = "password"

MAX_UPLOAD_WORKERS = 32   # concurrent async upload coroutines
BATCH_SIZE         = 50   # DICOM files per ZIP POST
PRELOAD_WORKERS    = 16   # threads for parallel disk reads

# DICOM tags we actually need for fallback scanning (avoids decoding whole header)
_DICOM_TAGS = [
    0x00100010,  # PatientName
    0x00100020,  # PatientID
    0x00100030,  # PatientBirthDate
    0x00100040,  # PatientSex
    0x00101010,  # PatientAge
    0x0020000D,  # StudyInstanceUID
    0x00081030,  # StudyDescription
    0x00080020,  # StudyDate
    0x00080060,  # Modality
]

app      = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
scan_cache: dict = {}


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


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
# Step 2a — DICOMDIR discovery
# ---------------------------------------------------------------------------

def find_dicomdir(drive_path: str) -> str | None:
    """
    Search for a DICOMDIR file at the drive root or one level deep.

    Priority:
      1. <drive_path>/DICOMDIR
      2. <drive_path>/DICOMDIR.  (some CDs omit extension separator)
      3. <drive_path>/<subdir>/DICOMDIR  for each immediate subdirectory
    Returns the absolute path if found, else None.
    """
    # Case-insensitive candidates at root
    candidates = ["DICOMDIR", "dicomdir", "Dicomdir"]
    for name in candidates:
        p = os.path.join(drive_path, name)
        if os.path.isfile(p):
            print(f"[DICOMDIR] Found at root: {p}")
            return p

    # One level deep
    try:
        for entry in os.scandir(drive_path):
            if entry.is_dir():
                for name in candidates:
                    p = os.path.join(entry.path, name)
                    if os.path.isfile(p):
                        print(f"[DICOMDIR] Found in subdirectory: {p}")
                        return p
    except PermissionError:
        pass

    print(f"[DICOMDIR] Not found under: {drive_path}")
    return None


# ---------------------------------------------------------------------------
# Step 2b — Parse DICOMDIR into studies dict (fast, single-file read)
# ---------------------------------------------------------------------------

def _norm_str(val) -> str:
    """Safely convert a DICOM attribute value to a clean string."""
    s = str(val).strip()
    return s if s else ""


def scan_from_dicomdir(dicomdir_path: str) -> dict:
    """
    Parse a DICOMDIR file and return the same studies dict structure as
    scan_drive_fallback(), but without touching any individual DICOM instance file.

    Returns:
        { patient_str: { study_uid: [ file_info_dict, ... ], ... }, ... }

    Each file_info_dict contains:
        path, patient, patient_id, patient_sex, patient_age,
        study_uid, desc, date, modality
    """
    # IMPORTANT: use absolute path so dirname is never empty string
    dicomdir_path = os.path.abspath(dicomdir_path)
    dicomdir_dir  = os.path.dirname(dicomdir_path)
    t0 = time.monotonic()
    print(f"[DICOMDIR] Parsing: {dicomdir_path}")
    print(f"[DICOMDIR] File-set root (base for file paths): {dicomdir_dir}")

    dicomdir = pydicom.dcmread(dicomdir_path)

    if not hasattr(dicomdir, "DirectoryRecordSequence"):
        print("[DICOMDIR] No DirectoryRecordSequence — cannot parse.")
        return {}

    studies: dict = defaultdict(lambda: defaultdict(list))

    # State carried through the flat record list
    current_patient     = "Unknown"
    current_patient_id  = ""
    current_patient_sex = ""
    current_patient_age = ""
    current_study_uid   = "Unknown"
    current_desc        = ""
    current_date        = ""
    current_modality    = ""

    image_count   = 0
    patient_count = 0
    study_count   = 0
    first_path_shown = False

    for record in dicomdir.DirectoryRecordSequence:
        rtype = _norm_str(getattr(record, "DirectoryRecordType", ""))

        if rtype == "PATIENT":
            current_patient     = _norm_str(getattr(record, "PatientName",  "Unknown")) or "Unknown"
            current_patient_id  = _norm_str(getattr(record, "PatientID",    ""))
            current_patient_sex = _norm_str(getattr(record, "PatientSex",   ""))
            current_patient_age = _norm_str(getattr(record, "PatientAge",   ""))
            patient_count += 1

        elif rtype == "STUDY":
            current_study_uid = _norm_str(getattr(record, "StudyInstanceUID", "Unknown")) or "Unknown"
            current_desc      = _norm_str(getattr(record, "StudyDescription", ""))
            current_date      = _norm_str(getattr(record, "StudyDate",        ""))
            study_count += 1

        elif rtype == "SERIES":
            # Modality is typically on the SERIES record
            current_modality = _norm_str(getattr(record, "Modality", ""))

        elif rtype == "IMAGE":
            ref_file_id = getattr(record, "ReferencedFileID", None)
            if ref_file_id is None:
                continue

            # ReferencedFileID may be:
            #   - A pydicom MultiValue / list: ['A', 'Z01']
            #   - A plain string with backslash separators: 'A\\Z01'
            #   - A single string with no separator: 'A/Z01'
            if hasattr(ref_file_id, '__iter__') and not isinstance(ref_file_id, str):
                # list / MultiValue — each element is one path component
                parts = [str(p).strip() for p in ref_file_id if str(p).strip()]
            else:
                raw = str(ref_file_id).strip()
                # Try backslash first (DICOM standard separator on ISO 9660)
                if "\\" in raw:
                    parts = [p for p in raw.split("\\") if p]
                elif "/" in raw:
                    parts = [p for p in raw.split("/") if p]
                else:
                    parts = [raw]

            if not parts:
                continue

            file_path = os.path.join(dicomdir_dir, *parts)

            # Show the first constructed path so we can spot issues immediately
            if not first_path_shown:
                first_path_shown = True
                exists = os.path.isfile(file_path)
                print(f"[DICOMDIR] Sample path: {file_path!r}  exists={exists}")
                if not exists:
                    print(f"[DICOMDIR] WARNING: first file not found — check path construction!")
                    print(f"[DICOMDIR]   dicomdir_dir={dicomdir_dir!r}  parts={parts}")

            studies[current_patient][current_study_uid].append({
                "path":        file_path,
                "patient":     current_patient,
                "patient_id":  current_patient_id,
                "patient_sex": current_patient_sex,
                "patient_age": current_patient_age,
                "study_uid":   current_study_uid,
                "desc":        current_desc,
                "date":        current_date,
                "modality":    current_modality,
            })
            image_count += 1

    elapsed = time.monotonic() - t0
    print(
        f"[DICOMDIR] ✓ Parsed in {_fmt_duration(elapsed)}: "
        f"{patient_count} patients, {study_count} studies, {image_count} images"
    )
    return {p: dict(s) for p, s in studies.items()}



# ---------------------------------------------------------------------------
# Step 2c — Fallback: parallel DICOM header scan (used when no DICOMDIR)
# ---------------------------------------------------------------------------

def _read_dicom_header(fpath: str) -> dict | None:
    """Read only the tags we need — faster than full header decode."""
    try:
        ds = pydicom.dcmread(fpath, specific_tags=_DICOM_TAGS)
        return {
            "path":        fpath,
            "patient":     _norm_str(getattr(ds, "PatientName",      "Unknown")) or "Unknown",
            "patient_id":  _norm_str(getattr(ds, "PatientID",        "")),
            "patient_sex": _norm_str(getattr(ds, "PatientSex",       "")),
            "patient_age": _norm_str(getattr(ds, "PatientAge",       "")),
            "study_uid":   _norm_str(getattr(ds, "StudyInstanceUID", "Unknown")) or "Unknown",
            "desc":        _norm_str(getattr(ds, "StudyDescription", "")),
            "date":        _norm_str(getattr(ds, "StudyDate",        "")),
            "modality":    _norm_str(getattr(ds, "Modality",         "")),
        }
    except Exception:
        return None


def scan_drive_fallback(drive_path: str) -> dict:
    """
    Fallback: walk the entire drive and scan DICOM headers in parallel.
    Used only when no DICOMDIR is present.
    """
    print(f"[SCAN] Fallback walk of: {drive_path}")
    t0 = time.monotonic()

    all_paths = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(drive_path)
        for fname in files
    ]
    print(f"[SCAN] Found {len(all_paths)} files. Scanning headers with "
          f"{PRELOAD_WORKERS} threads …")

    studies: dict = defaultdict(lambda: defaultdict(list))
    scanned = 0
    valid   = 0
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        for result in pool.map(_read_dicom_header, all_paths):
            scanned += 1
            if scanned % 200 == 0:
                print(f"[SCAN]   scanned {scanned}/{len(all_paths)} headers …")
            if result:
                valid += 1
                studies[result["patient"]][result["study_uid"]].append(result)

    elapsed = time.monotonic() - t0
    num_patients = len(studies)
    num_studies  = sum(len(s) for s in studies.values())
    print(f"[SCAN] ✓ Done in {_fmt_duration(elapsed)}: "
          f"{valid} DICOM files, {num_patients} patients, {num_studies} studies")
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
# Step 3a — ZIP buffer assembly
# ---------------------------------------------------------------------------

def _make_zip(batch: list[dict]) -> bytes:
    """Pack a list of pre-loaded file dicts into an in-memory ZIP."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        for item in batch:
            if item.get("data"):
                zf.writestr(os.path.basename(item["path"]), item["data"])
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Step 3b — Preload files into memory (reads directly from source drive)
# ---------------------------------------------------------------------------

def _load_file(info: dict) -> dict:
    try:
        with open(info["path"], "rb") as fh:
            return {**info, "data": fh.read()}
    except Exception as exc:
        # Print first few errors so path problems are immediately visible
        _load_file._err_count = getattr(_load_file, "_err_count", 0) + 1
        if _load_file._err_count <= 5:
            print(f"[PRELOAD]   ✗ Cannot read {info['path']!r}: {exc}")
        return {**info, "data": None, "load_error": str(exc)}


_load_file._err_count = 0  # reset on module load



def preload_files(files: list) -> list:
    _load_file._err_count = 0  # reset per-run so errors always print
    print(f"[PRELOAD] Loading {len(files)} files into memory with "
          f"{PRELOAD_WORKERS} threads …")
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        result = list(pool.map(_load_file, files))
    elapsed = time.monotonic() - t0
    ok = sum(1 for r in result if r.get("data"))
    failed_count = len(files) - ok
    print(f"[PRELOAD] ✓ Loaded {ok}/{len(files)} files in {_fmt_duration(elapsed)}"
          + (f" ({failed_count} FAILED)" if failed_count else ""))
    return result


# ---------------------------------------------------------------------------
# Step 3c — Upload helpers
# ---------------------------------------------------------------------------

async def _upload_batch(batch: list[dict], client: httpx.AsyncClient) -> list[dict]:
    """Upload a ZIP bundle to Orthanc."""
    study_uid  = batch[0].get("study_uid", "__unknown__") if batch else "__unknown__"
    ok_items   = [f for f in batch if f.get("data")]
    fail_items = [f for f in batch if not f.get("data")]

    results = [
        {"path": f["path"], "ok": False, "error": f.get("load_error", "read error"),
         "study_uid": f.get("study_uid", study_uid)}
        for f in fail_items
    ]

    if ok_items:
        try:
            zip_bytes = await asyncio.get_event_loop().run_in_executor(
                None, _make_zip, ok_items
            )
            r = await client.post(ORTHANC_URL, content=zip_bytes, timeout=60)
            success = r.status_code in (200, 409)
            for f in ok_items:
                results.append({
                    "path": f["path"], "ok": success, "status": r.status_code,
                    "study_uid": f.get("study_uid", study_uid),
                })
        except Exception as exc:
            for f in ok_items:
                results.append({
                    "path": f["path"], "ok": False, "error": str(exc),
                    "study_uid": f.get("study_uid", study_uid),
                })
    return results


def _chunk(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def collect_files(patient, study) -> list:
    files = []
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
    """SSE generator for file upload."""
    total      = len(files)
    done_count = 0
    failed     = 0
    wall_start = time.monotonic()

    print(f"[UPLOAD] Starting upload of {total} files to Orthanc …")
    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

    study_totals: dict[str, int]   = defaultdict(int)
    study_counts: dict[str, int]   = defaultdict(int)
    study_failed: dict[str, int]   = defaultdict(int)
    study_starts: dict[str, float] = {}
    for f in files:
        study_totals[f.get("study_uid", "__unknown__")] += 1

    # Preload files directly from source (CD/drive) into memory
    yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': 'Pre-loading files into memory…', 'ok': True, 'elapsed': 0.0, 'study_uid': ''})}\n\n"
    loaded = await asyncio.get_event_loop().run_in_executor(None, preload_files, files)

    batches = list(_chunk(loaded, BATCH_SIZE))
    limits  = httpx.Limits(
        max_connections=MAX_UPLOAD_WORKERS,
        max_keepalive_connections=MAX_UPLOAD_WORKERS,
    )
    sem = asyncio.Semaphore(MAX_UPLOAD_WORKERS)

    async def upload_batch_sem(batch):
        uid = batch[0].get("study_uid", "__unknown__") if batch else "__unknown__"
        if uid not in study_starts:
            study_starts[uid] = time.monotonic()
        async with sem:
            return await _upload_batch(batch, client)

    print(f"[UPLOAD] Uploading {len(batches)} batches (batch size={BATCH_SIZE}) "
          f"with {MAX_UPLOAD_WORKERS} concurrent workers …")

    async with httpx.AsyncClient(
        auth=(ORTHANC_USER, ORTHANC_PASS), limits=limits
    ) as client:
        tasks = [asyncio.create_task(upload_batch_sem(b)) for b in batches]

        for coro in asyncio.as_completed(tasks):
            for result in await coro:
                done_count += 1
                uid = result.get("study_uid", "__unknown__")
                study_counts[uid] += 1
                if not result["ok"]:
                    failed += 1
                    study_failed[uid] += 1

                elapsed = time.monotonic() - wall_start
                yield (
                    f"data: {json.dumps({'type': 'progress', 'done': done_count, 'total': total, 'failed': failed, 'file': os.path.basename(result['path']), 'ok': result['ok'], 'elapsed': round(elapsed, 1), 'study_uid': uid})}\n\n"
                )

                if study_counts[uid] == study_totals[uid]:
                    study_elapsed = time.monotonic() - study_starts.get(uid, wall_start)
                    s_ok = study_totals[uid] - study_failed[uid]
                    print(f"[UPLOAD] Study {uid}: {s_ok} succeeded, "
                          f"{study_failed[uid]} failed in {_fmt_duration(study_elapsed)}")
                    yield (
                        f"data: {json.dumps({'type': 'study_done', 'study_uid': uid, 'elapsed': round(study_elapsed, 1), 'elapsed_str': _fmt_duration(study_elapsed), 'succeeded': s_ok, 'failed': study_failed[uid]})}\n\n"
                    )
            await asyncio.sleep(0)

    total_elapsed = time.monotonic() - wall_start
    print(f"[UPLOAD] ✓ Upload complete: {total - failed} succeeded, "
          f"{failed} failed in {_fmt_duration(total_elapsed)}")

    yield (
        f"data: {json.dumps({'type': 'done', 'total': total, 'succeeded': total-failed, 'failed': failed, 'elapsed': round(total_elapsed, 1), 'elapsed_str': _fmt_duration(total_elapsed)})}\n\n"
    )


# ---------------------------------------------------------------------------
# Routes
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
    print(f"[PIPELINE] Starting DICOM import pipeline for drive: {drive}")
    print(f"{'='*60}")
    t_pipeline = time.monotonic()

    # --- Step 1: Search for DICOMDIR ---
    print(f"\n[PIPELINE] Step 1/2 — Searching for DICOMDIR …")
    dicomdir_path = await loop.run_in_executor(None, find_dicomdir, drive)

    if dicomdir_path:
        # --- Step 2a: Parse DICOMDIR (fast, single file) ---
        print(f"\n[PIPELINE] Step 2/2 — Parsing DICOMDIR …")
        studies_raw = await loop.run_in_executor(None, scan_from_dicomdir, dicomdir_path)
        scan_method = "DICOMDIR"
    else:
        # --- Step 2b: Fallback — parallel header scan ---
        print(f"\n[PIPELINE] Step 2/2 — No DICOMDIR found. Falling back to full scan …")
        studies_raw = await loop.run_in_executor(None, scan_drive_fallback, drive)
        scan_method = "Full Scan"

    scan_cache.clear()
    scan_cache.update(studies_raw)

    pipeline_elapsed = time.monotonic() - t_pipeline
    total_dicoms = sum(
        len(fl) for stds in studies_raw.values() for fl in stds.values()
    )
    print(
        f"\n[PIPELINE] Done in {_fmt_duration(pipeline_elapsed)}: "
        f"{total_dicoms} DICOM files, method={scan_method}"
    )
    print(f"[PIPELINE] Upload will begin when user confirms.\n")

    # Build flat study list for the template
    flat = []
    for patient, studies_dict in studies_raw.items():
        for uid, file_list in studies_dict.items():
            first = file_list[0] if file_list else {}
            flat.append({
                "patient":     patient,
                "patient_id":  first.get("patient_id",  ""),
                "patient_sex": first.get("patient_sex", ""),
                "patient_age": first.get("patient_age", ""),
                "study_uid":   uid,
                "desc":        first.get("desc",     ""),
                "date":        first.get("date",     ""),
                "modality":    first.get("modality", ""),
                "count":       len(file_list),
            })

    return templates.TemplateResponse(
        "results.html",
        {
            "request":     request,
            "studies":     flat,
            "drive":       drive,
            "scan_method": scan_method,
            "dicomdir":    dicomdir_path or "",
        },
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
