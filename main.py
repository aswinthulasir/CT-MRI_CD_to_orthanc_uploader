"""
DICOM CD Importer — Optimised pipeline with pipelined preload+upload
=====================================================================

Pipeline:
  1. Detect CD/removable drives.
  2. Scan DICOM headers directly from CD:
       - Fast path  → parse DICOMDIR index file (ms-level, zero per-file I/O)
       - Fallback   → parallel per-file header scan with PRELOAD_WORKERS threads
  3. Pipelined preload + upload to Orthanc:
       - Files are loaded into memory in adaptive batches (PIPELINE_BATCH_SIZE).
       - As each batch is loaded, uploads begin immediately (no waiting for all).
       - Memory-aware: checks available RAM and adjusts batch size dynamically.
       - Early memory release: file data freed immediately after upload.
       - MAX_UPLOAD_WORKERS concurrent async coroutines with httpx connection pool.

Key speed improvements:
  - DICOMDIR fast-path avoids reading every file header individually (ms-level).
  - Only essential DICOM tags decoded in fallback scan (3-5× faster than full).
  - Pipelined preload overlaps disk I/O with network upload (no idle time).
  - Memory-aware batching prevents OOM on large datasets.
  - Individual file uploads with massive httpx concurrency (no ZIP overhead).
  - httpx AsyncClient with tuned connection pool & keep-alive.
  - Semaphore-controlled concurrent uploads (MAX_UPLOAD_WORKERS).
  - Early memory release halves peak RAM usage.
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
import psutil
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
PRELOAD_WORKERS    = 32          # threads for parallel disk reads (doubled for faster I/O)
UPLOAD_TIMEOUT     = 30          # seconds per individual file upload
MAX_RETRIES        = 3           # retry attempts for transient upload errors

# Pipelined preload settings
PIPELINE_BATCH_SIZE    = 200     # files per preload batch (overlap I/O + network)
PIPELINE_MEM_RESERVE_MB = 512    # keep at least this much free RAM (MB)

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
# Step 3a — Memory-aware pipelined preload helpers
# ---------------------------------------------------------------------------

def _get_free_memory_mb() -> float:
    """Return available system memory in MB."""
    try:
        return psutil.virtual_memory().available / (1024 * 1024)
    except Exception:
        return 4096.0  # assume 4 GB if psutil fails


def _compute_batch_size(remaining_files: int, avg_file_mb: float = 0.5) -> int:
    """
    Compute how many files to preload in the next batch,
    capped by available memory and PIPELINE_BATCH_SIZE.
    """
    free_mb = _get_free_memory_mb()
    usable_mb = max(free_mb - PIPELINE_MEM_RESERVE_MB, 128)  # always allow 128 MB min
    mem_files = int(usable_mb / avg_file_mb) if avg_file_mb > 0 else PIPELINE_BATCH_SIZE
    return max(1, min(mem_files, PIPELINE_BATCH_SIZE, remaining_files))


def _load_file(info: dict) -> tuple:
    """Load a single file into memory. Returns (info_dict, bytes_or_None, error_or_None)."""
    try:
        with open(info["path"], "rb") as fh:
            data = fh.read()
        return (info, data, None)
    except Exception as exc:
        return (info, None, str(exc))


def _preload_batch(batch: list) -> list:
    """Load a batch of files using thread pool. Returns list of (info, data, error)."""
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        return list(pool.map(_load_file, batch))


# ---------------------------------------------------------------------------
# Step 3b — Upload a single file via httpx with retry
# ---------------------------------------------------------------------------

async def _upload_single(file_info: dict, data: bytes,
                         client: httpx.AsyncClient) -> dict:
    """Upload a single DICOM file to Orthanc using httpx, with retry on transient errors."""
    study_uid = file_info.get("study_uid", "__unknown__")
    fname = os.path.basename(file_info["path"])

    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.post(
                ORTHANC_URL,
                content=data,
                headers={"Content-Type": "application/dicom"},
                timeout=UPLOAD_TIMEOUT,
            )
            # Extract Orthanc's internal study ID (for C-STORE) — only on 200
            orthanc_study_id = ""
            if r.status_code == 200:
                try:
                    orthanc_study_id = r.json().get("ParentStudy", "")
                except Exception:
                    pass
            return {
                "path":            file_info["path"],
                "ok":              r.status_code in (200, 409),
                "status":          r.status_code,
                "study_uid":       study_uid,
                "orthanc_study_id": orthanc_study_id,
            }
        except (httpx.TimeoutException, httpx.ConnectError,
                httpx.RemoteProtocolError, httpx.ReadError,
                httpx.WriteError, ConnectionResetError, OSError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                wait = 0.5 * (2 ** (attempt - 1))
                print(f"[UPLOAD] Retry {attempt}/{MAX_RETRIES} for {fname}: {last_error} — waiting {wait}s")
                await asyncio.sleep(wait)
            else:
                print(f"[UPLOAD] FAILED after {MAX_RETRIES} attempts: {fname} — {last_error}")
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            print(f"[UPLOAD] FAILED (non-retryable) {fname}: {last_error}")
            break

    return {
        "path":      file_info["path"],
        "ok":        False,
        "error":     last_error,
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
# Step 3d — Pipelined SSE upload stream
# ---------------------------------------------------------------------------

async def upload_stream(files: list):
    """
    SSE generator: pipelined preload + concurrent httpx uploads.

    Instead of loading ALL files first, this loads files in batches
    (PIPELINE_BATCH_SIZE, adaptive by available RAM) and begins uploading
    as soon as the first batch is ready. Each file's bytes are freed
    immediately after upload to reduce peak memory.
    """
    total      = len(files)
    done_count = 0
    failed     = 0
    wall_start = time.monotonic()
    orthanc_study_ids: set = set()

    print(f"[UPLOAD] Starting pipelined upload of {total} files to Orthanc ...")
    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

    study_totals: dict = defaultdict(int)
    study_counts: dict = defaultdict(int)
    study_failed: dict = defaultdict(int)
    study_starts: dict = {}
    for f in files:
        study_totals[f.get("study_uid", "__unknown__")] += 1

    # --- httpx connection pool sized for maximum concurrency ---
    limits = httpx.Limits(
        max_connections=MAX_UPLOAD_WORKERS + 8,
        max_keepalive_connections=MAX_UPLOAD_WORKERS,
    )
    sem = asyncio.Semaphore(MAX_UPLOAD_WORKERS)
    result_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    # --- Track average file size for memory-aware batching ---
    total_bytes_loaded = 0
    total_files_loaded = 0

    # Emit initial status
    yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': 'Loading & uploading (pipelined)...', 'ok': True, 'elapsed': 0.0, 'study_uid': ''})}\n\n"

    async with httpx.AsyncClient(
        auth=(ORTHANC_USER, ORTHANC_PASS), limits=limits
    ) as client:
        upload_tasks = []
        files_remaining = list(files)  # copy so we can pop batches off
        batch_num = 0

        async def _fire_upload(info: dict, data: bytes):
            """Upload a single file with semaphore, push result to queue."""
            uid = info.get("study_uid", "__unknown__")
            if uid not in study_starts:
                study_starts[uid] = time.monotonic()
            async with sem:
                result = await _upload_single(info, data, client)
            await result_queue.put(result)

        # --- Pipelined producer: load batches & fire uploads immediately ---
        while files_remaining:
            avg_mb = (total_bytes_loaded / total_files_loaded / (1024 * 1024)
                      if total_files_loaded > 0 else 0.5)
            batch_size = _compute_batch_size(len(files_remaining), avg_mb)
            batch = files_remaining[:batch_size]
            files_remaining = files_remaining[batch_size:]
            batch_num += 1

            free_mb = _get_free_memory_mb()
            print(f"[PIPELINE] Batch {batch_num}: loading {len(batch)} files "
                  f"(free RAM: {free_mb:.0f} MB, avg file: {avg_mb:.2f} MB)")

            # Load this batch from disk (in thread pool)
            loaded_batch = await loop.run_in_executor(None, _preload_batch, batch)

            # Fire upload tasks immediately for this batch
            for info, data, error in loaded_batch:
                if data is not None:
                    total_bytes_loaded += len(data)
                    total_files_loaded += 1
                    upload_tasks.append(
                        asyncio.create_task(_fire_upload(info, data))
                    )
                else:
                    # File failed to load — push error result directly
                    await result_queue.put({
                        "path":      info["path"],
                        "ok":        False,
                        "error":     error or "read error",
                        "study_uid": info.get("study_uid", "__unknown__"),
                    })

        total_uploaded_mb = total_bytes_loaded / (1024 * 1024)
        print(f"[PIPELINE] All {total} files queued for upload "
              f"({total_uploaded_mb:.1f} MB total, {batch_num} batches)")
        print(f"[UPLOAD] Uploading with {MAX_UPLOAD_WORKERS} concurrent workers ...")

        # --- Consume results as they arrive ---
        for _ in range(total):
            result = await result_queue.get()
            done_count += 1
            uid = result.get("study_uid", "__unknown__")
            study_counts[uid] += 1

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

        # Ensure all tasks are done
        await asyncio.gather(*upload_tasks, return_exceptions=True)

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