"""
DICOM CD Importer — Optimised for 1000+ slices in <60 s on localhost
====================================================================

Pipeline:
  1. Detect CD/removable drives.
  2. FAST COPY: Mirror the entire CD to a local temp folder using parallel
     threads (eliminates slow optical-drive random reads for every subsequent
     step).
  3. Scan DICOM headers from the fast local copy.
  4. Upload to Orthanc in batched ZIPs from local copy.
  5. Delete the temp folder once upload is finished.

Key speed improvements:
  - Temp-folder mirror removes optical-drive latency from scan + upload.
  - Parallel mirror copy with MIRROR_WORKERS threads.
  - only 5 DICOM tags decoded (fast header scan).
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

<<<<<<< Updated upstream
<<<<<<< Updated upstream
MAX_UPLOAD_WORKERS = 32   # concurrent async upload coroutines
BATCH_SIZE         = 50   # DICOM files per ZIP POST
PRELOAD_WORKERS    = 16   # threads for parallel disk reads
MIRROR_WORKERS     = 24   # threads for parallel mirror copy from CD
COPY_BUFFER_SIZE   = 1024 * 1024  # 1 MB buffer for file copy

# Base temp directory for mirrored CD files
TEMP_BASE_DIR = os.environ.get(
    "DICOM_TEMP_DIR",
    os.path.join(tempfile.gettempdir(), "dicom_cd_temp"),
)
=======
MAX_UPLOAD_WORKERS   = 32
CD_SEMAPHORE = 2                   # default for optical drive (sequential is fastest)
=======
MAX_UPLOAD_WORKERS   = 32
CD_SEMAPHORE = 2                   # default for optical drive (sequential is fastest)

>>>>>>> Stashed changes

>>>>>>> Stashed changes

# DICOM tags we actually need (avoids decoding the whole header)
_DICOM_TAGS = [
    0x00100010,  # PatientName
    0x0020000D,  # StudyInstanceUID
    0x00081030,  # StudyDescription
    0x00080020,  # StudyDate
    0x00080060,  # Modality
]

app      = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
scan_cache: dict = {}

<<<<<<< Updated upstream
<<<<<<< Updated upstream
# Stores the current temp directory path so we can clean up after upload
_active_temp_dir: str | None = None
=======
# No mirror — always stream directly from CD
_STRATEGY = "STREAM_FROM_CD"
>>>>>>> Stashed changes
=======
# No mirror — always stream directly from CD
_STRATEGY = "STREAM_FROM_CD"
>>>>>>> Stashed changes


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
# Step 2 — Fast mirror: copy all files from CD to local temp folder
# ---------------------------------------------------------------------------

def _collect_all_file_paths(source_dir: str) -> list[str]:
    """Walk the source directory and return a flat list of all file paths."""
    paths = []
    for root, _dirs, files in os.walk(source_dir):
        for fname in files:
            paths.append(os.path.join(root, fname))
    return paths


def _copy_single_file(args: tuple[str, str, str]) -> tuple[str, str | None]:
    """
    Copy one file from the CD to the temp folder, preserving the relative
    directory structure.  Returns (dest_path, error_or_None).
    """
<<<<<<< Updated upstream
    src_path, source_root, dest_root = args
    try:
        rel = os.path.relpath(src_path, source_root)
        dest_path = os.path.join(dest_root, rel)
        dest_dir = os.path.dirname(dest_path)
        os.makedirs(dest_dir, exist_ok=True)
        # Use a large buffer to minimise CD read syscalls
        with open(src_path, "rb", buffering=COPY_BUFFER_SIZE) as fin, \
             open(dest_path, "wb", buffering=COPY_BUFFER_SIZE) as fout:
            shutil.copyfileobj(fin, fout, length=COPY_BUFFER_SIZE)
        return (dest_path, None)
    except Exception as exc:
        return ("", str(exc))


def mirror_cd_to_temp(drive_path: str) -> tuple[str, int, int, float]:
    """
    Copy every file from *drive_path* into a local temp directory using
    parallel threads.

    Returns
    -------
    (temp_dir, total_files, failed_count, elapsed_seconds)
    """
    global _active_temp_dir

    # Clean up any previous temp dir
    cleanup_temp_dir()

    # Create a fresh temp directory
    os.makedirs(TEMP_BASE_DIR, exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix="cd_", dir=TEMP_BASE_DIR)
    _active_temp_dir = temp_dir

    print(f"[MIRROR] Starting copy from CD '{drive_path}' → temp '{temp_dir}'")
    t0 = time.monotonic()

    # Enumerate all files on the CD
    all_src = _collect_all_file_paths(drive_path)
    total = len(all_src)
    print(f"[MIRROR] Found {total} files on CD. Copying with {MIRROR_WORKERS} threads …")

    # Parallel copy
    copy_args = [(p, drive_path, temp_dir) for p in all_src]
    failed = 0
    copied = 0
    with ThreadPoolExecutor(max_workers=MIRROR_WORKERS) as pool:
        for dest, err in pool.map(_copy_single_file, copy_args):
            if err:
                failed += 1
                print(f"[MIRROR]   ✗ copy failed: {err}")
            else:
                copied += 1
                # Progress every 100 files
                if copied % 100 == 0:
                    print(f"[MIRROR]   copied {copied}/{total} files …")

    elapsed = time.monotonic() - t0
    print(f"[MIRROR] ✓ Copy complete: {copied} files in {_fmt_duration(elapsed)} "
          f"({failed} failed)")
    return (temp_dir, total, failed, elapsed)


def cleanup_temp_dir():
    """Remove the active temp directory and all its contents."""
    global _active_temp_dir
    if _active_temp_dir and os.path.isdir(_active_temp_dir):
        print(f"[CLEANUP] Deleting temp folder: {_active_temp_dir}")
        try:
            shutil.rmtree(_active_temp_dir, ignore_errors=True)
            print(f"[CLEANUP] ✓ Temp folder deleted successfully")
        except Exception as exc:
            print(f"[CLEANUP] ✗ Error deleting temp folder: {exc}")
        _active_temp_dir = None


# ---------------------------------------------------------------------------
# Step 3 — Fast header scan (reads from local temp folder now)
# ---------------------------------------------------------------------------

def _read_dicom_header(fpath: str) -> dict | None:
    """Read only the 5 tags we need — 3-5× faster than full header decode."""
    try:
        ds = pydicom.dcmread(fpath, specific_tags=_DICOM_TAGS)
        return {
            "path":      fpath,
            "patient":   str(getattr(ds, "PatientName",      "Unknown")),
            "study_uid": str(getattr(ds, "StudyInstanceUID",  "Unknown")),
            "desc":      str(getattr(ds, "StudyDescription",  "")),
            "date":      str(getattr(ds, "StudyDate",         "")),
            "modality":  str(getattr(ds, "Modality",          "")),
=======
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
            patient_count    += 1
            raw_name          = _norm_str(getattr(record, "PatientName",  "Unknown"))
            cur_patient_id    = _norm_str(getattr(record, "PatientID",    ""))
            cur_patient_sex   = _norm_str(getattr(record, "PatientSex",   ""))
            cur_patient_age   = _norm_str(getattr(record, "PatientAge",   ""))
            cur_patient       = raw_name or "Unknown"

        elif rtype == "STUDY":
            study_count  += 1
            cur_study_uid  = _norm_str(getattr(record, "StudyInstanceUID", "Unknown")) or "Unknown"
            cur_desc       = _norm_str(getattr(record, "StudyDescription", ""))
            cur_date       = _norm_str(getattr(record, "StudyDate",        ""))
            cur_study_time = _norm_str(getattr(record, "StudyTime",        ""))
            cur_study_id   = _norm_str(getattr(record, "StudyID",          ""))
            cur_accession  = _norm_str(getattr(record, "AccessionNumber",  ""))

        elif rtype == "SERIES":
            series_count     += 1
            cur_modality      = _norm_str(getattr(record, "Modality",              ""))
            cur_series_desc   = _norm_str(getattr(record, "SeriesDescription",     ""))
            cur_series_num    = _norm_str(getattr(record, "SeriesNumber",          ""))
            cur_series_uid    = _norm_str(getattr(record, "SeriesInstanceUID",     ""))
            cur_manufacturer  = _norm_str(getattr(record, "Manufacturer",          ""))
            cur_model         = _norm_str(getattr(record, "ManufacturerModelName", ""))
            cur_institution   = _norm_str(getattr(record, "InstitutionName",       ""))

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
                    print(f"[DICOMDIR] WARNING: file not found — check path construction!")
                    print(f"[DICOMDIR]   dicomdir_dir={dicomdir_dir!r}  parts={parts}")

            studies[cur_patient][cur_study_uid].append({
                "path":         file_path,
                "patient":      cur_patient,
                "patient_id":   cur_patient_id,
                "patient_sex":  cur_patient_sex,
                "patient_age":  cur_patient_age,
                "study_uid":    cur_study_uid,
                "desc":         cur_desc,
                "date":         cur_date,
                "study_time":   cur_study_time,
                "study_id":     cur_study_id,
                "accession":    cur_accession,
                "modality":     cur_modality,
                "series_desc":  cur_series_desc,
                "series_num":   cur_series_num,
                "series_uid":   cur_series_uid,
                "manufacturer": cur_manufacturer,
                "model":        cur_model,
                "institution":  cur_institution,
                # Per-image metadata available from DICOMDIR
                "slice_thickness": _norm_str(getattr(record, "SliceThickness", "")),
                "rows":            _norm_str(getattr(record, "Rows",           "")),
                "columns":         _norm_str(getattr(record, "Columns",        "")),
                "instance_number": _norm_str(getattr(record, "InstanceNumber", "")),
            })
            image_count += 1

    elapsed = time.monotonic() - t0
    print(
        f"[DICOMDIR] ✓ Parsed in {elapsed*1000:.0f}ms — "
        f"{patient_count} patient(s), {study_count} study(ies), "
        f"{series_count} series, {image_count} images"
    )
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
# Step 2c — Fallback: parallel DICOM header scan (no DICOMDIR)
# ---------------------------------------------------------------------------

def _read_dicom_header(fpath: str) -> Optional[dict]:
    try:
        ds = pydicom.dcmread(fpath, specific_tags=_DICOM_TAGS)
        return {
            "path":         fpath,
            "patient":      _norm_str(getattr(ds, "PatientName",      "Unknown")) or "Unknown",
            "patient_id":   _norm_str(getattr(ds, "PatientID",        "")),
            "patient_sex":  _norm_str(getattr(ds, "PatientSex",       "")),
            "patient_age":  _norm_str(getattr(ds, "PatientAge",       "")),
            "study_uid":    _norm_str(getattr(ds, "StudyInstanceUID", "Unknown")) or "Unknown",
            "desc":         _norm_str(getattr(ds, "StudyDescription", "")),
            "date":         _norm_str(getattr(ds, "StudyDate",        "")),
            "study_time":   _norm_str(getattr(ds, "StudyTime",        "")),
            "study_id":     "",
            "accession":    "",
            "modality":     _norm_str(getattr(ds, "Modality",         "")),
            "series_desc":  "",
            "series_num":   _norm_str(getattr(ds, "SeriesNumber",     "")),
            "series_uid":   _norm_str(getattr(ds, "SeriesInstanceUID","Unknown")),
            "manufacturer": "",
            "model":        "",
            "institution":  "",
            "slice_thickness": "",
            "rows":         "",
            "columns":      "",
            "instance_number": "",
>>>>>>> Stashed changes
        }
    except Exception:
        return None


<<<<<<< Updated upstream
def scan_drive(drive_path: str) -> dict:
    """
    Scan directory for DICOM headers.
    Now reads from the local temp copy for maximum speed.
    """
    print(f"[SCAN] Walking directory: {drive_path}")
    t0 = time.monotonic()

=======
def scan_drive_fallback(drive_path: str) -> dict:
    """Fallback: walk drive and scan DICOM headers in parallel."""
    print(f"[SCAN] Fallback walk of: {drive_path}")
    t0 = time.monotonic()
>>>>>>> Stashed changes
    all_paths = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(drive_path)
        for fname in files
    ]
<<<<<<< Updated upstream
    print(f"[SCAN] Found {len(all_paths)} files. Scanning DICOM headers with "
          f"{PRELOAD_WORKERS} threads …")

    studies: dict = defaultdict(lambda: defaultdict(list))
    scanned = 0
    valid = 0
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
    num_studies = sum(len(s) for s in studies.values())
    print(f"[SCAN] ✓ Scan complete in {_fmt_duration(elapsed)}: "
          f"{valid} DICOM files, {num_patients} patients, {num_studies} studies")
=======
    print(f"[SCAN] Found {len(all_paths)} files. Scanning headers …")
    studies: dict = defaultdict(lambda: defaultdict(list))
    scanned = valid = 0
    with ThreadPoolExecutor(max_workers=16) as pool:
        for result in pool.map(_read_dicom_header, all_paths):
            scanned += 1
            if scanned % 200 == 0:
                print(f"[SCAN]   scanned {scanned}/{len(all_paths)} …")
            if result:
                valid += 1
                studies[result["patient"]][result["study_uid"]].append(result)
    elapsed = time.monotonic() - t0
    print(f"[SCAN] ✓ Done in {_fmt_duration(elapsed)}: {valid} DICOM, "
          f"{len(studies)} patients, {sum(len(s) for s in studies.values())} studies")
>>>>>>> Stashed changes
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
<<<<<<< Updated upstream
<<<<<<< Updated upstream
# Step 4a — ZIP buffer assembly
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
# Step 4b — Preload files into memory (fast from local temp folder)
# ---------------------------------------------------------------------------

def _load_file(info: dict) -> dict:
    try:
        with open(info["path"], "rb") as fh:
            return {**info, "data": fh.read()}
    except Exception as exc:
        return {**info, "data": None, "load_error": str(exc)}


def preload_files(files: list) -> list:
    print(f"[PRELOAD] Loading {len(files)} files into memory with "
          f"{PRELOAD_WORKERS} threads …")
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        result = list(pool.map(_load_file, files))
    elapsed = time.monotonic() - t0
    ok = sum(1 for r in result if r.get("data"))
    print(f"[PRELOAD] ✓ Loaded {ok}/{len(files)} files in {_fmt_duration(elapsed)}")
    return result


# ---------------------------------------------------------------------------
# Step 4c — Upload helpers
=======
# Step 3 — Stream Helpers
>>>>>>> Stashed changes
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


=======
# Step 3 — Stream Helpers
# ---------------------------------------------------------------------------

>>>>>>> Stashed changes
def collect_files(patient, study) -> list:
    files = []
    patients = [patient] if patient else list(scan_cache.keys())
    for p in patients:
        studies = [study] if (study and patient) else list(scan_cache.get(p, {}).keys())
        for s in studies:
            files.extend(scan_cache.get(p, {}).get(s, []))
    return files

def _read_file_buffered(path: str) -> Optional[bytes]:
    """Blocking file read — called via run_in_executor, throttled by cd_sem."""
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except Exception as exc:
        print(f"[READ] ✗ {path!r}: {exc}")
        return None

def _iter_chunks(data: bytes, chunk: int = 65536):
    """Yield bytes in 64 KB slices — httpx streams these to the socket."""
    for i in range(0, len(data), chunk):
        yield data[i: i + chunk]

# ---------------------------------------------------------------------------
<<<<<<< Updated upstream
<<<<<<< Updated upstream
# Step 4d — SSE upload stream
# ---------------------------------------------------------------------------

async def upload_stream(files: list):
    """SSE generator for file upload with temp cleanup on completion."""
=======
=======
>>>>>>> Stashed changes
# Step 4 — Main upload generator
# ---------------------------------------------------------------------------

async def upload_stream(files: list):
    """
    Pipelined SSE upload — no mirror, no batch barrier.

        asyncio.Semaphore(CD_SEMAPHORE)      ← caps simultaneous CD reads
        32 upload coroutines                 ← each acquires sem, reads file,
                                                releases sem, then POSTs bytes

    The CD is never idle while uploads are in-flight; Orthanc is never idle
    while reads are stalled. True overlap at all times.
    """
>>>>>>> Stashed changes
    total      = len(files)
    done_count = 0
    failed     = 0
    wall_start = time.monotonic()

<<<<<<< Updated upstream
<<<<<<< Updated upstream
    print(f"[UPLOAD] Starting upload of {total} files to Orthanc …")
    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

    study_totals: dict[str, int]   = defaultdict(int)
    study_counts: dict[str, int]   = defaultdict(int)
    study_failed: dict[str, int]   = defaultdict(int)
    study_starts: dict[str, float] = {}
    for f in files:
        study_totals[f.get("study_uid", "__unknown__")] += 1

    # Preload files from temp folder into memory
    yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': 'Pre-loading files into memory…', 'ok': True, 'elapsed': 0.0, 'study_uid': ''})}\n\n"
    loaded = await asyncio.get_event_loop().run_in_executor(None, preload_files, files)

    batches = list(_chunk(loaded, BATCH_SIZE))
    limits  = httpx.Limits(
        max_connections=MAX_UPLOAD_WORKERS,
=======
=======
>>>>>>> Stashed changes
    study_totals: dict = defaultdict(int)
    study_counts: dict = defaultdict(int)
    study_failed: dict = defaultdict(int)
    study_starts: dict = {}
    for f in files:
        study_totals[f.get("study_uid", "__unknown__")] += 1

    yield f"data: {json.dumps({'type': 'start', 'total': total, 'strategy': _STRATEGY})}\n\n"
    yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': 'Streaming directly from CD — upload starts immediately…', 'ok': True, 'elapsed': 0.0, 'study_uid': ''})}\n\n"

    # Semaphore limits simultaneous open file handles on the optical drive.
    cd_sem       = asyncio.Semaphore(CD_SEMAPHORE)
    work_queue:  asyncio.Queue = asyncio.Queue(maxsize=MAX_UPLOAD_WORKERS * 2)
    result_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    # ── Producer: enqueues file-info dicts one at a time (no batching) ────────
    async def producer():
        for info in files:
            await work_queue.put(info)
        for _ in range(MAX_UPLOAD_WORKERS):
            await work_queue.put(None)  # one sentinel per worker

    # ── Per-file: read (throttled) then upload (concurrent) ───────────────────
    async def stream_upload(info: dict, client: httpx.AsyncClient) -> dict:
        path = info["path"]
        study_uid = info.get("study_uid", "")
        
        # Check if file exists first
        if not os.path.isfile(path):
            error = f"file not found: {path}"
            print(f"[UPLOAD] ✗ {error}")
            return {"path": path, "ok": False, "error": error, "study_uid": study_uid}
        
        async with cd_sem:                          # throttle concurrent CD reads
            data = await loop.run_in_executor(None, _read_file_buffered, path)

        if data is None:
            error = "read failed"
            print(f"[UPLOAD] ✗ {path}: {error}")
            return {"path": path, "ok": False, "error": error, "study_uid": study_uid}
        
        try:
            r = await client.post(
                ORTHANC_URL,
                content=data,  # POST bytes directly instead of generator
                headers={"Content-Type": "application/dicom"},
            )
            success = r.status_code in (200, 409)
            if not success:
                print(f"[UPLOAD] ✗ {os.path.basename(path)}: HTTP {r.status_code}")
            return {"path": path, "ok": success,
                    "status": r.status_code, "study_uid": study_uid}
        except Exception as exc:
            print(f"[UPLOAD] ✗ {os.path.basename(path)}: {exc}")
            return {"path": path, "ok": False, "error": str(exc), "study_uid": study_uid}

    # ── Worker: drains queue, calls stream_upload ─────────────────────────────
    async def worker(client: httpx.AsyncClient):
        while True:
            info = await work_queue.get()
            if info is None:
                break
            result = await stream_upload(info, client)
            await result_queue.put(result)
        await result_queue.put(None)    # signals this worker finished

    # ── Launch ────────────────────────────────────────────────────────────────
    limits = httpx.Limits(
        max_connections=MAX_UPLOAD_WORKERS + 4,
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
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
=======
        auth=(ORTHANC_USER, ORTHANC_PASS),
        limits=limits,
        http2=True,
        timeout=httpx.Timeout(connect=5, read=60, write=60, pool=10),
    ) as client:
        prod    = asyncio.create_task(producer())
        workers = [asyncio.create_task(worker(client)) for _ in range(MAX_UPLOAD_WORKERS)]

        workers_done = 0
        while workers_done < MAX_UPLOAD_WORKERS:
            result = await result_queue.get()
            if result is None:
                workers_done += 1
                continue
>>>>>>> Stashed changes

                elapsed = time.monotonic() - wall_start
                yield (
                    f"data: {json.dumps({'type': 'progress', 'done': done_count, 'total': total, 'failed': failed, 'file': os.path.basename(result['path']), 'ok': result['ok'], 'elapsed': round(elapsed, 1), 'study_uid': uid})}\n\n"
                )

<<<<<<< Updated upstream
<<<<<<< Updated upstream
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

    # --- Step 5: Cleanup temp folder after upload ---
    print(f"[UPLOAD] Cleaning up temp files …")
    await asyncio.get_event_loop().run_in_executor(None, cleanup_temp_dir)

    yield (
        f"data: {json.dumps({'type': 'done', 'total': total, 'succeeded': total-failed, 'failed': failed, 'elapsed': round(total_elapsed, 1), 'elapsed_str': _fmt_duration(total_elapsed)})}\n\n"
=======
        await prod

    total_elapsed = time.monotonic() - wall_start
    print(f"[UPLOAD] ✓ Complete: {total - failed} ok, {failed} failed "
          f"in {_fmt_duration(total_elapsed)}")
    yield (
        f"data: {json.dumps({'type': 'done', 'total': total, 'succeeded': total - failed, 'failed': failed, 'elapsed': round(total_elapsed, 1), 'elapsed_str': _fmt_duration(total_elapsed), 'strategy': _STRATEGY})}\n\n"
>>>>>>> Stashed changes
    )

=======
        await prod

    total_elapsed = time.monotonic() - wall_start
    print(f"[UPLOAD] ✓ Complete: {total - failed} ok, {failed} failed "
          f"in {_fmt_duration(total_elapsed)}")
    yield (
        f"data: {json.dumps({'type': 'done', 'total': total, 'succeeded': total - failed, 'failed': failed, 'elapsed': round(total_elapsed, 1), 'elapsed_str': _fmt_duration(total_elapsed), 'strategy': _STRATEGY})}\n\n"
    )

>>>>>>> Stashed changes
# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

<<<<<<< Updated upstream
=======
@app.on_event("startup")
async def on_startup():
    print(f"[STARTUP] DICOM CD Importer ready — strategy: {_STRATEGY}")


>>>>>>> Stashed changes
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html", {"request": request, "cd_drives": detect_cd_drives()}
    )


@app.post("/scan", response_class=HTMLResponse)
async def scan(request: Request, drive: str = Form(...)):
    loop = asyncio.get_event_loop()

    # --- Step 2: Mirror CD to temp folder ---
    print(f"\n{'='*60}")
    print(f"[PIPELINE] Starting DICOM import pipeline for drive: {drive}")
    print(f"{'='*60}")

    t_pipeline = time.monotonic()

    print(f"\n[PIPELINE] Step 1/3 — Copying files from CD to local temp folder …")
    temp_dir, total_files, copy_failed, copy_time = await loop.run_in_executor(
        None, mirror_cd_to_temp, drive
    )

    # --- Step 3: Scan DICOM headers from temp folder (fast local reads) ---
    print(f"\n[PIPELINE] Step 2/3 — Scanning DICOM headers from temp folder …")
    studies_raw = await loop.run_in_executor(None, scan_drive, temp_dir)

    scan_cache.clear()
    scan_cache.update(studies_raw)
<<<<<<< Updated upstream
<<<<<<< Updated upstream
=======
    # Step 3: Set strategy
    strategy = _STRATEGY
>>>>>>> Stashed changes
=======
    # Step 3: Set strategy
    strategy = _STRATEGY
>>>>>>> Stashed changes

    pipeline_elapsed = time.monotonic() - t_pipeline
    total_dicoms = sum(
        len(fl) for stds in studies_raw.values() for fl in stds.values()
    )
    print(f"\n[PIPELINE] Steps 1-2 done in {_fmt_duration(pipeline_elapsed)}: "
          f"{total_files} files copied, {total_dicoms} DICOM files found")
    print(f"[PIPELINE] Step 3/3 — Upload will begin when user confirms.\n")

    flat = []
    for patient, studies_dict in studies_raw.items():
        for uid, file_list in studies_dict.items():
            first = file_list[0] if file_list else {}
            flat.append({
                "patient":   patient,
                "study_uid": uid,
                "desc":      first.get("desc", ""),
                "date":      first.get("date", ""),
                "modality":  first.get("modality", ""),
                "count":     len(file_list),
            })
    return templates.TemplateResponse(
        "results.html", {"request": request, "studies": flat, "drive": drive}
    )


@app.get("/upload-stream")
async def upload_stream_route(patient: str = "", study: str = ""):
    files = collect_files(patient or None, study or None)
    print(f"\n[PIPELINE] Step 3/3 — Starting upload of {len(files)} files to Orthanc")
    return StreamingResponse(
        upload_stream(files),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/detect-drives")
def detect_drives_route():
    return {"drives": detect_cd_drives()}
<<<<<<< Updated upstream
=======



<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
