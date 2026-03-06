"""
DICOM CD Importer — Strategy-Aware Pipelined Upload
=====================================================

Startup detects hardware (SSD/HDD, free space, free RAM) and picks the
fastest upload strategy automatically:

  MIRROR_SSD      → copy CD → SSD in background, upload from SSD (~6-9s)
  MIRROR_HDD      → copy CD → HDD in background, upload from HDD (~20-35s)
  STREAM_FROM_CD  → no mirror (disk full), pipeline directly from CD (~2.5 min)
  STREAM_LOW_MEM  → no mirror (low RAM), pipeline from CD, small queue
  (Old code: 5+ min because it read ALL files before uploading anything)

Key improvements over old code:
  - DICOMDIR parsed in <500ms — no full-drive scan needed
  - Pipelined read + upload (no full-preload barrier)
  - Direct per-file POST to Orthanc (no ZIP overhead on localhost)
  - HTTP/2 multiplexing for concurrent POSTs
  - Background mirror runs WHILE user reviews scan results
"""

import io
import json
import os
import platform
import shutil
import tempfile
import time
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import httpx
import psutil
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

MAX_UPLOAD_WORKERS   = 32
COPY_BUFFER_SIZE     = 1024 * 1024         # 1 MB read/write buffer for mirror
CD_READ_THREADS      = 2                   # default for optical drive (sequential is fastest)
MIN_FREE_SPACE_BYTES = 700 * 1024 * 1024   # 700 MB minimum to attempt mirror
MIN_FREE_RAM_BYTES   = 800 * 1024 * 1024   # 800 MB minimum for normal queue size
LOW_MEM_QUEUE_SIZE   = 20                  # files in queue when RAM is tight
NORMAL_QUEUE_SIZE    = 150                 # files in queue on normal machines

TEMP_BASE_DIR = os.environ.get(
    "DICOM_TEMP_DIR",
    os.path.join(tempfile.gettempdir(), "dicom_cd_temp"),
)

# DICOM tags for fallback header scan
_DICOM_TAGS = [
    0x00100010,  # PatientName
    0x00100020,  # PatientID
    0x00100030,  # PatientBirthDate
    0x00100040,  # PatientSex
    0x00101010,  # PatientAge
    0x0020000D,  # StudyInstanceUID
    0x00081030,  # StudyDescription
    0x00080020,  # StudyDate
    0x00080030,  # StudyTime
    0x00080060,  # Modality
    0x0020000E,  # SeriesInstanceUID
    0x00200011,  # SeriesNumber
]

# ---------------------------------------------------------------------------
# App + global state
# ---------------------------------------------------------------------------

app      = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
scan_cache: dict = {}

# Storage profile — populated once at startup
_storage_profile: dict = {}

# Mirror state
_active_temp_dir:   Optional[str]           = None
_source_drive_path: Optional[str]           = None
_mirror_failed:     bool                    = False
_mirror_task:       Optional[asyncio.Task]  = None
# Event is created lazily inside the startup handler (needs running loop)
_mirror_done_event: Optional[asyncio.Event] = None


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


# ---------------------------------------------------------------------------
# Step 0 — Hardware / storage profile detection
# ---------------------------------------------------------------------------

def _is_path_on_ssd(path: str) -> bool:
    """
    Return True if *path* is on a solid-state drive.
    Falls back to False (assumes HDD) on any error — safe default.
    """
    system = platform.system()
    try:
        if system == "Linux":
            import subprocess, re
            result = subprocess.run(
                ["df", "--output=source", path],
                capture_output=True, text=True, timeout=3,
            )
            device_line = result.stdout.strip().splitlines()[-1]
            dev = re.sub(r"[0-9]+$", "", os.path.basename(device_line))
            rotational = f"/sys/block/{dev}/queue/rotational"
            if os.path.isfile(rotational):
                with open(rotational) as f:
                    return f.read().strip() == "0"

        elif system == "Windows":
            import subprocess
            result = subprocess.run(
                ["powershell", "-Command",
                 "Get-PhysicalDisk | Select-Object MediaType | ConvertTo-Json"],
                capture_output=True, text=True, timeout=5,
            )
            return "SSD" in result.stdout

        elif system == "Darwin":
            import subprocess
            result = subprocess.run(
                ["diskutil", "info", path],
                capture_output=True, text=True, timeout=3,
            )
            for line in result.stdout.splitlines():
                if "Solid State" in line:
                    return "Yes" in line

    except Exception as exc:
        print(f"[PROFILE] SSD detection error ({exc}) — assuming HDD")

    return False


def detect_storage_profile() -> dict:
    """
    Run once at startup. Checks disk type, free space, and available RAM.
    Selects the fastest upload strategy this machine can safely support.
    """
    print("\n[PROFILE] ── Detecting storage profile ──")

    tmp        = tempfile.gettempdir()
    free_space = shutil.disk_usage(tmp).free
    free_ram   = psutil.virtual_memory().available
    is_ssd     = _is_path_on_ssd(tmp)

    disk_label = "SSD" if is_ssd else "HDD"
    print(f"[PROFILE]   Disk type  : {disk_label}")
    print(f"[PROFILE]   Free space : {free_space / (1024**3):.1f} GB")
    print(f"[PROFILE]   Free RAM   : {free_ram   / (1024**2):.0f} MB")

    can_mirror = free_space >= MIN_FREE_SPACE_BYTES
    low_ram    = free_ram   <  MIN_FREE_RAM_BYTES

    if low_ram:
        strategy     = "STREAM_LOW_MEM"
        queue_size   = LOW_MEM_QUEUE_SIZE
        read_threads = 1
    elif can_mirror and is_ssd:
        strategy     = "MIRROR_SSD"
        queue_size   = NORMAL_QUEUE_SIZE
        read_threads = 16
    elif can_mirror and not is_ssd:
        strategy     = "MIRROR_HDD"
        queue_size   = NORMAL_QUEUE_SIZE
        read_threads = 4
    else:
        strategy     = "STREAM_FROM_CD"
        queue_size   = NORMAL_QUEUE_SIZE
        read_threads = 2

    print(f"[PROFILE] ✓ Strategy: {strategy} | queue={queue_size} | readers={read_threads}\n")
    return {
        "strategy":     strategy,
        "is_ssd":       is_ssd,
        "free_space":   free_space,
        "free_ram":     free_ram,
        "queue_size":   queue_size,
        "read_threads": read_threads,
    }


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

def find_dicomdir(drive_path: str) -> Optional[str]:
    """Search for DICOMDIR at drive root or one level deep (case-insensitive)."""
    candidates = ["DICOMDIR", "dicomdir", "Dicomdir"]
    for name in candidates:
        p = os.path.join(drive_path, name)
        if os.path.isfile(p):
            print(f"[DICOMDIR] Found at root: {p}")
            return p
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
# Step 2b — Parse DICOMDIR into studies dict
# ---------------------------------------------------------------------------

def _norm_str(val) -> str:
    s = str(val).strip()
    return s if s else ""


def scan_from_dicomdir(dicomdir_path: str) -> dict:
    """
    Parse a DICOMDIR file → studies dict without touching any image file.
    Returns: { patient: { study_uid: [file_info, ...] } }
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
        }
    except Exception:
        return None


def scan_drive_fallback(drive_path: str) -> dict:
    """Fallback: walk drive and scan DICOM headers in parallel."""
    print(f"[SCAN] Fallback walk of: {drive_path}")
    t0 = time.monotonic()
    all_paths = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(drive_path)
        for fname in files
    ]
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
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
# Step 3 — Mirror helpers (MIRROR_SSD / MIRROR_HDD strategies)
# ---------------------------------------------------------------------------

def cleanup_mirror_temp():
    """Delete the active temp mirror directory after upload completes."""
    global _active_temp_dir
    if _active_temp_dir and os.path.isdir(_active_temp_dir):
        print(f"[MIRROR] Cleaning up: {_active_temp_dir}")
        try:
            shutil.rmtree(_active_temp_dir, ignore_errors=True)
            print(f"[MIRROR] ✓ Cleaned up")
        except Exception as exc:
            print(f"[MIRROR] ✗ Cleanup error: {exc}")
        _active_temp_dir = None


def resolve_read_path(original_path: str) -> str:
    """
    Remap a CD path → temp SSD/HDD path when a mirror exists.
    Falls back to the original CD path if mirror was skipped.
    """
    if _active_temp_dir and _source_drive_path:
        try:
            rel       = os.path.relpath(original_path, _source_drive_path)
            temp_path = os.path.join(_active_temp_dir, rel)
            if os.path.isfile(temp_path):
                return temp_path
        except ValueError:
            pass   # Windows cross-drive relpath
    return original_path


def _copy_file_for_mirror(args: tuple) -> tuple:
    """Copy one file preserving relative directory structure."""
    src_path, source_root, dest_root = args
    try:
        rel       = os.path.relpath(src_path, source_root)
        dest_path = os.path.join(dest_root, rel)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(src_path, "rb", buffering=COPY_BUFFER_SIZE) as fin, \
             open(dest_path, "wb", buffering=COPY_BUFFER_SIZE) as fout:
            shutil.copyfileobj(fin, fout, length=COPY_BUFFER_SIZE)
        return (dest_path, None)
    except Exception as exc:
        return ("", str(exc))


def mirror_cd_to_disk(drive_path: str) -> tuple:
    """
    Copy all CD files to a local temp folder on SSD/HDD.
    Uses CD_READ_THREADS=2 — sequential reads are fastest on optical.
    Returns: (temp_dir, total_files, failed_count, elapsed_seconds)
    """
    global _active_temp_dir, _mirror_failed

    cleanup_mirror_temp()
    os.makedirs(TEMP_BASE_DIR, exist_ok=True)
    temp_dir         = tempfile.mkdtemp(prefix="cd_", dir=TEMP_BASE_DIR)
    _active_temp_dir = temp_dir
    _mirror_failed   = False

    strategy = _storage_profile.get("strategy", "MIRROR_SSD")
    print(f"[MIRROR] ▶ [{strategy}] '{drive_path}' → '{temp_dir}'")
    t0 = time.monotonic()

    all_src = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(drive_path)
        for fname in files
    ]
    total = len(all_src)
    print(f"[MIRROR] {total} files. Using {CD_READ_THREADS} read thread(s) …")

    failed = copied = 0
    copy_args = [(p, drive_path, temp_dir) for p in all_src]
    with ThreadPoolExecutor(max_workers=CD_READ_THREADS) as pool:
        for dest, err in pool.map(_copy_file_for_mirror, copy_args):
            if err:
                failed += 1
                print(f"[MIRROR]   ✗ {err}")
            else:
                copied += 1
                if copied % 200 == 0:
                    rate = (copied * 524_000) / (time.monotonic() - t0) / 1_048_576
                    print(f"[MIRROR]   {copied}/{total} @ ~{rate:.1f} MB/s …")

    elapsed = time.monotonic() - t0
    if failed:
        _mirror_failed = True
    print(f"[MIRROR] ✓ Done in {_fmt_duration(elapsed)}: {copied} ok, {failed} failed")
    return (temp_dir, total, failed, elapsed)


async def start_background_mirror(drive_path: str):
    """Launch CD mirror as a non-blocking background asyncio task."""
    global _source_drive_path, _mirror_task, _mirror_done_event

    _source_drive_path = drive_path
    if _mirror_done_event is None:
        _mirror_done_event = asyncio.Event()
    _mirror_done_event.clear()
    loop = asyncio.get_event_loop()

    async def _run():
        await loop.run_in_executor(None, mirror_cd_to_disk, drive_path)
        _mirror_done_event.set()
        print(f"[MIRROR] ✓ Mirror ready — upload can proceed from local disk")

    _mirror_task = asyncio.create_task(_run())
    print(f"[MIRROR] Background mirror task launched")


# ---------------------------------------------------------------------------
# Step 4 — Per-file read and upload (replaces _load_file + _upload_batch)
# ---------------------------------------------------------------------------

def _read_one_file(info: dict) -> dict:
    """
    Read one DICOM file from its resolved path (SSD/HDD/CD per strategy).
    Called inside a ThreadPoolExecutor by the pipeline reader.
    """
    path = resolve_read_path(info["path"])
    try:
        with open(path, "rb") as fh:
            return {**info, "data": fh.read(), "resolved_path": path}
    except Exception as exc:
        _read_one_file._err_count = getattr(_read_one_file, "_err_count", 0) + 1
        if _read_one_file._err_count <= 5:
            print(f"[READ]   ✗ Cannot read {path!r}: {exc}")
        return {**info, "data": None, "load_error": str(exc), "resolved_path": path}


_read_one_file._err_count = 0


async def _upload_one_file(item: dict, client: httpx.AsyncClient) -> dict:
    """
    POST a single DICOM file directly to Orthanc /instances.
    No ZIP — direct binary POST is faster than ZIP+unzip on localhost.
    200 = new instance stored. 409 = already exists. Both = success.
    """
    if not item.get("data"):
        return {
            "path":      item["path"],
            "ok":        False,
            "error":     item.get("load_error", "read error"),
            "study_uid": item.get("study_uid", ""),
        }
    try:
        r = await client.post(
            ORTHANC_URL,
            content=item["data"],
            headers={"Content-Type": "application/dicom"},
            timeout=30,
        )
        return {
            "path":      item["path"],
            "ok":        r.status_code in (200, 409),
            "status":    r.status_code,
            "study_uid": item.get("study_uid", ""),
        }
    except Exception as exc:
        return {
            "path":      item["path"],
            "ok":        False,
            "error":     str(exc),
            "study_uid": item.get("study_uid", ""),
        }


# ---------------------------------------------------------------------------
# Step 5 — collect_files helper
# ---------------------------------------------------------------------------

def collect_files(patient, study) -> list:
    files = []
    patients = [patient] if patient else list(scan_cache.keys())
    for p in patients:
        studies_list = [study] if (study and patient) else list(scan_cache.get(p, {}).keys())
        for s in studies_list:
            files.extend(scan_cache.get(p, {}).get(s, []))
    return files


# ---------------------------------------------------------------------------
# Step 6 — Strategy-aware pipelined upload stream (SSE generator)
# ---------------------------------------------------------------------------

async def upload_stream(files: list):
    """
    Strategy-aware pipelined SSE upload generator.

          reader threads → asyncio.Queue → upload coroutines → Orthanc
           (read_threads)   (queue_size)   (MAX_UPLOAD_WORKERS)

    MIRROR strategies  : wait for _mirror_done_event, read from SSD/HDD
    STREAM strategies  : skip wait, read directly from CD, upload overlaps
    """
    total      = len(files)
    done_count = 0
    failed     = 0
    wall_start = time.monotonic()

    strategy     = _storage_profile.get("strategy",     "STREAM_FROM_CD")
    queue_size   = _storage_profile.get("queue_size",   NORMAL_QUEUE_SIZE)
    read_threads = _storage_profile.get("read_threads", CD_READ_THREADS)
    is_mirror    = strategy in ("MIRROR_SSD", "MIRROR_HDD")

    _read_one_file._err_count = 0  # reset error counter per upload run

    print(f"[UPLOAD] ▶ Strategy={strategy} | files={total} | "
          f"queue={queue_size} | readers={read_threads} | workers={MAX_UPLOAD_WORKERS}")
    yield f"data: {json.dumps({'type': 'start', 'total': total, 'strategy': strategy})}\n\n"

    # ── Phase 1: Mirror wait OR stream notice ─────────────────────────────
    if is_mirror and _mirror_done_event is not None:
        if not _mirror_done_event.is_set():
            disk_label = "SSD" if strategy == "MIRROR_SSD" else "hard disk"
            print(f"[UPLOAD] [{strategy}] Waiting for background mirror to {disk_label} …")
            yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': f'Waiting for mirror to {disk_label}…', 'ok': True, 'elapsed': 0.0, 'study_uid': ''})}\n\n"

            while not _mirror_done_event.is_set():
                await asyncio.sleep(1)
                elapsed_w = time.monotonic() - wall_start
                yield f"data: {json.dumps({'type': 'mirror_wait', 'done': 0, 'total': total, 'failed': 0, 'file': f'Copying CD to {disk_label}…', 'ok': True, 'elapsed': round(elapsed_w, 1), 'study_uid': ''})}\n\n"

            print(f"[UPLOAD] [{strategy}] Mirror complete. Starting upload.")
            yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': 'Mirror complete — uploading…', 'ok': True, 'elapsed': round(time.monotonic()-wall_start, 1), 'study_uid': ''})}\n\n"
        else:
            print(f"[UPLOAD] [{strategy}] Mirror already ready ✓")
    else:
        if strategy == "STREAM_LOW_MEM":
            msg = "Low memory mode — streaming directly from CD (slower, safe memory use)…"
        else:
            msg = "Streaming directly from CD — upload starts immediately…"
        print(f"[UPLOAD] [{strategy}] {msg}")
        yield f"data: {json.dumps({'type': 'progress', 'done': 0, 'total': total, 'failed': 0, 'file': msg, 'ok': True, 'elapsed': 0.0, 'study_uid': ''})}\n\n"

    # ── Phase 2: Study accounting ─────────────────────────────────────────
    study_totals: dict = defaultdict(int)
    study_counts: dict = defaultdict(int)
    study_failed: dict = defaultdict(int)
    study_starts: dict = {}
    for f in files:
        study_totals[f.get("study_uid", "__unknown__")] += 1

    # ── Phase 3: Build pipeline ───────────────────────────────────────────
    queue:        asyncio.Queue = asyncio.Queue(maxsize=queue_size)
    result_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    async def reader_task():
        """Reads files in chunks using read_threads, fills queue."""
        CHUNK = max(queue_size, 10)
        for i in range(0, len(files), CHUNK):
            chunk = files[i: i + CHUNK]

            def _read_chunk(batch):
                with ThreadPoolExecutor(max_workers=read_threads) as pool:
                    return list(pool.map(_read_one_file, batch))

            loaded = await loop.run_in_executor(None, _read_chunk, chunk)
            for item in loaded:
                await queue.put(item)

        # Send one sentinel per worker to signal end-of-stream
        for _ in range(MAX_UPLOAD_WORKERS):
            await queue.put(None)

    async def upload_worker(client: httpx.AsyncClient):
        """Drains queue, POSTs each file directly to Orthanc."""
        while True:
            item = await queue.get()
            if item is None:
                break
            result = await _upload_one_file(item, client)
            await result_queue.put(result)
        await result_queue.put(None)   # signals this worker is done

    # ── Phase 4: Launch reader + workers concurrently ─────────────────────
    limits = httpx.Limits(
        max_connections=MAX_UPLOAD_WORKERS + 4,
        max_keepalive_connections=MAX_UPLOAD_WORKERS,
    )
    async with httpx.AsyncClient(
        auth=(ORTHANC_USER, ORTHANC_PASS),
        limits=limits,
        http2=True,   # HTTP/2 multiplexing for concurrent POSTs to localhost
    ) as client:
        reader  = asyncio.create_task(reader_task())
        workers = [
            asyncio.create_task(upload_worker(client))
            for _ in range(MAX_UPLOAD_WORKERS)
        ]

        # ── Phase 5: Collect results + stream SSE ─────────────────────────
        workers_done = 0
        while workers_done < MAX_UPLOAD_WORKERS:
            result = await result_queue.get()
            if result is None:
                workers_done += 1
                continue

            done_count += 1
            uid = result.get("study_uid", "__unknown__")
            study_counts[uid] += 1
            if uid not in study_starts:
                study_starts[uid] = time.monotonic()
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
                print(f"[UPLOAD] Study {uid}: {s_ok} ok, "
                      f"{study_failed[uid]} failed in {_fmt_duration(study_elapsed)}")
                yield (
                    f"data: {json.dumps({'type': 'study_done', 'study_uid': uid, 'elapsed': round(study_elapsed, 1), 'elapsed_str': _fmt_duration(study_elapsed), 'succeeded': s_ok, 'failed': study_failed[uid]})}\n\n"
                )

        await reader

    # ── Phase 6: Cleanup ──────────────────────────────────────────────────
    total_elapsed = time.monotonic() - wall_start
    print(f"[UPLOAD] ✓ [{strategy}] Complete: {total-failed} ok, "
          f"{failed} failed in {_fmt_duration(total_elapsed)}")

    if is_mirror:
        await loop.run_in_executor(None, cleanup_mirror_temp)

    yield (
        f"data: {json.dumps({'type': 'done', 'total': total, 'succeeded': total-failed, 'failed': failed, 'elapsed': round(total_elapsed, 1), 'elapsed_str': _fmt_duration(total_elapsed), 'strategy': strategy})}\n\n"
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    """Detect storage profile once when the server starts."""
    global _storage_profile, _mirror_done_event
    loop = asyncio.get_event_loop()
    _storage_profile   = await loop.run_in_executor(None, detect_storage_profile)
    _mirror_done_event = asyncio.Event()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html", {"request": request, "cd_drives": detect_cd_drives()}
    )


@app.post("/scan", response_class=HTMLResponse)
async def scan(request: Request, drive: str = Form(...)):
    loop = asyncio.get_event_loop()

    print(f"\n{'='*60}")
    print(f"[PIPELINE] DICOM import pipeline starting for drive: {drive}")
    print(f"{'='*60}")
    t_pipeline = time.monotonic()

    # Step 1: Find + parse DICOMDIR (fast)
    print(f"\n[PIPELINE] Step 1/2 — Searching for DICOMDIR …")
    dicomdir_path = await loop.run_in_executor(None, find_dicomdir, drive)

    if dicomdir_path:
        print(f"\n[PIPELINE] Step 2/2 — Parsing DICOMDIR …")
        studies_raw = await loop.run_in_executor(None, scan_from_dicomdir, dicomdir_path)
        scan_method = "DICOMDIR"
    else:
        print(f"\n[PIPELINE] Step 2/2 — No DICOMDIR found. Falling back to full scan …")
        studies_raw = await loop.run_in_executor(None, scan_drive_fallback, drive)
        scan_method = "Full Scan"

    scan_cache.clear()
    scan_cache.update(studies_raw)

    # Step 3: Conditionally start background mirror
    strategy = _storage_profile.get("strategy", "STREAM_FROM_CD")
    if strategy in ("MIRROR_SSD", "MIRROR_HDD"):
        print(f"\n[PIPELINE] [{strategy}] Launching background mirror of '{drive}' …")
        await start_background_mirror(drive)
    else:
        reason = "insufficient disk space" if strategy == "STREAM_FROM_CD" else "low RAM"
        print(f"\n[PIPELINE] [{strategy}] Skipping mirror ({reason}) — "
              f"will stream directly from CD at upload time.")

    pipeline_elapsed = time.monotonic() - t_pipeline
    total_dicoms = sum(len(fl) for stds in studies_raw.values() for fl in stds.values())
    print(f"\n[PIPELINE] ✓ Scan done in {_fmt_duration(pipeline_elapsed)}: "
          f"{total_dicoms} DICOM files, method={scan_method}, strategy={strategy}")
    print(f"[PIPELINE] Upload will begin when user confirms.\n")

    # Build flat study list for template
    flat = []
    for patient, studies_dict in studies_raw.items():
        for uid, file_list in studies_dict.items():
            if not file_list:
                continue
            first = file_list[0]

            # Collect unique series for this study
            seen_series: dict = {}
            for f in file_list:
                s_uid = f.get("series_uid", "")
                if s_uid and s_uid not in seen_series:
                    seen_series[s_uid] = {
                        "series_desc": f.get("series_desc", ""),
                        "series_num":  f.get("series_num",  ""),
                        "modality":    f.get("modality",    ""),
                        "count":       0,
                    }
                if s_uid:
                    seen_series[s_uid]["count"] += 1

            flat.append({
                "patient":      first.get("patient",     ""),
                "patient_id":   first.get("patient_id",  ""),
                "patient_sex":  first.get("patient_sex", ""),
                "patient_age":  first.get("patient_age", ""),
                "study_uid":    uid,
                "desc":         first.get("desc",        ""),
                "date":         first.get("date",        ""),
                "study_time":   first.get("study_time",  ""),
                "study_id":     first.get("study_id",    ""),
                "accession":    first.get("accession",   ""),
                "institution":  first.get("institution", ""),
                "manufacturer": first.get("manufacturer",""),
                "model":        first.get("model",       ""),
                "modality":     first.get("modality",    ""),
                "series_list":  list(seen_series.values()),
                "count":        len(file_list),
                "scan_method":  scan_method,
            })

    return templates.TemplateResponse(
        "results.html",
        {
            "request":     request,
            "studies":     flat,
            "drive":       drive,
            "scan_method": scan_method,
            "strategy":    strategy,
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


@app.get("/storage-profile")
def storage_profile_route():
    """Debug endpoint — shows active strategy and hardware profile."""
    return _storage_profile


@app.get("/mirror-status")
def mirror_status_route():
    """
    Polled by results.html to update the upload-readiness banner.
    Returns strategy + readiness so the frontend shows the right message.
    """
    strategy = _storage_profile.get("strategy", "STREAM_FROM_CD")
    is_mirror = strategy in ("MIRROR_SSD", "MIRROR_HDD")
    ready     = (_mirror_done_event is not None and _mirror_done_event.is_set()) \
                if is_mirror else True
    return {
        "strategy":  strategy,
        "is_mirror": is_mirror,
        "ready":     ready,
        "failed":    _mirror_failed,
    }
