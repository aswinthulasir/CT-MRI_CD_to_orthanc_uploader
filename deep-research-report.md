# Executive Summary

This system ingests medical-image CDs (typically in DICOM format) by scanning their file contents and streaming each image file to an Orthanc PACS server via HTTP. It uses a *fast path* when a `DICOMDIR` index is present on the CD (parsing the directory of images without reading each file), and a *fallback path* that walks the entire drive and reads only essential DICOM header tags in parallel. Once the list of files is known, the system **preloads** each file into memory (using multiple threads) to eliminate disk I/O bottlenecks, then **uploads** each file individually to Orthanc’s REST API `/instances` endpoint using `httpx.AsyncClient` at very high concurrency. Uploads are done with controlled parallelism (semaphore-limited asyncio tasks), basic-auth, tuned connection pooling, and exponential-retry on transient HTTP errors. The system logs detailed progress (files sent, succeeded/failed counts, per-study summaries) and exposes a streaming “progress” JSON feed to the caller.

Key features and optimizations include:

- **DICOMDIR fast-path:** If the CD contains a `DICOMDIR` (the standard DICOM media index file【17†L189-L193】【27†L1749-L1751】), the script parses it with *pydicom* to collect *all* image file paths in milliseconds without per-file I/O.
- **Parallel header scanning:** If no `DICOMDIR` or it’s empty, it falls back to `os.walk()` and uses a thread pool to quickly read only a small set of DICOM tags (`specific_tags`) from each file【21†L656-L662】. This is much faster than decoding full headers or pixel data, and automatically skips non-DICOM files.
- **High-concurrency uploads:** Each image file is POSTed as `Content-Type: application/dicom` to `http://localhost:8042/instances` (Orthanc’s upload endpoint【8†L160-L168】) using a single `httpx.AsyncClient`. A custom `httpx.Limits` setting keeps a large pool of persistent connections, and an `asyncio.Semaphore` caps the number of simultaneous POSTs (e.g. 64)【12†L88-L96】. This parallelism often yields 10× the throughput of naive sequential uploads.
- **Memory preloading:** Before uploading, the system reads all file bytes into memory (via multiple threads) so that uploads are not limited by disk I/O. This pipelining lets the CPU/network work continuously without waiting for the CD’s slower disk reads. The trade-off is increased RAM usage (sum of all files), but on high-speed networks this often yields a net speed gain.
- **Robust error handling and logging:** Each upload is retried up to 3 times on transient network errors (timeouts, resets, etc.). Each file result is tracked, counting successes vs failures. HTTP 409 “Conflict” (already exists) is treated as success to allow idempotent re-runs. The system logs per-study and overall summaries (e.g. “Study XYZ: 500 succeeded, 2 failed in 5.2s”) and streams real-time progress updates (done/total/failed).

A high-level flowchart of the end-to-end process is shown below. It illustrates the conditional DICOMDIR fast-path and the concurrent upload stage:

```mermaid
flowchart LR
    A[Start: Mount/Select CD drive path] 
    B{DICOMDIR on CD?}
    C[Parse DICOMDIR (pydicom) → File list]
    D[Walk all files & Read headers (thread pool) → File list]
    A --> B
    B -- Yes --> C
    B -- No  --> D
    C --> E[Collect all image file paths]
    D --> E
    E --> F[Preload files into memory (multithreaded)]
    F --> G[Create asyncio upload tasks]
    G --> H[Upload each file via httpx.AsyncClient]
    H --> I{HTTP status or error}
    I -- 200/409 --> J[Mark OK]
    I -- other / timeout --> K[Retry up to N times]
    K --> I
    J --> L[Track success/failure counts, log progress]
    L --> M{All files done?}
    M -- No --> G
    M -- Yes --> N[Done: report total sent, elapsed, failures]
```

# System Components and Workflow

## CD Detection and File Discovery

The entry point is typically a FastAPI server (Python 3.8+) that presents a simple web form to select a drive (or folder) to scan. When triggered, it launches the pipeline above. *Physically,* this can be any mounted removable volume (CD/DVD/Blu-ray or USB drive) on Windows, Linux or macOS. In practice the script simply receives a path (e.g. `D:\` on Windows) – auto-detection of drives is not implemented here, but could use OS APIs if needed.

Once the path is known, **Step 1** is file discovery:

- **Fast path – DICOMDIR:** The script checks for a file named `DICOMDIR` or `dicomdir` (case-insensitive) at the root of the drive. Per the DICOM standard (PS3.10), a valid DICOM media storage will include exactly one DICOMDIR file in the file-set【27†L1749-L1751】. (If missing, the disk isn’t strictly DICOM-standard.) If found, the script uses *pydicom* to parse it. The `DICOMDIR` contains a directory record of all images (organized by patient, study, series, etc.) without having to open each image file. The parser extracts each referenced file path (resolving multi-part IDs) and reads key attributes. This yields the complete file list and metadata in **milliseconds** (even for thousands of files), with zero per-file disk reads beyond reading the single DICOMDIR file.

- **Fallback – Recursive scan:** If no DICOMDIR exists or if parsing fails, the script **walks the entire directory tree** (`os.walk(drive_path)`) to collect all files. It then uses a `ThreadPoolExecutor(max_workers=PRELOAD_WORKERS)` to run a function `_read_dicom_header(path)` on each file path. That function calls `pydicom.dcmread(path, specific_tags=_DICOM_TAGS)`【21†L656-L662】 to load only a small set of tags (e.g. PatientName, StudyInstanceUID, Modality, etc.) instead of decoding the full dataset. Because pixel data and unused tags are skipped, this “partial read” is much faster than a full decode. Files that are not valid DICOM (or cause any exception) simply return `None` and are ignored. Valid results include the file path plus extracted tags. The results are aggregated by patient and study UID. The script prints progress every few hundred files (e.g. “scanned 400/1200 headers…”).

This two-stage scan yields a dictionary of patients → studies → list of file records. Example result (fallback mode) might look like `{"John^Doe": {"1.2.3": [ {path: "D:/.../IMG0001.dcm", study_uid:"1.2.3", patient: "Doe^John", ...}, {...} ], ...}, ...}`. The code then computes `total_dicoms` = total number of files found (sum of all lists).

*Error handling:* During scanning, any read error (corrupt file, permission, etc.) is caught and logged (skipped), so one bad file does not abort the scan. The fallback scan prints a summary (files found, patients, studies, time) at the end.

**DICOM context:** As noted in official DICOM guidance, each CD’s file-set *should* include a DICOMDIR at its root【27†L1749-L1751】. That file’s absence is a warning sign, but many scanners (especially older or proprietary ones) may omit it. Hence the fallback. The script treats the presence of DICOMDIR as a “fast path” to avoid the slow overhead of reading thousands of individual headers. If the disk is huge (1000+ images), the DICOMDIR path is orders of magnitude faster.

## Memory Preloading

After scanning, **Step 2** is to preload files into memory. All discovered files (typically hundreds or thousands) are loaded into RAM before upload. This is done via another `ThreadPoolExecutor` (size = `PRELOAD_WORKERS`, default 16). Each thread simply does `data = open(path,"rb").read()` for its file. The code collects the result list as `[{path, other_info…, data: bytes}, ...]`. It reports how many files were successfully loaded (some may fail if locked or unreadable).

**Why preload?** This pipelining decouples disk I/O from network upload. Without it, the async upload tasks would each read from disk when needed, potentially causing disk seeks and slowing the pipeline. By reading everything up front (in parallel), we maximize the chance that uploads run continuously from memory buffers. On a fast network (Gbps LAN), the disk (especially a CD-ROM drive) might otherwise be the bottleneck. The trade-off is memory usage: *all* file bytes are held simultaneously. In practice, for e.g. 1000 CT slices of 1MB each, that’s ~1 GB of RAM. If RAM is limited, one could modify the design to upload in batches instead (e.g. load 100 at a time), at some slight loss in throughput.

Memory is freed when the process ends or the list is discarded. If loading fails for a file, its record will have no `data` and upload will be skipped (error logged).

## Uploading to Orthanc

**Step 3** streams each file to the Orthanc server via its REST API. Orthanc’s documented upload endpoint is `POST http://<host>:8042/instances` (or whatever base URL) with the raw DICOM content【8†L160-L168】. The script uses the `httpx` library (an async HTTP client) to perform this efficiently.

- **HTTPX AsyncClient:** A single `httpx.AsyncClient` instance is created (in a single `async with` block) with `auth=(ORTHANC_USER, ORTHANC_PASS)` for HTTP Basic Auth, and with connection limits (`httpx.Limits`) tuned for high concurrency. For example, `max_connections = MAX_UPLOAD_WORKERS + 4` and `max_keepalive_connections = MAX_UPLOAD_WORKERS`. This means up to ~64 simultaneous TCP connections are kept open and reused (no handshake overhead per request)【12†L88-L96】.
- **Concurrency control:** The script does *not* fire off all tasks at once. Instead, it creates up to `MAX_UPLOAD_WORKERS` (default 64) asyncio tasks using a semaphore. Each task runs `upload_single_sem(file, client)` where it `async with sem: await _upload_single(file, client)`. In practice, this means at most 64 uploads happen at the same time; excess tasks queue behind the semaphore. This prevents overwhelming the OS or network stack【12†L88-L96】.
- **Upload loop:** For each file’s data, the code calls:
  ```python
  r = await client.post(
      ORTHANC_URL,
      content=file_info["data"],
      headers={"Content-Type": "application/dicom"},
      timeout=UPLOAD_TIMEOUT
  )
  ```
  where `ORTHANC_URL` is `http://localhost:8042/instances` by default. The file bytes are sent as the request body. (Alternatively, one could stream a file handle or use `files=`, but here it’s a simple raw body upload.) Orthanc expects `application/dicom` or raw bytes【8†L160-L168】.
- **Response handling:** On success, Orthanc returns JSON with keys like `"ParentStudy"`. The code checks `r.status_code`; it treats 200 (created) and 409 (Conflict) both as “ok”. (409 likely means the instance already exists or was duplicate; counting it as success makes the process idempotent on re-run.) It also captures `r.json().get("ParentStudy")` for C-STORE linkage if needed. Non-200 codes are counted as failure.
- **Retries and errors:** The upload loop wraps each POST in a `for attempt in 1..MAX_RETRIES` with `try/except`. It catches various `httpx` network exceptions (`TimeoutException, ConnectError, RemoteProtocolError, ReadError, WriteError`) as well as generic `OSError`. On a catchable exception, it logs an error message and, if under retry limit, waits an exponential backoff before retrying. After `MAX_RETRIES` failures, it logs a final failure for that file. Non-HTTP errors break out immediately. This ensures transient network issues don’t lose files. (Refer to HTTPX exceptions hierarchy【24†L120-L128】【24†L148-L156】.)
- **Streaming vs. buffered:** Note this implementation uses *buffered* uploads (all data preloaded). HTTPX also supports streaming requests (chunked upload) if memory were tight. Using streams (`client.stream` or passing an iterator) could reduce peak memory but adds complexity. Since we already preloaded into memory for speed, buffering is acceptable here.

**Progress and logging:** The code maintains counters: total files, done count, and failed count. As each file result returns, it increments these counters and sends a Server-Sent-Events (SSE) message with a JSON payload like `{"type":"progress","done":X,"total":Y,"failed":Z,"file":<name>,"ok":<bool>}`. When a study’s files complete, it also emits a `{"type":"study","study_uid":..., "failed":N}` event. At the end it yields a `{"type":"done","total":..., "succeeded":..., "failed":..., "elapsed":..., "orthanc_study_ids":[...]}**.** Meanwhile, it also prints to console summary lines like `"[UPLOAD] Study <UID>: 500 succeeded, 2 failed in 5.2s"`. This provides real-time feedback. (The use of SSE or similar is part of the FastAPI endpoint and is beyond the core logic, but the payload fields are documented in code for completeness.)

## File and Metadata Handling

The system uploads exactly those files identified as DICOM. In the fast-path (DICOMDIR), only files referenced by the directory (usually `.dcm` or vendor-specific extensions) are listed. In the fallback, *all* files on the disk are attempted; any non-DICOM file causes `pydicom` to throw and `_read_dicom_header` returns `None`, so they are ignored. Thus there is no explicit extension filter: presence of valid DICOM tags is the filter. The `DICOMDIR` file itself is never uploaded; it serves only for indexing.

Each file’s dictionary includes minimal metadata (patient name, IDs, series/study UIDs, etc.) extracted by `_read_dicom_header`. Before upload, the code logs the `study_uid` for grouping. The Orthanc upload API automatically organizes files into studies/series/patients based on their DICOM tags. (In fact, Orthanc’s response includes `"ParentStudy"`, which the script collects for potential C-STORE actions, but in this pipeline only REST upload is used.)

No additional metadata mapping is needed: Orthanc will parse the DICOM tags itself. The script does not send separate “metadata” fields beyond the raw DICOM content. 

## Concurrency and HTTPX Details

Key HTTPX usage details:

- **Persistent connections:** By default, `AsyncClient` pools connections. The code explicitly sets `max_keepalive_connections = MAX_UPLOAD_WORKERS` and `max_connections = MAX_UPLOAD_WORKERS+4`. According to HTTPX documentation, using a single `Client` with pooling avoids reconnects【12†L88-L96】. This greatly improves performance versus opening a new HTTP session per file (as would happen with the top-level `httpx.post()`).
- **Timeouts:** Each file upload sets a per-request timeout (`UPLOAD_TIMEOUT`, default 30s). This prevents hangs on a bad request. If a timeout occurs, it triggers the retry logic.
- **Concurrency control:** The use of `asyncio.Semaphore` with a fixed worker limit implements *backpressure* at the application level. Without this, spawning 1000+ coroutines could overwhelm the system; with it, we cap it to (say) 64 simultaneous POSTs. This keeps memory and CPU bounded.
- **Streaming vs buffered:** As noted, uploads send `content=bytes`. HTTPX also allows `data=` with a stream or `files=` for multipart, but those were not used. Streaming could reduce memory but is slower for small files due to overhead. For future work, one might compare keeping current fully-buffered vs streaming from disk (see table below).
- **HTTP methods:** Only POST is used (the DICOM standard C-STORE over HTTP). The code *does not* ZIP or batch multiple files; each file is its own POST. (Orthanc does support uploading a ZIP archive of many DICOMs in one request【8†L160-L168】, but here that strategy is deliberately avoided to maximize asynchronous concurrency and avoid ZIP overhead.)

## Counting, Atomicity and Failure Modes

Every file produces a result dict with `ok: True/False`. The system maintains atomic counters: `done_count` increments on every returned result (success or failure), and `failed` increments on `ok==False`. Thus **succeeded = total - failed** at the end. Because each file is independent, the operation is not atomic overall (some files may succeed while others fail), but each successful upload is final – the script does not attempt rollback of entire studies.

Idempotency is achieved by treating HTTP 409 (Conflict) as `ok`. In practice, if a file was already in Orthanc (from a previous run or duplicate on the CD), Orthanc may return 409; the code counts this as success, so repeated runs do not break on duplicates. True errors (network failures, 500 codes) after all retries are logged as failures; the final report includes the count and which files failed.

All counters (total, done, failed) and metadata (elapsed time, IDs of created studies) are reported back to the caller (and printed). This allows monitoring or integrating with other systems if needed.

# Configuration and Assumptions

- **Environment:** The code assumes Python 3.7+ (for `async/await` and `httpx`), with the `fastapi`, `pydicom`, and `httpx` libraries installed. It runs on any OS where Python can access the CD drive path (e.g. Windows or Linux). Multithreading on Windows for CD I/O should be fine, but performance depends on the CD drive speed.
- **Orthanc:** Assumed Orthanc v1.8+ running locally or reachable over TCP at `localhost:8042`. The REST API basics have been stable across versions. Basic-auth credentials are hardcoded by default (`admin`/`password`) but configurable.
- **Network:** Best-case scenario is a fast local network (Gigabit) to Orthanc. If Orthanc were remote (slow link), the pipeline speed might be throttled by bandwidth and latency; further tuning (e.g. gzip or HTTP/2) could help.
- **Hardware:** The target of “1000+ slices in <60s” assumes a reasonably fast CPU, SSD (or fast CD drive), and low-latency network. On very low-end hardware or busy drives, timing will vary.

# Performance Comparison Tables

To evaluate alternatives, consider the following comparisons:

**Upload Strategy Comparison**

| Strategy                        | Description                         | Pros                               | Cons                                | Use Case / Notes                |
|---------------------------------|-------------------------------------|------------------------------------|-------------------------------------|---------------------------------|
| **Individual POST (current)**   | One HTTP POST per file (`instances`)| Maximum control; parallelizable. Uses persistent HTTP connections (keep-alive)【12†L88-L96】. | Overhead per HTTP request. Cannot atomically bundle a whole study. | Good for many small files; easily parallelized. |
| **Batch ZIP POST**              | ZIP multiple DICOMs and send single POST【8†L160-L168】. | Less HTTP overhead (fewer requests). Simplifies one-call import. Orthanc can unpack zip of images. | Need CPU/time to create ZIP. Orthanc must unzip; single upload failure aborts many files. Hard to parallelize. | If many files share study and network is slower, might reduce protocol overhead. |
| **DICOMweb STOW-RS**            | Use DICOMweb /studies or /instances endpoint if Orthanc has it. | Standard protocol, possibly can send multiple files in one JSON/MIME. | More complex to implement. May not improve speed. Orthanc setup required. | Useful in DICOMweb-centric workflows. |
| **Filesystem-to-Store (C-STORE)** | Use Orthanc as DICOM SCP over network (not HTTP). | Native DICOM C-STORE protocol, used by imaging devices. | Orthanc must run DICOM listener (storescp). Requires configuring AE titles. Less flexible for general scripting. | When integrating with scanners or legacy tools. |

**Memory Preload Options**

| Option                          | How It Works                         | Pros                                         | Cons                                             | Scenario                                   |
|---------------------------------|--------------------------------------|----------------------------------------------|--------------------------------------------------|--------------------------------------------|
| **Preload all (current)**       | Read all files to RAM (multithread). | Eliminates disk waits during upload; maximal pipeline throughput. | High memory use = sum of all file sizes. Long initial delay for large data. | Use when RAM plenty and network faster than disk. |
| **Stream from disk**            | Open file handle per upload (no preload). | Low memory overhead (only one file’s bytes at a time). | Each upload still hits disk I/O; slows throughput if disk slow. | Use when RAM limited or single big files. |
| **Memory-mapped files (mmap)**  | `mmap` each file instead of read all bytes. | OS handles paging; can be efficient for repeated reads. | Still loads from disk on demand; complexity for Windows CD-ROM support. | Rarely needed; more for huge files with random access. |
| **Async file I/O (aiofiles)**   | Use asyncio to read files without blocking thread. | Can integrate with async upload loop. Frees Python threads for other tasks. | Python `aiofiles` still uses threads under hood; not true non-blocking for CDs. | If rewriting fully async pipeline. |
| **Batched preload**             | Preload in chunks (e.g. 100 files, upload, then next). | Reduces peak memory. Still parallel disk/network. | Slightly more complex logic. Some overhead between batches. | If files count or size is very large. |

**Failure-Handling Strategies**

| Strategy             | Behavior                                             | Pros                                      | Cons                                                | Example / Note                               |
|----------------------|------------------------------------------------------|-------------------------------------------|-----------------------------------------------------|----------------------------------------------|
| **Current (retry & skip)** | Retry transient errors up to N times, then log failure but continue with other files. | Robust: a few network glitches don’t stop the whole process. Most errors isolated. | Needs careful count tracking. Failed files must be reprocessed manually later. | Good general approach for large batches.      |
| **Fail-fast (abort)** | On first error, abort entire upload pipeline.          | Ensures 100% integrity only if all-or-nothing needed. | Very unforgiving: one hiccup can lose partial progress. Not user-friendly. | Rarely desired for bulk import; more for critical transactions. |
| **Transactional rollback** | If any file fails, attempt to remove previously uploaded ones. | True atomicity (all files or none in Orthanc). | Very complex: Orthanc would need deletes and consistency. Likely impractical. | Not implemented here; usually not needed. |
| **Unlimited retry queue** | Keep retrying failed uploads indefinitely until success. | Handles very unreliable networks (e.g. satellite link). | Risk of hang or long delays. Difficult to give up. | Could be implemented, but adds complexity (tracking state). |
| **Partial audit log** | On failure, record file and reason to a persistent log for later retry. | Provides traceability of exactly which files failed and why. | Additional I/O (logging), complexity. | Useful in production to re-run later. |

# Performance Improvement Vision

For very high-speed scenarios, the following enhancements could yield gains. Each is rated by **Estimated Impact** (how much it could speed up or stabilize uploads) and **Implementation Complexity**:

1. **Increase Upload Concurrency:** The default `MAX_UPLOAD_WORKERS=64` can be raised if Orthanc and the network can handle more. Impact: *Medium–High*. If tests show CPU/network not saturated at 64, raising to 100+ could further parallelize uploads. Complexity: *Low*. Just change a constant. (Caveat: too many connections may overwhelm Orthanc or the OS.)

2. **Enable HTTP/2 / Compression:** HTTPX supports HTTP/2, which can multiplex requests over fewer connections. If Orthanc is configured with HTTP/2, this might reduce latency. Similarly, enabling request compression (e.g. HTTP gzip) could shrink payload sizes. Impact: *Low–Medium*. DICOM files may not compress well (often already compressed JPEG), so gains may be modest. Complexity: *Medium*. Requires server support and some httpx config (e.g. use `client.http2=True`, or compress payloads manually).

3. **Batch Upload (ZIP) Experiment:** Try grouping images into ZIP archives for upload. Impact: *Medium*. This reduces per-file HTTP overhead, but adds CPU cost to compress/uncompress. If many small slices, zipping 50–100 at once may speed overall. Complexity: *Medium*. Would require changes: chunk the file list, zip each chunk, POST zip (with appropriate header or file type) and track returned instances (Orthanc returns IDs of all images in the zip). Need careful error handling per-file inside zip.

4. **Streaming Upload Without Preload:** Modify the pipeline to avoid full preloading, instead streaming each file from disk directly into `httpx.post(content=...)`. Impact: *Medium*. This saves memory and startup time. In tests, memory was not the bottleneck, but on machines with limited RAM or very large files, it could help. Complexity: *Low–Medium*. Instead of reading all bytes at once, you could use `client.post(..., content=open(path,"rb"))` (httpx accepts a file-like object). Note: Python may still read internally, but perceived memory usage drops.

5. **Asynchronous File I/O:** Use an async file I/O library (like `aiofiles`) to overlap disk reads with other async tasks. Impact: *Low–Medium*. If the CD drive is extremely slow, true non-blocking reads may improve throughput. Complexity: *High*. Python’s async I/O for file systems is limited (often still threads under the hood), so gains might not justify effort.

6. **Backpressure and Rate Limiting:** Implement dynamic rate control: measure current upload throughput and adapt `MAX_UPLOAD_WORKERS` or sleep intervals if errors spike. Impact: *Low*. This would mostly stabilize performance under varying network conditions. Complexity: *High*. Not usually needed on a fast LAN.

7. **Monitoring and Metrics:** Integrate with a monitoring system (Prometheus/Grafana). Impact: *Low–Medium*. Doesn’t directly speed up, but helps detect bottlenecks. Complexity: *Medium*. Export counters (files/sec, error count) and timings for real-time insight.

8. **Hardware/Network Upgrades:** Use a high-speed NIC (10GbE), SSD for temp caching, or faster CD/DVD drive (if reading from physical discs). Impact: *Medium–High*. If the bottleneck is hardware, these yield immediate benefit. Complexity: *High* (requires new hardware).

9. **Thread Affinity / Parallel Scanning:** Improve how the system scans files. For example, use multiple processes (multiprocessing) instead of threads to parse headers, or tune thread count. Impact: *Low–Medium*. Useful if Python’s GIL or disk seeks are limiting. Complexity: *Medium*. Current thread-pool is usually sufficient for I/O-bound tasks.

10. **Use of GPUs or Accelerators:** Not applicable; uploading is I/O-bound, not compute-bound.

In summary, the **highest-impact, low-complexity** improvements are to **tune concurrency and try batched uploads**. For example, increasing `MAX_UPLOAD_WORKERS` (Impact ~+20–50% throughput if under-utilized) is easy. Implementing batch ZIP uploads (Impact *uncertain; test needed*) could reduce overhead by ~5–10% or more if network latency is an issue. More complex changes (async I/O, monitoring) can follow once the main bottleneck is addressed. Each change should be profiled: e.g., measure “files/sec” before and after.

Ultimately, the vision is to make the pipeline as close to **line-rate** as possible: saturating the network while keeping CPU/disk busy but not idle. With proper tuning and hardware, uploads of thousands of images should complete in seconds to a few tens of seconds on a gigabit link.

# References

- Orthanc REST API – uploading DICOM instances via `/instances` (HTTP POST with `--data-binary`)【8†L160-L168】.  
- DICOM Standard (PS3.10) – each file-set must include a `DICOMDIR` index file【27†L1749-L1751】.  
- Pydicom documentation – `dcmread(..., specific_tags=…)` to read only selected DICOM tags【21†L656-L662】.  
- HTTPX Clients – reusing connections via `AsyncClient` yields pooled connections【12†L88-L96】.  
- DICOMCD guidance – “Look for a file named DICOMDIR on the CD”【17†L189-L193】 for fast media indexing.  

