# welf.cache.cache_document(cache_key, company_number, dtype, str(path), True, ...)_insolvency_cached.py
# Cached pipeline + dedup utilities + entrypoints.
from __future__ import annotations

import collections
import time
import io, tempfile, os, requests, threading
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import re
from datetime import datetime as _dt
import queue
from typing import Callable
from taxonomy import PROCEDURES, ALIASES  # NEW
from threading import BoundedSemaphore
from wc_drive_upload_manager import DriveUploadManager, UploadJob
from functools import lru_cache



from drive_client import GoogleDriveClient, CreateSpec, DRIVE_SCOPE_FILE, DRIVE_SCOPE_FULL
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from settings import DOC_SOURCE, CH_API_KEY, cfg, refresh_cfg
from providers.base_provider import FilingProvider, FilingItem, filing_identity
from providers.ch_api_provider import CHApiProvider, make_api_provider, ProviderRateLimited

FH_MAX_PAGES = cfg("FH_MAX_PAGES", 200, int)
MAX_DOCS_DEFAULT = cfg("MAX_DOCUMENTS_PER_COMPANY", 999_999, int)


from wc_async_core import (
    CacheConfig,
    IntelligentCacheManager,
    WorldClassGazetteCompaniesHousePipeline,
    tls_session,
    TokenBucket
)


# =============================================================================
# DEDUP UTILS (kept from your world-class script)
# =============================================================================

def select_best_notice(notices: List[Dict]) -> Dict:
    priorities = {
        "Appointment of Liquidators": 1,
        "Meetings of Creditors": 2,
        "Notices to Creditors": 3,
        "Annual Liquidation Meetings": 4,
        "Resolutions for Winding-up": 5,
        "Resolutions for Winding Up": 5,
        "Final Meetings": 6,
    }

    def key(n):
        pr = priorities.get(n.get("notice_type", ""), 999)
        nid = int(n.get("notice_id", "0") or "0")
        return (pr, -nid)

    return sorted(notices, key=key)[0]

def optimize_company_deduplication(companies: List[Dict]) -> Tuple[List[Dict], Dict]:
    groups = defaultdict(list)
    without_nums = 0
    for c in companies:
        num = (c.get("company_number") or "").strip()
        if num:
            groups[num].append(c)
        else:
            without_nums += 1

    unique: List[Dict] = []
    meta = {
        "total_notices": len(companies),
        "unique_companies": len(groups),
        "duplicate_notices": 0,
        "companies_without_numbers": without_nums,
        "companies_with_multiple_notices": 0,
        "selection_strategy": {},
        "notice_combinations": defaultdict(int),
        "sample_duplicates": {},
    }

    samples = {}
    for num, notices in groups.items():
        if len(notices) == 1:
            unique.append(notices[0])
        else:
            chosen = select_best_notice(notices)
            unique.append(chosen)
            meta["duplicate_notices"] += len(notices) - 1
            meta["companies_with_multiple_notices"] += 1
            combo = " + ".join(sorted([n["notice_type"] for n in notices]))
            meta["notice_combinations"][combo] += 1
            meta["selection_strategy"][num] = {
                "total_notices": len(notices),
                "selected": chosen["notice_type"],
                "all_types": sorted({n["notice_type"] for n in notices}),
            }
            if len(samples) < 5:
                samples[num] = [{"notice_type": n["notice_type"], "notice_id": n.get("notice_id", "")} for n in notices]
    meta["sample_duplicates"] = samples
    return unique, meta

def _sanitize_desc_for_filename(desc: str, max_len: int = 160) -> str:
    # remove HTML
    txt = re.sub(r"<[^>]+>", "", desc or "")
    # replace forbidden filename chars
    txt = re.sub(r'[<>:"/\\|?*]', "_", txt)
    # collapse whitespace to underscores
    txt = re.sub(r"\s+", "_", txt.strip()).strip("_")
    if not txt:
        txt = "Document"
    # optional: limit length
    return txt[:max_len].rstrip("_") or "Document"

def _extract_doc_id_from_url(url: str) -> str | None:
    m = re.search(r"/document/([A-Za-z0-9_\-]+)", url or "")
    return m.group(1) if m else None

def _doc_cache_key(url: str, item=None) -> str:
    """
    Canonical cache key for filings - STABLE across runs.
    Priority: document_id > extracted_id > normalized_url
    """
    # 1) FilingItem with document_id
    if isinstance(item, FilingItem):
        did = (item.document_id or "").strip()
        if did:
            return f"chdoc://{did}"
    
    # 2) dict with document_id
    if isinstance(item, dict):
        did = (item.get("document_id") or "").strip()
        if did:
            return f"chdoc://{did}"
    
    # 3) Extract from URL (most reliable for HTML flow)
    m = re.search(r"/filing-history/([A-Za-z0-9_\-]+)/document", url or "")
    if m:
        return f"chdoc://{m.group(1)}"
    
    # 4) Normalize URL (remove transient params)
    if url:
        # Strip query params that change between sessions
        base_url = url.split("?")[0]
        # Add back only format=pdf if present
        if "format=pdf" in url:
            base_url += "?format=pdf"
        return base_url
    
    # 5) Fallback
    return f"chdoc://unknown/{hash(str(item))}"

# =============================================================================
# CACHED PIPELINE
# =============================================================================

class RateLimitFallback(Exception):
    """Signal to abandon API fast-path and fall back to HTML scraping."""
    pass


class CachedGazetteCompaniesHousePipeline(WorldClassGazetteCompaniesHousePipeline):
    """World-class pipeline augmented with multi-layer caching + Drive upload."""
    def __init__(self, base_download_folder="InsolvencyDocuments", cache_config=None, verbose=True, http_max_inflight=12, drive_upload_concurrency=20,
        doc_concurrency_per_company=5, max_concurrent_companies=6, provider: FilingProvider | None = None):
        super().__init__(base_download_folder, verbose)
        self.cache = IntelligentCacheManager(cache_config or CacheConfig())
        self.provider = provider or ProviderFactory.create()
        self._provider_lock = threading.RLock()
        # Diagnostic: Show which provider we're using
        if isinstance(self.provider, CHApiProvider):
            self.log(f"✓ API Provider initialized (key={'*' * 8 if CH_API_KEY else 'MISSING'})", "INFO")
            self._wire_provider_listeners(self.provider)
        else:
            self.log(f"⚠ HTML fallback active (API key: {'present' if CH_API_KEY else 'MISSING'}, DOC_SOURCE: {DOC_SOURCE or 'not set'})", "WARNING")
        self.cache_perf = {
            "companies_from_cache": 0,
            "companies_processed_fresh": 0,
            "documents_from_cache": 0,
            "documents_downloaded_fresh": 0,
        }
        # Drive config (set via attach_drive)
        self.drive_client: Optional[GoogleDriveClient] = None
        self.drive_root_id: Optional[str] = None
        self._forced_folder: Optional[str] = None  # NEW
        self.drive_delete_local = True
        self.drive_only = True  # default to upload-only (no local disk)
        self._drive_lock = threading.RLock()  # <-- NEW: make Drive ops thread-safe
        self._drive_folder_lock = threading.RLock()
        self._folder_cache = {} # Cache for remote folder IDs
        self._http_tokens = BoundedSemaphore(value=int(http_max_inflight))
        # Drive upload concurrency cap + semaphore
        self._drive_upload_cap = int(drive_upload_concurrency)
        self._drive_sema = BoundedSemaphore(value=self._drive_upload_cap)
        # track confirmed Drive uploads per company for this run
        self._uploaded_by_company = defaultdict(int)
        # track enqueued (not-yet-confirmed) per company for this run (optional UI)
        self._enqueued_by_company = defaultdict(int)
        # UI emitter
        self.emit_progress: Callable[[dict], None] = lambda _msg: None  # no-op until wired by app

        # honor the constructor flags directly (no kwargs here)
        self.delete_local = True  # used only for local files (kept for symmetry)

        self._doc_workers_per_company = int(doc_concurrency_per_company)
        self._max_concurrent_companies = int(max_concurrent_companies)
        self._http_token_cap = int(http_max_inflight)

        # --- Async upload decoupling (disabled until workers started) ---
        self._upload_queue: "queue.Queue[dict]" = queue.Queue(
            maxsize=int(os.getenv("DRIVE_UPLOAD_QUEUE_MAX", "256"))
        )
        # self._workers: list[threading.Thread] = []
        self._upload_workers_running = threading.Event()
        self._upload_workers_count: int = 0
        self._stop_evt = threading.Event()
        # self._upload_q = None
        self._uploaded_total = 0
        self._enqueued_total = 0
        self._uploaded_by_company = defaultdict(int)
        self._upload_threads: list[threading.Thread] = []
        # --- Small-upload batching (batch API) ---
        self._batch_threshold_bytes = int(os.getenv("DRIVE_MULTIPART_THRESHOLD", "4000000"))  # match drive_client
        self._batch_max = int(os.getenv("DRIVE_BATCH_MAX", "100"))
        self._batch_flush_sec = float(os.getenv("DRIVE_BATCH_FLUSH_SEC", "3.0"))
        self._batch_lock = threading.RLock()
        self._batch_specs: list[tuple[CreateSpec, dict]] = []  # (spec, job_meta)
        self._batch_last_flush = time.time()
        self._uploaded_total = 0

        self.log(
            f"Concurrency — companies={self._max_concurrent_companies}, "
            f"per_company={self._doc_workers_per_company}, "
            f"http_tokens={self._http_token_cap}, "
            f"drive_upload={self._drive_upload_cap}"
        )
        # --- Token bucket (global HTTP pacing) ---
        rate = float(os.getenv("HTTP_TOKEN_RATE", "25"))           # tokens/sec
        burst = int(os.getenv("HTTP_TOKEN_BURST", str(self._http_token_cap)))
        self.http_tokens = TokenBucket(rate=rate, burst=burst)
        self._api_rate_lock = threading.Lock()
        self._api_suspended_until = 0.0
        self._api_rate_limited = False
        self._last_rate_event = 0.0
        # --- Run-level document reuse (micro-cache to avoid duplicate fetches) ---
        self._doc_reuse_lock = threading.Lock()
        self._doc_reuse_pool: "collections.OrderedDict[str, bytes]" = collections.OrderedDict()
        self._doc_reuse_bytes = 0
        self._doc_reuse_soft_limit = int(cfg("DOC_REUSE_SOFT_LIMIT_MB", 128, int)) * 1024 * 1024
        self._doc_reuse_max_entries = int(cfg("DOC_REUSE_MAX_ENTRIES", 512, int))
        self._doc_reuse_single_cap = int(cfg("DOC_REUSE_SINGLE_MAX_MB", 6, int)) * 1024 * 1024
        self._drive_fast_threshold = int(cfg("DRIVE_FAST_LANE_MB", 1, int)) * 1024 * 1024
        self._drive_fast_workers = max(1, int(cfg("DRIVE_FAST_LANE_WORKERS", 6, int)))
        self._drive_bulk_workers = max(1, int(cfg("DRIVE_BULK_WORKERS", 20, int)))

    def _should_cancel(self, cancel_event) -> bool:
        return bool(cancel_event and cancel_event.is_set())

    def _looks_like_pdf(self, data: bytes) -> bool:
        if not data:
            return False
        # accept very small files; just require %PDF within first 2048 bytes
        return b"%PDF" in data[:2048]

    def _batch_upload_to_drive(self, files: List[Tuple[bytes, str]], parent_id: str, number: str) -> Tuple[int, List[int]]:
        """
        Dual-lane uploader. Small payloads (<= fast threshold) run on a "fast lane"
        so users see documents hit Drive immediately, while larger filings run on
        a bulk lane with higher parallelism. Returns (count, [successful indexes]).
        """
        if not self._drive_is_ready() or not files:
            return 0, []

        indexed = [(idx, data, name, len(data)) for idx, (data, name) in enumerate(files)]
        fast_entries = [entry for entry in indexed if entry[3] <= self._drive_fast_threshold]
        bulk_entries = [entry for entry in indexed if entry[3] > self._drive_fast_threshold]

        success_idx: set[int] = set()

        def _upload_lane(entries, workers: int, lane: str) -> None:
            if not entries:
                return
            pending = {"value": len(entries)}
            lane_lock = threading.Lock()

            def _mark_done(delta_success: int) -> None:
                with lane_lock:
                    pending["value"] = max(0, pending["value"] - 1)
                if delta_success:
                    self._emit_upload_progress(number, lane, delta_success, pending["value"])

            max_workers = max(1, min(workers, len(entries)))

            def _upload_one(entry) -> Optional[int]:
                idx, data, filename, _ = entry
                for attempt in range(2):
                    try:
                        fid = self.drive_client.upload_pdf_stream(data, filename, parent_id)
                        if fid:
                            return idx
                    except Exception as exc:
                        if attempt == 0:
                            time.sleep(0.1)
                            continue
                        self.log(f"Upload failed for {filename}: {exc}", "ERROR")
                return None

            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"drive-{lane}-{number}") as ex:
                futures = {ex.submit(_upload_one, entry): entry for entry in entries}
                for fut in as_completed(futures):
                    idx = fut.result()
                    if idx is not None:
                        success_idx.add(idx)
                        _mark_done(1)
                    else:
                        _mark_done(0)

        _upload_lane(fast_entries, self._drive_fast_workers, "fast")
        _upload_lane(bulk_entries, self._drive_bulk_workers, "bulk")

        ordered_success = sorted(success_idx)
        return len(ordered_success), ordered_success
    def _check_api_health(self) -> bool:
        """Check if we're hitting rate limits."""
        try:
            if hasattr(self, '_rate'):
                error_rate = getattr(self._rate, 'error_count', 0) / max(1, getattr(self._rate, 'success_count', 1))
                if error_rate > 0.3:  # >30% errors
                    self.log(f"High API error rate: {error_rate:.1%} - throttling", "WARNING")
                    time.sleep(2)
                    return False
            return True
        except Exception:
            return True

    # --- Document reuse helpers -------------------------------------------------
    def _document_reuse_get(self, doc_id: Optional[str]) -> Optional[bytes]:
        if not doc_id:
            return None
        with self._doc_reuse_lock:
            data = self._doc_reuse_pool.get(doc_id)
            if data is None:
                return None
            # LRU bump
            self._doc_reuse_pool.move_to_end(doc_id)
            return data

    def _document_reuse_store(self, doc_id: Optional[str], data: Optional[bytes]) -> None:
        if not doc_id or not data:
            return
        if len(data) > self._doc_reuse_single_cap:
            return
        with self._doc_reuse_lock:
            if doc_id in self._doc_reuse_pool:
                return
            while (
                self._doc_reuse_bytes + len(data) > self._doc_reuse_soft_limit
                or len(self._doc_reuse_pool) >= self._doc_reuse_max_entries
            ):
                old_id, old_data = self._doc_reuse_pool.popitem(last=False)
                self._doc_reuse_bytes -= len(old_data)
            self._doc_reuse_pool[doc_id] = data
            self._doc_reuse_bytes += len(data)

    def _emit_upload_progress(self, company_number: str, lane: str, delta: int, pending: int) -> None:
        try:
            self._emit(
                kind="upload_progress",
                company_number=company_number,
                lane=lane,
                delta=delta,
                pending=max(0, pending),
            )
        except Exception:
            pass

    def _emit_doc_progress(self, company_number: str, fresh: int = 0, cached: int = 0) -> None:
        delta_fresh = int(fresh or 0)
        delta_cached = int(cached or 0)
        if not (delta_fresh or delta_cached):
            return
        try:
            self._emit(
                kind="document_progress",
                company_number=company_number,
                fresh=delta_fresh,
                cached=delta_cached,
            )
        except Exception:
            pass

    def _handle_rate_limit_event(self, info: dict) -> None:
        if not info:
            return
        status = info.get("status", "limited")
        delay = float(info.get("delay") or 0.0)
        resume_at = float(info.get("cooldown_until") or 0.0)
        with self._api_rate_lock:
            if status == "limited":
                resume = max(time.time() + delay, resume_at)
                self._api_suspended_until = max(self._api_suspended_until, resume)
                self._api_rate_limited = True
            elif status == "recovered":
                self._api_rate_limited = False
                self._api_suspended_until = min(self._api_suspended_until, time.time())
        try:
            self._emit(
                kind="rate_limit",
                status=status,
                delay=delay,
                resume_at=max(resume_at, self._api_suspended_until),
                key_suffix=info.get("key_suffix", ""),
            )
        except Exception:
            pass

    def _wire_provider_listeners(self, provider):
        """Attach rate-limit callbacks exactly once per provider instance."""
        if not isinstance(provider, CHApiProvider):
            return
        if getattr(provider, "_wc_listener_registered", False):
            return
        try:
            provider.register_rate_limit_listener(self._handle_rate_limit_event)
            setattr(provider, "_wc_listener_registered", True)
        except Exception as exc:
            self.log(f"Could not wire provider listeners: {exc}", "WARNING")

    def _should_use_api_fast_path(self) -> bool:
        if not isinstance(self.provider, CHApiProvider):
            return False
        with self._api_rate_lock:
            suspended = self._api_suspended_until
            limited = self._api_rate_limited
        now = time.time()
        if now >= suspended:
            if limited:
                with self._api_rate_lock:
                    self._api_rate_limited = False
                try:
                    self._emit(kind="rate_limit", status="recovered", resume_at=now, key_suffix="auto")
                except Exception:
                    pass
            return True
        return False

    def _assert_api_window(self) -> None:
        if not self._should_use_api_fast_path():
            raise RateLimitFallback("API suspended")
    
    # --- Drive integration ---
    def attach_drive(self, drive_client, root_folder_id: str,
                 drive_only: bool = False,
                 delete_local_after_upload: bool = True):
        
        self.drive_client = drive_client
        self.drive_root_id = root_folder_id
        self._stop_upload_workers(drain_timeout=0.5)
        self.drive_only = bool(drive_only)
        self.drive_delete_local = bool(delete_local_after_upload)
        self.log(f"Drive attached (root={root_folder_id}; drive_only={self.drive_only}; delete_local={self.drive_delete_local})")
        self._folder_cache.clear()
        # NEW: Only start old workers if NOT using batch mode
        if os.getenv("USE_LEGACY_QUEUE_UPLOAD", "0") == "1":
            self._start_upload_workers()
            self.log(f"Upload workers active: {len(self._upload_threads)}")
        else:
            self.log(f"Using direct batch upload (no queue workers)")

    def detach_drive(self):
        """Flush any pending uploads, stop workers, and detach Drive state so we can reattach later."""
        try:
            self.stop_upload_workers(timeout=None)
        except Exception as _e:
            self.log(f"detach_drive: error stopping workers: {_e}", "WARNING")

        # Clear Drive-related state
        self.drive_client = None
        self.drive_root_id = None
        self._folder_cache.clear()
        self._forced_folder = None
        self.log("Drive detached; workers stopped; state cleared")


    def _ensure_provider_ready(self) -> Optional[FilingProvider]:
        """
        Guarantee that self.provider exists and, if it's an API provider, its sessions are primed.
        Falls back to HTML provider (None) when API bootstrap fails.
        """
        with self._provider_lock:
            provider = getattr(self, "provider", None)
            if provider is None:
                provider = ProviderFactory.create()
                self.provider = provider
        self._wire_provider_listeners(provider)

        if isinstance(provider, CHApiProvider):
            try:
                provider.ensure_ready_sync()
            except Exception as exc:
                self.log(f"[PROVIDER] ensure_ready failed ({exc}); reinitialising", "WARNING")
                try:
                    with self._provider_lock:
                        provider = make_api_provider()
                        self.provider = provider
                    if isinstance(provider, CHApiProvider):
                        provider.ensure_ready_sync()
                except Exception as inner:
                    self.log(f"[PROVIDER] Fallback to HTML path: {inner}", "ERROR")
                    provider = None
                    with self._provider_lock:
                        self.provider = None
        return provider

    @lru_cache(maxsize=4096)
    def _cached_get_or_create_folder_id(self, parent_id: str, name: str) -> str:
        # Reuse your existing function here; typical name is _ensure_drive_folder_with_retry
        return self._ensure_drive_folder_with_retry(parent_id, name)
    
    def _is_valid_cached_artifact(self, path: Optional[str]) -> bool:
        # must either be a Drive file reference or an existing local file
        return bool(path and (path.startswith("gdrive://") or os.path.exists(path)))
    
    def _safe_name(self, prof, fallback):
        return (prof or {}).get("company_name") or fallback

    
    def reconcile_pending_uploads(self):
        """
        Find cache rows with pending_upload=True and no Drive file path.
        Re-enqueue them to guarantee eventual consistency across runs.
        Requires cache.list_pending_uploads() -> iterable of records with:
        {url, company_number, dtype, drive_parent_id, drive_file_name}
        """
        try:
            pendings = getattr(self.cache, "list_pending_uploads", None)
            if not pendings:
                return
            for rec in self.cache.list_pending_uploads():
                url   = rec.get("url")
                num   = rec.get("company_number")
                dtype = rec.get("dtype", "DOC")
                pid   = rec.get("drive_parent_id")
                name  = rec.get("drive_file_name")
                if not (url and num and pid and name):
                    continue
                # mark as still pending; bytes are gone, so we must refetch via URL
                pdf = self._download_bytes(url)
                if not pdf:
                    continue
                self._enqueue_upload(pdf, name, pid, url, num, dtype)
                self._enqueued_by_company[num] += 1
        except Exception as e:
            self.log(f"reconcile_pending_uploads error: {e}", "WARNING")



    def ensure_company_folder_once(self, root_folder_id: str, company_number: str) -> str:
        # Adapt the folder structure you actually use; simplest is one folder per company
        return self._cached_get_or_create_folder_id(root_folder_id, company_number)

    def _drive_is_ready(self) -> bool:
        """Return True if Drive client + root folder are attached."""
        return bool(getattr(self, "drive_client", None) and getattr(self, "drive_root_id", None))
    
    def _flush_small_upload_batch(self, force: bool = False):
        with self._batch_lock:
            n = len(self._batch_specs)
            due = (time.time() - self._batch_last_flush) >= self._batch_flush_sec
            if not force and (n == 0 or (n < self._batch_max and not due)):
                return
            
            # Snapshot specs to upload
            metas = self._batch_specs[:self._batch_max]
            specs = [spec for (spec, _meta) in metas]
            del self._batch_specs[:self._batch_max]

        # Execute batch outside lock
        results = None
        try:
            if not self.drive_client:
                self.log("Drive client not available for batch", "WARNING")
                results = [None] * len(specs)
            elif not specs:
                return
            else:
                results = self.drive_client.upload_batch_create(
                    specs, batch_size=min(len(specs), self._batch_max)
                )
        except AttributeError as e:
            self.log(f"Batch upload AttributeError: {e} - drive_client may not support upload_batch_create", "ERROR")
            results = [None] * len(specs)
        except HttpError as e:
            code = getattr(getattr(e, "resp", None), "status", "unknown")
            self.log(f"Batch upload failed (HTTP {code})", "ERROR")
            results = [None] * len(specs)
        except Exception as e:
            self.log(f"Batch upload failed: {e.__class__.__name__}: {e}", "ERROR")
            results = [None] * len(specs)

        # Robust result normalization
        try:
            if results is None:
                results = [None] * len(metas)
            elif isinstance(results, dict):
                # Map dict by name/id
                norm = []
                for (_spec, meta) in metas:
                    name = meta.get("remote_name", "")
                    r = results.get(name) or results.get(meta.get("cache_key", ""))
                    norm.append(r)
                results = norm
            else:
                results = list(results)
                # Pad or trim to match
                if len(results) < len(metas):
                    results.extend([None] * (len(metas) - len(results)))
                elif len(results) > len(metas):
                    results = results[:len(metas)]
        except Exception as e:
            self.log(f"Result normalization error: {e}", "WARNING")
            results = [None] * len(metas)

        # Process results
        ok_cnt = 0
        for res, (_spec, meta) in zip(results, metas):
            cache_key = meta["cache_key"]
            number = meta["company_number"]
            dtype = meta["dtype"]
            name = meta["remote_name"]
            parent_id = meta["parent_id"]

            # Extract file ID (robust against different formats)
            fid = None
            if isinstance(res, dict):
                fid = res.get("id") or res.get("fileId") or res.get("idString")
            elif isinstance(res, str) and res.strip():
                fid = res.strip()

            if fid:
                ok_cnt += 1
                self.cache.cache_document(
                    cache_key, number, dtype, f"gdrive://{fid}", True,
                    metadata={"upload_time": 0.0, "drive_parent_id": parent_id, "drive_file_name": name, "batched": True}
                )
                self._uploaded_by_company[number] += 1
                self._uploaded_total += 1
                self._emit(kind="upload_done", file=name, company_number=number)
            else:
                self.cache.cache_document(cache_key, number, dtype, None, False, error_message="Batch upload failed")
                self._emit(kind="upload_failed", file=name, company_number=number)

        fail_cnt = len(metas) - ok_cnt
        self.log(f"BATCH SUMMARY: created={ok_cnt} failed={fail_cnt} pending_specs={len(self._batch_specs)}", "INFO")

        with self._batch_lock:
            self._batch_last_flush = time.time()

    def set_progress_emitter(self, fn: Callable[[dict], None] | None):
        """
        Wire a callback that receives small dict events, e.g. {"kind": "upload_done", ...}.
        Streamlit should pass `queue.put` here. If None, uses a no-op.
        """
        self.emit_progress = fn or (lambda _msg: None)

    def _emit(self, **payload):
        """Internal: fire a progress event; never let UI exceptions bubble back."""
        try:
            self.emit_progress(payload)
        except Exception:
            pass

    def _start_upload_workers(self, num_workers: int | None = None):
        """Start background Drive upload workers + a periodic flusher."""
        if not self._drive_is_ready():
            return
        if self._upload_threads:
            return  # Already started
        
        # Determine number of workers from: param > env > constructor default
        if num_workers is None:
            num_workers = int(os.getenv("DRIVE_UPLOAD_WORKERS", str(self._drive_upload_cap)))
        n = max(1, num_workers)
        
        self._upload_workers_running.set()
        self._upload_threads = []
        
        for i in range(n):
            t = threading.Thread(
                target=self._upload_worker_loop, 
                name=f"drive-upl-{i+1}", 
                daemon=True
            )
            t.start()
            self._upload_threads.append(t)
        
        # Start periodic batch flusher (optional, if you have one)
        if hasattr(self, '_start_batch_flusher'):
            self._start_batch_flusher()
        
        self.log(f"Upload workers active: {len(self._upload_threads)}")
    def _ensure_upload_workers(self, n: int = 10):
        """Ensure upload workers are running (legacy queue system)."""
        # Only if legacy mode enabled
        if os.getenv("USE_LEGACY_QUEUE_UPLOAD", "0") != "1":
            return
        
        if getattr(self, "_upload_workers_running", threading.Event()).is_set():
            if self._upload_threads and all(t.is_alive() for t in self._upload_threads):
                return
        
        self._start_upload_workers(n)

    def _flusher_loop(self):
        """Flush small batched items periodically while workers are running."""
        tick = max(0.15, min(self._batch_flush_sec, 1.0))  # gentle cadence
        while self._upload_workers_running.is_set():
            try:
                self._flush_small_upload_batch(force=False)
            except Exception as _e:
                self.log(f"Batch flusher error: {_e.__class__.__name__}", "WARNING")
            time.sleep(tick)
        # One last forced flush on exit
        try:
            self._flush_small_upload_batch(force=True)
        except Exception as _e:
            self.log(f"Final batch flusher error: {_e.__class__.__name__}", "WARNING")

    def wait_for_uploads(self, timeout: float | None = None) -> None:
        """Block until the upload queue AND any batched small items are fully flushed."""
        if not self._upload_threads:
            try: self._flush_small_upload_batch(force=True)
            except Exception: pass
            return

        end = None if timeout is None else time.time() + max(0.0, timeout)
        while True:
            # opportunistic flush
            try: self._flush_small_upload_batch(force=False)
            except Exception: pass

            with self._batch_lock:
                batch_pending = len(self._batch_specs)

            queue_empty = (self._upload_queue.unfinished_tasks == 0)
            # Also wait until no batched specs remain
            with self._batch_lock:
                pending_specs = len(self._batch_specs)
            if pending_specs > 0:
                try:
                    self._flush_small_upload_batch(force=True)
                except Exception:
                    pass
                continue
            if queue_empty and batch_pending == 0:
                try: self._flush_small_upload_batch(force=True)
                except Exception: pass
                break

            if end is not None and time.time() >= end:
                try: self._flush_small_upload_batch(force=True)
                except Exception: pass
                break

            time.sleep(0.05)

    def flush_and_stop_upload_workers(self, timeout: float = 10.0):
        if not getattr(self, "_upload_workers_running", False):
            return
        if getattr(self, "_stop_evt", None):
            self._stop_evt.set()

        # Unblock workers
        try:
            for _ in self._upload_threads:
                self._upload_queue.put_nowait(None)
        except Exception:
            pass

        deadline = time.time() + timeout
        for w in getattr(self, "_workers", []):
            try:
                w.join(max(0, deadline - time.time()))
            except RuntimeError:
                pass

        self._workers = []
        self._upload_workers_running = False
        self.log("Upload workers stopped")


    def _stop_upload_workers(self, drain_timeout: float = 2.0):
        if not getattr(self, "_upload_threads", None):
            try: self._flush_small_upload_batch(force=True)
            except Exception: pass
            return

        self._upload_workers_running.clear()

        # Unblock workers
        try:
            for _ in self._upload_threads:
                self._upload_queue.put_nowait(None)
        except Exception:
            pass

        deadline = time.time() + drain_timeout
        for t in self._upload_threads:
            rem = max(0.0, deadline - time.time())
            t.join(timeout=rem)

        self._upload_threads = []

        # Stop/join flusher
        flusher = getattr(self, "_flusher_thread", None)
        if flusher:
            try: flusher.join(timeout=1.0)
            except Exception: pass
            self._flusher_thread = None

        # Final forced flush
        try: self._flush_small_upload_batch(force=True)
        except Exception as _e:
            self.log(f"Final batch flush error: {_e.__class__.__name__}", "WARNING")

        # Optional: drain any leftover sentinels from the queue (best effort)
        try:
            while True:
                self._upload_queue.get_nowait()
                self._upload_queue.task_done()
        except Exception:
            pass

        self.log("Upload workers stopped")

    def _upload_path_with_name(self, local_path: str, parent_id: str, remote_name: str) -> Optional[str]:
        """
        Drive client .upload_pdf(path, parent_id, delete_local_after=...) does not accept a name kwarg.
        We ensure the file's basename matches remote_name before calling it.
        """
        try:
            dirname = os.path.dirname(local_path) or "."
            target  = os.path.join(dirname, remote_name)
            # If the path already has the right name, use it as-is
            if os.path.abspath(local_path) != os.path.abspath(target):
                # Prefer atomic rename; if it fails, copy as fallback
                try:
                    os.replace(local_path, target)
                    local_path = target
                except Exception:
                    with open(local_path, "rb") as f:
                        data = f.read()
                    # keep extension if present, else default .pdf
                    _, ext = os.path.splitext(remote_name)
                    fd, tmp = tempfile.mkstemp(prefix="upl_named_", suffix=ext or ".pdf", dir=dirname)
                    with os.fdopen(fd, "wb") as out:
                        out.write(data)
                    try:
                        os.unlink(local_path)
                    except Exception:
                        pass
                    local_path = tmp

            # Now call the client without a name kwarg
            fid = self.drive_client.upload_pdf(local_path, parent_id, delete_local_after=True)
            return fid
        except Exception as e:
            self.log(f"_upload_path_with_name error for {remote_name}: {e}", "ERROR")
            return None

    
    def _upload_worker_loop(self):
        """Background loop: consume queue and upload to Drive.
        Small files are batched; large files upload immediately (resumable)."""
        while self._upload_workers_running.is_set():
            try:
                item = self._upload_queue.get(timeout=0.25)
            except queue.Empty:
                # Opportunistic time-based flush if enough time has passed
                try:
                    self._flush_small_upload_batch(force=False)
                except Exception as _e:
                    self.log(f"Batch flush tick error: {_e.__class__.__name__}", "WARNING")
                continue

            if item is None:  # sentinel to stop
                break

            try:
                data        = item.get("data")
                temp_path   = item.get("temp_path")
                remote_name = item["remote_name"]
                parent_id   = item["parent_id"]
                cache_key        = item["cache_key"]
                number      = item["company_number"]
                dtype       = item["dtype"]

                # If we spilled to disk, prefer uploading from file to keep memory low
                if temp_path:
                    try:
                        self._drive_sema.acquire()
                        try:
                            fid = self.drive_client.upload_pdf(temp_path, parent_id, delete_local_after=True)
                        finally:
                            self._drive_sema.release()
                        if fid:
                            self.cache.cache_document(
                                cache_key, number, dtype, f"gdrive://{fid}", True,
                                metadata={"upload_time": 0.0, "drive_parent_id": parent_id, "drive_file_name": remote_name, "batched": False, "spilled": True}
                            )
                            self._uploaded_by_company[number] += 1
                            self._uploaded_total += 1                     # NEW
                            self._emit(kind="upload_done", file=remote_name, company_number=number)

                        else:
                            self.cache.cache_document(cache_key, number, dtype, None, False, error_message="Drive upload failed (spilled)")
                            self._emit(kind="upload_failed", file=remote_name, company_number=number)
                    finally:
                        try:
                            os.unlink(temp_path)
                        except Exception:
                            pass
                    # opportunistic flush of small-batch queue
                    try:
                        self._flush_small_upload_batch(force=False)
                    except Exception as _e:
                        self.log(f"Batch flush tick error: {_e.__class__.__name__}", "WARNING")
                    continue


                # Ensure we own the bytes buffer in this thread
                if isinstance(data, (bytearray, memoryview)):
                    data = bytes(data)

                size_hint = len(data) if isinstance(data, (bytes, bytearray)) else None

                # ---- Small file path → enqueue for batch (no Drive call here) ----
                if size_hint is not None and size_hint <= self._batch_threshold_bytes:
                    media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/pdf", resumable=False)
                    spec  = CreateSpec(name=remote_name, parent_id=parent_id, media=media)
                    meta  = {
                        "cache_key": cache_key,        # NOTE: this field now carries the canonical key
                        "company_number": number,
                        "dtype": dtype,
                        "remote_name": remote_name,
                        "parent_id": parent_id,
                    }
  
                    with self._batch_lock:
                        self._batch_specs.append((spec, meta))
                        self._enqueued_by_company[number] += 1
                        should_flush = (
                            len(self._batch_specs) >= self._batch_max or
                            (time.time() - self._batch_last_flush) >= self._batch_flush_sec
                        )

                    # Mark as pending so a crash doesn’t immediately redownload on this run
                    self.cache.cache_document(
                        cache_key, number, dtype, None, False,
                        metadata={
                            "pending_upload": True,
                            "drive_parent_id": parent_id,
                            "drive_file_name": remote_name,
                            "batched": True,
                        }
                    )
                    self._emit(kind="upload_enqueued", file=remote_name, company_number=number)

                    if should_flush:
                        try:
                            self._flush_small_upload_batch(force=False)
                        except Exception as e:
                            self.log(f"Batch flush error: {e.__class__.__name__}", "ERROR")

                    # done with this item; success/failure will be handled by the batch flush
                    continue

                # ---- Large/unknown size → upload immediately (resumable) ----
                t0 = time.time()
                self._drive_sema.acquire()
                try:
                    fid = self.drive_client.upload_bytes_resumable(
                        data=data,
                        name=remote_name,
                        parent_id=parent_id,
                        mime_type="application/pdf",
                    )
                finally:
                    self._drive_sema.release()

                    # Optional fallback to simple upload if resumable returns None
                    if not fid and size_hint is not None and size_hint <= (self._batch_threshold_bytes * 2):
                        fid = self.drive_client.upload_pdf_stream(data, remote_name, parent_id)

                rt = time.time() - t0
                if fid:
                    self.cache.cache_document(
                        cache_key, number, dtype, f"gdrive://{fid}", True,
                        metadata={
                            "upload_time": rt,
                            "drive_parent_id": parent_id,
                            "drive_file_name": remote_name,
                            "batched": False,
                        }
                    )
                    self._uploaded_by_company[number] += 1
                    self._uploaded_total += 1                     # NEW
                    self._emit(kind="upload_done", file=remote_name, company_number=number)

                else:
                    self.cache.cache_document(
                        cache_key, number, dtype, None, False,
                        error_message="Drive upload failed"
                    )
                    self._emit(kind="upload_failed", file=remote_name, company_number=number)

                # Opportunistic time-based flush for any pending small items
                try:
                    self._flush_small_upload_batch(force=False)
                except Exception as _e:
                    self.log(f"Batch flush tick error: {_e.__class__.__name__}", "WARNING")

            except Exception as e:
                self.log(f"Upload worker error: {e!r}\n{traceback.format_exc()}", "ERROR")
            finally:
                try:
                    self._upload_queue.task_done()
                except Exception:
                    pass

    def _enqueue_upload(self, data: bytes, remote_name: str, parent_id: str, cache_key: str, number: str, dtype: str):
        """
        Queue an upload job; large files spill to disk. If the queue is saturated
        or workers aren't running, fall back to a synchronous upload (best effort)
        so we never spin forever or leak memory.
        """
        self._ensure_upload_workers()
        size = len(data) if isinstance(data, (bytes, bytearray)) else None
        spill_threshold = int(os.getenv("DRIVE_UPLOAD_SPILL_BYTES", "262144"))  # 256 KB default

        job: Dict[str, Any] = {
            "remote_name": remote_name,
            "parent_id": parent_id,
            "cache_key": cache_key,
            "company_number": number,
            "dtype": dtype,
        }

        if size is not None and size > spill_threshold:
            tmpdir = os.getenv("UPLOAD_TMPDIR", None)
            fd, tmp_path = tempfile.mkstemp(prefix="upl_", suffix=".pdf", dir=tmpdir)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            job["temp_path"] = tmp_path
            job["data"] = None
        else:
            # keep truly small payloads in-memory; still bounded by queue size
            job["data"] = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
            job["temp_path"] = None

        # ---- bounded enqueue with deadline ----
        deadline = time.time() + float(os.getenv("DRIVE_ENQUEUE_TIMEOUT_SEC", "10"))
        while True:
            # If workers are not running, bail out to fallback
            if not self._upload_workers_running.is_set() or not getattr(self, "_upload_threads", None):
                self.log("Upload workers not running; invoking synchronous fallback", "WARNING")
                self._upload_fallback(job)
                return

            try:
                self._upload_queue.put(job, timeout=0.5)
                return   # enqueued
            except queue.Full:
                if time.time() >= deadline:
                    self.log("Upload queue saturated; invoking synchronous fallback", "WARNING")
                    self._upload_fallback(job)
                    return
                time.sleep(0.05)

    def _upload_fallback(self, job: Dict[str, Any]) -> None:
        """
        Best-effort, synchronous path used when queue is saturated or workers are down.
        Writes bytes to temp if needed, then uploads directly (respects Drive sema).
        Always records cache state to avoid re-downloading next run.
        """
        if not self._drive_is_ready():
            # No Drive configured: just persist locally if folder_path is available.
            try:
                # No easy local path context here; mark as pending in cache so a later run can retry.
                self.cache.cache_document(
                    job["cache_key"], job["company_number"], job["dtype"], None, False,
                    error_message="No Drive configured; deferred (fallback)"
                )
            except Exception:
                pass
            return

        # Ensure we have a file on disk to upload
        temp_path = job.get("temp_path")
        if not temp_path:
            # spill in-memory data to temp
            data = job.get("data") or b""
            tmpdir = os.getenv("UPLOAD_TMPDIR", None)
            fd, temp_path = tempfile.mkstemp(prefix="upl_fb_", suffix=".pdf", dir=tmpdir)
            with os.fdopen(fd, "wb") as f:
                f.write(data)

        parent_id   = job["parent_id"]
        remote_name = job["remote_name"]
        cache_key   = job["cache_key"]
        number      = job["company_number"]
        dtype       = job["dtype"]

        try:
            self._drive_sema.acquire()
            try:
                fid = self.drive_client.upload_pdf(temp_path, parent_id, delete_local_after=True)
            finally:
                self._drive_sema.release()
            if fid:
                self.cache.cache_document(
                    cache_key, number, dtype, f"gdrive://{fid}", True,
                    metadata={"fallback": True, "drive_parent_id": parent_id, "drive_file_name": remote_name}
                )
                self._uploaded_by_company[number] += 1
                self._uploaded_total += 1                     # NEW
                self._emit(kind="upload_done", file=remote_name, company_number=number)

            else:
                self.cache.cache_document(cache_key, number, dtype, None, False, error_message="Drive upload failed (fallback)")
                self._emit(kind="upload_failed", file=remote_name, company_number=number)
        except Exception as e:
            self.cache.cache_document(cache_key, number, dtype, None, False, error_message=f"Fallback error: {e}")
            self._emit(kind="upload_failed", file=remote_name, company_number=number)
        finally:
            try:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
            except Exception:
                pass
    
    def _resolve_procedure_folder_name(self, notice_type: str) -> str:
        # Classify from the notice text
        folder = self._classify_direct(notice_type)
        # Normalize via aliases (e.g., “cvl” → “Creditors’ Voluntary Winding Up”)
        folder = ALIASES.get(folder, folder)
        # Only allow folders defined in PROCEDURES
        valid_folders = {p.folder for p in PROCEDURES}
        return folder if folder in valid_folders else "Other"
    
    def download_company_by_number(
        self,
        company_number: str,
        max_documents: int | str | None = 0,
        research_root: str = "Research",
        cancel_event=None,
    ) -> Dict:
        """
        Skip Gazette. Pull filing history & docs by company number only.
        Save under Research/<CompanyName>_<Number>/ (Drive or local).
        """
        t0 = time.time()
    
        provider = self._ensure_provider_ready()
        api_fast_path = isinstance(provider, CHApiProvider) and self._should_use_api_fast_path()
        provider_type = type(provider).__name__ if provider else "None"
        is_api = isinstance(provider, CHApiProvider)
        self.log(f"[DIAGNOSTIC] download_company_by_number called for {company_number}", "INFO")
        self.log(f"[DIAGNOSTIC] Provider type: {provider_type}, is_api={is_api}", "INFO")

        result = {
            "success": False,
            "company_number": company_number,
            "company_name": "",
            "procedure_folder": research_root,
            "folder_path": "",
            "documents_found": 0,
            "documents_downloaded": 0,
            "documents_from_cache": 0,
            "cache_hits": [],
            "errors": [],
            "processing_time": 0.0,
        }

        try:
            # 1) Get name + filing history
            # before: name = self.ch_downloader.get_company_name(company_number)
            # ---- API fast-path (if provider is CHApiProvider) ----
            if api_fast_path:
                self.log(f"Using API fast-path for {company_number}", "INFO")
                try:
                    self._assert_api_window()
                    prof = provider.get_company_profile_sync(company_number)
                    result["company_name"] = (prof or {}).get("company_name", company_number)
                except ProviderRateLimited as exc:
                    raise RateLimitFallback(str(exc)) from exc
                except RateLimitFallback:
                    self.log(f"[API-PATH] Rate limited before profile lookup for {company_number}", "WARNING")
                    api_fast_path = False
                except Exception as e:
                    self.log(f"Failed to get company profile via API: {e}", "WARNING")
                    result["company_name"] = result["company_name"] or company_number

            if not api_fast_path:
                if not result["company_name"]:
                    result["company_name"] = self.ch_downloader.get_company_name(company_number)

            # ---- API fast-path (if provider is CHApiProvider) ----
            if api_fast_path:
                try:
                    # 0) Ensure Drive/local target
                    use_drive = self._drive_is_ready()
                    drive_streaming = use_drive and bool(getattr(self, "drive_only", False))

                    number = company_number
                    self._assert_api_window()
                    try:
                        filings_raw = provider.get_all_filings_sync(number) if isinstance(provider, CHApiProvider) else []
                    except ProviderRateLimited as exc:
                        raise RateLimitFallback(str(exc)) from exc
                    if isinstance(provider, CHApiProvider):
                        try:
                            prof = provider.get_company_profile_sync(number) or {}
                        except ProviderRateLimited as exc:
                            raise RateLimitFallback(str(exc)) from exc
                        result["company_name"] = prof.get("company_name") or result["company_name"] or number

                        if (not filings_raw) and hasattr(self.ch_downloader, "get_company_filing_history"):
                            filings_raw = self.ch_downloader.get_company_filing_history(number, max_pages=FH_MAX_PAGES)

                        items = []
                        for f in filings_raw:
                            link = (f.get("links", {}) or {}).get("document_metadata", "")
                            m = re.search(r"/document/([A-Za-z0-9_\-]+)", link)
                            doc_id = m.group(1) if m else None
                            date = f.get("date", "0000-00-00")
                            ftype = f.get("type", "DOC")
                            desc = f.get("description", "") or ""
                            safe = _sanitize_desc_for_filename(desc, max_len=160)
                            fn = f"{date}_{ftype}_{safe}.pdf"
                            if len(fn) > 200:
                                room = 200 - len(date) - len(ftype) - 6
                                safe = _sanitize_desc_for_filename(desc, max_len=max(1, room))
                                fn = f"{date}_{ftype}_{safe}.pdf"
                            items.append((doc_id, date, ftype, desc, fn))

                    # Destination
                    safe_company = self.sanitize_folder_name(result["company_name"]) + f"_{number}"
                    if use_drive:
                        parent_id = self._ensure_remote_company_folder(research_root, safe_company)
                        result["folder_path"] = f"gdrive://{parent_id}"
                    else:
                        base = self.base_folder / research_root / safe_company
                        base.mkdir(parents=True, exist_ok=True)
                        result["folder_path"] = str(base)

                    # Sort newest first (by date string)
                    items.sort(key=lambda x: x[1], reverse=True)

                    # --- Deduplicate API items by (doc_id or filename) ---
                    _seen = set()
                    _filtered = []
                    for tup in items:
                        doc_id = tup[0]
                        key = f"chdoc://{doc_id}" if doc_id else tup[4]  # fall back to filename
                        if key in _seen:
                            continue
                        _seen.add(key)
                        _filtered.append(tup)
                    items = _filtered

                    # Decide max documents
                    def _wants_all(max_documents_param) -> bool:
                        return max_documents_param in (None, 0, -1, "ALL", "all", "All") \
                            or bool(getattr(self, "download_all", False))
                    to_get = items if _wants_all(max_documents) else items[:int(max_documents)]
                    result["documents_found"] = len(items)

                    # Download + enqueue
                    # Concurrent API fetching (similar to HTML path)
                    # ===== BATCH FETCH VERSION (10x faster!) =====
                    downloaded, from_cache, errors = 0, 0, 0

                    # 1) Build FilingItems and check cache
                    upload_batch: List[Tuple[bytes, str, str, str, Optional[str]]] = []
                    items_to_fetch = []
                    item_lookup = {}  # Map filing_identity -> (filename, dtype, date_iso, doc_id, cache_key)

                    for (doc_id, date_iso, dtype, desc, filename) in to_get:
                        cache_key = f"chdoc://{doc_id}" if doc_id else f"chdoc://{number}/{date_iso}/{dtype}"
                        
                        # Check cache first
                        ok, cached_path = self.cache.is_document_cached(cache_key, number)
                        if ok and cached_path and not self.cache.config.force_redownload_documents:
                            from_cache += 1
                            self._emit_doc_progress(number, cached=1)
                            continue
                        
                        # Need to fetch this one
                        if doc_id:
                            reuse_bytes = self._document_reuse_get(doc_id)
                            if reuse_bytes:
                                upload_batch.append((reuse_bytes, filename, cache_key, dtype, doc_id))
                                from_cache += 1
                                self._emit_doc_progress(number, cached=1)
                                continue
                            item = FilingItem(
                                company_number=number,
                                company_name=result["company_name"],
                                date=date_iso,
                                type=dtype,
                                category="",
                                description=desc,
                                document_id=doc_id,
                                filename=filename
                            )
                            items_to_fetch.append(item)
                            item_key = filing_identity(item)
                            item_lookup[item_key] = (filename, dtype, date_iso, doc_id, cache_key)

                    # upload_batch populated above (reuse + freshly fetched)

                    def _flush_upload_batch():
                        nonlocal downloaded, upload_batch
                        if not upload_batch:
                            return
                        count_emitted = 0
                        if use_drive and drive_streaming:
                            parent_id = result["folder_path"].split("gdrive://", 1)[-1]
                            t_upload = time.time()
                            files_to_upload = [(data, fname) for data, fname, *_rest in upload_batch]
                            uploaded_count, success_idx = self._batch_upload_to_drive(files_to_upload, parent_id, number)
                            count_emitted = uploaded_count
                            success_set = set(success_idx)
                            for i, (data, fname, cache_key, dtype, doc_id_val) in enumerate(upload_batch):
                                if i in success_set:
                                    self.cache.cache_document(
                                        cache_key,
                                        number,
                                        dtype,
                                        "gdrive://uploaded",
                                        True,
                                        metadata={"upload_time": 0.0, "drive_file_name": fname},
                                    )
                                    downloaded += 1
                                    self._emit_doc_progress(number, fresh=1)
                                    if doc_id_val:
                                        self._document_reuse_store(doc_id_val, data)
                        else:
                            base_path = self.base_folder / research_root / safe_company
                            for data, fname, cache_key, dtype, doc_id_val in upload_batch:
                                path = base_path / fname
                                path.write_bytes(data)
                                self.cache.cache_document(
                                    cache_key,
                                    number,
                                    dtype,
                                    str(path),
                                    True,
                                    metadata={"download_time": 0.0},
                                )
                                downloaded += 1
                                self._emit_doc_progress(number, fresh=1)
                                if doc_id_val:
                                    self._document_reuse_store(doc_id_val, data)
                            count_emitted = len(upload_batch)
                        if count_emitted:
                            self._emit(
                                kind="documents_uploaded",
                                company_number=number,
                                count=count_emitted,
                                mode="drive" if use_drive and drive_streaming else "local",
                            )
                        upload_batch.clear()

                    if items_to_fetch and isinstance(provider, CHApiProvider):
                        self._check_api_health()
                        chunk_size = max(3, min(12, self._doc_workers_per_company * 2))
                        for chunk_start in range(0, len(items_to_fetch), chunk_size):
                            chunk = items_to_fetch[chunk_start : chunk_start + chunk_size]
                            try:
                                self._assert_api_window()
                                pdf_dict = provider.fetch_pdfs_batch_sync(chunk, max_concurrent=self._doc_workers_per_company)
                            except ProviderRateLimited as exc:
                                raise RateLimitFallback(str(exc)) from exc
                            except Exception as e:
                                self.log(f"Batch fetch error in Research: {e}", "ERROR")
                                pdf_dict = {}
                            for item in chunk:
                                item_key = filing_identity(item)
                                meta = item_lookup.get(item_key)
                                if not meta:
                                    continue
                                filename, dtype, date_iso, doc_id, doc_cache_key = meta
                                pdf_bytes = pdf_dict.get(item_key)
                                if not pdf_bytes or b"%PDF" not in pdf_bytes[:2048]:
                                    self.cache.cache_document(doc_cache_key, number, dtype, None, False, error_message="Invalid PDF")
                                    errors += 1
                                    continue
                                upload_batch.append((pdf_bytes, filename, doc_cache_key, dtype, doc_id))
                            errors += max(0, len(chunk) - len(pdf_dict))
                            if len(upload_batch) >= 8:
                                _flush_upload_batch()
                    elif items_to_fetch:
                        errors += len(items_to_fetch)

                    _flush_upload_batch()

                    result["documents_downloaded"] = downloaded
                    result["documents_from_cache"] = from_cache
                    result["documents_found"] = downloaded + from_cache if downloaded or from_cache else len(items)
                    if errors:
                        result["errors"].append(f"{errors} document(s) failed")
                    result["success"] = True
                    result["processing_time"] = time.time() - t0

                    doc_links = [
                        {
                            "document_id": doc_id,
                            "date": date_iso,
                            "type": dtype,
                            "description": desc,
                            "url": f"/document/{doc_id}" if doc_id else "",
                            "filename": filename,
                        }
                        for (doc_id, date_iso, dtype, desc, filename) in items
                    ]
                    try:
                        self.cache.cache_company(
                            number,
                            result["company_name"],
                            result.get("procedure_folder", research_root) or research_root,
                            None,
                            doc_links,
                            success=True,
                            metadata={
                                "source": "api_fast_path_research",
                                "documents_found": len(items),
                                "documents_downloaded": downloaded,
                            },
                        )
                    except Exception as cache_err:
                        self.log(f"[API-PATH] cache save failed for {number}: {cache_err}", "WARNING")
                    
                    self.log(f"[API-PATH] ✓ Success! Downloaded {downloaded} files for {company_number}", "INFO")
                    return result
                    
                except RateLimitFallback as exc:
                    api_fallback_reason = str(exc) or "rate limited"
                    api_fast_path = False
                    result["errors"].append(f"API fast-path throttled: {api_fallback_reason}")
                    self.log(f"[API-PATH] Rate limited mid-flight for {company_number}; falling back to HTML", "WARNING")
                except Exception as e:
                    self.log(f"[API-PATH] ✗ Exception caught: {e}", "ERROR")
                    import traceback
                    self.log(f"[API-PATH] Traceback:\n{traceback.format_exc()}", "ERROR")
                    result["errors"].append(f"API path failed: {e}")
                    result["success"] = False
                    result["processing_time"] = time.time() - t0
                    return result

            else:
                if isinstance(provider, CHApiProvider):
                    self.log(f"[API] throttled — using HTML scraping for {company_number}", "WARNING")
                else:
                    self.log(f"⚠ Using HTML scraping fallback for {company_number} (API provider: {type(provider).__name__ if provider else 'None'})", "WARNING")
            if api_fallback_reason:
                self.log(
                    f"[HTML-PATH] Retrying {company_number} because API fast-path was {api_fallback_reason}",
                    "WARNING",
                )
            soup = self.circuit_breaker.call(self._get_filing_history_with_retry, company_number)
            if not soup:
                self.log(f"No filing history for {company_number}", "WARNING")
                result["success"] = True
                result["processing_time"] = time.time() - t0
                return result

            links = self.ch_downloader.find_document_links(soup, company_number, max_pages=FH_MAX_PAGES)
            if not links:
                result["success"] = True
                result["processing_time"] = time.time() - t0
                return result

            result["documents_found"] = len(links)
            if self._should_cancel(cancel_event):
                result["success"] = True
                result["processing_time"] = time.time() - t0
                result["errors"].append("Cancelled before downloads started")
                return result


            # 2) Decide target (Drive vs local) under Research/<CompanyName_Number>
            use_drive = self._drive_is_ready()
            drive_streaming = use_drive and bool(getattr(self, "drive_only", False))
            # you already set result["company_name"] earlier
            safe_company = self.sanitize_folder_name(result["company_name"]) + f"_{company_number}"

            if use_drive:
                parent_id = self._ensure_remote_company_folder(research_root, safe_company)
                result["folder_path"] = f"gdrive://{parent_id}"
            else:
                base = self.base_folder / research_root / safe_company
                base.mkdir(parents=True, exist_ok=True)
                result["folder_path"] = str(base)

            # 3) Download loop (reusing the same logic as the main method)
            docs_sorted = self._ensure_consistent_sorting(links, newest_first=True)

            def _wants_all(max_documents_param) -> bool:
                return max_documents_param in (None, 0, -1, "ALL", "all", "All") \
                    or bool(getattr(self, "download_all", False))

            to_get = docs_sorted if _wants_all(max_documents) else docs_sorted[:int(max_documents)]
            # --- Deduplicate by canonical cache key ---
            _seen = set()
            _filtered = []
            for _d in to_get:
                _ck = _doc_cache_key(_d.get("url", ""), _d)
                if _ck in _seen:
                    continue
                _seen.add(_ck)
                _filtered.append(_d)
            to_get = _filtered

            # ===== BATCH DOWNLOAD + UPLOAD (HTML path) =====
            downloaded = 0
            upload_batch: List[Tuple[bytes, str, str, str, Optional[str]]] = []
            download_jobs: List[Dict[str, Any]] = []

            for d in to_get:
                if self._should_cancel(cancel_event):
                    break
                url = d.get("url", "")
                dtype = d.get("type", "UNKNOWN")
                cache_key = _doc_cache_key(url, d)
                doc_id = _extract_doc_id_from_url(url)
                ok, cached_path = self.cache.is_document_cached(cache_key, company_number)
                if ok and self._is_valid_cached_artifact(cached_path):
                    result["documents_from_cache"] += 1
                    if result["documents_from_cache"] <= 3:
                        self.log(f"  From cache: {cached_path}")
                    continue

                try:
                    date_iso = self._parse_doc_date(d.get("date", "")).strftime("%Y-%m-%d")
                    if date_iso == "0001-01-01":
                        date_iso = "0000-00-00"
                except Exception:
                    date_iso = "0000-00-00"

                desc = d.get("description") or ""
                safe_desc = self.sanitize_folder_name(desc)[:80] or "Document"
                for prefix in ["Strong_", "strong_", "STRONG_"]:
                    if safe_desc.startswith(prefix):
                        safe_desc = safe_desc[len(prefix):]
                safe_desc = re.sub(r"<[^>]+>", "", safe_desc)
                safe_desc = re.sub(r"\s+", "_", safe_desc.strip()).strip("_")
                filename = f"{date_iso}_{dtype}_{safe_desc}.pdf"
                if len(filename) > 200:
                    max_desc_len = 200 - len(date_iso) - len(dtype) - 6
                    safe_desc = safe_desc[:max_desc_len].rstrip("_")
                    filename = f"{date_iso}_{dtype}_{safe_desc}.pdf"
                if doc_id:
                    reuse_blob = self._document_reuse_get(doc_id)
                    if reuse_blob:
                        upload_batch.append((reuse_blob, filename, cache_key, dtype, doc_id))
                        result["documents_from_cache"] += 1
                        continue

                download_jobs.append(
                    {
                        "doc": d,
                        "url": url,
                        "dtype": dtype,
                        "cache_key": cache_key,
                        "filename": filename,
                        "doc_id": doc_id,
                    }
                )

            cancel_requested = self._should_cancel(cancel_event)

            if drive_streaming and download_jobs and not cancel_requested:
                max_workers = max(1, min(self._doc_workers_per_company, len(download_jobs)))

                def _fetch_bytes(job: Dict[str, Any]):
                    if self._should_cancel(cancel_event):
                        return ("cancelled", job, None)
                    start = time.time()
                    data = self._download_bytes(job["url"], cancel_event=cancel_event)
                    if self._should_cancel(cancel_event):
                        return ("cancelled", job, None)
                    if not data:
                        self.rate_limiter.record_error("download_failed")
                        self.cache.cache_document(job["cache_key"], company_number, job["dtype"], None, False, error_message="PDF fetch failed")
                        return ("failed", job, None)
                    self.rate_limiter.record_success(time.time() - start)
                    return ("ok", job, data)

                with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"html-dl-{company_number}") as pool:
                    futures = {pool.submit(_fetch_bytes, job): job for job in download_jobs}
                    for fut in as_completed(futures):
                        status, job, payload = fut.result()
                        if status == "cancelled":
                            cancel_requested = True
                            break
                        if status == "failed":
                            result["errors"].append(f"Download failed: {job['filename']}")
                            continue
                        upload_batch.append((payload, job["filename"], job["cache_key"], job["dtype"], job.get("doc_id")))
                        if len(upload_batch) <= 3:
                            self.log(f"  Downloaded: {job['filename']}")

            elif download_jobs and not cancel_requested:
                base_folder_path = Path(result["folder_path"])
                max_workers = max(1, min(self._doc_workers_per_company, len(download_jobs)))

                def _fetch_local(job: Dict[str, Any]):
                    if self._should_cancel(cancel_event):
                        return ("cancelled", job, None)
                    target = base_folder_path / job["filename"]
                    start = time.time()
                    ok = self._download_single_document_robust(job["url"], target)
                    if self._should_cancel(cancel_event):
                        return ("cancelled", job, None)
                    if not ok:
                        self.rate_limiter.record_error("download_failed")
                        self.cache.cache_document(job["cache_key"], company_number, job["dtype"], None, False, error_message="Download failed")
                        return ("failed", job, None)
                    elapsed = time.time() - start
                    self.rate_limiter.record_success(elapsed)
                    self.cache.cache_document(job["cache_key"], company_number, job["dtype"], str(target), True, metadata={"download_time": elapsed})
                    if use_drive and not drive_streaming:
                        self._upload_if_configured(str(target), research_root, safe_company)
                    self._emit_doc_progress(company_number, fresh=1)
                    return ("ok", job, str(target))

                with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"html-dl-{company_number}") as pool:
                    futures = {pool.submit(_fetch_local, job): job for job in download_jobs}
                    for fut in as_completed(futures):
                        status, job, _payload = fut.result()
                        if status == "cancelled":
                            cancel_requested = True
                            break
                        if status == "failed":
                            result["errors"].append(f"Download failed: {job['filename']}")
                            continue
                        downloaded += 1
                        if downloaded <= 3:
                            self.log(f"  Downloaded locally: {job['filename']}")

            if cancel_requested:
                upload_batch.clear()
                download_jobs.clear()

            # ===== BATCH UPLOAD ALL FILES AT ONCE! =====
            if upload_batch and use_drive and drive_streaming:
                parent_id = result["folder_path"].split("gdrive://", 1)[-1]
                t_upload = time.time()
                
                self.log(f"Starting batch upload of {len(upload_batch)} files...")
                
                # Direct parallel upload (no queueing!)
                files_to_upload = [(data, fname) for data, fname, *_rest in upload_batch]
                uploaded_count, success_idx = self._batch_upload_to_drive(files_to_upload, parent_id, company_number)
                
                upload_time = time.time() - t_upload
                self.log(f"✓ Batch uploaded {uploaded_count}/{len(upload_batch)} files in {upload_time:.1f}s ({uploaded_count/upload_time*60:.0f} files/min)")
                
                # Update cache for all uploaded files
                success_set = set(success_idx)
                for i, (data, fname, cache_key, dtype, doc_id_val) in enumerate(upload_batch):
                    if i in success_set:
                        self.cache.cache_document(cache_key, company_number, dtype, f"gdrive://uploaded", True,
                            metadata={"upload_time": 0.0, "drive_file_name": fname})
                        downloaded += 1
                        self._emit_doc_progress(company_number, fresh=1)
                        if doc_id_val:
                            self._document_reuse_store(doc_id_val, data)
                if uploaded_count:
                    self._emit(kind="documents_uploaded", company_number=company_number, count=uploaded_count, mode="drive")
            result["documents_downloaded"] = downloaded
            if downloaded == 0 and result["documents_from_cache"] == 0:
                result["success"] = False
                fallback_msg = api_fallback_reason or "HTML fallback produced no documents"
                result["errors"].append(fallback_msg)
                self.log(
                    f"[HTML-PATH] ✗ No documents downloaded for {company_number} ({fallback_msg})",
                    "WARNING",
                )
            else:
                result["success"] = True
                self.log(
                    f"[HTML-PATH] ✓ Downloaded {downloaded} files for {company_number}",
                    "INFO",
                )
        except Exception as e:
            result["errors"].append(str(e))
            self.log(f"download_company_by_number failed: {e}", "ERROR")
        finally:
            result["processing_time"] = time.time() - t0

        return result
    
    # --- in wc_insolvency_cached.py ---

    def _download_bytes(self, url: str, cancel_event=None) -> Optional[bytes]:
        """Fetch PDF bytes via synchronous request."""
        sess = tls_session(self.ch_downloader.session.headers, getattr(self.ch_downloader.session, "cookies", None))
        try:
            self.http_tokens.acquire()
            r = sess.get(url, stream=True, timeout=60)
            if r.status_code != 200:
                self.log(f"HTTP {r.status_code} for {url}", "ERROR")
                return None
            chunks = []
            for chunk in r.iter_content(8192):
                if cancel_event and cancel_event.is_set():
                    return None
                if chunk:
                    chunks.append(chunk)
            data = b"".join(chunks)
            if self._looks_like_pdf(data):
                return data

            # One-shot retry forcing PDF rendition
            alt = url if "format=pdf" in url else (url + ("&" if "?" in url else "?") + "format=pdf")
            self.http_tokens.acquire()
            r2 = sess.get(alt, stream=True, timeout=60)
            if r2.status_code != 200:
                self.log(f"HTTP {r2.status_code} for retry {alt}", "ERROR")
                return None
            chunks2 = []
            for chunk in r2.iter_content(8192):
                if cancel_event and cancel_event.is_set():
                    return None
                if chunk:
                    chunks2.append(chunk)
            data2 = b"".join(chunks2)
            return data2 if self._looks_like_pdf(data2) else None

        except Exception as e:
            self.log(f"_download_bytes error: {e}", "ERROR")
            return None
        finally:
            try:
                sess.close()
            except Exception:
                pass

    def _stream_to_drive(self, url: str, remote_name: str, parent_folder_id: str, cancel_event=None) -> Optional[str]:
        sess = tls_session(self.ch_downloader.session.headers, getattr(self.ch_downloader.session, "cookies", None))
        try:
            self.http_tokens.acquire()
            r = sess.get(url, stream=True, timeout=60)
            if r.status_code != 200:
                self.log(f"HTTP {r.status_code} for {url}", "ERROR")
                return None
            chunks = []
            for chunk in r.iter_content(8192):
                if cancel_event and cancel_event.is_set():
                    self.log(f"Cancelled while downloading (Drive): {remote_name}", "INFO")
                    return None
                if chunk:
                    chunks.append(chunk)
            data = b"".join(chunks)

            # Accept tolerant PDF header; else one-shot retry with format=pdf
            if not self._looks_like_pdf(data):
                alt = url if "format=pdf" in url else (url + ("&" if "?" in url else "?") + "format=pdf")
                self.http_tokens.acquire()
                r2 = sess.get(alt, stream=True, timeout=60)
                if r2.status_code != 200:
                    self.log(f"HTTP {r2.status_code} for retry {alt}", "ERROR")
                    return None
                chunks2 = []
                for chunk in r2.iter_content(8192):
                    if cancel_event and cancel_event.is_set():
                        self.log(f"Cancelled while downloading (Drive, retry): {remote_name}", "INFO")
                        return None
                    if chunk:
                        chunks2.append(chunk)
                data = b"".join(chunks2)
                if not self._looks_like_pdf(data):
                    self.log(f"Not a valid PDF after retry: {remote_name}", "ERROR")
                    return None

            self._drive_sema.acquire()
            try:
                return self.drive_client.upload_pdf_stream(data, remote_name, parent_folder_id)
            finally:
                self._drive_sema.release()

        except Exception as e:
            self.log(f"_stream_to_drive error for {remote_name}: {e}", "ERROR")
            return None
        finally:
            try:
                sess.close()
            except Exception:
                pass

    def _ensure_remote_company_folder(self, procedure_folder: str, company_folder_name: str) -> str | None:
        if not self._drive_is_ready():
            return None
        key = (procedure_folder, company_folder_name)
        # fast path (no lock)
        fid = self._folder_cache.get(key)
        if fid:
            return fid
        # single-writer section
        with self._drive_folder_lock:
            fid = self._folder_cache.get(key)
            if fid:
                return fid
            fid = self.drive_client.ensure_path(self.drive_root_id, [procedure_folder, company_folder_name])
            if fid:
                self._folder_cache[key] = fid
            return fid

    def _upload_if_configured(self, local_path: str, procedure_folder: str, company_folder_name: str):
        if not self._drive_is_ready():
            return
        parent_id = self._ensure_remote_company_folder(procedure_folder, company_folder_name)
        if not parent_id:
            return
        with self._drive_sema:
            fid = self._upload_path_with_name(local_path, parent_id, os.path.basename(local_path))
        self.log(f"  Uploaded to Drive: {os.path.basename(local_path)} -> fileId={fid}")

    def _parse_doc_date(self, d: str):
        """
        Parse Companies House date strings into datetime objects.
        Returns datetime.max for unparseable dates so they sort to the end (oldest).
        """
        if not d or not d.strip():
            return _dt.min  # Empty dates go to end (oldest)
        
        d = d.strip()
        
        # Companies House common formats (most common first for performance)
        formats = [
            "%d %b %Y",        # "09 Sep 2025" - Most common CH format
            "%d/%m/%Y",        # "09/09/2025" - UK format  
            "%d %B %Y",        # "09 September 2025" - Full month name
            "%Y-%m-%d",        # "2025-09-09" - ISO format
            "%d-%m-%Y",        # "09-09-2025" - Alternative UK format
            "%d.%m.%Y",        # "09.09.2025" - European format
            "%d-%b-%Y",        # "09-Sep-2025" - Alternative format
            "%b %d, %Y",       # "Sep 09, 2025" - US format (rare but possible)
        ]
        
        for fmt in formats:
            try:
                return _dt.strptime(d, fmt)
            except ValueError:
                continue
        
        # Try to extract date parts with regex for malformed dates
        
        # Pattern for "DD MMM YYYY" with flexible spacing
        match = re.search(r'(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})', d)
        if match:
            day, month_str, year = match.groups()
            try:
                # Map month abbreviations
                month_map = {
                    'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
                    'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6,
                    'jul': 7, 'july': 7, 'aug': 8, 'august': 8, 'sep': 9, 'september': 9,
                    'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12
                }
                month_num = month_map.get(month_str.lower())
                if month_num:
                    return _dt(int(year), month_num, int(day))
            except (ValueError, TypeError):
                pass
        
        # Pattern for "DD/MM/YYYY" or "DD-MM-YYYY"
        match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', d)
        if match:
            day, month, year = match.groups()
            try:
                return _dt(int(year), int(month), int(day))
            except ValueError:
                pass
        
        if self.verbose:
            print(f"DEBUG: Could not parse date '{d}' - treating as oldest")
        return _dt.min  # Unparseable dates sort to end (oldest)
    
    def _ensure_consistent_sorting(self, doc_links: List[Dict], newest_first: bool = True) -> List[Dict]:
        """
        Sort documents by date with consistent behavior.
        
        Args:
            doc_links: List of document dictionaries with 'date' field
            newest_first: If True, newest documents first; if False, oldest first
        
        Returns:
            Sorted list of documents
        """
        def sort_key(doc):
            parsed_date = self._parse_doc_date(doc.get("date", ""))
            # For consistent secondary sorting, use URL or description
            secondary = doc.get("url", "") or doc.get("description", "")
            return (parsed_date, secondary)
        
        return sorted(doc_links, key=sort_key, reverse=newest_first)

    
    def process_company_with_intelligent_caching(self, company_data: Dict, max_documents: int | str | None = 0, cancel_event=None) -> Dict:
        name = company_data.get("company_name", "Unknown")
        number = company_data.get("company_number", "")
        notice_type = company_data.get("notice_type", "Unknown")

        # DIAGNOSTIC: Verify provider
        provider_type = type(self.provider).__name__ if self.provider else "None"
        is_api = isinstance(self.provider, CHApiProvider) if self.provider else False
        self.log(f"[GAZETTE-DIAG] Company {number}: Provider={provider_type}, is_api={is_api}", "INFO")

        result = {
            "success": False,
            "skipped_classification": False,
            "skipped_checkpoint": False,
            "used_cache": False,
            "company_name": name,
            "company_number": number,
            "notice_type": notice_type,
            "procedure_folder": "",
            "folder_path": "",
            "documents_found": 0,
            "documents_downloaded": 0,
            "documents_from_cache": 0,
            "cache_hits": [],
            "errors": [],
            "processing_time": 0.0,
        }
        t0 = time.time()
        if self._should_cancel(cancel_event):
            result["success"] = True
            result["errors"].append("Cancelled before start")
            result["processing_time"] = time.time() - t0
            return result

        try:
            self.log(f"Processing (cache-aware): {name}")

            # ---------- Company-level cache ----------
            is_cached, cached = self.cache.is_company_cached(number)
            if is_cached and cached and cached.get("success"):
                result["used_cache"] = True
                result["success"] = True
                result["documents_found"] = len(cached.get("document_links", []))
                result["cache_hits"].append("company_data")
                self.log(
                    f"[CACHE] Reusing cached snapshot for {number}: {result['documents_found']} filings",
                    "INFO"
                )

                # Populate a destination path for UI (Drive if attached else local)
                use_drive = self._drive_is_ready()
                if use_drive:
                    procedure_folder_name = self._resolve_procedure_folder_name(notice_type)
                    # After you compute procedure_folder_name (either via _resolve_procedure_folder_name or classification):
                    if getattr(self, "_forced_folder", None):
                        procedure_folder_name = self._forced_folder

                    if procedure_folder_name:
                        company_folder_name = self.sanitize_folder_name(name) + (f"_{number}" if number else "")
                        drive_parent_id = self._ensure_remote_company_folder(procedure_folder_name, company_folder_name)
                        result["procedure_folder"] = procedure_folder_name
                        result["folder_path"] = f"gdrive://{drive_parent_id}"
                else:
                    folder = self.create_folder_structure_optimized(cached.get("notice_type", notice_type), name, number)
                    if folder:
                        result["procedure_folder"] = folder.parent.name
                        result["folder_path"] = str(folder)

                # Count doc cache hits (Drive gdrive:// paths are treated as cached too)
                docs = cached.get("document_links", [])[:max_documents]
                available = 0
                for d in docs:
                    ck = _doc_cache_key(d.get("url", ""), d)
                    ok, path = self.cache.is_document_cached(ck, number)
                    if not ok:
                        # legacy: try the raw URL as a fallback key
                        raw = d.get("url", "")
                        ok, path = self.cache.is_document_cached(raw, number)
                    if ok:
                        available += 1
                        result["cache_hits"].append(f"document_{d.get('type','UNKNOWN')}")
                result["documents_from_cache"] = available

                if available == 0:
                    fallback_cached = 0
                    if metadata.get("api_path") or not docs:
                        fallback_cached = self.cache.count_cached_documents(number)
                    if fallback_cached > 0:
                        available = fallback_cached
                        result["documents_from_cache"] = fallback_cached
                        result["documents_found"] = fallback_cached

                if available > 0:
                    self._emit_doc_progress(number, cached=available)
                    result["documents_downloaded"] = available
                    result["processing_time"] = time.time() - t0
                    result = self._validate_and_fix_document_stats(result)
                    return result
                else:
                    self.log(f"⚠ Cache entry for {number} had no usable documents; reprocessing fresh", "WARNING")
    
            # ---------- API FAST-PATH (applies to Gazette/Research/CVA/Moratorium) ----------
            provider = self._ensure_provider_ready()
            api_fast_path = isinstance(provider, CHApiProvider) and self._should_use_api_fast_path()
            api_fallback_reason = ""
            if api_fast_path:
                self.log(f"[GAZETTE-API] ✓ Using API fast-path for {number}", "INFO")
                try:
                    self._assert_api_window()
                    use_drive       = self._drive_is_ready()
                    drive_streaming = use_drive and bool(getattr(self, "drive_only", False))

                    # Resolve destination (Drive folder or local folder) using your existing naming rules
                    procedure_folder_name = self._resolve_procedure_folder_name(notice_type)
                    if getattr(self, "_forced_folder", None):
                        procedure_folder_name = self._forced_folder

                    if use_drive:
                        company_folder_name = self.sanitize_folder_name(name) + (f"_{number}" if number else "")
                        parent_id = self._ensure_remote_company_folder(procedure_folder_name, company_folder_name)
                        result["procedure_folder"] = procedure_folder_name
                        result["folder_path"] = f"gdrive://{parent_id}"
                    else:
                        folder = self.create_folder_structure_optimized(notice_type, name, number)
                        if folder is None:
                            result["skipped_classification"] = True
                            self.cache.cache_company(number, name, notice_type, None, [], success=False, error_message="Skipped classification")
                            result["processing_time"] = time.time() - t0
                            return result
                        result["procedure_folder"] = folder.parent.name
                        result["folder_path"] = str(folder)

                    self._assert_api_window()
                    try:
                        profile = provider.get_company_profile_sync(number) if isinstance(provider, CHApiProvider) else {}
                    except ProviderRateLimited as exc:
                        raise RateLimitFallback(str(exc)) from exc
                    if profile.get("company_name"):
                        result["company_name"] = profile["company_name"]

                    self._assert_api_window()
                    try:
                        filings = provider.get_all_filings_sync(number) if isinstance(provider, CHApiProvider) else []
                    except ProviderRateLimited as exc:
                        raise RateLimitFallback(str(exc)) from exc

                    # Extract doc_id and compose a nice filename for each filing
                    items = []
                    for f in filings:
                        # doc id from links.document_metadata
                        link = (f.get("links", {}) or {}).get("document_metadata", "") or ""
                        m = re.search(r"/document/([A-Za-z0-9_\-]+)", link)
                        doc_id = m.group(1) if m else None

                        # filename: date_type_cleaned-description.pdf  (intuitive, ID-free)
                        date  = f.get("date", "0000-00-00")
                        ftype = f.get("type", "DOC")
                        desc  = (f.get("description") or "").strip()

                        # clean description
                        desc_txt = re.sub(r"<[^>]+>", "", desc)
                        desc_txt = re.sub(r"[<>:\"/\\|?*]", "_", desc_txt)
                        desc_txt = re.sub(r"\s+", "_", desc_txt).strip("_") or "Document"

                        filename = f"{date}_{ftype}_{desc_txt}.pdf"
                        if len(filename) > 200:
                            # keep under common Google Drive safe limits
                            max_desc = 200 - len(date) - len(ftype) - 6
                            desc_txt = desc_txt[:max(0, max_desc)].rstrip("_") or "Document"
                            filename = f"{date}_{ftype}_{desc_txt}.pdf"

                        items.append((doc_id, date, ftype, desc, filename))

                    # Sort newest first (by date string)
                    items.sort(key=lambda x: x[1], reverse=True)

                    # --- Deduplicate API items by document_id (stable cache key) ---
                    _seen = set()
                    _filtered = []
                    for tup in items:
                        doc_id = tup[0]
                        # Use doc_id as canonical identifier (same as cache key)
                        key = doc_id if doc_id else tup[4]  # fallback to filename if no ID
                        if key in _seen:
                            continue
                        _seen.add(key)
                        _filtered.append(tup)
                    items = _filtered

                    # Decide max documents
                    def _wants_all(max_documents_param) -> bool:
                        return max_documents_param in (None, 0, -1, "ALL", "all", "All") \
                            or bool(getattr(self, "download_all", False))
                    to_get = items if _wants_all(max_documents) else items[:int(max_documents)]
                    result["documents_found"] = len(items)

                    # ===== API BATCH FETCH (Gazette primary path) =====
                    downloaded = 0
                    from_cache = 0
                    errors = 0

                    # Track processed doc_ids (prevent double-counting)
                    processed_doc_ids = set()

                    # 1) Build FilingItems and check cache
                    upload_batch: List[Tuple[bytes, str, str, str, Optional[str]]] = []
                    items_to_fetch = []
                    item_lookup = {}

                    for (doc_id, date_iso, dtype, desc, filename) in to_get:
                        # Skip duplicates within this run
                        if doc_id in processed_doc_ids:
                            continue
                        processed_doc_ids.add(doc_id)
                        
                        cache_key = f"chdoc://{doc_id}" if doc_id else f"chdoc://{number}/{date_iso}/{dtype}"
                        
                        # Check cache first
                        ok, cached_path = self.cache.is_document_cached(cache_key, number)
                        if ok and cached_path and not self.cache.config.force_redownload_documents:
                            # cache hit shouldn't count as new download
                            from_cache += 1
                            self._emit_doc_progress(number, cached=1)
                            continue
                        if doc_id:
                            reuse_blob = self._document_reuse_get(doc_id)
                            if reuse_blob:
                                upload_batch.append((reuse_blob, filename, cache_key, dtype, doc_id))
                                from_cache += 1
                                self._emit_doc_progress(number, cached=1)
                                continue
                        
                        # Need to fetch this one
                        if doc_id:
                            item = FilingItem(
                                company_number=number,
                                company_name=result["company_name"],
                                date=date_iso,
                                type=dtype,
                                category="",
                                description=desc,
                                document_id=doc_id,
                                filename=filename
                            )
                            items_to_fetch.append(item)
                            item_key = filing_identity(item)
                            item_lookup[item_key] = (filename, dtype, date_iso, doc_id, cache_key)

                    def _flush_upload_batch():
                        nonlocal downloaded, upload_batch
                        if not upload_batch:
                            return
                        count_emitted = 0
                        if use_drive and drive_streaming:
                            parent_id = result["folder_path"].split("gdrive://", 1)[-1]
                            t_upload = time.time()
                            files_to_upload = [(data, fname) for data, fname, *_rest in upload_batch]
                            uploaded_count, success_idx = self._batch_upload_to_drive(files_to_upload, parent_id, number)
                            self.log(
                                f"✓ Batch uploaded {uploaded_count}/{len(upload_batch)} files in {time.time()-t_upload:.1f}s"
                            )
                            success_set = set(success_idx)
                            for i, (data, fname, cache_key, dtype, doc_id_val) in enumerate(upload_batch):
                                if i in success_set:
                                    self.cache.cache_document(
                                        cache_key,
                                        number,
                                        dtype,
                                        "gdrive://uploaded",
                                        True,
                                        metadata={"upload_time": 0.0, "drive_file_name": fname},
                                    )
                                    downloaded += 1
                                    self._emit_doc_progress(number, fresh=1)
                                    if doc_id_val:
                                        self._document_reuse_store(doc_id_val, data)
                            count_emitted = uploaded_count
                        else:
                            target_folder = Path(result["folder_path"]) if result["folder_path"] else None
                            for data, fname, cache_key, dtype, doc_id_val in upload_batch:
                                if target_folder:
                                    path = target_folder / fname
                                    path.write_bytes(data)
                                    stored_path = str(path)
                                else:
                                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
                                    tmp.write(data)
                                    tmp.close()
                                    stored_path = tmp.name
                                self.cache.cache_document(
                                    cache_key,
                                    number,
                                    dtype,
                                    stored_path,
                                    True,
                                    metadata={"download_time": 0.0},
                                )
                                downloaded += 1
                                self._emit_doc_progress(number, fresh=1)
                                if doc_id_val:
                                    self._document_reuse_store(doc_id_val, data)
                            count_emitted = len(upload_batch)
                        if count_emitted:
                            self._emit(
                                kind="documents_uploaded",
                                company_number=number,
                                count=count_emitted,
                                mode="drive" if use_drive and drive_streaming else "local",
                            )
                        upload_batch.clear()

                    if items_to_fetch and isinstance(provider, CHApiProvider):
                        self._check_api_health()
                        chunk_size = max(3, min(12, self._doc_workers_per_company * 2))
                        for chunk_start in range(0, len(items_to_fetch), chunk_size):
                            chunk = items_to_fetch[chunk_start : chunk_start + chunk_size]
                            try:
                                self._assert_api_window()
                                pdf_dict = provider.fetch_pdfs_batch_sync(
                                    chunk, max_concurrent=self._doc_workers_per_company
                                )
                            except ProviderRateLimited as exc:
                                raise RateLimitFallback(str(exc)) from exc
                            except Exception as e:
                                self.log(f"Batch fetch error: {e}", "ERROR")
                                pdf_dict = {}
                            for item in chunk:
                                item_key = filing_identity(item)
                                meta = item_lookup.get(item_key)
                                if not meta:
                                    continue
                                filename, dtype, date_iso, doc_id, doc_cache_key = meta
                                pdf_bytes = pdf_dict.get(item_key)
                                if not pdf_bytes or b"%PDF" not in pdf_bytes[:2048]:
                                    self.cache.cache_document(
                                        doc_cache_key, number, dtype, None, False, error_message="Invalid PDF"
                                    )
                                    errors += 1
                                    continue
                                upload_batch.append((pdf_bytes, filename, doc_cache_key, dtype, doc_id))
                            errors += max(0, len(chunk) - len(pdf_dict))
                            if len(upload_batch) >= 8:
                                _flush_upload_batch()
                    elif items_to_fetch:
                        errors += len(items_to_fetch)

                    _flush_upload_batch()
                    result["documents_downloaded"] = downloaded
                    result["documents_from_cache"] = from_cache
                    if errors:
                        result["errors"].append(f"{errors} document(s) failed")
                    result["success"] = True
                    result["processing_time"] = time.time() - t0

                    api_doc_links = [
                        {
                            "document_id": doc_id,
                            "date": date_iso,
                            "type": dtype,
                            "description": desc,
                            "url": f"/document/{doc_id}" if doc_id else "",
                            "filename": filename,
                        }
                        for (doc_id, date_iso, dtype, desc, filename) in items
                    ]
                    try:
                        self.cache.cache_company(
                            number,
                            result["company_name"],
                            notice_type,
                            None,
                            api_doc_links,
                            success=True,
                            metadata={
                                "source": "api_fast_path_gazette",
                                "documents_found": len(items),
                                "documents_downloaded": downloaded,
                                "procedure_folder": result.get("procedure_folder", ""),
                            },
                        )
                    except Exception as cache_err:
                        self.log(f"[API-PATH] cache save failed for {number}: {cache_err}", "WARNING")
                    
                    self.log(f"[API-PATH] ✓ Completed {number}: {downloaded} downloaded, {from_cache} cached", "INFO")
                    return result

                except Exception as e:
                    # If API path fails hard, fall back to HTML path below
                    self.log(f"API path failed for {number}, falling back to HTML: {e}", "WARNING")


            if is_cached and cached and not cached.get("success") and not self.cache.config.allow_cache_override:
                result["skipped_checkpoint"] = True
                result["success"] = True
                result["errors"].append(f"Previously failed: {cached.get('error_message', 'unknown')}")
                result["processing_time"] = time.time() - t0
                return result

            # ---------- Decide output mode (Drive vs Local) ----------
            use_drive = self._drive_is_ready()
            drive_streaming = use_drive and bool(getattr(self, "drive_only", False))

            if use_drive:
                procedure_folder_name = self._resolve_procedure_folder_name(notice_type)

                if getattr(self, "_forced_folder", None):
                    procedure_folder_name = self._forced_folder

                if not procedure_folder_name:
                    result["skipped_classification"] = True
                    self.cache.cache_company(number, name, notice_type, None, [], success=False, error_message="Skipped classification")
                    result["processing_time"] = time.time() - t0
                    return result
                company_folder_name = self.sanitize_folder_name(name) + (f"_{number}" if number else "")
                drive_parent_id = self._ensure_remote_company_folder(procedure_folder_name, company_folder_name)
                result["procedure_folder"] = procedure_folder_name
                result["folder_path"] = f"gdrive://{drive_parent_id}"
            else:
                folder = self.create_folder_structure_optimized(notice_type, name, number)
                if folder is None:
                    result["skipped_classification"] = True
                    self.cache.cache_company(number, name, notice_type, None, [], success=False, error_message="Skipped classification")
                    result["processing_time"] = time.time() - t0
                    return result
                result["procedure_folder"] = folder.parent.name
                result["folder_path"] = str(folder)

            # ---------- Filing history (use company cache if present) ----------
            filing_html = None
            doc_links: List[Dict] = []

            if is_cached and cached and cached.get("filing_history_html"):
                filing_html = cached["filing_history_html"]
                doc_links = cached.get("document_links", [])
                result["cache_hits"].append("filing_history")

            if self._should_cancel(cancel_event):
                self.cache.cache_company(number, name, notice_type, filing_html, doc_links, success=True)
                result["success"] = True
                result["processing_time"] = time.time() - t0
                return result

            else:
                try:
                    soup = self.circuit_breaker.call(self._get_filing_history_with_retry, number)
                except Exception as e:
                    msg = f"Circuit breaker open: {e}"
                    result["errors"].append(msg)
                    self.cache.cache_company(number, name, notice_type, None, [], success=False, error_message=msg)
                    result["processing_time"] = time.time() - t0
                    return result

                if not soup:
                    self.cache.cache_company(number, name, notice_type, None, [], success=True)
                    result["success"] = True
                    result["processing_time"] = time.time() - t0
                    return result

                if hasattr(self.ch_downloader, "find_document_links"):
                    doc_links = self.ch_downloader.find_document_links(soup, number, max_pages=FH_MAX_PAGES)
                filing_html = soup.prettify() if hasattr(soup, "prettify") else str(soup)

            if not doc_links:
                self.cache.cache_company(number, name, notice_type, filing_html, [], success=True)
                result["success"] = True
                result["processing_time"] = time.time() - t0
                return result

            # ---------- Document loop ----------
            result["documents_found"] = len(doc_links)

            # 1) Sort newest → oldest (unknown dates sink to the bottom)
            docs_sorted = self._ensure_consistent_sorting(doc_links, newest_first=True)

            # 2) Elegantly allow “download all”
            def _wants_all(max_documents_param) -> bool:
                return (
                    max_documents_param in (None, 0, -1, "ALL", "all", "All")
                    or bool(getattr(self, "download_all", False))
                )
            if _wants_all(max_documents):
                to_get = docs_sorted
            else:
                to_get = docs_sorted[:int(max_documents)]

            downloaded = 0
            from_cache = 0
            errors = 0
            upload_batch = []
            
            # Track processed cache keys (prevent double-counting)
            processed_cache_keys = set()

            def _tlog(self, msg, level="INFO"):
                self.log(f"[{threading.current_thread().name}] {msg}", level)

            def _download_one(d):
                try:
                    if self._should_cancel(cancel_event):
                        return ("cancelled", d, None)

                    url      = d.get("url", "")
                    dtype    = d.get("type", "UNKNOWN")
                    desc     = d.get("description") or ""
                    raw_date = (d.get("date") or "").strip()
                    
                    # Use stable cache key (filing history ID from URL)
                    cache_key = _doc_cache_key(url, d)
                    
                    # Skip if already processed in this run
                    if cache_key in processed_cache_keys:
                        return ("duplicate", d, None)
                    processed_cache_keys.add(cache_key)
                    
                    ok, cached_path = self.cache.is_document_cached(cache_key, number)
                    if ok and self._is_valid_cached_artifact(cached_path) and not self.cache.config.force_redownload_documents:
                        _tlog(f"CACHE HIT {number} {dtype} {raw_date}")
                        return ("cached", d, cached_path)

                    # Global HTTP token
                    _tlog(f"GET token wait {number} {dtype} {raw_date}")
                    with self._http_tokens:
                        _tlog(f"START GET {url} for {number}")
                        time.sleep(self.rate_limiter.get_delay())

                        # Deterministic filename
                        try:
                            date_iso = self._parse_doc_date(raw_date).strftime("%Y-%m-%d")
                            if date_iso == "0001-01-01":
                                date_iso = "0000-00-00"
                        except Exception:
                            date_iso = "0000-00-00"

                        safe_desc = self.sanitize_folder_name(desc)[:80] or "Document"
                        for prefix in ["Strong_", "strong_", "STRONG_"]:
                            if safe_desc.startswith(prefix):
                                safe_desc = safe_desc[len(prefix):]
                        safe_desc = re.sub(r'<[^>]+>', '', safe_desc)
                        safe_desc = re.sub(r'\s+', '_', safe_desc.strip()).strip('_')

                        filename = f"{date_iso}_{dtype}_{safe_desc}.pdf"
                        if len(filename) > 200:
                            max_desc = 200 - len(date_iso) - len(dtype) - 6
                            safe_desc = safe_desc[:max_desc].rstrip('_')
                            filename = f"{date_iso}_{dtype}_{safe_desc}.pdf"

                        t0d = time.time()
                        
                        if drive_streaming:
                            pdf_bytes = self._download_bytes(url, cancel_event=cancel_event)
                            rt = time.time() - t0d
                            if not pdf_bytes:
                                self.log(f"DL_FAIL: {url}", "WARNING")
                                self.rate_limiter.record_error("download_failed")
                                self.cache.cache_document(cache_key, number, dtype, None, False, error_message="PDF fetch failed")
                                return ("failed", d, None)

                            self.rate_limiter.record_success(rt)
                            return ("downloaded_bytes", d, {
                                "data": pdf_bytes,
                                "filename": filename,
                                "cache_key": cache_key,
                                "dtype": dtype
                            })
                        
                        # Local/hybrid
                        path = Path(result["folder_path"]) / filename
                        ok2 = self._download_single_document_robust(url, path)
                        rt  = time.time() - t0d
                        if ok2:
                            self.rate_limiter.record_success(rt)
                            self.cache.cache_document(cache_key, number, dtype, str(path), True, metadata={"download_time": rt})
                            _tlog(f"SAVE done {filename} for {number}")
                            if use_drive and not drive_streaming:
                                self._upload_if_configured(str(path), result["procedure_folder"], Path(result["folder_path"]).name)
                            return ("downloaded", d, str(path))

                        self.rate_limiter.record_error("download_failed")
                        self.cache.cache_document(cache_key, number, dtype, None, False, error_message="Download failed")
                        return ("failed", d, None)

                except Exception as e:
                    self.rate_limiter.record_error("exception")
                    self.log(f"[{threading.current_thread().name}] download error {number} {url}: {type(e).__name__} {e}", "WARNING")
                    return ("error", d, str(e))

            # Launch per-company pool
            max_workers = self._doc_workers_per_company
            pending = []
            it = iter(to_get)

            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"docs-{number}") as doc_ex:
                for _ in range(min(max_workers, len(to_get))):
                    try:
                        d = next(it)
                        pending.append(doc_ex.submit(_download_one, d))
                    except StopIteration:
                        break

                while pending:
                    fut = pending.pop(0)
                    status, d, payload = fut.result()
                    
                    if status == "cancelled":
                        break
                    elif status == "duplicate":
                        pass
                    elif status == "cached":
                        # ✅ FIXED: Only increment from_cache, NOT downloaded
                        from_cache += 1
                        self._emit_doc_progress(number, cached=1)
                        if from_cache <= 3: self.log(f"  From cache: {d.get('type','')}")
                    elif status == "downloaded_bytes":
                        if payload and isinstance(payload, dict):
                            upload_batch.append(payload)
                        if len(upload_batch) <= 3:
                            self.log(f"  Downloaded: {payload.get('filename', '')}")
                    elif status in ("uploaded", "downloaded"):
                        downloaded += 1
                        self._emit_doc_progress(number, fresh=1)
                        if downloaded <= 3: self.log(f"  Saved: {d.get('type','')}")
                    elif status in ("failed", "error"):
                        self.log(f"[{threading.current_thread().name}] download status {status} for {number} {d.get('url','')} -> payload={payload}", "WARNING")
                        errors += 1
                    else:
                        errors += 0

                    try:
                        dnext = next(it)
                        pending.append(doc_ex.submit(_download_one, dnext))
                    except StopIteration:
                        pass

            # Batch upload
            if upload_batch and use_drive and drive_streaming:
                parent_id = result["folder_path"].split("gdrive://", 1)[-1]
                t_upload = time.time()
                
                files_to_upload = [(item["data"], item["filename"]) for item in upload_batch]
                uploaded_count, success_idx = self._batch_upload_to_drive(files_to_upload, parent_id, number)
                
                self.log(f"✓ Batch uploaded {uploaded_count}/{len(upload_batch)} files")
                
                success_set = set(success_idx)
                for i, item in enumerate(upload_batch):
                    if i in success_set:
                        self.cache.cache_document(
                            item["cache_key"], number, item["dtype"], 
                            f"gdrive://uploaded", True,
                            metadata={"upload_time": 0.0, "drive_file_name": item["filename"]}
                        )
                        downloaded += 1
                        self._emit_doc_progress(number, fresh=1)

            result["documents_downloaded"] = downloaded
            result["documents_from_cache"] = from_cache
            result["success"] = True
            if errors:
                result["errors"].append(f"{errors} document(s) failed")

            # Persist company snapshot (for future cache hits)
            self.cache.cache_company(number, name, notice_type, filing_html, doc_links, success=True,
                                    metadata={"processing_time": time.time() - t0})
            self.log(
                f"RESEARCH LOOP SUMMARY {number}: to_get={len(to_get)} | downloaded(enqueued)={downloaded} | from_cache={result['documents_from_cache']} | errors={len(result['errors'])}",
                "INFO"
            )


        except Exception as e:
            msg = f"Company processing failed: {e}"
            self.log(msg, "ERROR")
            result["errors"].append(msg)
            self.cache.cache_company(number, name, notice_type, None, [], success=False, error_message=msg)

        result["processing_time"] = time.time() - t0
        return result

    # High-level (search + dedup + process)
    def extract_and_download_all_with_intelligent_caching(
        self,
        start_date: str,
        end_date: str,
        categories: List[str] = None,
        max_pages: int = 10,
        max_documents_per_company: int = MAX_DOCS_DEFAULT,
        download_documents: bool = True,
        max_concurrent_companies: int = 1,
    ) -> Dict:
        self._current_search_categories = categories or []
        self.log("=" * 80)
        self.log("STARTING CACHED WORLD-CLASS PIPELINE")
        self.log(f"Cache base: {self.cache.base_path}")
        self.log(f"Force Refresh Companies: {self.cache.config.force_refresh_companies}")
        self.log(f"Force Redownload Documents: {self.cache.config.force_redownload_documents}")
        self.log("=" * 80)

        # Maintenance
        self.cache.cleanup()
        # Start background upload workers if Drive attached
        self.reconcile_pending_uploads()
        try:
            self._start_upload_workers()
        except Exception as _e:
            self.log(f"Could not start upload workers: {_e}", "WARNING")


        # Gazette
        self.log("STEP 1: Extracting from The Gazette...")
        gazette = self.gazette_scraper.search_insolvencies(
            start_date=start_date, end_date=end_date, categories=categories, max_pages=max_pages, fetch_company_numbers=True
        )

        companies = [c for c in gazette["companies"] if c.get("company_number")]
        if not companies or not download_documents:
            return {
                "gazette_results": gazette,
                "download_results": {"skipped": not companies or not download_documents},
                "cache_stats": self.cache.stats_snapshot(),
                "base_folder": str(self.base_folder),
            }

        # Dedup
        unique_companies, dedup_meta = optimize_company_deduplication(companies)
        self.log(f"Dedup: {len(companies)} notices -> {len(unique_companies)} unique companies")

        # Process (parallel by default, bounded)
        stats = {
            "companies_processed": 0,
            "companies_successful": 0,
            "companies_from_cache": 0,
            "companies_skipped_classification": 0,
            "companies_skipped_checkpoint": 0,
            "total_documents_found": 0,
            "total_documents_downloaded": 0,
            "documents_from_cache": 0,
            "cache_hits_total": 0,
            "errors": 0,
            "folder_structure": {},
            "processing_time": 0.0,
            "optimization_mode": "cached_world_class",
        }
        t0 = time.time()

        def _accumulate(res):
            stats["companies_processed"] += 1
            if res["success"]:
                stats["companies_successful"] += 1
                stats["total_documents_found"] += res["documents_found"]
                stats["total_documents_downloaded"] += res["documents_downloaded"]
                stats["documents_from_cache"] += res["documents_from_cache"]
                stats["cache_hits_total"] += len(res["cache_hits"])
                if res["used_cache"]:
                    stats["companies_from_cache"] += 1
                folder = res["procedure_folder"]
                if folder:
                    stats["folder_structure"].setdefault(folder, []).append(
                        {
                            "company_name": res["company_name"],
                            "company_number": res["company_number"],
                            "notice_type": res["notice_type"],
                            "folder_path": res["folder_path"],
                            "documents_downloaded": res["documents_downloaded"],
                            "documents_from_cache": res["documents_from_cache"],
                            "cache_hits": res["cache_hits"],
                            "used_cache": res["used_cache"],
                        }
                    )
            else:
                if res.get("skipped_classification"):
                    stats["companies_skipped_classification"] += 1
                elif res.get("skipped_checkpoint"):
                    stats["companies_skipped_checkpoint"] += 1
                else:
                    stats["errors"] += len(res.get("errors", []))

        with ThreadPoolExecutor(max_workers=min(4, max(1, int(max_concurrent_companies)))) as ex:
            futures = [
                ex.submit(self.process_company_with_intelligent_caching, c, max_documents=max_documents_per_company)
                for c in unique_companies
            ]
            for i, fut in enumerate(as_completed(futures), 1):
                try:
                    _accumulate(fut.result())
                except Exception as e:
                    self.log(f"Processing error: {e}", "ERROR")
                    stats["errors"] += 1
                    stats["companies_processed"] += 1
                if (i % 10 == 0) or (i == len(futures)):
                    elapsed = time.time() - t0
                    rate = i / elapsed * 60 if elapsed > 0 else 0.0
                    cache_eff = (stats["companies_from_cache"] / max(stats["companies_processed"], 1)) * 100
                    self.log(f"Progress {i}/{len(futures)} | Rate {rate:.1f}/min | CacheEff {cache_eff:.1f}%")

        stats["processing_time"] = time.time() - t0
        # Log files/min (downloaded == confirmed uploads if Drive is attached and we drained)
        total_files = stats.get("total_documents_downloaded", 0)
        elapsed = max(0.0001, stats["processing_time"])
        files_per_min = total_files / (elapsed / 60.0)
        self.log(f"SUMMARY: files={total_files}, seconds={elapsed:.2f}, files_per_minute={files_per_min:.2f}", "INFO")


        self.log("-" * 80)
        self.log("CACHED PIPELINE COMPLETE")
        self.log(
            f"Companies: {stats['companies_processed']} | Success: {stats['companies_successful']} | From cache: {stats['companies_from_cache']}"
        )
        self.log(
            f"Documents: {stats['total_documents_downloaded']} (cached: {stats['documents_from_cache']})"
        )
        self.log(f"Time: {stats['processing_time']:.1f}s")
        self.log("=" * 80)

        self.reconcile_pending_uploads()
        # Stop workers (best effort). If uploads are still in flight, they’ll flush quickly.
        # Drain then stop workers (ensures UI shows “done” only when uploads are done)
        try:
            self.stop_upload_workers(timeout=None)  # wait to drain fully
        except Exception as _e:
            self.log(f"Error stopping upload workers: {_e}", "WARNING")

        # Prefer the atomic counter; fall back to per-company sum if needed
        total_confirmed = (self._uploaded_total or sum(self._uploaded_by_company.values()))
        stats["total_documents_uploaded"] = total_confirmed   # new, accurate metric

        # Don’t clobber what “downloaded” meant (processed). If UI expects downloaded,
        # keep it, but if it is 0 while we have confirmed uploads, show confirmed.
        if stats.get("total_documents_downloaded", 0) == 0 and total_confirmed > 0:
            stats["total_documents_downloaded"] = total_confirmed

        elapsed = max(0.0001, stats["processing_time"])
        files_per_min = total_confirmed / (elapsed / 60.0)
        self.log(f"SUMMARY: files={total_confirmed}, seconds={elapsed:.2f}, files_per_minute={files_per_min:.2f}", "INFO")

        return {
            "gazette_results": gazette,
            "download_results": stats,
            "cache_stats": self.cache.stats_snapshot(),
            "cache_performance": self.cache_perf,
            "deduplication_metadata": dedup_meta,
            "base_folder": str(self.base_folder),
        }

class ProviderFactory:
    @staticmethod
    def create() -> FilingProvider:
        src = DOC_SOURCE or "auto"
        if src == "api" and CH_API_KEY:
            return make_api_provider()
        if src == "auto" and CH_API_KEY:
            return make_api_provider()
        # fallback to your existing HTML downloader bound in WorldClass* via self.ch_downloader
        # We don't need an HtmlProvider here because the existing pipeline already uses ch_downloader for HTML flows.
        return None  # HTML path handled by existing ch_downloader members


# =============================================================================
# CONFIG + ENTRYPOINTS
# =============================================================================

def create_optimized_cache_config(
    memory_size_mb: int = 256, force_refresh: bool = False, force_redownload: bool = False, cache_ttl_hours: int = 168
) -> CacheConfig:
    return CacheConfig(
        enable_memory_cache=True,
        enable_disk_cache=True,
        enable_database_cache=True,
        memory_cache_size_mb=memory_size_mb,
        memory_default_ttl_s=cache_ttl_hours * 3600,
        disk_cache_size_mb=memory_size_mb * 4,
        disk_default_ttl_s=cache_ttl_hours * 3600,
        cache_base_path="cache",
        database_file="insolvency_cache.db",
        company_data_ttl=cache_ttl_hours * 3600,
        document_metadata_ttl=(cache_ttl_hours // 2) * 3600,
        cleanup_interval_hours=24,
        max_cache_age_days=30,
        compression_enabled=True,
        allow_cache_override=True,
        force_refresh_companies=force_refresh,
        force_redownload_documents=force_redownload,
    )


def extract_with_intelligent_caching(
    start_date: str,
    end_date: str,
    categories: List[str],
    max_pages: int = 20,
    max_documents_per_company: int = 10,
    base_folder: str = "InsolvencyDocuments_Cached",
    force_refresh: bool = False,
    force_redownload: bool = False,
    drive_folder_id: Optional[str] = None,
    drive_scope: str = "file",            # "file" (upload-only) or "full"
    client_secret_path: str = "secrets/client_secret.json",
    token_path: str = "token.json",
    delete_local_after_upload: bool = True,
    drive_only: bool = True,
    max_concurrent_companies: int = 2,
) -> Dict:
    cfg = create_optimized_cache_config(force_refresh=force_refresh, force_redownload=force_redownload)
    pipeline = CachedGazetteCompaniesHousePipeline(base_download_folder=base_folder, cache_config=cfg, verbose=True)
    pipeline._current_search_categories = categories or []

    # If caller didn’t pass a positive limit, treat as “download all”
    if max_documents_per_company in (None, 0, -1, "ALL", "all", "All"):
        pipeline.download_all = True

    # Attach Drive if provided
    if drive_folder_id:
        scopes = [DRIVE_SCOPE_FILE] if drive_scope == "file" else [DRIVE_SCOPE_FULL]
        drive = GoogleDriveClient(
            client_secret_path=client_secret_path,
            token_path=token_path,
            scopes=scopes,
        )
        pipeline.attach_drive(
            drive_client=drive,
            root_folder_id=drive_folder_id,
            drive_only=drive_only,
            delete_local_after_upload=delete_local_after_upload,
        )


    return pipeline.extract_and_download_all_with_intelligent_caching(
        start_date=start_date,
        end_date=end_date,
        categories=categories,
        max_pages=max_pages,
        max_documents_per_company=max_documents_per_company,
        download_documents=True,
        max_concurrent_companies=max_concurrent_companies,
    )


def extract_cvl_cached():
    return extract_with_intelligent_caching(
        start_date="18/08/2025",
        end_date="19/08/2025",
        categories=["cvl"],
        max_pages=25,
        base_folder="CVL_Cached",
    )


def extract_administration_cached():
    return extract_with_intelligent_caching(
        start_date="18/08/2025",
        end_date="19/08/2025",
        categories=["administration"],
        max_pages=15,
        base_folder="Administration_Cached",
    )


def extract_court_liquidation_cached():
    return extract_with_intelligent_caching(
        start_date="18/08/2025",
        end_date="19/08/2025",
        categories=["liquidation_by_court"],
        max_pages=20,
        base_folder="Court_Liquidation_Cached",
    )


if __name__ == "__main__":
    # Example run (CVL only). Adjust dates/pages for real use.
    results = extract_cvl_cached()
    print("\n=== SUMMARY ===")
    print(f"Companies (success): {results['download_results']['companies_successful']}")
    print(f"Documents (downloaded): {results['download_results']['total_documents_downloaded']}")
    print(f"Cache hits total: {results['download_results']['cache_hits_total']}")
    print(f"Cache stats: {results['cache_stats']}")
