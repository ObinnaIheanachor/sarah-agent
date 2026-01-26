# ch_api_provider.py
from __future__ import annotations

import asyncio
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, Iterable, List, Optional
from collections import deque

import httpx

from .base_provider import FilingItem, FilingProvider, filing_identity
from settings import cfg, refresh_cfg
from wc_async_core import TokenBucket

CH_BASE = "https://api.company-information.service.gov.uk"
CH_DOC_BASE = "https://document-api.company-information.service.gov.uk"


class ProviderRateLimited(Exception):
    """Raised when Companies House throttles us so callers can downgrade gracefully."""


def get_api_key() -> str:
    refresh_cfg()
    return cfg("CH_API_KEY", "4a905619-1bf3-499b-b643-479d202bd8b6", str)


def get_api_keys() -> List[str]:
    # Ensure we re-read secrets/env between runs
    refresh_cfg()
    raw = cfg("CH_API_KEYS", "", str)
    if isinstance(raw, str):
        items = [k.strip() for k in raw.split(",") if k.strip()]
    elif isinstance(raw, Iterable):
        items = [str(k).strip() for k in raw if str(k).strip()]
    else:
        items = []
    return items


def get_api_rps() -> float:
    return cfg("CH_API_RPS", 4.0, float)


def get_api_timeout() -> int:
    return cfg("CH_API_TIMEOUT", 45, int)


def get_api_max_conc() -> int:
    return cfg("CH_API_MAX_CONC", 24, int)

def get_key_max_conc() -> int:
    return cfg("CH_KEY_MAX_CONC", 6, int)

def get_key_rate_window() -> float:
    return max(0.5, cfg("CH_KEY_RATE_WINDOW", 1.2, float))

def get_key_soft_utilization() -> float:
    val = cfg("CH_KEY_SOFT_UTILIZATION", 0.85, float)
    return min(0.98, max(0.3, float(val)))


def _sanitize(s: str, max_len: int = 80) -> str:
    s = (s or "").strip()
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"[<>:\"/\\|?*]", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len] if s else "Document"


def _filename(date_iso: str, ftype: str, desc: str) -> str:
    base = f"{(date_iso or '0000-00-00')}_{ftype or 'DOC'}_{_sanitize(desc, 80)}"
    fn = f"{base}.pdf"
    if len(fn) > 200:
        trim = 200 - len((date_iso or '0000-00-00')) - len(ftype or 'DOC') - 6
        fn = f"{(date_iso or '0000-00-00')}_{ftype or 'DOC'}_{_sanitize(desc, trim)}.pdf"
    return fn


class KeyContext:
    __slots__ = (
        "api_key",
        "bucket",
        "sem",
        "core_client",
        "doc_client",
        "cooldown_until",
        "fail_count",
        "max_concurrency",
        "rate_window",
        "soft_util_target",
        "soft_util_current",
        "request_times",
        "_rate_callback",
        "_was_throttled",
        "_lock",
    )

    def __init__(
        self,
        api_key: str,
        timeout: int,
        max_conc: int,
        rps: float,
        rate_window: float,
        soft_util: float,
        rate_callback: Optional[Callable[[dict], None]] = None,
    ):
        self.api_key = api_key
        self.bucket = TokenBucket(rate=rps, burst=max(6, int(max_conc * 2)))
        self.max_concurrency = max(1, max_conc)
        self.sem = threading.Semaphore(self.max_concurrency)
        self.rate_window = rate_window
        self.soft_util_target = soft_util
        self.soft_util_current = soft_util
        self.request_times = deque()
        self._rate_callback = rate_callback
        self._was_throttled = False
        headers = {"User-Agent": "SARAH/CH-API"}
        limits = httpx.Limits(max_keepalive_connections=max(2, max_conc), max_connections=max(2, max_conc))
        self.core_client = httpx.Client(
            base_url=CH_BASE,
            auth=(api_key, ""),
            timeout=timeout,
            headers=headers,
            limits=limits,
            http2=False,
        )
        self.doc_client = httpx.Client(
            base_url=CH_DOC_BASE,
            auth=(api_key, ""),
            timeout=timeout,
            headers=headers,
            limits=limits,
            http2=False,
        )
        self.cooldown_until = 0.0
        self.fail_count = 0
        self._lock = threading.Lock()

    def _trim_requests_locked(self, now: Optional[float] = None) -> None:
        now = now or time.time()
        cutoff = now - self.rate_window
        while self.request_times and self.request_times[0] < cutoff:
            self.request_times.popleft()

    def predictive_wait(self) -> float:
        with self._lock:
            self._trim_requests_locked()
            allowed = max(1.0, self.soft_util_current * self.rate_window * self.bucket.rate)
            current = len(self.request_times)
            if current < allowed:
                return 0.0
            earliest = self.request_times[0]
            return max(0.0, earliest + self.rate_window - time.time())

    def register_request(self) -> None:
        now = time.time()
        with self._lock:
            self.request_times.append(now)
            self._trim_requests_locked(now)

    def close(self) -> None:
        try:
            if self.core_client and not self.core_client.is_closed:
                self.core_client.close()
        except Exception:
            pass
        try:
            if self.doc_client and not self.doc_client.is_closed:
                self.doc_client.close()
        except Exception:
            pass

    def mark_rate_limited(self, retry_after: Optional[str]) -> None:
        with self._lock:
            self.fail_count = min(self.fail_count + 1, 6)
            delay = 0.5 * (2 ** (self.fail_count - 1))
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except Exception:
                    pass
            self.cooldown_until = max(self.cooldown_until, time.time() + delay)
            self.soft_util_current = max(0.3, self.soft_util_current * 0.7)
            self._was_throttled = True
            if self._rate_callback:
                try:
                    self._rate_callback(
                        {
                            "status": "limited",
                            "key": self.api_key,
                            "delay": delay,
                            "cooldown_until": self.cooldown_until,
                        }
                    )
                except Exception:
                    pass

    def mark_success(self) -> None:
        with self._lock:
            self.fail_count = 0
            self.cooldown_until = 0.0
            if self.soft_util_current < self.soft_util_target:
                self.soft_util_current = min(
                    self.soft_util_target,
                    self.soft_util_current + 0.05,
                )
            if self._was_throttled and self._rate_callback:
                try:
                    self._rate_callback(
                        {
                            "status": "recovered",
                            "key": self.api_key,
                            "cooldown_until": self.cooldown_until,
                        }
                    )
                except Exception:
                    pass
            self._was_throttled = False

    def ready_in(self) -> float:
        with self._lock:
            return max(0.0, self.cooldown_until - time.time())


class CHApiProvider(FilingProvider):
    """
    Thread-safe Companies House API provider with support for multiple API keys.
    Each key maintains its own token bucket and connection pool so we can
    stagger requests and stay within per-key limits while increasing overall throughput.
    """

    def __init__(self, api_key: Optional[str] = None, api_keys: Optional[List[str]] = None):
        keys = list(api_keys) if api_keys else []
        if api_key:
            keys.append(api_key)
        if not keys:
            keys = [get_api_key()]

        seen = set()
        self.api_keys: List[str] = []
        for key in keys:
            key = key.strip()
            if key and key not in seen:
                seen.add(key)
                self.api_keys.append(key)

        if not self.api_keys:
            raise ValueError("No Companies House API keys configured.")

        self._timeout = get_api_timeout()
        total_conc = max(1, get_api_max_conc())
        total_rps = max(0.5, get_api_rps())

        per_key_rps = max(0.5, total_rps / len(self.api_keys))
        base_conc = max(1, total_conc // len(self.api_keys))
        remainder = total_conc % len(self.api_keys)

        per_key_cap = max(1, get_key_max_conc())
        rate_window = get_key_rate_window()
        soft_util = get_key_soft_utilization()
        self._rate_listeners: List[Callable[[dict], None]] = []
        self._contexts: List[KeyContext] = []
        for idx, key in enumerate(self.api_keys):
            per_key_conc = base_conc + (1 if idx < remainder else 0)
            per_key_conc = min(per_key_conc or 1, per_key_cap)
            self._contexts.append(
                KeyContext(
                    key,
                    self._timeout,
                    per_key_conc,
                    per_key_rps,
                    rate_window,
                    soft_util,
                    rate_callback=self._notify_rate_event,
                )
            )

        self._cycle_lock = threading.Lock()
        self._cycle_index = 0

        self.metrics = {"requests": 0, "errors": 0, "cache_hits": 0}

        self.log(
            f"Ready — API client initialised (effective rps≈{per_key_rps * len(self.api_keys):.2f})"
        )

    # ------------------------------------------------------------------
    # lifecycle helpers
    # ------------------------------------------------------------------
    def log(self, msg: str, level: str = "INFO") -> None:
        try:
            print(f"[CHApiProvider] {level}: {msg}")
        except Exception:
            pass

    def close(self) -> None:
        for ctx in self._contexts:
            ctx.close()

    def register_rate_limit_listener(self, callback: Callable[[dict], None]) -> None:
        if not callable(callback):
            return
        self._rate_listeners.append(callback)

    def _notify_rate_event(self, info: dict) -> None:
        if not info:
            return
        key = info.get("key", "")
        info["key_suffix"] = key[-6:] if key else ""
        for cb in list(self._rate_listeners):
            try:
                cb(info.copy())
            except Exception:
                continue

    # ------------------------------------------------------------------
    # request helpers
    # ------------------------------------------------------------------
    def _acquire_context(self) -> KeyContext:
        while True:
            min_wait: Optional[float] = None
            for _ in range(len(self._contexts)):
                with self._cycle_lock:
                    idx = self._cycle_index
                    self._cycle_index = (self._cycle_index + 1) % len(self._contexts)
                    ctx = self._contexts[idx]
                wait = ctx.ready_in()
                predictive = ctx.predictive_wait()
                wait = max(wait, predictive)
                if wait > 0:
                    min_wait = wait if min_wait is None else min(min_wait, wait)
                    continue
                if not ctx.sem.acquire(blocking=False):
                    continue
                ctx.bucket.acquire()
                return ctx
            sleep_for = min_wait if min_wait is not None else 0.05
            time.sleep(min(0.25, max(sleep_for, 0.01)))

    def _request(self, which: str, url: str, **kwargs) -> Optional[httpx.Response]:
        ctx = self._acquire_context()
        client = ctx.core_client if which == "core" else ctx.doc_client
        try:
            resp = client.get(url, **kwargs)
            ctx.register_request()
        except Exception as exc:
            self.metrics["errors"] += 1
            self.log(f"HTTP error for {url} (key …{ctx.api_key[-4:]}): {exc}", "WARNING")
            ctx.mark_rate_limited(None)
            resp = None
        finally:
            ctx.sem.release()

        if resp is None:
            return None

        self.metrics["requests"] += 1
        if resp.status_code >= 400:
            self.metrics["errors"] += 1
        if resp.status_code == 429:
            ctx.mark_rate_limited(resp.headers.get("Retry-After"))
        elif 200 <= resp.status_code < 400:
            ctx.mark_success()
        return resp

    # ------------------------------------------------------------------
    # synchronous API surface
    # ------------------------------------------------------------------
    def get_company_profile_sync(self, number: str) -> Dict:
        resp = self._request("core", f"/company/{number}")
        if not resp:
            return {}
        if resp.status_code == 429:
            raise ProviderRateLimited(f"Profile lookup throttled for {number}")
        if resp.status_code != 200:
            return {}
        try:
            return resp.json()
        except Exception:
            return {}

    def _get_filings_page_sync(self, number: str, start_index: int) -> Dict:
        resp = self._request(
            "core",
            f"/company/{number}/filing-history",
            params={"items_per_page": 100, "start_index": start_index},
        )
        if not resp:
            return {"items": [], "total_count": 0}
        if resp.status_code == 429:
            raise ProviderRateLimited(f"Filing history throttled for {number}")
        if resp.status_code != 200:
            return {"items": [], "total_count": 0}
        try:
            return resp.json()
        except Exception:
            return {"items": [], "total_count": 0}

    def get_all_filings_sync(self, number: str) -> List[Dict]:
        first = self._get_filings_page_sync(number, 0)
        items = first.get("items", []) or []
        total = int(first.get("total_count", len(items)) or 0)
        for start in range(100, total, 100):
            page = self._get_filings_page_sync(number, start)
            items.extend(page.get("items", []) or [])
        return items

    def fetch_pdf_sync(self, document_id: str) -> Optional[bytes]:
        if not document_id:
            return None
        attempts = 0
        saw_rate_limit = False
        while attempts < 4:
            resp = self._request(
                "doc",
                f"/document/{document_id}/content",
                headers={"Accept": "application/pdf"},
                follow_redirects=True,
            )
            attempts += 1
            if not resp:
                time.sleep(0.5 * attempts)
                continue

            if resp.status_code == 200 and "application/pdf" in (resp.headers.get("Content-Type", "").lower()):
                data = resp.content or b""
                if data.startswith(b"%PDF"):
                    return data
                self.log(f"Invalid PDF signature for {document_id}", "WARNING")
                return None

            if resp.status_code == 429:
                saw_rate_limit = True
                self.log(f"Rate limited on {document_id}; backing off", "WARNING")
                if attempts >= 2:
                    raise ProviderRateLimited(f"PDF fetch throttled for {document_id}")
                time.sleep(1.2 * attempts)
                continue

            if 500 <= resp.status_code < 600:
                time.sleep(0.8 * attempts)
                continue

            try:
                payload = resp.json()
            except Exception:
                payload = {}
            if isinstance(payload, dict) and payload.get("errors"):
                err = payload["errors"][0] if payload["errors"] else {}
                msg = err.get("error") or err.get("message") or ""
                self.log(f"PDF unavailable for {document_id}: {msg}", "INFO")
            else:
                self.log(f"Unexpected response {resp.status_code} for {document_id}", "WARNING")
            return None
        if saw_rate_limit:
            raise ProviderRateLimited(f"Repeated PDF throttling for {document_id}")
        return None

    def fetch_pdfs_batch_sync(self, items: List[FilingItem], max_concurrent: int = 5) -> Dict[str, bytes]:
        if not items:
            return {}

        max_workers = max(1, min(len(items), max_concurrent))
        results: Dict[str, bytes] = {}

        def _worker(item: FilingItem) -> Optional[bytes]:
            return self.fetch_pdf_sync(item.document_id or "")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_map = {pool.submit(_worker, item): item for item in items if item.document_id}
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    pdf_bytes = future.result()
                except ProviderRateLimited:
                    for fut in future_map:
                        fut.cancel()
                    raise
                except Exception as exc:
                    self.log(f"PDF fetch exception for {item.document_id}: {exc}", "WARNING")
                    continue
                if not pdf_bytes:
                    continue
                results[filing_identity(item)] = pdf_bytes
        return results

    # ------------------------------------------------------------------
    # async wrappers (to satisfy existing async interface)
    # ------------------------------------------------------------------
    def ensure_ready_sync(self) -> None:
        return

    async def ensure_ready(self) -> "CHApiProvider":
        await asyncio.to_thread(self.ensure_ready_sync)
        return self

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    async def __aenter__(self) -> "CHApiProvider":
        await self.ensure_ready()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def get_company_profile(self, number: str) -> Dict:
        return await asyncio.to_thread(self.get_company_profile_sync, number)

    async def get_all_filings(self, number: str) -> List[Dict]:
        return await asyncio.to_thread(self.get_all_filings_sync, number)

    async def list_filings(self, number: str) -> List[FilingItem]:
        filings = await self.get_all_filings(number)
        prof = await self.get_company_profile(number)
        name = prof.get("company_name", number)
        items: List[FilingItem] = []
        for f in filings:
            link = (f.get("links", {}) or {}).get("document_metadata", "")
            m = re.search(r"/document/([A-Za-z0-9_\-]+)", link)
            doc_id = m.group(1) if m else None
            date = f.get("date", "0000-00-00")
            ftype = f.get("type", "DOC")
            desc = f.get("description", "") or ""
            items.append(
                FilingItem(
                    company_number=number,
                    company_name=name,
                    date=date,
                    type=ftype,
                    category=f.get("category", ""),
                    description=desc,
                    document_id=doc_id,
                    filename=_filename(date, ftype, desc),
                )
            )
        return items

    async def fetch_pdf(self, item: FilingItem) -> Optional[bytes]:
        return await asyncio.to_thread(self.fetch_pdf_sync, item.document_id or "")

    async def fetch_pdfs_batch(self, items: List[FilingItem], max_concurrent: int = 5) -> Dict[str, bytes]:
        return await asyncio.to_thread(self.fetch_pdfs_batch_sync, items, max_concurrent)


def make_api_provider() -> CHApiProvider:
    keys = get_api_keys()
    if keys:
        return CHApiProvider(api_keys=keys)
    return CHApiProvider(api_key=get_api_key())
