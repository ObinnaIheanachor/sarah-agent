# wc_insolvency_core.py
# Core primitives (rate limiter, breaker, queues, checkpoint), multi-layer cache,
# Gazette scraper, Companies House downloader, base + world-class pipelines.
# Safe default timeouts and robust logging included.

from __future__ import annotations

import json
from multiprocessing import pool
import os
import re
import time
import gzip
import pickle
import random
import shutil
import sqlite3
import argparse
import sys
import queue
import threading
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import asyncio
from settings import cfg, refresh_cfg

# Token bucket defaults can now come from secrets.toml OR env
_GAZ_BUCKET = None
_GAZ_BUCKET_LOCK = threading.Lock()
HTTP_TOKEN_RATE  = cfg("HTTP_TOKEN_RATE", 25.0, float)
HTTP_TOKEN_BURST = cfg("HTTP_TOKEN_BURST", cfg("HTTP_MAX_INFLIGHT", 80, int), int)
try:
    import httpx
except Exception:
    httpx = None

_tls = threading.local()


def get_gazette_bucket() -> "TokenBucket":
    return _LimiterReg.gazette()

def get_ch_web_bucket() -> "TokenBucket":
    return _LimiterReg.ch_web()

def get_ch_api_bucket() -> "TokenBucket":
    return _LimiterReg.ch_api()

# =============================================================================
# WORLD-CLASS PERFORMANCE COMPONENTS
# =============================================================================

def boost_session(sess: requests.Session, ua: str) -> requests.Session:
    """Add pooled adapters + robust retries to a session (pool sizes from env)."""
    sess.headers.update({"User-Agent": ua})

    pool = cfg("HTTP_MAX_INFLIGHT", 120, int)
    keepalive = cfg("HTTP_MAX_KEEPALIVE", max(40, pool // 2), int)
    total_retry = cfg("HTTP_TOTAL_RETRY", 5, int)

    retry = Retry(
        total=total_retry,
        connect=min(3, total_retry),
        read=min(3, total_retry),
        backoff_factor=cfg("HTTP_BACKOFF", 0.4, float),
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("HEAD", "GET", "OPTIONS"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    # Keep-Alive header (helps some servers)
    sess.headers.setdefault("Connection", "keep-alive")
    sess.headers.setdefault("Keep-Alive", f"timeout=60, max={keepalive}")
    return sess

class AsyncHttpPool:
    """
    Shared AsyncClient (HTTP/2) with connection reuse. 
    Only used if httpx is available and ASYNC_NET=1.
    """
    _client: Optional["httpx.AsyncClient"] = None
    _lock = threading.Lock()

    @classmethod
    def enable(cls) -> bool:
        """Enable async HTTP/2 when httpx is present and ASYNC_NET=1."""
        return bool(httpx) and os.getenv("ASYNC_NET", "0") == "1"

    @classmethod
    def get(cls) -> "httpx.AsyncClient":
        if not cls.enable():
            raise RuntimeError("Async HTTP disabled or httpx not installed")
        with cls._lock:
            if cls._client is None:
                pool = cfg("HTTP_MAX_INFLIGHT", 80, int)
                keepalive = cfg("HTTP_MAX_KEEPALIVE", max(20, pool // 2), int)
                timeout = cfg("HTTP_TIMEOUT", 30, float)
                cls._client = httpx.AsyncClient(
                    http2=True,
                    timeout=timeout,
                    limits=httpx.Limits(max_connections=pool, max_keepalive_connections=keepalive),
                    headers={"User-Agent": "Mozilla/5.0", "Connection": "keep-alive"},
                )
            return cls._client

    @classmethod
    async def close(cls):
        c = None
        with cls._lock:
            if cls._client:
                c = cls._client
                cls._client = None
        if c:
            await c.aclose()

async def shutdown_network_clients():
    """Optional: close async HTTP/2 client cleanly (used by CLI/tests)."""
    try:
        await AsyncHttpPool.close()
    except Exception:
        pass



def tls_session(base_headers: dict | None = None, cookies=None) -> requests.Session:
    """
    Create a short-lived requests.Session carrying our standard headers/TLS config.
    Reuses your boost_session() for retries/pools. Caller should close().
    """
    s = requests.Session()
    # Reuse your robust adapters/retries + UA
    ua = (base_headers or {}).get("User-Agent", "Mozilla/5.0")
    boost_session(s, ua=ua)
    if base_headers:
        s.headers.update(base_headers)
    if cookies:
        try:
            # requests cookies obj or dict both supported
            s.cookies.update(cookies.get_dict() if hasattr(cookies, "get_dict") else cookies)
        except Exception:
            pass
    return s


class DocumentPriority(Enum):
    CRITICAL = 1    # AA, CS01, AR01
    HIGH = 2        # CH01, CC01, NEWINC
    MEDIUM = 3      # RP02, RP03, MR01
    LOW = 4         # everything else


class AdaptiveRateLimiter:
    """Spotify-style adaptive rate limiting that learns from results."""
    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.success_streak = 0
        self.error_streak = 0
        self.last_error_time = 0.0
        self.lock = threading.Lock()
        self.total_requests = 0
        self.total_errors = 0
        self.response_times: List[float] = []

    def get_delay(self) -> float:
        with self.lock:
            if self.error_streak > 0:
                backoff_multiplier = min(2 ** self.error_streak, 16)
                self.current_delay = min(self.base_delay * backoff_multiplier, self.max_delay)
            elif self.success_streak > 10:
                self.current_delay = max(self.current_delay * 0.9, self.base_delay * 0.5)

            return self.current_delay * random.uniform(0.8, 1.2)

    def record_success(self, response_time: float = 0.0):
        with self.lock:
            self.success_streak += 1
            self.error_streak = 0
            self.total_requests += 1
            if response_time > 0:
                self.response_times.append(response_time)
                if len(self.response_times) > 100:
                    self.response_times.pop(0)

    def record_error(self, _error_type: str = "generic"):
        with self.lock:
            self.error_streak += 1
            self.success_streak = 0
            self.total_errors += 1
            self.last_error_time = time.time()

    def get_stats(self) -> Dict[str, float]:
        with self.lock:
            success_rate = ((self.total_requests - self.total_errors) / self.total_requests * 100) if self.total_requests else 0.0
        avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0.0
        return {
            "success_rate": success_rate,
            "total_requests": float(self.total_requests),
            "total_errors": float(self.total_errors),
            "current_delay": float(self.current_delay),
            "avg_response_time": float(avg_response_time),
            "success_streak": float(self.success_streak),
            "error_streak": float(self.error_streak),
        }
class TokenBucket:
    """
    Thread-safe token bucket. Rate = tokens per second, burst = max bucket size.
    Acquire() blocks briefly (busy-waits in short sleeps) until a token is available.
    """
    def __init__(self, rate: float = 20.0, burst: int = 40):
        self.rate = float(rate)
        self.capacity = int(burst)
        self._tokens = float(burst)
        self._last = time.time()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.time()
        elapsed = now - self._last
        if elapsed <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last = now

    def acquire(self) -> None:
        # Fast path: try to take a token; if empty, short sleep & retry.
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(0.005)  # very short yield to avoid CPU spin


# ---- Shared Rate Limiter Registry -------------------------------------------

class _LimiterReg:
    _init = False
    _lock = threading.Lock()
    _gazette = None
    _ch_web = None
    _ch_api = None
    _ch_api_adaptive = None  # ✅ ADD THIS LINE

    @classmethod
    def _ensure(cls):
        if cls._init:
            return
        with cls._lock:
            if cls._init:
                return
            # Gazette site (HTML) – conservative defaults
            gaz_rate  = cfg("GAZETTE_TOKEN_RATE", 2.0, float)
            gaz_burst = cfg("GAZETTE_TOKEN_BURST", 10, int)
            cls._gazette = TokenBucket(rate=gaz_rate, burst=gaz_burst)

            # CH public website (HTML)
            chweb_rate  = cfg("CH_WEB_TOKEN_RATE", 3.0, float)
            chweb_burst = cfg("CH_WEB_TOKEN_BURST", 10, int)
            cls._ch_web = TokenBucket(rate=chweb_rate, burst=chweb_burst)

            # CH API (REST) – your requirement
            chapi_rate  = cfg("CH_API_TOKEN_RATE", 1.9, float)
            chapi_burst = cfg("CH_API_TOKEN_BURST", 6, int)
            cls._ch_api = TokenBucket(rate=chapi_rate, burst=chapi_burst)
            
            # ✅ ADD THESE LINES:
            # CH API adaptive limiter (shared across all provider instances)
            base_delay = 1.0 / chapi_rate  # Base delay matches rate
            max_delay = cfg("CH_API_TIMEOUT", 30, int)
            cls._ch_api_adaptive = AdaptiveRateLimiter(base_delay=base_delay, max_delay=max_delay)

            cls._init = True

    @classmethod
    def gazette(cls) -> "TokenBucket":
        cls._ensure(); return cls._gazette

    @classmethod
    def ch_web(cls) -> "TokenBucket":
        cls._ensure(); return cls._ch_web

    @classmethod
    def ch_api(cls) -> "TokenBucket":
        cls._ensure(); return cls._ch_api
    
    @classmethod
    def ch_api_adaptive(cls) -> "AdaptiveRateLimiter":
        """Global adaptive rate limiter for CH API (shared across all provider instances)."""
        cls._ensure()
        return cls._ch_api_adaptive
    
class CircuitBreaker:
    """Netflix-style circuit breaker to prevent cascade failures."""
    def __init__(self, failure_threshold: int = 5, timeout: int = 60, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.name = name
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        with self.lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = "HALF_OPEN"
                    print(f"Circuit breaker {self.name}: HALF_OPEN (testing)")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN - API unavailable")

        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception:
            self._record_failure()
            raise

    def _record_success(self):
        with self.lock:
            self.failure_count = 0
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                print(f"Circuit breaker {self.name}: CLOSED (recovered)")

    def _record_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                print(f"Circuit breaker {self.name}: OPEN (API failing)")

    def get_state(self) -> str:
        return self.state


from queue import PriorityQueue

class SmartDocumentQueue:
    """Priority queue system for intelligent document processing."""
    def __init__(self):
        self.high_priority = PriorityQueue()
        self.medium_priority = PriorityQueue()
        self.low_priority = PriorityQueue()
        self.retry_queue = PriorityQueue()

        self.priority_mapping = {
            "AA": DocumentPriority.CRITICAL,
            "CS01": DocumentPriority.CRITICAL,
            "AR01": DocumentPriority.CRITICAL,
            "CH01": DocumentPriority.HIGH,
            "CC01": DocumentPriority.HIGH,
            "NEWINC": DocumentPriority.HIGH,
            "MR01": DocumentPriority.MEDIUM,
            "RP02": DocumentPriority.MEDIUM,
            "RP03": DocumentPriority.MEDIUM,
        }

    def add_document(self, document: Dict, retry_count: int = 0):
        priority = self.priority_mapping.get(document.get("type", "UNKNOWN"), DocumentPriority.LOW)
        priority_value = priority.value + retry_count
        item = (priority_value, time.time(), document, retry_count)

        if retry_count > 0:
            self.retry_queue.put(item)
        elif priority == DocumentPriority.CRITICAL:
            self.high_priority.put(item)
        elif priority in (DocumentPriority.HIGH, DocumentPriority.MEDIUM):
            self.medium_priority.put(item)
        else:
            self.low_priority.put(item)

    def get_next_batch(self, batch_size: int, current_error_rate: float) -> List[Dict]:
        batch: List[Dict] = []

        if current_error_rate > 0.2:
            batch_size = max(1, batch_size // 3)
        elif current_error_rate > 0.1:
            batch_size = max(2, batch_size // 2)

        while not self.retry_queue.empty() and len(batch) < batch_size:
            _, _, doc, retry = self.retry_queue.get()
            if retry < 3:
                batch.append({"doc": doc, "retry_count": retry})

        for q in (self.high_priority, self.medium_priority, self.low_priority):
            while not q.empty() and len(batch) < batch_size:
                _, _, doc, retry = q.get()
                batch.append({"doc": doc, "retry_count": retry})

        return batch

    def get_queue_stats(self) -> Dict[str, int]:
        return {
            "high_priority": self.high_priority.qsize(),
            "medium_priority": self.medium_priority.qsize(),
            "low_priority": self.low_priority.qsize(),
            "retry_queue": self.retry_queue.qsize(),
            "total": self.high_priority.qsize() + self.medium_priority.qsize() + self.low_priority.qsize() + self.retry_queue.qsize(),
        }


class CheckpointManager:
    """File-based checkpointing for resume capability."""
    def __init__(self, checkpoint_file: str = "download_progress.json"):
        self.checkpoint_file = checkpoint_file
        self.lock = threading.Lock()
        self.progress = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
        return {}

    def save_progress(self, company_id: str, status: str, documents: Optional[List[str]] = None, error: Optional[str] = None):
        with self.lock:
            self.progress[company_id] = {
                "status": status,
                "documents": documents or [],
                "timestamp": time.time(),
                "error": error,
            }
            try:
                with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                    json.dump(self.progress, f, indent=2)
            except Exception as e:
                print(f"Error saving checkpoint: {e}")

    def is_completed(self, company_id: str) -> bool:
        return self.progress.get(company_id, {}).get("status") == "completed"

    def get_failed_companies(self) -> List[str]:
        return [cid for cid, data in self.progress.items() if data.get("status") == "failed"]

    def get_stats(self) -> Dict[str, int]:
        completed = sum(1 for d in self.progress.values() if d.get("status") == "completed")
        failed = sum(1 for d in self.progress.values() if d.get("status") == "failed")
        in_progress = sum(1 for d in self.progress.values() if d.get("status") == "in_progress")
        return {
            "total_tracked": len(self.progress),
            "completed": completed,
            "failed": failed,
            "in_progress": in_progress,
        }


# =============================================================================
# INTELLIGENT MULTI-LAYER CACHE
# =============================================================================

class CacheLevel(Enum):
    MEMORY = 1
    DISK = 2
    DATABASE = 3


@dataclass
class CacheEntry:
    key: str
    value: Any
    timestamp: float
    ttl: Optional[float] = None
    access_count: int = 0
    last_accessed: float = 0.0
    size_bytes: int = 0
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if not self.last_accessed:
            self.last_accessed = self.timestamp

    def is_expired(self) -> bool:
        return bool(self.ttl) and (time.time() - self.timestamp) > float(self.ttl)

    def touch(self):
        self.access_count += 1
        self.last_accessed = time.time()


@dataclass
class CacheConfig:
    enable_memory_cache: bool = True
    enable_disk_cache: bool = True
    enable_database_cache: bool = True

    memory_cache_size_mb: int = 256
    memory_default_ttl_s: int = 7 * 24 * 3600

    disk_cache_size_mb: int = 1024
    disk_default_ttl_s: int = 7 * 24 * 3600
    cache_base_path: str = "cache"

    database_file: str = "insolvency_cache.db"

    company_data_ttl: int = 7 * 24 * 3600
    document_metadata_ttl: int = 3 * 24 * 3600

    cleanup_interval_hours: int = 24
    max_cache_age_days: int = 30
    compression_enabled: bool = True

    allow_cache_override: bool = True
    force_refresh_companies: bool = False
    force_redownload_documents: bool = False


class IntelligentCacheManager:
    """World-class multi-layer cache: memory + disk + SQLite (documents)."""
    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self.base_path = Path(self.config.cache_base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        self.memory_cache: Dict[str, CacheEntry] = {}
        self.memory_size_bytes = 0
        self.lock = threading.RLock()
        self.stats = defaultdict(int)

        # Disk layer
        self.disk_path = self.base_path / "disk_cache"
        if self.config.enable_disk_cache:
            self.disk_path.mkdir(parents=True, exist_ok=True)

        # Database layer
        self.db_path = self.base_path / self.config.database_file
        if self.config.enable_database_cache:
            self._init_db()

        self._last_cleanup = 0.0
        print(f"Intelligent Cache initialized: Memory + Disk + DB at '{self.base_path}'")

    # ---------- DB ----------
    def _init_db(self):
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            # Speed up SQLite
            cur.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS company_cache (
                    company_number TEXT PRIMARY KEY,
                    company_name TEXT,
                    notice_type TEXT,
                    filing_history_html TEXT,
                    document_links_json TEXT,
                    last_updated REAL,
                    ttl REAL,
                    success INTEGER DEFAULT 1,
                    error_message TEXT,
                    metadata_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS document_cache (
                    document_url TEXT PRIMARY KEY,
                    company_number TEXT,
                    document_type TEXT,
                    file_path TEXT,
                    file_size INTEGER,
                    download_date REAL,
                    checksum TEXT,
                    success INTEGER DEFAULT 1,
                    error_message TEXT,
                    metadata_json TEXT
                )
                """
            )
            # Helpful indices
            cur.execute("CREATE INDEX IF NOT EXISTS idx_company_updated ON company_cache(last_updated)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_company ON document_cache(company_number)")
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"DB init error: {e}")
            self.config.enable_database_cache = False

    # ---------- keys/size ----------
    def _key(self, prefix: str, *parts: Any) -> str:
        base = prefix + "_" + "_".join(str(p) for p in parts)
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

    def _size(self, value: Any) -> int:
        try:
            if isinstance(value, (dict, list)):
                return len(json.dumps(value, default=str).encode("utf-8"))
            if isinstance(value, str):
                return len(value.encode("utf-8"))
            if isinstance(value, bytes):
                return len(value)
            return len(pickle.dumps(value))
        except Exception:
            return 1024

    # ---------- helpers ----------
    def _coerce_cache_entry(self, payload: Any, key: str) -> CacheEntry:
        """
        Normalise historical cache payloads (dicts, dataclasses with slots, legacy CacheEntry).
        Ensures we always end up with a modern CacheEntry instance.
        """
        if isinstance(payload, CacheEntry):
            entry = payload
        else:
            data: Dict[str, Any]
            if isinstance(payload, dict):
                data = dict(payload)
            elif hasattr(payload, "__dict__"):
                data = dict(vars(payload))
            elif hasattr(payload, "__slots__"):
                data = {slot: getattr(payload, slot, None) for slot in getattr(payload, "__slots__", [])}
            else:
                raise TypeError(f"Unsupported cache payload type: {type(payload)}")

            # Normalise required fields
            data.setdefault("key", key)
            data.setdefault("timestamp", data.get("timestamp") or time.time())
            data.setdefault("ttl", data.get("ttl"))
            data.setdefault("access_count", data.get("access_count", 0))
            ts = data["timestamp"]
            data.setdefault("last_accessed", data.get("last_accessed") or ts)
            if "value" not in data and hasattr(payload, "value"):
                data["value"] = getattr(payload, "value")
            value = data.get("value")
            data.setdefault("size_bytes", data.get("size_bytes") or self._size(value))
            data.setdefault("metadata", data.get("metadata") or {})
            entry = CacheEntry(**data)

        # Final guard rails for older snapshots
        if not entry.key:
            entry.key = key
        if entry.metadata is None:
            entry.metadata = {}
        if not entry.last_accessed:
            entry.last_accessed = entry.timestamp
        return entry

    # ---------- Memory ----------
    def _mem_get(self, key: str) -> Optional[CacheEntry]:
        if not self.config.enable_memory_cache:
            return None
        with self.lock:
            entry = self.memory_cache.get(key)
            if entry:
                if entry.is_expired():
                    self.memory_size_bytes -= entry.size_bytes
                    del self.memory_cache[key]
                    self.stats["mem_expired"] += 1
                    return None
                entry.touch()
                self.stats["mem_hits"] += 1
                return entry
            self.stats["mem_miss"] += 1
        return None

    def _mem_set(self, key: str, value: Any, ttl: Optional[float], metadata: Optional[Dict[str, Any]] = None):
        if not self.config.enable_memory_cache:
            return
        with self.lock:
            size = self._size(value)
            limit = self.config.memory_cache_size_mb * 1024 * 1024
            while self.memory_size_bytes + size > limit and self.memory_cache:
                # LRU eviction
                lru_key = min(self.memory_cache.keys(), key=lambda k: self.memory_cache[k].last_accessed)
                self.memory_size_bytes -= self.memory_cache[lru_key].size_bytes
                del self.memory_cache[lru_key]
                self.stats["mem_evict"] += 1

            entry = CacheEntry(key=key, value=value, timestamp=time.time(), ttl=ttl, size_bytes=size, metadata=metadata or {})
            self.memory_cache[key] = entry
            self.memory_size_bytes += size
            self.stats["mem_set"] += 1

    # ---------- Disk ----------
    def _disk_file(self, key: str) -> Path:
        return self.disk_path / f"{key}.cache"

    def _disk_get(self, key: str) -> Optional[CacheEntry]:
        if not self.config.enable_disk_cache:
            return None
        try:
            f = self._disk_file(key)
            if not f.exists():
                self.stats["disk_miss"] += 1
                return None
            raw = f.read_bytes()
            data = gzip.decompress(raw) if self.config.compression_enabled else raw
            loaded = pickle.loads(data)
            entry = self._coerce_cache_entry(loaded, key)
            if entry.is_expired():
                f.unlink(missing_ok=True)
                self.stats["disk_expired"] += 1
                return None
            # Promote
            self._mem_set(key, entry.value, entry.ttl, entry.metadata)
            self.stats["disk_hit"] += 1
            return entry
        except Exception as e:
            print(f"Disk get error {key}: {e}")
            self.stats["disk_error"] += 1
            return None

    def _disk_set(self, key: str, value: Any, ttl: Optional[float], metadata: Optional[Dict[str, Any]] = None):
        if not self.config.enable_disk_cache:
            return
        try:
            entry = CacheEntry(key=key, value=value, timestamp=time.time(), ttl=ttl, size_bytes=self._size(value), metadata=metadata or {})
            payload = asdict(entry)
            data = pickle.dumps(payload)
            if self.config.compression_enabled:
                data = gzip.compress(data)
            self._disk_file(key).write_bytes(data)
            self.stats["disk_set"] += 1
        except Exception as e:
            print(f"Disk set error {key}: {e}")
            self.stats["disk_error"] += 1

    # ---------- DB (companies) ----------
    def _db_get_company(self, company_number: str) -> Optional[Dict]:
        if not self.config.enable_database_cache:
            return None
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                """
                SELECT company_name, notice_type, filing_history_html, document_links_json,
                       last_updated, ttl, success, error_message, metadata_json
                FROM company_cache WHERE company_number=?
                """,
                (company_number,),
            )
            row = cur.fetchone()
            conn.close()
            if not row:
                self.stats["db_company_miss"] += 1
                return None

            last_updated, ttl = row[4], row[5]
            if ttl and (time.time() - last_updated) > ttl:
                self._db_delete_company(company_number)
                self.stats["db_company_expired"] += 1
                return None

            self.stats["db_company_hit"] += 1
            return {
                "company_name": row[0],
                "notice_type": row[1],
                "filing_history_html": row[2],
                "document_links": json.loads(row[3]) if row[3] else [],
                "success": bool(row[6]),
                "error_message": row[7],
                "metadata": json.loads(row[8]) if row[8] else {},
            }
        except Exception as e:
            print(f"DB get company error: {e}")
            self.stats["db_error"] += 1
            return None

    def _db_set_company(
        self,
        company_number: str,
        company_name: str,
        notice_type: str,
        filing_history_html: Optional[str],
        document_links: Optional[List[Dict]],
        success: bool,
        error_message: Optional[str],
        metadata: Optional[Dict],
    ):
        if not self.config.enable_database_cache:
            return
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO company_cache
                (company_number, company_name, notice_type, filing_history_html, document_links_json,
                 last_updated, ttl, success, error_message, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company_number,
                    company_name,
                    notice_type,
                    filing_history_html,
                    json.dumps(document_links or []),
                    time.time(),
                    self.config.company_data_ttl,
                    1 if success else 0,
                    error_message,
                    json.dumps(metadata or {}),
                ),
            )
            conn.commit()
            conn.close()
            self.stats["db_company_set"] += 1
        except Exception as e:
            print(f"DB set company error: {e}")
            self.stats["db_error"] += 1

    def _db_delete_company(self, company_number: str):
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute("DELETE FROM company_cache WHERE company_number=?", (company_number,))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"DB del company error: {e}")

    # ---------- DB (documents) ----------
    def _db_get_document(self, document_url: str) -> Optional[Dict]:
        if not self.config.enable_database_cache:
            return None
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                """
                SELECT company_number, document_type, file_path, file_size, download_date,
                       checksum, success, error_message, metadata_json
                FROM document_cache WHERE document_url=?
                """,
                (document_url,),
            )
            row = cur.fetchone()
            conn.close()
            if not row:
                self.stats["db_doc_miss"] += 1
                return None
            info = {
                "company_number": row[0],
                "document_type": row[1],
                "file_path": row[2],
                "file_size": row[3],
                "download_date": row[4],
                "checksum": row[5],
                "success": bool(row[6]),
                "error_message": row[7],
                "metadata": json.loads(row[8]) if row[8] else {},
            }
            self.stats["db_doc_hit"] += 1
            return info
        except Exception as e:
            print(f"DB get doc error: {e}")
            self.stats["db_error"] += 1
            return None

    def _db_set_document(
        self,
        document_url: str,
        company_number: str,
        document_type: str,
        file_path: Optional[str],
        success: bool,
        error_message: Optional[str],
        metadata: Optional[Dict],
    ):
        if not self.config.enable_database_cache:
            return
        try:
            size = None
            checksum = None
            if success and file_path and Path(file_path).exists():
                p = Path(file_path)
                size = p.stat().st_size
                checksum = self._sha256(p)

            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO document_cache
                (document_url, company_number, document_type, file_path, file_size,
                 download_date, checksum, success, error_message, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document_url,
                    company_number,
                    document_type,
                    file_path,
                    size,
                    time.time(),
                    checksum,
                    1 if success else 0,
                    error_message,
                    json.dumps(metadata or {}),
                ),
            )
            conn.commit()
            conn.close()
            self.stats["db_doc_set"] += 1
        except Exception as e:
            print(f"DB set doc error: {e}")
            self.stats["db_error"] += 1

    def count_cached_documents(self, company_number: str) -> int:
        """
        Return number of successful cached documents for a company (DB-backed).
        """
        if not company_number or not self.config.enable_database_cache:
            return 0
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM document_cache WHERE company_number=? AND success=1",
                (company_number,),
            )
            row = cur.fetchone()
            conn.close()
            return int(row[0] or 0) if row else 0
        except Exception as e:
            print(f"DB count doc error: {e}")
            self.stats["db_error"] += 1
            return 0

    def _sha256(self, path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    # ---------- Public API ----------
    def is_company_cached(self, company_number: str, respect_force_redownload: bool = True) -> Tuple[bool, Optional[Dict]]:
        if respect_force_redownload and self.config.force_refresh_companies:
            return False, None

        key = self._key("company", company_number)
        e = self._mem_get(key)
        if e:
            return True, e.value

        e = self._disk_get(key)
        if e:
            return True, e.value

        db = self._db_get_company(company_number)
        if db:
            self._mem_set(key, db, self.config.company_data_ttl)
            self._disk_set(key, db, self.config.company_data_ttl)
            return True, db

        return False, None

    def find_company_by_name(self, company_name: str) -> Optional[Dict]:
        """
        Case-insensitive lookup of a cached company record by name.
        Returns the cached payload (same shape as is_company_cached) with company_number injected.
        """
        if not company_name or not self.config.enable_database_cache:
            return None
        try:
            term = f"%{company_name.strip()}%"
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                """
                SELECT company_number FROM company_cache
                WHERE UPPER(company_name) LIKE UPPER(?)
                ORDER BY LENGTH(company_name)
                LIMIT 1
                """,
                (term,),
            )
            row = cur.fetchone()
            conn.close()
            if row and row[0]:
                ok, data = self.is_company_cached(row[0])
                if ok and data:
                    data = data.copy()
                    data["company_number"] = row[0]
                    return data
            # fallback: try metadata lookup
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute(
                """
                SELECT company_number FROM company_cache
                WHERE metadata_json LIKE ?
                LIMIT 1
                """,
                (f'%"{company_name.strip()}"%',),
            )
            row = cur.fetchone()
            conn.close()
            if row and row[0]:
                ok, data = self.is_company_cached(row[0])
                if ok and data:
                    data = data.copy()
                    data["company_number"] = row[0]
                    return data
            return None
        except Exception as e:
            print(f"find_company_by_name error: {e}")
            return None

    def cache_company(
        self,
        company_number: str,
        company_name: str,
        notice_type: str,
        filing_history_html: Optional[str],
        document_links: List[Dict],
        success: bool = True,
        error_message: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        value = {
            "company_name": company_name,
            "notice_type": notice_type,
            "filing_history_html": filing_history_html,
            "document_links": document_links or [],
            "success": success,
            "error_message": error_message,
            "metadata": metadata or {},
        }
        key = self._key("company", company_number)
        self._mem_set(key, value, self.config.company_data_ttl, metadata)
        self._disk_set(key, value, self.config.company_data_ttl, metadata)
        self._db_set_company(
            company_number, company_name, notice_type, filing_history_html, document_links, success, error_message, metadata
        )

    def is_document_cached(self, document_url: str, company_number: str, respect_force_redownload: bool = True) -> Tuple[bool, Optional[str]]:
        if respect_force_redownload and self.config.force_redownload_documents:
            return False, None
        info = self._db_get_document(document_url)
        if not info or not info.get("success"):
            return False, None
        path = info.get("file_path")
        # Treat Drive uploads as cached (no local file needed)
        if path and isinstance(path, str) and path.startswith("gdrive://"):
            return True, path
        # legacy local-file path behavior
        if not path or not Path(path).exists():
            return False, None
        if info.get("checksum"):
            if self._sha256(Path(path)) != info["checksum"]:
                return False, None
        return True, path

    def cache_document(
        self,
        document_url: str,
        company_number: str,
        document_type: str,
        file_path: Optional[str],
        success: bool,
        error_message: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        self._db_set_document(document_url, company_number, document_type, file_path, success, error_message, metadata)

    def cleanup(self, force: bool = False) -> Dict[str, float]:
        now = time.time()
        if not force and (now - self._last_cleanup) < self.config.cleanup_interval_hours * 3600:
            return {}
        freed_mb = 0.0

        # Memory: drop expired
        with self.lock:
            expired = [k for k, v in self.memory_cache.items() if v.is_expired()]
            for k in expired:
                self.memory_size_bytes -= self.memory_cache[k].size_bytes
                del self.memory_cache[k]

        # Disk: age-based cleanup
        if self.config.enable_disk_cache:
            for f in self.disk_path.glob("*.cache"):
                try:
                    age = now - f.stat().st_mtime
                    if age > self.config.max_cache_age_days * 86400:
                        size = f.stat().st_size
                        f.unlink(missing_ok=True)
                        freed_mb += size / (1024 * 1024)
                except Exception as e:
                    print(f"Disk cleanup error {f}: {e}")

        self._last_cleanup = now
        return {"space_freed_mb": freed_mb}

    def stats_snapshot(self) -> Dict[str, Any]:
        d = dict(self.stats)
        d["mem_entries"] = len(self.memory_cache)
        d["mem_size_mb"] = self.memory_size_bytes / (1024 * 1024)
        if self.config.enable_database_cache and self.db_path.exists():
            d["db_size_mb"] = self.db_path.stat().st_size / (1024 * 1024)
        return d

    def get_company_document_manifest(self, company_number: str) -> list:
        """
        Get list of all cached document IDs for a company.
        Returns: List of cache keys for cached documents
        """
        if not self.db_conn:
            return []
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT cache_key, document_type, created_at
                FROM cache_index
                WHERE cache_key LIKE ?
                AND cached = 1
                ORDER BY created_at DESC
            """, (f"%{company_number}%",))
            
            results = cursor.fetchall()
            return [row[0] for row in results]
        except Exception as e:
            self.log(f"Error getting company manifest: {e}")
            return []
    
    def is_company_fully_cached(self, company_number: str, max_documents: int = 50) -> tuple[bool, int]:
        """
        Check if a company is fully cached (all documents available).
        
        Args:
            company_number: Company number to check
            max_documents: Maximum expected documents (to avoid false positives)
        
        Returns:
            (is_fully_cached, cached_count): 
                - is_fully_cached: True if company appears to be complete in cache
                - cached_count: Number of documents found in cache
        """
        if not self.db_conn:
            return False, 0
        
        try:
            # Get all cached documents for this company
            manifest = self.get_company_document_manifest(company_number)
            cached_count = len(manifest)
            
            # If we have no cached docs, definitely not fully cached
            if cached_count == 0:
                return False, 0
            
            # Check if we have a company-level success marker
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT success, metadata
                FROM cache_index
                WHERE cache_key = ?
            """, (f"company://{company_number}",))
            
            row = cursor.fetchone()
            
            if row:
                success = row[0]
                # If company was previously marked as successful, trust it
                if success:
                    return True, cached_count
            
            # Heuristic: If we have a reasonable number of docs (3+), likely complete
            # This catches companies processed in old runs before company-level marker
            if cached_count >= 3:
                return True, cached_count
            
            # Conservative: If only 1-2 docs, might be incomplete
            return False, cached_count
            
        except Exception as e:
            self.log(f"Error checking company cache: {e}")
            return False, 0
# =============================================================================
# GAZETTE SCRAPER
# =============================================================================

class GazetteInsolvencyScraper:
    def __init__(self, verbose: bool = True):
        self.base_url = "https://www.thegazette.co.uk"
        self.session = requests.Session()
        self.verbose = verbose
        boost_session(
            self.session,
            ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        )
        self.rate_limiter = AdaptiveRateLimiter(base_delay=0.0, max_delay=5.0)
        self.category_mappings = {
            "administration": "G305010100",
            "cvl": "G305010200",
            "insolvency_practitioner_applications": "G405010001",
            "liquidation_by_court": "G305010300",
            "mvl": "G305010400",
            "moratoria": "G405010002",
            "notices_of_dividends": "G405010003",
            "ocin": "G405010004",
            "otcbi": "G405010005",
            "qualifying_decision_procedure": "G405010007",
            "re_use_of_a_prohibited_name": "G405010006",
            "receivership": "G305010500",
        }

    def log(self, msg: str, level: str = "INFO"):
        if self.verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {level}: {msg}")

    def _resolve_categories(self, categories: Optional[List[str]]) -> List[str]:
        """
        Accept either:
        - human keys (e.g., 'administration', 'cvl', 'liquidation_by_court'), or
        - Gazette codes (e.g., 'G305010100')
        Return a de-duplicated list of *uppercase* Gazette codes.
        """
        if not categories:
            # default (your previous behavior)
            return ["G305010100", "G305010200", "G305010300"]

        resolved: List[str] = []
        for cat in categories:
            raw = (cat or "").strip()
            if not raw:
                continue

            lower = raw.lower()
            # If they passed a taxonomy key, map via category_mappings
            if lower in self.category_mappings:
                resolved.append(self.category_mappings[lower])
                continue

            # If they passed a Gazette code, accept 'G...' or 'g...' (don’t break on lowercase)
            if (raw.startswith("G") or raw.startswith("g")) and len(raw) == 10:
                resolved.append(raw.upper())
                continue

            # Otherwise, warn once per bad input
            self.log(f"Unknown category: {cat}", "WARNING")

        return sorted(set(resolved))

    def _build_search_url(self, start_date: str, end_date: str, categories: List[str], page: int = 1) -> str:
        base = f"{self.base_url}/insolvency/notice"
        params = ["text="]
        params += [f"categorycode={c}" for c in categories]
        params.append("categorycode=-2")
        params += [
            "location-postcode-1=",
            "location-distance-1=1",
            "location-local-authority-1=",
            "numberOfLocationSearches=1",
            f"start-publish-date={start_date.replace('/', '%2F')}",
            f"end-publish-date={end_date.replace('/', '%2F')}",
            "edition=",
            "london-issue=",
            "edinburgh-issue=",
            "belfast-issue=",
            "sort-by=",
            "results-page-size=10",
            f"results-page={page}",
        ]
        return f"{base}?{'&'.join(params)}"

    def _extract_total_count(self, soup: BeautifulSoup) -> int:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"(\d+)\s*-\s*(\d+)\s*of\s*(\d+)", text)
        if m:
            return int(m.group(3))
        m2 = re.search(r"of\s+(\d+)\s+notices?", text, re.IGNORECASE)
        return int(m2.group(1)) if m2 else 0

    def _extract_company_from_article(self, article) -> Optional[Dict]:
        title_link = article.select_one("h3.title a")
        if not title_link:
            return None
        company_name = title_link.get_text(strip=True)
        href = title_link.get("href", "")
        notice_url = urljoin(self.base_url, href)
        notice_id = ""
        m = re.search(r"/notice/(\d+)", href)
        if m:
            notice_id = m.group(1)
        pub_date = ""
        t = article.select_one("time[datetime]")
        if t:
            pub_date = t.get("datetime", "") or t.get_text(strip=True)
        notice_type = ""
        t2 = article.select_one("dl.metadata.notice-type dd")
        if t2:
            notice_type = t2.get_text(strip=True)
        company_number_from_summary = ""
        p = article.select_one("div.content p")
        if p:
            txt = p.get_text(" ")
            # Debug logging for this specific case
            if "AVONDALE ENVIRONMENTAL" in txt.upper():
                print(f"DEBUG AVONDALE - Full text: {txt}")
                print(f"DEBUG AVONDALE - Company number search area: {txt[txt.lower().find('company'):txt.lower().find('company')+50] if 'company' in txt.lower() else 'Not found'}")

            m2 = re.search(r"Company Number[:\s]*([A-Z0-9]{7,8})", txt, re.IGNORECASE)
            if m2:
                company_number_from_summary = m2.group(1)
                # Debug for this specific case
                if "AVONDALE" in txt.upper():
                    print(f"DEBUG AVONDALE - Regex matched: '{company_number_from_summary}'")
                    print(f"DEBUG AVONDALE - Full match: '{m2.group(0)}'")
        return {
            "company_name": company_name,
            "company_number": company_number_from_summary,
            "notice_id": notice_id,
            "notice_type": notice_type,
            "publication_date": pub_date,
            "notice_url": notice_url,
        }

    def _extract_companies_from_page(self, soup: BeautifulSoup) -> List[Dict]:
        companies: List[Dict] = []
        for article in soup.find_all("article", id=lambda x: x and x.startswith("item-")):
            try:
                info = self._extract_company_from_article(article)
                if info:
                    companies.append(info)
            except Exception as e:
                self.log(f"Article parse error: {e}", "ERROR")
        return companies

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"(\d+)\s*-\s*(\d+)\s*of\s*(\d+)", text)
        if m:
            end_result = int(m.group(2))
            total = int(m.group(3))
            self.log(f"Page {current_page}: showing up to {end_result} of {total}")
            return end_result < total
        links = soup.select('a[href*="results-page="]')
        max_page = 0
        for a in links:
            mm = re.search(r"results-page=(\d+)", a.get("href", ""))
            if mm:
                max_page = max(max_page, int(mm.group(1)))
        if max_page:
            return current_page < max_page
        articles = soup.find_all("article", id=lambda x: x and x.startswith("item-"))
        return len(articles) >= 10
    
    def _fetch_company_number(self, notice_url: str) -> str:
        sess = tls_session(self.session.headers, self.session.cookies)
        try:
            r = sess.get(notice_url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, "html.parser")
            
            # PRIORITY 1: Look for company links first (most reliable)
            company_links = soup.find_all("a", href=re.compile(r"/company/\d+"))
            if company_links:
                for link in company_links:
                    href = link.get("href", "")
                    m = re.search(r"/company/(\d+)", href)
                    if m:
                        number = m.group(1)
                        # Ensure 8-digit format for English companies
                        if len(number) < 8 and number.isdigit():
                            number = number.zfill(8)
                        return number
            
            # PRIORITY 2: Look for Scottish companies (SC prefix)
            sc_links = soup.find_all("a", href=re.compile(r"/company/SC\d+"))
            if sc_links:
                for link in sc_links:
                    href = link.get("href", "")
                    m = re.search(r"/company/(SC\d+)", href)
                    if m:
                        return m.group(1)
            
            # PRIORITY 3: Fallback to text extraction (least reliable)
            page_text = soup.get_text(" ", strip=True)
            for pat in [
                r"Company Number[:\s]*([A-Z]{2}\d{6})",    # Scottish
                r"Company Number[:\s]*(\d{8})",           # 8-digit
                r"Company Number[:\s]*(\d{6,7})",         # 6-7 digit
            ]:
                m = re.search(pat, page_text, re.IGNORECASE)
                if m:
                    found = m.group(1)
                    if found.isdigit() and len(found) < 8:
                        found = found.zfill(8)
                    return found
                    
            return ""
            
        except Exception as e:
            self.log(f"Company number fetch error: {e}", "ERROR")
            return ""

    def search_insolvencies(
        self,
        start_date: str,
        end_date: str,
        categories: Optional[List[str]] = None,
        max_pages: int = 10,
        fetch_company_numbers: bool = True,
    ) -> Dict:
        resolved = self._resolve_categories(categories)
        self.log("=" * 80)
        self.log("STARTING GAZETTE INSOLVENCY SEARCH")
        self.log(f"Date Range: {start_date} -> {end_date}")
        self.log(f"Categories: {resolved}")
        self.log("=" * 80)

        all_companies: List[Dict] = []
        processed_notice_ids = set()
        validation = {
            "search_url": "",
            "total_notices_on_website": 0,
            "pages_processed": 0,
            "unique_companies_found": 0,
            "duplicate_notices_skipped": 0,
            "errors_encountered": 0,
            "processing_time_seconds": 0,
        }

        t0 = time.time()
        page = 1
        empty_pages = 0


        while page <= max_pages:
            sess = tls_session(self.session.headers, self.session.cookies)
            try:
                url = self._build_search_url(start_date, end_date, resolved, page)
                if page == 1:
                    validation["search_url"] = url
                # before sess.get(url, ...)
                get_gazette_bucket().acquire()

                r = sess.get(url, timeout=30)
                r.raise_for_status()
                try:
                    soup = BeautifulSoup(r.content, "lxml")
                except Exception:
                    soup = BeautifulSoup(r.content, "html.parser")

                if page == 1:
                    validation["total_notices_on_website"] = self._extract_total_count(soup)

                companies = self._extract_companies_from_page(soup)
                if not companies:
                    empty_pages += 1
                    if empty_pages >= 3:
                        self.log("3 consecutive empty pages; stopping", "WARNING")
                        break
                    page += 1
                    continue
                empty_pages = 0

                new_companies = []
                for c in companies:
                    nid = c.get("notice_id", "")
                    if nid and nid not in processed_notice_ids:
                        new_companies.append(c)
                        processed_notice_ids.add(nid)
                    elif nid:
                        validation["duplicate_notices_skipped"] += 1

                if fetch_company_numbers:
                    for i, c in enumerate(new_companies):
                        try:
                            current_number = c.get("company_number", "")
                            needs_refetch = (
                                not current_number
                                or (current_number.isdigit() and len(current_number) < 8)
                                or current_number.endswith("...")
                                or len(current_number) < 6
                            )
                            if needs_refetch:
                                self.log(f"Fetching full notice for: {c.get('company_name','Unknown')} (invalid: '{current_number}')")
                                c["company_number"] = self._fetch_company_number(c["notice_url"])
                            else:
                                self.log(f"Already has valid number: {current_number}")

                            # optional metric (safe)
                            if hasattr(self, "rate_limiter"):
                                try: self.rate_limiter.record_success(0)
                                except: pass

                            # pacing token before next fetch/page
                            get_gazette_bucket().acquire()

                        except Exception as e:
                            self.log(f"Company number error: {e}", "ERROR")
                            # Only blank it if we *still* have no number
                            if not c.get("company_number"):
                                c["company_number"] = ""
                            validation["errors_encountered"] += 1

                # if fetch_company_numbers:
                #     for i, c in enumerate(new_companies):
                #         try:
                #             if (i + 1) % 5 == 0:
                #                 self.log(f"Fetching company number {i+1}/{len(new_companies)}")
                #             c["company_number"] = c.get("company_number") or self._fetch_company_number(c["notice_url"])
                #             time.sleep(0.4)
                #         except Exception as e:
                #             self.log(f"Company number error: {e}", "ERROR")
                #             c["company_number"] = ""

                all_companies.extend(new_companies)
                validation["pages_processed"] = page

                if not self._has_next_page(soup, page):
                    break
                page += 1

                self.rate_limiter.record_success(0)  # keep stats ticking

                # Acquire 1 token before the next page fetch
                get_gazette_bucket().acquire()
            except Exception as e:
                self.log(f"Page error: {e}", "ERROR")
                page += 1
                if validation["errors_encountered"] > 5:
                    self.log("Too many errors; stopping", "ERROR")
                    break
            finally:
                try: sess.close()
                except: pass

        validation["processing_time_seconds"] = round(time.time() - t0, 2)
        validation["unique_companies_found"] = len(all_companies)
        self.log("SEARCH COMPLETE")
        return {
            "companies": all_companies,
            "validation": validation,
            "categories_searched": resolved,
            "search_parameters": {
                "start_date": start_date,
                "end_date": end_date,
                "max_pages": max_pages,
                "fetch_company_numbers": fetch_company_numbers,
            },
        }
    
    def search_insolvencies_streaming(
        self,
        start_date: str,
        end_date: str,
        categories: Optional[List[str]] = None,
        max_pages: int = 10,
        fetch_company_numbers: bool = True,
        company_queue: "queue.Queue" = None,
        cancel_event: "threading.Event" = None,
        progress_callback: Callable = None
    ) -> Dict:
        """
        Streaming version that pushes companies to queue as they're discovered.
        Enables parallel downloading while scraping continues.
        """
        
        resolved = self._resolve_categories(categories)
        self.log("=" * 80)
        self.log("STARTING GAZETTE INSOLVENCY SEARCH (STREAMING)")
        self.log(f"Date Range: {start_date} -> {end_date}")
        self.log(f"Categories: {resolved}")
        self.log("=" * 80)

        stats = {
            "total_pages": 0,
            "total_companies": 0,
            "companies_with_numbers": 0,
            "elapsed_time": 0.0
        }

        t0 = time.time()
        page = 1
        empty_pages = 0
        processed_notice_ids = set()

        while page <= max_pages:
            if cancel_event and cancel_event.is_set():
                break

            sess = tls_session(self.session.headers, self.session.cookies)
            try:
                url = self._build_search_url(start_date, end_date, resolved, page)
                get_gazette_bucket().acquire()
                
                r = sess.get(url, timeout=30)
                r.raise_for_status()
                
                try:
                    soup = BeautifulSoup(r.content, "lxml")
                except Exception:
                    soup = BeautifulSoup(r.content, "html.parser")

                companies = self._extract_companies_from_page(soup)
                
                if not companies:
                    empty_pages += 1
                    if empty_pages >= 3:
                        self.log("3 consecutive empty pages; stopping", "WARNING")
                        break
                    page += 1
                    continue
                
                empty_pages = 0
                stats["total_pages"] = page

                # Filter duplicates and stream to queue
                stop_stream = False
                for c in companies:
                    if cancel_event and cancel_event.is_set():
                        stop_stream = True
                        break
                    nid = c.get("notice_id", "")
                    if nid and nid in processed_notice_ids:
                        continue
                    
                    if nid:
                        processed_notice_ids.add(nid)
                    
                    stats["total_companies"] += 1
                    
                    # Fetch company number if needed
                    if fetch_company_numbers:
                        current_number = c.get("company_number", "")
                        needs_refetch = (
                            not current_number
                            or (current_number.isdigit() and len(current_number) < 8)
                            or current_number.endswith("...")
                            or len(current_number) < 6
                        )
                        
                        if needs_refetch:
                            try:
                                c["company_number"] = self._fetch_company_number(c["notice_url"])
                                get_gazette_bucket().acquire()
                            except Exception as e:
                                self.log(f"Company number error: {e}", "ERROR")
                                if not c.get("company_number"):
                                    c["company_number"] = ""
                    
                    # Stream to queue if company has number
                    if c.get("company_number"):
                        stats["companies_with_numbers"] += 1
                        
                        if company_queue:
                            try:
                                company_queue.put(c, timeout=5.0)
                            except queue.Full:
                                self.log("Queue full, blocking...", "WARNING")
                                if cancel_event and cancel_event.is_set():
                                    stop_stream = True
                                    break
                                company_queue.put(c)  # Block until space
                        
                        # Progress callback
                        if progress_callback:
                            try:
                                progress_callback(page, stats["total_companies"])
                            except Exception:
                                pass
                if stop_stream:
                    break
                
                # Check for next page
                if not self._has_next_page(soup, page):
                    break
                
                page += 1
                get_gazette_bucket().acquire()
                
            except Exception as e:
                self.log(f"Page error: {e}", "ERROR")
                page += 1
            finally:
                try:
                    sess.close()
                except Exception:
                    pass

        # Signal end of stream
        if company_queue:
            company_queue.put(None)  # Sentinel

        stats["elapsed_time"] = time.time() - t0
        self.log("SEARCH COMPLETE (STREAMING)")
        return stats


# =============================================================================
# Companies House downloader

# Updated CompaniesHouseDocumentDownloader class with multi-page support
# This replaces the existing class in wc_insolvency_core.py

class CompaniesHouseDocumentDownloader:
    def __init__(self, verbose: bool = True):
        self.session = requests.Session()
        self.base_url = "https://find-and-update.company-information.service.gov.uk"
        self.verbose = verbose
        boost_session(
            self.session,
            ua="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        )

    def log(self, msg: str, level: str = "INFO"):
        if self.verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {level}: {msg}")

    def get_company_filing_history_page(self, company_number: str, page: int = 1):
        url = f"{self.base_url}/company/{company_number}/filing-history"
        if page > 1:
            url += f"?page={page}"

        sess = tls_session(self.session.headers, self.session.cookies)
        try:
            get_ch_web_bucket().acquire()
            r = sess.get(url, timeout=30)
            if r.status_code == 200:
                return BeautifulSoup(r.content, "html.parser")
            self.log(f"Filing history HTTP {r.status_code} for {company_number} page {page}", "ERROR")
            return None
        finally:
            sess.close()
    
    def get_company_name(self, company_number: str) -> str:
        """ 
        Fetch the Companies House company page and extract the <h1 class="heading-xlarge"> name.
        Falls back to the number if not found.
        """
        url = f"{self.base_url}/company/{company_number}"
        sess = tls_session(self.session.headers, self.session.cookies)
        try:
            # before sess.get(url, ...)
            get_ch_web_bucket().acquire()

            r = sess.get(url, timeout=30)
            if r.status_code != 200:
                self.log(f"Company name HTTP {r.status_code} for {company_number}", "ERROR")
                return company_number
            try:
                soup = BeautifulSoup(r.content, "lxml")
            except Exception:
                soup = BeautifulSoup(r.content, "html.parser")
            h1 = soup.select_one("h1.heading-xlarge")
            return h1.get_text(strip=True) if h1 and h1.get_text(strip=True) else company_number
        finally:
            sess.close()

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        """Check if there's a next page of filing history"""
        try:
            # Look for pagination links
            pagination = soup.find('nav', {'aria-label': 'Pagination'}) or soup.find('div', class_='pager')
            if not pagination:
                return False
            
            # Check for "Next" link
            next_link = pagination.find('a', string=re.compile(r'Next', re.IGNORECASE))
            if next_link:
                return True
            
            # Check for numbered page links higher than current page
            page_links = pagination.find_all('a', href=re.compile(r'page=(\d+)'))
            max_page = current_page
            for link in page_links:
                match = re.search(r'page=(\d+)', link.get('href', ''))
                if match:
                    page_num = int(match.group(1))
                    max_page = max(max_page, page_num)
            
            return max_page > current_page
            
        except Exception as e:
            self.log(f"Error checking for next page: {e}", "ERROR")
            return False

    def _extract_links_from_page(self, soup: BeautifulSoup, company_number: str, page_num: int) -> List[Dict]:
        """Extract document links from a single page"""
        links: List[Dict] = []
        
        table = soup.find("table", id="fhTable") or soup.find("table")
        if not table:
            self.log(f"No filing history table found on page {page_num}", "WARNING")
            return links

        tbody = table.find("tbody")
        rows = (tbody.find_all("tr") if tbody else table.find_all("tr"))
        data_rows = [r for r in rows if r.find_all("td")]
        
        if page_num == 1:  # Only log total for first page to avoid spam
            self.log(f"Found {len(data_rows)} filing entries on page {page_num}")

        for i, row in enumerate(data_rows):
            try:
                tds = row.find_all("td")
                if len(tds) < 2:
                    continue

                date = tds[0].get_text(strip=True)
                # Even better approach:
                if len(tds) > 2:
                    desc_cell = tds[2]
                    # Remove strong tags but keep the text
                    for strong in desc_cell.find_all('strong'):
                        strong.unwrap()
                    description = desc_cell.get_text(' ', strip=True)
                else:
                    description = "Unknown"
                # description = tds[2].get_text(strip=True) if len(tds) > 2 else "Unknown"
                dl_cell = tds[-1]
                a = dl_cell.find("a", href=True)
                if not a:
                    continue

                href = a["href"]
                
                # Skip non-downloadable / orderable "missing image" rows explicitly
                if "orderable/missing-image-deliveries" in href:
                    continue

                if "document" in href and ("pdf" in href or "format=" in href):
                    full_url = urljoin(self.base_url, href)
                    
                    filing_type = "UNKNOWN"
                    for c in tds:
                        tx = c.get_text(strip=True)
                        if len(tx) < 10 and tx.isupper():
                            filing_type = tx
                            break

                    pages = "1"
                    txt = dl_cell.get_text(" ", strip=True)
                    m = re.search(r"\((\d+)\s*page", txt, re.IGNORECASE)
                    if m:
                        pages = m.group(1)

                    links.append({
                        "url": full_url,
                        "date": date,
                        "type": filing_type,
                        "description": description,
                        "pages": pages,
                        "raw_href": href,
                        "source_page": page_num,  # Track which page this came from
                    })
                    
            except Exception as e:
                self.log(f"Row {i+1} parse error on page {page_num}: {e}", "ERROR")

        return links

    def find_document_links(self, soup: BeautifulSoup, company_number: str, need: int | None = None, max_pages: int = 100, page_pause: float = 0.0) -> list[dict]:
        """Collect document links across filing-history pages, with early stop.

        Args:
            soup: First page soup (already fetched).
            company_number: CH number.
            need: If provided, stop once at least this many links are collected.
            max_pages: Hard safety cap.
            page_pause: Optional per-page pause (defaults to 0; rely on global limiter).

        Returns:
            List of link dicts (de-duplicated by URL), newest/oldest ordering left to caller.
        """
        self.log(f"Finding document links for {company_number}...")
        all_links: list[dict] = []
        seen_urls: set[str] = set()

        # page 1
        page = 1
        links = self._extract_links_from_page(soup, company_number, page)
        for d in links:
            u = d.get("url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_links.append(d)
        if need and len(all_links) >= need:
            return all_links[:need]

        # fast path: single page
        if not self._has_next_page(soup, page):
            self.log(f"Single page: found {len(all_links)} downloadable documents")
            return all_links if not need else all_links[:need]

        # multi-page
        consecutive_empty = 0
        while self._has_next_page(soup, page) and page < max_pages:
            page += 1
            try:
                if page_pause:
                    time.sleep(page_pause)

                next_soup = self.get_company_filing_history_page(company_number, page)
                if not next_soup:
                    self.log(f"Failed to fetch page {page}, stopping", "WARNING")
                    break

                links = self._extract_links_from_page(next_soup, company_number, page)
                if links:
                    for d in links:
                        u = d.get("url")
                        if u and u not in seen_urls:
                            seen_urls.add(u)
                            all_links.append(d)
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        # two empty pages in a row—likely at the end
                        break

                # early stop once we have enough
                if need and len(all_links) >= need:
                    break

                soup = next_soup

            except Exception as e:
                self.log(f"Error processing page {page}: {e}", "ERROR")
                break

        self.log(f"Scanned up to page {page}, gathered {len(all_links)} documents")
        return all_links if not need else all_links[:need]


    def stream_document_links(self, soup: BeautifulSoup, company_number: str, on_batch, max_pages: int = 20,need: int | None = None, page_pause: float = 0.0):
        """
        Stream filing links page-by-page and call `on_batch(links_for_that_page)`
        immediately. Stops early if `need` reached.
        """
        collected = 0
        seen_urls: set[str] = set()

        # page 1
        page = 1
        links = self._extract_links_from_page(soup, company_number, page)
        page_links = []
        for d in links:
            u = d.get("url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                page_links.append(d)
        if page_links:
            on_batch(page_links)
            collected += len(page_links)
            if need and collected >= need:
                return

        # more pages (as needed)
        while self._has_next_page(soup, page) and page < max_pages:
            page += 1
            if page_pause:
                time.sleep(page_pause)

            next_soup = self.get_company_filing_history_page(company_number, page)
            if not next_soup:
                self.log(f"Failed to fetch page {page}, stopping", "WARNING")
                break

            links = self._extract_links_from_page(next_soup, company_number, page)
            page_links = []
            for d in links:
                u = d.get("url")
                if u and u not in seen_urls:
                    seen_urls.add(u)
                    page_links.append(d)

            if page_links:
                on_batch(page_links)
                collected += len(page_links)
                if need and collected >= need:
                    return

            soup = next_soup    

# =============================================================================
# Base + World-Class Pipelines (no caching)
# =============================================================================

class GazetteCompaniesHousePipeline:
    def __init__(self, base_download_folder="InsolvencyDocuments", verbose: bool = True):
        self.gazette_scraper = GazetteInsolvencyScraper(verbose=verbose)
        self.ch_downloader = CompaniesHouseDocumentDownloader(verbose=verbose)
        self.base_folder = Path(base_download_folder)
        self.base_folder.mkdir(parents=True, exist_ok=True)
        # FIX: create a session before boost_session
        self.session = requests.Session()
        boost_session(self.session, ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36")

        self.verbose = verbose
        self._current_search_categories: List[str] = []
        self.rate_limiter = AdaptiveRateLimiter(base_delay=0.0, max_delay=0.0)


    def log(self, msg: str, level: str = "INFO"):
        if self.verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {level}: {msg}")

    def sanitize_folder_name(self, name: str) -> str:
        s = re.sub(r'[<>:"/\\|?*]', "_", name)
        s = re.sub(r"[^\w\s\-_\.]", "_", s)
        s = re.sub(r"\s+", "_", s).strip("_.")
        return s[:100] or "Unknown_Company"

    def _classify_direct(self, procedure_type: str) -> str:
        mapping = {
            "creditors' voluntary liquidation": "CVL",
            "Creditors' Voluntary Liquidation": "CVL",
            "creditors voluntary liquidation": "CVL",
            "Creditors Voluntary Liquidation": "CVL",
            "annual liquidation meetings": "CVL",
            "Annual Liquidation Meetings": "CVL",
            "Annual liquidation meetings": "CVL",
            "Resolutions for Winding Up": "CVL",
            "resolutions for winding-up": "CVL",
            "Resolutions for Winding-up": "CVL",
            "meetings of creditors": "CVL",
            "Meetings of Creditors": "CVL",
            "notices to creditors": "CVL",
            "Notices to Creditors": "CVL",
            "appointment of liquidators": "CVL",
            "Appointment of Liquidators": "CVL",
            "final meetings": "CVL",
            "Final Meetings": "CVL",
            "winding-up orders": "Court_Liquidation",
            "Winding-Up Orders": "Court_Liquidation",
            "winding up orders": "Court_Liquidation",
            "Winding Up Orders": "Court_Liquidation",
            "petitions to wind up (companies)": "Court_Liquidation",
            "Petitions to Wind Up (Companies)": "Court_Liquidation",
            "petitions to wind up": "Court_Liquidation",
            "Petitions to Wind Up": "Court_Liquidation",
            "public examinations": "Court_Liquidation",
            "Public Examinations": "Court_Liquidation",
            "service of petition": "Court_Liquidation",
            "Service of Petition": "Court_Liquidation",
            "compulsory liquidation": "Court_Liquidation",
            "Compulsory Liquidation": "Court_Liquidation",
            "administration": "Administration",
            "Administration": "Administration",
            "appointment of administrators": "Administration",
            "Appointment of Administrators": "Administration",
            "administration orders": "Administration",
            "Administration Orders": "Administration",
            "notices to members": "Administration",
            "Notices to Members": "Administration",
            "members' voluntary liquidation": "MVL",
            "Members' Voluntary Liquidation": "MVL",
            "members voluntary liquidation": "MVL",
            "Members Voluntary Liquidation": "MVL",
            "resolution for voluntary winding-up": "MVL",
            "Resolution for Voluntary Winding-up": "MVL",
            "receivership": "Receivership",
            "Receivership": "Receivership",
            "appointment of administrative receivers": "Receivership",
            "Appointment of Administrative Receivers": "Receivership",
            "moratoria": "Moratoria",
            "Moratoria": "Moratoria",
            "notices of dividends": "Notices_of_Dividends",
            "Notices of Dividends": "Notices_of_Dividends",
            "overseas territories and dependencies": "Overseas_Territories",
            "Overseas Territories and Dependencies": "Overseas_Territories",
        }
        if procedure_type in mapping:
            return mapping[procedure_type]
        return self._pattern_based_classification(procedure_type)

    def _pattern_based_classification(self, procedure_type: str) -> str:
        t = procedure_type.lower()
        if any(x in t for x in ["winding-up order", "winding up order", "petition to wind up", "public examination", "compulsory liquidation"]):
            return "Court_Liquidation"
        if any(x in t for x in ["administration order", "appointment of administrators", "administrator appointed"]):
            return "Administration"
        if any(x in t for x in ["creditors' voluntary", "creditors voluntary", "resolution for winding up", "resolution for winding-up"]):
            return "CVL"
        if any(x in t for x in ["members' voluntary", "members voluntary", "resolution for voluntary winding"]):
            return "MVL"
        if any(x in t for x in ["liquidation", "liquidator", "winding"]):
            if any(y in t for y in ["petition", "order", "court", "compulsory"]):
                return "Court_Liquidation"
            if "members" in t:
                return "MVL"
            return "CVL"
        if any(x in t for x in ["creditors", "creditor"]):
            if any(y in t for y in ["administration", "administrator"]):
                return "Administration"
            return "CVL"
        if any(x in t for x in ["administration", "administrator"]):
            return "Administration"
        if any(x in t for x in ["receiver", "receivership"]):
            return "Receivership"
        if any(x in t for x in ["moratorium", "moratoria"]):
            return "Moratoria"
        if "dividend" in t:
            return "Notices_of_Dividends"
        if any(x in t for x in ["overseas", "territories"]):
            return "Overseas_Territories"
        if "prohibited name" in t:
            return "Reuse_of_Prohibited_Name"
        return "CVL"


class WorldClassGazetteCompaniesHousePipeline(GazetteCompaniesHousePipeline):
    def __init__(self, base_download_folder="InsolvencyDocuments", verbose: bool = True):
        super().__init__(base_download_folder, verbose)
        self.rate_limiter = AdaptiveRateLimiter(base_delay=1.0, max_delay=30.0)
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60, name="CompaniesHouse")
        self.document_queue = SmartDocumentQueue()
        self.checkpoint_manager = CheckpointManager()
        self.performance_stats = {
            "start_time": 0.0,
            "companies_processed": 0,
            "documents_downloaded": 0,
            "total_errors": 0,
            "api_calls": 0,
            "avg_response_time": 0.0,
        }

    def _get_filing_history_with_retry(self, company_number: str):
        t0 = time.time()
        try:
            soup = self.ch_downloader.get_company_filing_history_page(company_number)
            self.performance_stats["api_calls"] += 1
            rt = time.time() - t0
            n = self.performance_stats["api_calls"]
            self.performance_stats["avg_response_time"] = ((self.performance_stats["avg_response_time"] * (n - 1)) + rt) / n
            return soup
        except Exception:
            self.performance_stats["total_errors"] += 1
            raise


# =============================================================================
# CLI (run: python -m wc_insolvency_core --close | --async-test URL)
# =============================================================================

def _cli_async_enabled() -> bool:
    """True when httpx is available and ASYNC_NET=1 (same rule as AsyncHttpPool)."""
    return AsyncHttpPool.enable()

async def _async_fetch_once(url: str) -> tuple[int, int]:
    """One-shot async GET using the shared AsyncHttpPool client; returns (status, bytes)."""
    client = AsyncHttpPool.get()
    r = await client.get(url)
    return (r.status_code, len(r.content or b""))

def _run_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="wc_insolvency_core",
        description="Maintenance helpers for pooled HTTP clients used by the insolvency pipeline.",
    )
    parser.add_argument(
        "--close",
        action="store_true",
        help="Close the shared async HTTP/2 client (useful after runs/tests).",
    )
    parser.add_argument(
        "--async-test",
        metavar="URL",
        help="Fetch a URL once using the async HTTP/2 client (requires ASYNC_NET=1).",
    )

    args = parser.parse_args(argv)

    # --async-test first (can be run without --close)
    if args.async_test:
        if not _cli_async_enabled():
            print("Async HTTP/2 is disabled. Set ASYNC_NET=1 and ensure httpx is installed.")
            return 2
        import asyncio as _asyncio
        try:
            status, nbytes = _asyncio.run(_async_fetch_once(args.async_test))
            print(f"[async-test] {args.async_test} -> HTTP {status}, bytes={nbytes}")
        except Exception as e:
            print(f"[async-test] error: {e}")
            return 3

    # --close: always safe to call (no-op if not initialized)
    if args.close:
        import asyncio as _asyncio
        try:
            _asyncio.run(shutdown_network_clients())
            print("Closed async HTTP/2 client successfully.")
        except Exception as e:
            print(f"Close error: {e}")
            return 4

    if not args.close and not args.async_test:
        parser.print_help()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(_run_cli(sys.argv[1:]))
