# settings.py (unified)
from __future__ import annotations
import os
from typing import Any, Callable, Optional
from functools import lru_cache


# --- helpers ---------------------------------------------------------------
def _coerce(val: Any, caster: Optional[Callable], default: Any):
    if val is None:
        return default
    if caster is None:
        return val
    try:
        return caster(val)
    except Exception:
        return default

def _get_from_streamlit(key: str, secrets_section: Optional[str] = None):
    try:
        import streamlit as st  # safe: caught if not present
        src = st.secrets
        if secrets_section:
            src = st.secrets[secrets_section]
        return src.get(key)
    except Exception:
        return None

# --- single, cached accessor ----------------------------------------------
@lru_cache(maxsize=None)
def cfg(
    key: str,
    default: Any = None,
    cast: Optional[Callable] = None,
    *,
    env: Optional[str] = None,
    secrets_section: Optional[str] = None,
) -> Any:
    """
    Resolve config in order: ENV -> streamlit.secrets -> default.
    Cached for performance; call refresh_cfg() if you change env/secrets at runtime.
    """
    # 1) ENV
    env_key = env or key
    if env_key in os.environ:
        return _coerce(os.getenv(env_key), cast, default)

    # 2) streamlit.secrets (optional)
    secret_val = _get_from_streamlit(key, secrets_section)
    if secret_val is not None:
        return _coerce(secret_val, cast, default)

    # 3) default
    return default

def refresh_cfg():
    """Clear the memoized cache if env/secrets changed mid-run."""
    cfg.cache_clear()

# --- optional: dynamic attribute fallback for old code ---------------------
# This lets legacy code that imported `from settings import CH_API_RPS` keep working,
# but values are resolved dynamically via cfg() rather than frozen at import.
def __getattr__(name: str):
    if name == "CH_API_KEY":
        primary = cfg("CH_API_KEY", "", str)
        if primary:
            return primary
        keys_val = cfg("CH_API_KEYS", "", str)
        if isinstance(keys_val, str):
            for token in keys_val.split(","):
                token = token.strip()
                if token:
                    return token
        elif isinstance(keys_val, (list, tuple, set)):
            for token in keys_val:
                token = str(token).strip()
                if token:
                    return token
        return ""
    # Supply sensible defaults/types for known keys here if you like:
    DEFAULTS = {
        "DOC_SOURCE": ("auto", str),
        "CH_API_KEYS": ("4a905619-1bf3-499b-b643-479d202bd8b6, e252236f-2e5b-4547-9db0-22db041b4608, f3c0f262-b573-41ff-820b-56f9801ad908, 824e0849-0778-46b3-a021-7a3690590a47, df4fc086-cfcd-48e8-af04-3797035784be", str),
        "CH_API_RPS": (8.0, float),
        "CH_API_MAX_CONC": (36, int),
        "CH_KEY_MAX_CONC": (6, int),
        "CH_KEY_RATE_WINDOW": (1.2, float),
        "CH_KEY_SOFT_UTILIZATION": (0.72, float),
        "CH_API_TIMEOUT": (45, int),
        "CH_ITEMS_PER_PAGE": (250, int),
        "DRIVE_ROOT_ID": ("", str),
        "DRIVE_UPLOAD_WORKERS": (30, int),
        "DRIVE_BATCH_SIZE": (50, int),  # keep a single definition
        "DRIVE_SCOPE": ("file", str),
        "DRIVE_ONLY": ("true", str),
        "DELETE_LOCAL_AFTER": ("true", str),
        "DRIVE_UPLOAD_CONCURRENCY": (10, int),
        "DOC_CONCURRENCY_PER_COMPANY": (6, int),
        "MAX_CONCURRENT_COMPANIES": (6, int),
        "MAX_CONCURRENCY_CAP": (12, int),
        "DOC_REUSE_SOFT_LIMIT_MB": (128, int),
        "DOC_REUSE_MAX_ENTRIES": (512, int),
        "DOC_REUSE_SINGLE_MAX_MB": (6, int),
        "DRIVE_FAST_LANE_MB": (1, int),
        "DRIVE_FAST_LANE_WORKERS": (6, int),
        "DRIVE_BULK_WORKERS": (20, int),
        "MAX_DOCUMENTS_PER_COMPANY": (999999, int),
        "FH_MAX_PAGES": (200, int),
        "MAX_GAZETTE_PAGES": (250, int),
        "DOWNLOAD_ALL_DOCS": ("true", str),
        "HTTP_MAX_INFLIGHT": (54, int),
        "HTTP_MAX_KEEPALIVE": (64, int),
        "HTTP_TOKEN_RATE": (35.0, float),
        "HTTP_TOKEN_BURST": (96, int),
        "ASYNC_NET": ("1", str),
        "GAZETTE_POOL": (24, int),
        "GAZETTE_TIMEOUT": (30, int),
        "GAZETTE_QPS": (6.0, float),
        "GAZETTE_BURST": (12, int),
        "CH_POOL": (96, int),
        "CH_TIMEOUT": (40, int),
        "CH_QPS": (20.0, float),
        "CH_BURST": (40, int),
        # Add others you truly need…
    }
    default, caster = DEFAULTS.get(name, (None, None))
    return cfg(name, default=default, cast=caster)
