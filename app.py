# app.py — SARAH Theme UI Update

import streamlit as st
# import streamlit_antd_components as sac
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from threading import Event
from uuid import uuid4
import threading
from collections import defaultdict
import pytz
import base64
from PIL import Image
import io, os
os.environ.setdefault("ASYNC_NET", "1") # ensure async networking is enabled
import re
from queue import Queue, Empty
from taxonomy import PROCEDURES, ALIASES
import io, csv, time, hashlib
import pandas as pd
from utils.text_sanitize import clean_company_name

from cva_store import query_between, get_date_bounds
import cva_store as CStore
from cva_refresh import refresh_index
from moratorium_store import ensure_mora_index_fresh, last_refresh_epoch, query_month_range, get_month_bounds
from moratorium_fetch import refresh_moratorium_index
from wc_insolvency_caached import (
    CachedGazetteCompaniesHousePipeline,
    create_optimized_cache_config,
)
from drive_client import GoogleDriveClient, DRIVE_SCOPE_FILE, DRIVE_SCOPE_FULL
from settings import cfg
from settings import DOC_SOURCE, CH_API_KEY
from report_generators import (
    generate_run_report_csv,
    generate_run_report_excel,
    generate_run_report_pdf,
)

MAX_DOCS_DEFAULT        = cfg("MAX_DOCUMENTS_PER_COMPANY", 999_999, int, env="MAX_DOCUMENTS_PER_COMPANY")
MAX_PAGES_DEFAULT       = cfg("MAX_GAZETTE_PAGES", 250, int, env="MAX_GAZETTE_PAGES")
MAX_CONCURRENCY_DEFAULT = cfg("MAX_CONCURRENT_COMPANIES", 10, int, env="MAX_CONCURRENT_COMPANIES")
SHOW_TELEMETRY = False

# Set UK timezone
UK_TZ = pytz.timezone('Europe/London')  # Handles GMT/BST automatically


# --------------------------
# Page Configuration
# --------------------------
st.set_page_config(
    page_title="SARAH CASE - Corporate Insolvency Agent",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --------------------------
# Logo Handling Functions
# --------------------------
def get_base64_image(image_path):
    """Convert image to base64 for embedding"""
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except FileNotFoundError:
        return None

def resize_logo_if_needed(image_path, max_width=150):
    """Optionally resize logo for web display"""
    try:
        img = Image.open(image_path)
        if img.width > max_width:
            ratio = max_width / img.width
            new_size = (max_width, int(img.height * ratio))
            img = img.resize(new_size, Image.LANCZOS)
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            return base64.b64encode(buffer.getvalue()).decode()
        return get_base64_image(image_path)
    except:
        return get_base64_image(image_path)

# Logo handling
LOGO_PATH = Path("SARAH Logo.png")  # Relative to app.py location
logo_base64 = get_base64_image(LOGO_PATH) if LOGO_PATH.exists() else None

# --------------------------
# SARAH Theme Styling (spec-aligned)
# --------------------------
st.markdown(f"""
<style>
  .stApp {{
    background: linear-gradient(135deg, #101b2b 0%, #101b2b 25%, #002a36 50%, #101b2b 75%, #101b2b 100%);
    min-height: 100vh;
    position: relative;
  }}

  @import url('https://fonts.googleapis.com/css2?family=Rowdies:wght@400;700&display=swap');

  .main-header {{
    background: transparent;
    color: white;
    padding: 1.5rem;                /* tighter to avoid scroll */
    margin-bottom: 1rem;
    text-align: center;
  }}

  .main-header h1 {{
    font-family: 'Rowdies', sans-serif;
    font-size: 3rem;
    color: #00f7ff;
    text-shadow: 0 0 20px rgba(0, 247, 255, 0.5);
    margin-bottom: 0.25rem;         /* tighter */
    letter-spacing: 2px;
    text-transform: uppercase;
  }}

  /* SARAH (bold, large) + CASE (regular, smaller), baseline aligned */
  .brand-logo{{ --baseline-nudge: 0.09em; display:inline-flex; align-items:flex-end; gap:0.15rem; margin:0; line-height:1; }}
  .brand-sarah{{ font-family:'Rowdies',sans-serif; font-weight:700; font-size:3rem; color:#00f7ff; letter-spacing:2px; text-transform:uppercase; text-shadow:0 0 20px rgba(0,247,255,.5); line-height:.9; }}
  .brand-case{{  font-family:Avenir, -apple-system, sans-serif; font-weight:400; font-size:.58em; color:#00f7ff; letter-spacing:1px; text-transform:uppercase; transform: translateY(var(--baseline-nudge)); }}

  .main-header .subtitle {{
    font-family: Avenir, -apple-system, sans-serif;
    color: white;
    font-size: 1.1rem;
    opacity: 0.9;
  }}

  /* Tabs — no underlines */
  .stTabs [data-baseweb="tab-list"] {{ background:transparent; gap:8px; border-bottom:none !important; }}
  .stTabs [data-baseweb="tab"] {{
    color:white; background:transparent; border:none !important; box-shadow:none !important;
    font-family:Avenir,-apple-system,sans-serif; font-weight:600; padding:.5rem 1rem; outline:none !important;
  }}
  .stTabs [data-baseweb="tab"][aria-selected="true"] {{ color:#00f7ff !important; background-color:rgba(0,247,255,.10); }}

  /* Buttons */
  .stButton > button {{
    background:#1c7790; color:white; border:none; border-radius:5px; padding:.75rem 1.5rem;
    font-family:Avenir,-apple-system,sans-serif; font-weight:600; font-size:16px; text-transform:uppercase;
    letter-spacing:1px; transition:.3s ease; min-height:48px;
  }}
  .stButton > button:hover {{ background:#00f7ff; color:#101b2b; transform:translateY(-2px); box-shadow:0 5px 15px rgba(0,247,255,.3); }}
  .stButton > button:disabled {{ background:#4a5568; opacity:.5; cursor:not-allowed; }}

  /* Inputs */
  .stTextInput input, .stDateInput input, .stTextArea textarea {{
    background:white; color:#535b67; border:2px solid rgba(0,247,255,.5); border-radius:5px;
    font-family:Avenir,-apple-system,sans-serif; padding:.75rem;
  }}
  .stTextInput input:focus, .stDateInput input:focus, .stTextArea textarea:focus {{
    border-color:#00f7ff; box-shadow:0 0 10px rgba(0,247,255,.3); outline:none;
  }}

  /* subtle vertical spacing between selector, dates, and button */
    .stMultiSelect {{ margin-bottom: 12px; }}
    .stDateInput  {{ margin-bottom: 12px; }}
    .stButton     {{ margin-top: 12px; }}


  .stMultiSelect > div > div {{ background:white; color:#535b67; border:2px solid rgba(0,247,255,.5); border-radius:5px; }}
  .stMultiSelect > div > div:focus-within {{ border-color:#00f7ff; box-shadow:0 0 10px rgba(0,247,255,.3); }}

  .stProgress > div > div > div {{ background:linear-gradient(90deg, #1c7790, #00f7ff); }}

  /* Make section titles WHITE (per spec) + reduce top margin to avoid scroll */
  h2 {{
    color: white !important;                 /* <-- changed from cyan */
    font-family: Avenir, -apple-system, sans-serif;
    border-bottom: 2px solid rgba(0, 247, 255, 0.3);
    padding-bottom: 0.4rem;
    margin-top: 1.25rem;                     /* tighter */
    margin-bottom: 0.6rem;                   /* tighter */
  }}

  .stMarkdown, .stText {{ color:white !important; font-family:Avenir,-apple-system,sans-serif; }}

  /* Ensure checkbox labels are white (main + sidebar) */
  .stCheckbox label, .stCheckbox p,
  section[data-testid="stSidebar"] .stCheckbox label,
  section[data-testid="stSidebar"] .stCheckbox p {{ color:white !important; }}

  label {{ color:white !important; font-family:Avenir,-apple-system,sans-serif; font-weight:500; }}

  [data-testid="metric-container"] {{
    background:rgba(28,119,144,.2); border:1px solid rgba(0,247,255,.5); border-radius:5px; padding:1rem; backdrop-filter:blur(10px);
  }}
  [data-testid="metric-container"] label {{ color:#00f7ff !important; font-size:.9rem; text-transform:uppercase; letter-spacing:1px; }}
  [data-testid="metric-container"] [data-testid="stMetricValue"] {{ color:white !important; font-size:1.8rem; font-weight:bold; }}

  .stAlert {{ background:rgba(28,119,144,.2); border:1px solid #00f7ff; color:white; border-radius:5px; backdrop-filter:blur(10px); }}
  div[data-baseweb="notification"] {{ background:rgba(28,119,144,.2); border:1px solid #00f7ff; color:white; }}

  .streamlit-expanderHeader {{ background:rgba(28,119,144,.2); color:white !important; border:1px solid rgba(0,247,255,.5); border-radius:5px; }}
  .streamlit-expanderContent {{ background:rgba(16,27,43,.5); border:1px solid rgba(0,247,255,.3); border-top:none; border-radius:0 0 5px 5px; }}

  .stDataFrame {{ background:rgba(255,255,255,.95); border-radius:5px; overflow:hidden; }}

  section[data-testid="stSidebar"] {{ background:linear-gradient(180deg,#101b2b 0%,#002a36 100%); border-right:2px solid #00f7ff; }}
  section[data-testid="stSidebar"] .stMarkdown {{ color:white !important; }}

  .footer {{
    color:white; text-align:center; padding:1.25rem;   /* tighter */
    font-family:Avenir,-apple-system,sans-serif; margin-top:2rem;
    border-top:1px solid rgba(0,247,255,.3);
  }}
  .footer a {{ color:#00f7ff; text-decoration:none; transition:color .3s; }}
  .footer a:hover {{ color:white; text-decoration:underline; }}

  /* Reduce global vertical padding so the tab fits above the fold */
  .block-container {{ padding-top: 0.75rem; padding-bottom: 0.75rem; }}

  @media (max-width: 768px) {{
    .main-header h1 {{ font-size: 2rem; }}
    .main-header .subtitle {{ font-size: 0.95rem; }}
    .stButton > button {{ font-size: 14px; padding: 0.6rem 1rem; }}
  }}
</style>
""", unsafe_allow_html=True)


st.markdown("""
<style>
  /* Reset any previous 'fill: white' that created a solid disc */
  [data-testid="stTooltipIcon"] svg * {
    fill: none !important;
  }

  /* Keep any outer circle transparent, with a soft white stroke */
  [data-testid="stTooltipIcon"] svg circle {
    fill: transparent !important;
    stroke: rgba(255,255,255,0.55) !important;   /* tweak 0.45–0.75 as you like */
    stroke-width: 1.2;
  }

  /* Draw the question mark (or glyph) with a brighter white stroke */
  [data-testid="stTooltipIcon"] svg path {
    fill: transparent !important;
    stroke: rgba(255,255,255,0.9) !important;     /* tweak 0.8–1.0 as you like */
    stroke-linecap: round;
    stroke-linejoin: round;
    stroke-width: 1.6;
  }

  /* Subtle overall fade + crisp on hover/focus */
  [data-testid="stTooltipIcon"] {
    opacity: .9;
    transition: opacity .15s ease;
  }
  [data-testid="stTooltipIcon"]:hover,
  [data-testid="stTooltipIcon"]:focus {
    opacity: 1;
  }

  /* Optional: tooltip popover contrast on dark themes */
  div[data-baseweb="popover"] {
    background: rgba(16,27,43,0.98) !important;
    color: #fff !important;
    border: 1px solid rgba(0,247,255,0.25) !important;
  }
</style>
""", unsafe_allow_html=True)


st.markdown("""
<style>
  .footer {
    position: sticky;
    bottom: 0;
    padding: 14px 0 18px;
  }

  /* Centered text block (no right padding now—logo won't be constrained by it) */
  .footer-inner {
    max-width: 1100px;
    margin: 0 auto;
    text-align: center;
  }

  .footer-text { color: #fff; line-height: 1.35; }
  .footer a { color: #00f7ff; text-decoration: none; }
  .footer a:hover { color: #fff; text-decoration: underline; }

  /* Logo is now positioned against the full footer area (not the inner box) */
  .footer-logo {
    position: absolute;            /* key change */
    right: 32px;                   /* nudged further right */
    bottom: 12px;                  /* closer to the bottom edge */
    width: 120px;
    height: auto;
    opacity: 0.9;
    pointer-events: none;
  }

  /* Make sure the footer itself is the positioning context */
  .footer { position: sticky; bottom: 0; }
  .footer { position: relative; }  /* gives .footer-logo its anchor */

  /* Keep page tall enough so footer sits at bottom on short pages */
  .block-container { min-height: calc(100vh - 120px); }

  @media (max-width: 768px) {
    .footer-logo { right: 20px; bottom: 10px; width: 96px; }
  }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  .inline-note{
    display:inline-flex;
    align-items:center;
    gap:.5rem;
    padding:.5rem .75rem;
    border:1px solid rgba(0,247,255,.6);
    background:rgba(28,119,144,.18);
    border-radius:6px;
    color:#fff;
    font-family:Avenir,-apple-system,sans-serif;
    line-height:1.2;
    width: fit-content;      /* key: only as wide as the text */
    max-width: 100%;
    white-space: nowrap;     /* keeps it on one line */
    margin-top:6px;
    align-self:flex-end;
    position:relative;
    top:6px;
  }
  .inline-note .dot{
    width:8px; height:8px; border-radius:50%;
    background:#00f7ff; box-shadow:0 0 6px rgba(0,247,255,.8);
    flex: 0 0 auto;
  }
  .progress-meta{
    display:flex;
    flex-wrap:wrap;
    gap:.5rem;
    font-family:Avenir,-apple-system,sans-serif;
    font-size:0.95rem;
    margin-top:.35rem;
    color:#cfe8ff;
  }
  .progress-meta .meta-item{
    display:flex;
    align-items:center;
    gap:.35rem;
    padding:0.15rem 0.65rem;
    border-radius:999px;
    background:rgba(0, 139, 204, .18);
    border:1px solid rgba(0, 205, 255, .25);
  }
  .progress-meta .meta-item.success{
    background:rgba(20,160,120,.22);
    border-color:rgba(60,220,160,.35);
    color:#baffe1;
  }
  .progress-meta .meta-item.docs{
    background:rgba(0,166,255,.16);
    border-color:rgba(60,198,255,.35);
  }
  .progress-meta .meta-item.docs .cache{
    color:rgba(207,232,255,.8);
  }
  .progress-meta .meta-item.warn{
    background:rgba(255,140,0,.18);
    border-color:rgba(255,180,90,.35);
    color:#ffe0ba;
  }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  .report-pill-row{
    display:flex;
    flex-wrap:nowrap;
    gap:0.35rem;
    margin-top:1rem;
    justify-content:center;
  }
  .report-pill,
  .report-pill:link,
  .report-pill:visited{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    gap:.45rem;
    border-radius:8px;
    padding:.55rem 2.3rem;
    border:none;
    color:#fff;
    background: transparent;
    font-weight:600;
    letter-spacing:.02em;
    text-decoration:none;
    min-width:260px;
    height:42px;
    transition:background-color .18s ease,color .18s ease,box-shadow .18s ease,transform .18s ease;
    box-shadow:0 6px 15px rgba(0,0,0,0.4);
  }
  .pill-icon{
    margin-right:.65rem;
    color:inherit;
  }
  .report-pill span{
    color:inherit;
  }
  .report-pill:hover,
  .report-pill:focus-visible{
    background:#00f7ff;
    color:#050f1a;
    box-shadow:0 14px 30px rgba(0,199,180,.45);
    transform:translateY(-1px);
  }
  .report-pill:active{
    background:#00c6d8;
    color:#050f1a;
  }
  .report-pill-disabled{
    border-color:rgba(255,255,255,0.22);
    color:#9ca6ba;
    background:rgba(255,255,255,0.08);
    cursor:not-allowed;
  }
  .report-divider{
    margin:1.25rem 0 0.75rem;
    border-top:1px solid rgba(250,250,250,0.2);
  }
</style>
""", unsafe_allow_html=True)




# --------------------------
def _progress_widgets():
    return st.progress(0), st.empty()

def str_to_bool(v, default=False):
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _drain_progress(q, slot):
    """Pull pending progress events and render compactly."""
    events: List[dict] = []
    while True:
        try:
            evt = q.get_nowait()
        except Exception:
            break
        events.append(evt)
        kind = evt.get("kind", "")
        if kind == "downloaded_bytes":
            slot.write(f"⬇️ downloaded file bytes: {evt.get('file','')} (#{evt.get('company_number','')})")
        elif kind == "upload_done":
            slot.write(f"☁️ uploaded to Drive: {evt.get('file','')} (#{evt.get('company_number','')})")
        elif kind == "upload_failed":
            slot.write(f"⚠️ upload failed: {evt.get('file','')} (#{evt.get('company_number','')})")
        elif kind == "documents_uploaded":
            # Progress stats consume these events; no UI noise.
            continue
        # add more kinds as needed
    return events


def _ensure_run_state(prefix: str):
    """Initialize all required state variables for a tab"""
    st.session_state.setdefault(f"{prefix}_run_state", "idle")
    st.session_state.setdefault(f"{prefix}_run_id", None)
    st.session_state.setdefault(f"{prefix}_cancel_event", None)
    st.session_state.setdefault(f"{prefix}_executor", None)
    st.session_state.setdefault(f"{prefix}_futures", set())
    st.session_state.setdefault(f"{prefix}_cleanup_thread", None)

def _start_run(prefix: str) -> str:
    """Start a new run and return the run ID"""
    _ensure_run_state(prefix)
    rid = str(uuid4())
    st.session_state[f"{prefix}_run_state"] = "running"
    st.session_state[f"{prefix}_run_id"] = rid
    st.session_state[f"{prefix}_cancel_event"] = Event()
    st.session_state[f"{prefix}_futures"] = set()
    st.session_state[f"{prefix}_cleanup_thread"] = None
    # NEW: mark run start (for files/min logging)
    st.session_state[f"{prefix}_run_started_at"] = time.time()
    return rid


def _request_stop(prefix: str):
    """Request graceful stop of the current run"""
    ev = st.session_state.get(f"{prefix}_cancel_event")
    if ev:
        ev.set()
    
    st.session_state[f"{prefix}_run_state"] = "cancelled"
    
    def cleanup_executor():
        try:
            executor = st.session_state.get(f"{prefix}_executor")
            futures = st.session_state.get(f"{prefix}_futures", set())
            
            if executor:
                for fut in futures:
                    if not fut.done():
                        fut.cancel()
                executor.shutdown(wait=False, cancel_futures=True)

            st.session_state[f"{prefix}_executor"] = None
            st.session_state[f"{prefix}_futures"] = set()
            
        except Exception as e:
            st.error(f"Cleanup error: {e}")
            st.session_state[f"{prefix}_run_state"] = "cancelled"
    
    cleanup_thread = st.session_state.get(f"{prefix}_cleanup_thread")
    if cleanup_thread is None or not cleanup_thread.is_alive():
        cleanup_thread = threading.Thread(target=cleanup_executor, daemon=True)
        st.session_state[f"{prefix}_cleanup_thread"] = cleanup_thread
        cleanup_thread.start()

def _is_cancelled(prefix: str) -> bool:
    """Check if the current run has been cancelled"""
    ev = st.session_state.get(f"{prefix}_cancel_event")
    return bool(ev and ev.is_set())

def _render_progress_summary(prog, slot, stats: dict, total_companies: int, label="Processed", show_documents: bool = True):
    processed = stats.get("processed", 0)
    successful = stats.get("successful", 0)
    display_total = max(total_companies, processed, 1)
    if display_total <= 0:
        percentage = 0
    else:
        percentage = int(min(100, (processed / display_total) * 100))
    progress_text = f"{processed}/{display_total} companies"
    downloaded = stats.get("downloaded", 0)
    from_cache = stats.get("from_cache", 0)
    if show_documents:
        docs_summary = f"{downloaded} new / {from_cache} cache"
        progress_text += f" · {docs_summary}"
    else:
        docs_summary = f"{downloaded} downloaded"
    prog.progress(percentage, text=progress_text)
    companies_line = f"{processed}/{display_total} companies processed"
    docs_line = f"{docs_summary}"
    html = (
        f"<div class='progress-meta'>"
        f"<span class='meta-item'>{companies_line}</span>"
        f"<span class='meta-item success'>✅ {successful} successful</span>"
    )
    if show_documents:
        html += f"<span class='meta-item docs'>📄 {docs_line}</span>"
    else:
        html += f"<span class='meta-item docs'>📄 {docs_line}</span>"
    errors = stats.get("errors", 0)
    if errors:
        html += f"<span class='meta-item warn'>⚠️ {errors} issues</span>"
    html += "</div>"
    slot.markdown(html, unsafe_allow_html=True)

def _init_upload_telemetry_state():
    return {
        "fast_pending": 0,
        "bulk_pending": 0,
        "last_lane": "",
        "last_company": "",
        "last_delta": 0,
        "timestamp": 0.0,
        "rate_status": "",
        "rate_updated": 0.0,
        "rate_kind": "",
    }

def _update_upload_telemetry_state(state: dict, evt: dict):
    if not evt:
        return
    kind = evt.get("kind")
    if kind == "upload_progress":
        lane = (evt.get("lane") or "bulk").lower()
        pending = max(0, int(evt.get("pending", 0) or 0))
        delta = int(evt.get("delta", 0) or 0)
        company = evt.get("company_number", "")
        if lane not in ("fast", "bulk"):
            lane = "bulk"
        state[f"{lane}_pending"] = pending
        state["last_lane"] = lane
        state["last_company"] = company
        state["last_delta"] = delta
        state["timestamp"] = time.time()
    elif kind == "rate_limit":
        status = evt.get("status", "limited")
        if status == "limited":
            resume = float(evt.get("resume_at") or 0.0)
            eta = max(0.0, resume - time.time())
            key_suffix = evt.get("key_suffix", "")
            state["rate_status"] = f"API throttled {key_suffix or ''} (~{eta:.1f}s)"
        elif status == "recovered":
            state["rate_status"] = "API recovered"
        else:
            state["rate_status"] = status
        state["rate_kind"] = status
        state["rate_updated"] = time.time()

def _render_upload_telemetry(slot, state: dict):
    if slot is None or state is None:
        return
    fast = state.get("fast_pending", 0)
    bulk = state.get("bulk_pending", 0)
    last_lane = state.get("last_lane") or "–"
    last_company = state.get("last_company") or "–"
    delta = state.get("last_delta", 0)
    line = (
        f"🚚 Drive queue • fast: {fast} | bulk: {bulk} "
        f"(last {last_lane} lane #{last_company}"
    )
    if delta:
        line += f" +{delta}"
    line += ")"
    rate = state.get("rate_status")
    if rate:
        line += f" • {rate}"
    slot.caption(line)

def _make_cached_result(pipeline, comp: dict, cached_data: dict, cached_count: int, max_documents: int) -> tuple[bool, dict]:
    def _coerce_limit(raw):
        if raw in (None, 0, -1, "ALL", "all", "All"):
            return None
        try:
            val = int(raw)
            return val if val > 0 else None
        except Exception:
            return None

    notice_type = cached_data.get("notice_type") or comp.get("notice_type", "")
    name = cached_data.get("company_name") or comp.get("company_name") or comp.get("company_number") or "Unknown"
    number = comp.get("company_number") or ""
    procedure_folder = getattr(pipeline, "_forced_folder", None)
    if not procedure_folder:
        procedure_folder = pipeline._resolve_procedure_folder_name(notice_type)
    folder_path = ""
    metadata = cached_data.get("metadata") or {}
    if isinstance(metadata, dict):
        folder_path = metadata.get("folder_path") or ""
    doc_links = cached_data.get("document_links") or []
    limit = _coerce_limit(max_documents)
    if limit is not None:
        doc_links = doc_links[:limit]

    total_cached = 0
    cache = getattr(pipeline, "cache", None)
    if cache:
        total_cached = cache.count_cached_documents(number)
    if total_cached == 0:
        total_cached = len(doc_links) if doc_links else cached_count
    if limit is not None and total_cached:
        total_cached = min(total_cached, limit)

    if total_cached <= 0:
        return False, {}

    result = {
        "success": True,
        "used_cache": True,
        "skipped_classification": False,
        "skipped_checkpoint": False,
        "company_name": name,
        "company_number": number,
        "notice_type": notice_type,
        "procedure_folder": procedure_folder or "",
        "folder_path": folder_path,
        "documents_found": total_cached,
        "documents_downloaded": 0,
        "documents_downloaded_fresh": 0,
        "documents_from_cache": total_cached,
        "cache_hits": ["company_data"] if total_cached else [],
        "errors": [],
        "processing_time": 0.0,
    }
    return True, result

def _finish_run(prefix: str, cancelled: bool = False):
    """Clean finish of a run"""
    executor = st.session_state.get(f"{prefix}_executor")
    if executor:
        try:
            executor.shutdown(wait=False)
        except:
            pass
    
    st.session_state[f"{prefix}_run_state"] = "cancelled" if cancelled else "completed"
    st.session_state[f"{prefix}_run_id"] = None
    st.session_state[f"{prefix}_cancel_event"] = None
    st.session_state[f"{prefix}_executor"] = None
    st.session_state[f"{prefix}_futures"] = set()
    st.session_state[f"{prefix}_cleanup_thread"] = None

def _reset_run(prefix: str):
    """Reset run state for a new run while preserving results"""
    executor = st.session_state.get(f"{prefix}_executor")
    if executor:
        try:
            executor.shutdown(wait=False)
        except:
            pass
    
    # Store last results before clearing (if they exist)
    last_stats = st.session_state.get(f"{prefix}_last_stats")
    last_results = st.session_state.get(f"{prefix}_last_results")
    last_timestamp = st.session_state.get(f"{prefix}_last_run_timestamp")
    
    # Clear run state
    st.session_state[f"{prefix}_run_state"] = "idle"
    st.session_state[f"{prefix}_run_id"] = None
    st.session_state[f"{prefix}_cancel_event"] = None
    st.session_state[f"{prefix}_executor"] = None
    st.session_state[f"{prefix}_futures"] = set()
    st.session_state[f"{prefix}_cleanup_thread"] = None
    
    # Restore last results if they existed
    if last_stats:
        st.session_state[f"{prefix}_last_stats"] = last_stats
    if last_results:
        st.session_state[f"{prefix}_last_results"] = last_results
    if last_timestamp:
        st.session_state[f"{prefix}_last_run_timestamp"] = last_timestamp

def _save_results(prefix: str, stats: dict, results: list = None, run_config: dict | None = None):
    """Save results to persist across runs"""
    st.session_state[f"{prefix}_last_stats"] = stats.copy()
    if results:
        st.session_state[f"{prefix}_last_results"] = results.copy()
    stats.setdefault("downloaded", 0)
    stats.setdefault("from_cache", 0)
    stats.setdefault("processed", 0)
    stats.setdefault("successful", 0)
    stats.setdefault("errors", 0)
    stats.setdefault("total_documents", stats["downloaded"] + stats["from_cache"])
    started = st.session_state.get(f"{prefix}_run_started_at")
    elapsed = max(0.0001, time.time() - started) if started else 0.0
    stats.setdefault("processing_time", elapsed)
    if run_config:
        st.session_state[f"{prefix}_last_report_config"] = run_config.copy()
    else:
        run_config = st.session_state.get(f"{prefix}_last_report_config")
    if results is not None and run_config:
        _prepare_report_assets(prefix, stats, results, run_config)
    
    # Store UK time instead of UTC
    uk_tz = pytz.timezone('Europe/London')
    uk_time = datetime.now(uk_tz)
    st.session_state[f"{prefix}_last_run_timestamp"] = uk_time.timestamp()

def _get_last_results(prefix: str):
    """Get last run results if available"""
    return (
        st.session_state.get(f"{prefix}_last_stats"),
        st.session_state.get(f"{prefix}_last_results"),
        st.session_state.get(f"{prefix}_last_run_timestamp")
    )

def _log_download_rate(prefix: str, downloaded: int, cached: int = 0):
    """
    Log 'files downloaded per minute' to the backend console (not UI).
    Safe no-op if we don't have a start timestamp.
    """
    try:
        started = st.session_state.get(f"{prefix}_run_started_at")
        if not started:
            return
        mins = max((time.time() - started) / 60.0, 1e-6)
        total = downloaded + cached
        rate_new = downloaded / mins
        rate_total = total / mins
        print(
            f"[{prefix}] Download summary — new: {downloaded}, cached: {cached}, total: {total} | "
            f"minutes: {mins:.2f} | new/min: {rate_new:.2f} | total/min: {rate_total:.2f}"
        )
    except Exception as e:
        print(f"[{prefix}] rate log error: {e}")


# ---------- Zero-result handling helpers (add below your other helpers) ----------

def show_zero_results_tip(msg: str, suggestions: list[str] = None, actions: list[tuple] = None):
    """
    Consistent ‘no results’ UI with optional suggestions and quick actions.
    actions: list of tuples -> [("Button Label", callable)]
    """
    with st.container():
        st.warning(msg)
        if suggestions:
            st.markdown("**Try:** " + " • ".join(suggestions))
        if actions:
            cols = st.columns(len(actions))
            for i, (label, on_click) in enumerate(actions):
                if cols[i].button(label, width='stretch', key=f"zerobtn_{label}"):
                    on_click()

def validate_dates(start_date, end_date) -> bool:
    if not start_date or not end_date:
        st.error("Please choose a valid start and end date.")
        return False
    if end_date <= start_date:
        st.error("End Date must be after Start Date.")
        return False
    today_ = date.today()
    if start_date > today_ or end_date > today_:
        st.error("Dates cannot be in the future.")
        return False
    return True

@st.cache_data(show_spinner=False)
def preflight_gazette_count(start_date_str: str, end_date_str: str, categories: list[str]) -> int:
    """
    Very fast count-check using the Gazette scraper’s own URL builder.
    Returns 0 if the site shows 0, or -1 if we couldn’t determine.
    """
    from bs4 import BeautifulSoup
    p = get_pipeline(
        base_folder=st.secrets.get("BASE_FOLDER", "InsolvencyDocuments_Cached"),
        force_refresh=False,
        force_redownload=False,
    )
    cats = p.gazette_scraper._resolve_categories(categories)
    url = p.gazette_scraper._build_search_url(start_date_str, end_date_str, cats, page=1)
    try:
        r = p.gazette_scraper.session.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        return p.gazette_scraper._extract_total_count(soup)
    except Exception:
        return -1  # unknown -> let the run proceed

def make_auto_expand_dates(start_dt: date, end_dt: date, days: int = 7) -> tuple[date, date]:
    """Expand date range by N days (both sides), clamp to today."""
    new_start = max(date(2000, 1, 1), start_dt - timedelta(days=days))
    new_end = min(date.today(), end_dt + timedelta(days=days))
    return new_start, new_end

def sanitize_company_numbers(raw: str) -> list[str]:
    """Basic validation + dedup for the company-number tab."""
    import re as _re
    nums, seen = [], set()
    for line in (raw or "").splitlines():
        n = line.strip().upper()
        if not n:
            continue
        if (n.isdigit() and len(n) in (7, 8)) or _re.match(r"^[A-Z]{2}\d{6}$", n):
            if n not in seen:
                seen.add(n)
                nums.append(n)
    return nums

def _is_valid_company_number(number: str) -> bool:
    if not number:
        return False
    n = number.strip().upper()
    if n.isdigit() and len(n) in (7, 8):
        return True
    import re as _re
    return bool(_re.match(r"^[A-Z]{2}\d{6}$", n))


def _apply_notice_filters(companies, regex_list):
    if not regex_list:
        return companies
    out = []
    for c in companies:
        nt = (c.get("notice_type") or "").strip()
        if any(r.search(nt) for r in regex_list):
            out.append(c)
    return out

def _render_results_summary(prefix: str, stats: dict, items: list = None):
    """Render results summary that persists across runs"""
    cols = st.columns(4)
    with cols[0]:
        st.metric("Companies", stats.get("processed", 0))
    with cols[1]:
        st.metric("Successful", stats.get("successful", 0))
    with cols[2]:
        total_docs = stats.get("total_documents")
        if total_docs is None:
            total_docs = stats.get("downloaded", 0) + stats.get("from_cache", 0)
        st.metric("Documents", total_docs)
    with cols[3]:
        st.metric("From Cache", stats.get("from_cache", 0))

    _render_report_buttons(prefix)


def _report_file_name(prefix: str, extension: str) -> str:
    ts = st.session_state.get(f"{prefix}_last_run_timestamp")
    if ts:
        try:
            ts_dt = datetime.fromtimestamp(ts, tz=UK_TZ)
        except Exception:
            ts_dt = datetime.now(UK_TZ)
    else:
        ts_dt = datetime.now(UK_TZ)
    return f"{prefix}_report_{ts_dt.strftime('%Y%m%d_%H%M%S')}.{extension}"


def _link_data_uri(data, mime: str) -> str | None:
    if data is None:
        return None
    if isinstance(data, str):
        payload = data.encode("utf-8")
    else:
        payload = bytes(data)
    b64 = base64.b64encode(payload).decode("ascii")
    return f"data:{mime};base64,{b64}"


def _render_report_buttons(prefix: str) -> None:
    if st.session_state.get(f"{prefix}_run_state") == "running":
        return
    assets = st.session_state.get(f"{prefix}_report_assets") or {}
    if not any(assets.values()):
        return
    run_config = st.session_state.get(f"{prefix}_last_report_config", {})
    caption_parts = []
    start_date = run_config.get("start_date")
    end_date = run_config.get("end_date")
    if start_date or end_date:
        caption_parts.append(f"{start_date or 'N/A'} → {end_date or 'N/A'}")
    categories = run_config.get("categories")
    if categories:
        caption_parts.append(str(categories))

    st.markdown('<div class="report-divider"></div>', unsafe_allow_html=True)
    st.markdown("### 📬 Export Results")
    if caption_parts:
        st.caption(" | ".join(caption_parts))

    buttons = [
        (
            "Download CSV",
            assets.get("csv"),
            _report_file_name(prefix, "csv"),
            "text/csv",
            "📄",
        ),
        (
            "Download Excel",
            assets.get("excel"),
            _report_file_name(prefix, "xlsx"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "📊",
        ),
        (
            "Download PDF Report",
            assets.get("pdf"),
            _report_file_name(prefix, "pdf"),
            "application/pdf",
            "🗞️",
        ),
    ]

    cols = st.columns(3, gap="small")
    for col, (label, data, file_name, mime, icon) in zip(cols, buttons):
        data_uri = _link_data_uri(data, mime)
        if data_uri:
            col.markdown(
                f'<a class="report-pill" download="{file_name}" href="{data_uri}"><span class="pill-icon">{icon}</span>{label}</a>',
                unsafe_allow_html=True,
            )
        else:
            col.markdown(
                f'<span class="report-pill report-pill-disabled"><span class="pill-icon">{icon}</span>{label}</span>',
                unsafe_allow_html=True,
            )


def _prepare_report_assets(prefix: str, stats: dict, results: list, run_config: dict) -> None:
    safe_results = results or []
    report_stats = stats.copy()
    report_stats.setdefault("total_documents_downloaded", stats.get("downloaded", 0) + stats.get("from_cache", 0))
    report_stats.setdefault("total_documents_downloaded_fresh", stats.get("downloaded", 0))
    report_stats.setdefault("documents_from_cache", stats.get("from_cache", 0))
    report_stats.setdefault("companies_processed", stats.get("processed", 0))
    report_stats.setdefault("companies_successful", stats.get("successful", 0))
    report_stats.setdefault("processing_time", stats.get("processing_time", 0))

    def _safe_emit(generator, pl_format):
        try:
            return generator(report_stats, safe_results, run_config)
        except Exception as exc:
            print(f"Report generation failed ({pl_format}): {exc}")
            return None

    assets = {
        "csv": _safe_emit(generate_run_report_csv, "csv"),
        "excel": _safe_emit(generate_run_report_excel, "excel"),
        "pdf": _safe_emit(generate_run_report_pdf, "pdf"),
    }
    st.session_state[f"{prefix}_report_assets"] = assets

def render_control_buttons(prefix: str):
    """Enhanced control bar with proper state handling - SARAH themed"""
    state = st.session_state.get(f"{prefix}_run_state", "idle")

    if state == "running":
        cols = st.columns([1, 4])  # keep your STOP button layout
        with cols[0]:
            if st.button("STOP", key=f"stop_{prefix}", width='stretch'):
                _request_stop(prefix)
                st.rerun()
        with cols[1]:
            st.markdown(
                '<div class="inline-note">'
                '<span class="dot"></span>'
                'Download in progress… Click <strong>STOP</strong> to save current progress and prevent further downloads.'
                '</div>',
                unsafe_allow_html=True
            )
        return

    if state in ("completed", "cancelled"):
        msg = "✅ Download completed successfully" if state == "completed" else "Download cancelled"
        cols = st.columns([3, 1])
        with cols[0]:
            (st.success if state == "completed" else st.warning)(msg)
        with cols[1]:
            if st.button("NEW SEARCH", key=f"new_{prefix}", width='stretch'):
                _reset_run(prefix)
                st.rerun()

def _needs_refresh(row: dict | None, force: bool, max_age_days: int = 30) -> bool:
    """Decide if a row needs a (re)fetch of commencement_date."""
    if force:
        return True
    if not row:
        return True
    if not row.get("commencement_date"):
        return True
    last = int(row.get("last_checked") or 0)
    return (time.time() - last) / 86400.0 >= max_age_days

def _export_index_to_drive_csv(pipeline, df: pd.DataFrame, drive_folder="CVA"):
    """
    Best-effort export of the current CVA index to Drive for audit.
    Safe no-op if your pipeline doesn't expose a drive uploader.
    """
    try:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        buf = io.BytesIO(csv_bytes)
        fname = f"cva_index_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        # If you have a Drive helper hanging off the pipeline, use it:
        # pipeline.drive_upload(file_like=buf, filename=fname, folder=drive_folder)
        # Fallback: stash locally under cache/
        from pathlib import Path
        out = Path("cache") / fname
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(csv_bytes)
    except Exception:
        pass  # never block the run because of an export

# --------------------------
# Cached Resources (Identical to original)
# --------------------------
@st.cache_resource(show_spinner=False)
def get_drive_client() -> GoogleDriveClient:
    secrets_dir = Path(cfg("SECRETS_DIR", "secrets"))
    client_secret_path = secrets_dir / cfg("GOOGLE_CLIENT_SECRET_FILE", "client_secret.json")
    token_path = Path(cfg("GOOGLE_TOKEN_PATH", "token.json"))
    scopes = [DRIVE_SCOPE_FILE] if cfg("DRIVE_SCOPE", "file") == "file" else [DRIVE_SCOPE_FULL]

    token_path.parent.mkdir(parents=True, exist_ok=True)
    return GoogleDriveClient(
        client_secret_path=str(client_secret_path),
        token_path=str(token_path),
        scopes=scopes,
    )

@st.cache_resource(show_spinner=False)
def get_provider():
    """
    Lazily create and cache the Companies House API provider when configured.
    Returns None when DOC_SOURCE isn't API-capable so the pipeline falls back to HTML.
    """
    # Safe import here to avoid import cost if not needed
    from providers.ch_api_provider import make_api_provider
    use_api = (DOC_SOURCE in ("api", "auto")) and bool(CH_API_KEY)
    return make_api_provider() if use_api else None

@st.cache_resource(show_spinner=False)
def get_pipeline(base_folder: str, force_refresh: bool, force_redownload: bool) -> CachedGazetteCompaniesHousePipeline:
    cfg_obj = create_optimized_cache_config(
        force_refresh=force_refresh,
        force_redownload=force_redownload
    )


    p = CachedGazetteCompaniesHousePipeline(
        base_download_folder=base_folder,
        cache_config=cfg_obj,
        verbose=True,
        http_max_inflight=cfg("HTTP_MAX_INFLIGHT", 12, int),
        drive_upload_concurrency=cfg("DRIVE_UPLOAD_CONCURRENCY", 20, int),
        doc_concurrency_per_company=cfg("DOC_CONCURRENCY_PER_COMPANY", 6, int),
        max_concurrent_companies=cfg("MAX_CONCURRENT_COMPANIES", 6, int),
        provider=get_provider(),
    )
    # p = CachedGazetteCompaniesHousePipeline(
    #     base_download_folder=base_folder,                               # <-- use the arg
    #     cache_config=cfg_obj,
    #     verbose=True,
    #     http_max_inflight=cfg("HTTP_MAX_INFLIGHT", 12, int),
    #     # Raise Drive upload concurrency to feed the worker queue hard:
    #     drive_upload_concurrency=cfg("DRIVE_UPLOAD_CONCURRENCY", 20, int),
    #     doc_concurrency_per_company=cfg("DOC_CONCURRENCY_PER_COMPANY", 3, int),
    #     max_concurrent_companies=cfg("MAX_CONCURRENT_COMPANIES", 6, int),
    # )

    # Allow “download all” via config/env
    p.download_all = bool(str(cfg("DOWNLOAD_ALL_DOCS", "false")).lower() in ("1","true","yes"))

    # Attach Drive if configured
    drive_root = cfg("DRIVE_ROOT_ID", "", str)
    if drive_root:
        drive = get_drive_client()
        p.attach_drive(
            drive_client=drive,
            root_folder_id=drive_root,
            drive_only=bool(str(cfg("DRIVE_ONLY", "true")).lower() == "true"),
            delete_local_after_upload=bool(str(cfg("DELETE_LOCAL_AFTER", "true")).lower() == "true"),
        )
    return p


# --------------------------
# Header - SARAH Theme
# --------------------------
st.markdown("""
<div class="main-header">
  <h1 class="brand-logo" aria-label="SARAH CASE">
    <span class="brand-sarah">SARAH</span><span class="brand-case">CASE</span>
  </h1>
  <p class="subtitle">An AI Agent for UK Corporate Insolvency Case Extraction</p>
</div>
""", unsafe_allow_html=True)



# --------------------------
# Sidebar
# --------------------------
with st.sidebar:
    st.markdown("### ⚙️ Pipeline Options")
    
    force_companies = st.checkbox(
        "🔄 Force refresh companies",
        help="Ignore cache and fetch fresh company data from The Gazette"
        , key="force_companies"
    )
    
    force_docs = st.checkbox(
        "📄 Force re-download documents", 
        help="Re-download documents even if they exist in cache"
        , key="force_docs"
    )
    
    st.markdown("---")
    
    # Show current session activity
    gazette_state = st.session_state.get("gazette_run_state", "idle")
    research_state = st.session_state.get("research_run_state", "idle")
    
    if gazette_state == "running":
        st.info("🔍 Search by Procedure in progress...")
    elif research_state == "running":
        st.info("🏢 Company search in progress...")
    elif gazette_state == "completed":
        last_stats = st.session_state.get("gazette_last_stats", {})
        if last_stats:
            st.success(f"✅ Last run: {last_stats.get('processed', 0)} companies")
    elif research_state == "completed":
        last_stats = st.session_state.get("research_last_stats", {})
        if last_stats:
            st.success(f"✅ Last run: {last_stats.get('processed', 0)} companies")
    else:
        st.markdown("*Ready for extraction*")
    
    st.markdown("---")
    st.markdown(
        "<small>💡 **Tip:** Use single procedure types for best classification accuracy</small>",
        unsafe_allow_html=True
    )

# --------------------------
# Main Navigation - SARAH Theme
# --------------------------
tabs = st.tabs([
    "Search by Procedure",         # Gazette-backed
    "Search by Company Number",    # CH direct
    "Company Voluntary Arrangements",  # NEW, metadata-driven
    "Moratorium"                  # NEW, spreadsheet-driven

])

# --------------------------
# TAB 1: Search by Procedure (using gazette_codes + optional notice_filters)
# --------------------------
with tabs[0]:
    _ensure_run_state("gazette")

    # CSS: placeholder color + compact spacing
    st.markdown("""
    <style>
      .stMultiSelect [data-baseweb="select"] input::placeholder { color: #000 !important; opacity: 1; }
      .stMultiSelect [data-baseweb="select"] [class*="placeholder"] { color: #000 !important; }
      .stMultiSelect [data-baseweb="select"] div[data-baseweb="placeholder"] { color: #000 !important; }
      .stMultiSelect { margin-bottom: 12px; }
      .stDateInput  { margin-bottom: 12px; }
      .stButton     { margin-top: 12px; }
    </style>
    """, unsafe_allow_html=True)

    # Title (white via global CSS)
    st.markdown("## Find and download cases using procedure type and date range")

    # Control bar (STOP / NEW SEARCH etc.)
    render_control_buttons("gazette")

    gazette_locked = st.session_state["gazette_run_state"] == "running"

    # Build choices from taxonomy (only Gazette-backed procedures)
    GAZETTE_CHOICES = [p for p in PROCEDURES if getattr(p, "enabled_in_gazette", False)]
    options_labels = [p.label for p in GAZETTE_CHOICES]
    label_to_proc = {p.label: p for p in GAZETTE_CHOICES}

    # Selector (labels)
    selected_labels = st.multiselect(
        "Select Company Insolvency Procedure",
        options=options_labels,
        default=[],
        placeholder="Choose single or multiple options from drop down",
        help="💡 For most accurate results, select a single procedure type. Multiple selections may affect classification precision.",
        label_visibility="visible",    # keep help icon visible
        disabled=gazette_locked,
    )
    selected_defs = [label_to_proc[lbl] for lbl in selected_labels]

    # Collect Gazette category codes (what the scraper accepts)
    categories_for_scraper = sorted({
        code for p in selected_defs for code in (p.gazette_codes or [])
    })

    # Gather optional notice filters.
    # Only apply when a single procedure is selected to avoid over-filtering.
    notice_filters = []
    if len(selected_defs) == 1:
        only_def = selected_defs[0]
        if getattr(only_def, "notice_filters", None):
            notice_filters.extend(only_def.notice_filters or [])

    # Dates
    col1, col2 = st.columns(2)
    today = date.today()
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=today - timedelta(days=1),
            format="DD/MM/YYYY",
            max_value=today,
            disabled=gazette_locked,
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=today,
            format="DD/MM/YYYY",
            min_value=start_date + timedelta(days=1),
            max_value=today,
            disabled=gazette_locked,
        )

    # Download button (require at least one choice)
    # --- Download button (disabled until a procedure is selected) ---
    if st.session_state["gazette_run_state"] == "idle":
        # 1) Map selected labels -> ProcedureDef -> gazette codes
        selected_defs = [label_to_proc[lbl] for lbl in selected_labels]
        selected_codes = [code for d in selected_defs for code in d.gazette_codes]

        # 2) Validate + enable/disable
        dates_ok = validate_dates(start_date, end_date)
        disabled_btn = (len(selected_codes) == 0) or (not dates_ok)

        if st.button(
            "DOWNLOAD",
            type="primary",
            width='stretch',
            key="download_gazette",
            disabled=disabled_btn,
        ):
            # 3) Preflight count on page 1
            s_str = start_date.strftime("%d/%m/%Y")
            e_str = end_date.strftime("%d/%m/%Y")
            total = preflight_gazette_count(s_str, e_str, selected_codes)

            if total == 0:
                # Don’t start a run; show actionable guidance
                def widen_7():
                    ns, ne = make_auto_expand_dates(start_date, end_date, days=7)
                    st.session_state["_tab1_start"] = ns
                    st.session_state["_tab1_end"] = ne
                    st.rerun()

                show_zero_results_tip(
                    "No notices found for your filters.",
                    suggestions=[
                        "Widen the date range",
                        "Select different or additional procedures",
                        "Double-check category mappings",
                    ],
                    actions=[("Widen date range", widen_7)]
                )
            else:
                # 4) Proceed with run
                # Pass the **gazette codes** to the pipeline (not labels)
                st.session_state["_tab1_selected_codes"] = selected_codes
                _start_run("gazette")
                st.rerun()

    # If user clicked “Widen by 7 days”, apply the new dates before inputs render again
    if "_tab1_start" in st.session_state and "_tab1_end" in st.session_state:
        start_date = st.session_state.pop("_tab1_start")
        end_date = st.session_state.pop("_tab1_end")


    # --------------------------
    # RUNNING FLOW
    # --------------------------
    if st.session_state["gazette_run_state"] == "running":
        cancel_event = st.session_state["gazette_cancel_event"]

        st.markdown("### 📊 Extraction Progress")
        prog, summary_ph = _progress_widgets()
        telemetry_ph = st.empty()
        log_ph = st.empty()
        upload_telemetry = _init_upload_telemetry_state()

        pipeline = get_pipeline(
            base_folder=st.secrets.get("BASE_FOLDER", "InsolvencyDocuments_Cached"),
            force_refresh=force_companies,
            force_redownload=force_docs,
        )

        progress_q: Queue = Queue(maxsize=2048)
        pipeline.set_progress_emitter(progress_q.put)

        # Context for classification; the scraper can accept raw G-codes
        pipeline._current_search_categories = categories_for_scraper

        # If exactly one procedure selected, force the final folder name to the client’s label
        if len(selected_defs) == 1:
            pipeline._forced_folder = selected_defs[0].folder
        else:
            if hasattr(pipeline, "_forced_folder"):
                delattr(pipeline, "_forced_folder")

        start_date_str = start_date.strftime("%d/%m/%Y")
        end_date_str = end_date.strftime("%d/%m/%Y")

        if _is_cancelled("gazette"):
            st.warning("Run cancelled after gazette search.")
            _finish_run("gazette", cancelled=True)
            st.rerun()

        if not categories_for_scraper:
            st.warning("Please select at least one procedure type.")
            _finish_run("gazette", cancelled=True)
            st.rerun()

        selected_codes = st.session_state.pop("_tab1_selected_codes", [])

        if _is_cancelled("gazette"):
            st.warning("Run cancelled before starting.")
            _finish_run("gazette", cancelled=True)
            st.rerun()

        if not categories_for_scraper:
            st.warning("Please select at least one procedure type.")
            _finish_run("gazette", cancelled=True)
            st.rerun()

        max_docs = int(MAX_DOCS_DEFAULT)
        cache = getattr(pipeline, "cache", None)
        company_queue: Queue = Queue()  # unbounded so downloads can start instantly
        cached_results_queue: Queue = Queue()
        stream_stats_holder: Dict[str, Any] = {}
        stream_error_holder: Dict[str, Exception] = {}
        producer_finished = threading.Event()
        planner_finished = threading.Event()
        future_lock = threading.Lock()
        st.session_state.setdefault("gazette_futures", set())
        _gazette_futures_fallback: set[Any] = set()

        def _get_gazette_futures() -> set[Any]:
            """Access session_state safely even when called off the Streamlit thread."""
            try:
                return st.session_state.setdefault("gazette_futures", set())
            except Exception:
                return _gazette_futures_fallback

        def _producer():
            try:
                stats_ret = pipeline.gazette_scraper.search_insolvencies_streaming(
                    start_date=start_date_str,
                    end_date=end_date_str,
                    categories=selected_codes,
                    max_pages=int(MAX_PAGES_DEFAULT),
                    fetch_company_numbers=True,
                    company_queue=company_queue,
                    cancel_event=cancel_event,
                )
                stream_stats_holder["value"] = stats_ret
            except Exception as exc:
                stream_error_holder["value"] = exc
            finally:
                producer_finished.set()
                # Always signal EOF even if cancellation fired mid-stream
                try:
                    company_queue.put(None)
                except Exception:
                    pass

        producer_thread = threading.Thread(target=_producer, name="gazette-stream", daemon=True)
        producer_thread.start()

        planner_progress = {"seen": 0}
        planner_lock = threading.Lock()

        def _planner_mark_seen():
            with planner_lock:
                planner_progress["seen"] += 1

        def _planner_seen_total() -> int:
            with planner_lock:
                return planner_progress["seen"]

        executor_holder: Dict[str, ThreadPoolExecutor | None] = {"pool": None}
        st.session_state["gazette_executor"] = None
        future_to_company: Dict[Any, Dict] = {}
        active_futures: set = set()

        def _submit_company(comp: dict):
            """Schedule a company download immediately (planner thread)."""
            if cancel_event.is_set():
                return
            pool = executor_holder["pool"]
            if pool is None:
                pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY_DEFAULT)
                executor_holder["pool"] = pool
                st.session_state["gazette_executor"] = pool
            fut = pool.submit(
                pipeline.process_company_with_intelligent_caching,
                comp,
                max_documents=max_docs,
                cancel_event=cancel_event,
            )
            with future_lock:
                future_to_company[fut] = comp
                active_futures.add(fut)
                _get_gazette_futures().add(fut)

        def _planner():
            local_seen: set[str] = set()
            while True:
                if cancel_event.is_set():
                    break
                try:
                    comp = company_queue.get(timeout=0.2)
                except Empty:
                    if producer_finished.is_set():
                        break
                    continue
                if comp is None:
                    break
                number = (comp.get("company_number") or "").strip()
                if not number:
                    continue
                if notice_filters:
                    nt = (comp.get("notice_type") or "")
                    if not any(pat.search(nt) for pat in notice_filters):
                        continue
                if number in local_seen:
                    continue
                local_seen.add(number)
                _planner_mark_seen()
                if cache and not (force_companies or force_docs):
                    ok, cached_data = cache.is_company_cached(number)
                    if ok and cached_data and cached_data.get("success"):
                        doc_links = cached_data.get("document_links") or []
                        ok_cached, cached_result = _make_cached_result(
                            pipeline,
                            comp,
                            cached_data,
                            len(doc_links),
                            max_docs,
                        )
                        if ok_cached and cached_result:
                            cached_results_queue.put(cached_result)
                            continue
                            # cache invalid; fall through to fresh execution
                _submit_company(comp)
            planner_finished.set()

        planner_thread = threading.Thread(target=_planner, name="gazette-planner", daemon=True)
        planner_thread.start()

        stats = {
            "processed": 0,
            "successful": 0,
            "errors": 0,
            "downloaded": 0,
            "from_cache": 0,
            "total_documents": 0,
            "items": []
        }
        stats_display = stats.copy()
        inflight_downloads = defaultdict(int)
        results: list = []
        cancelled = False
        total_estimate = 1
        last_planner_seen = 0
        company_target_hint = 0
        def _apply_progress_events(events: List[dict]):
            for evt in events:
                if evt.get("kind") == "documents_uploaded":
                    company = evt.get("company_number")
                    count = int(evt.get("count", 0) or 0)
                    if company and count:
                        inflight_downloads[company] += count
                elif evt.get("kind") == "document_progress":
                    company = evt.get("company_number")
                    fresh = int(evt.get("fresh", 0) or 0)
                    cached = int(evt.get("cached", 0) or 0)
                    delta = fresh + cached
                    if company and delta:
                        inflight_downloads[company] += delta
                elif evt.get("kind") in ("upload_progress", "rate_limit"):
                    _update_upload_telemetry_state(upload_telemetry, evt)
        def _refresh_display_stats():
            stats_display.update(
                {
                    "processed": stats["processed"],
                    "successful": stats["successful"],
                    "from_cache": stats["from_cache"],
                    "errors": stats["errors"],
                }
            )
            in_flight = sum(inflight_downloads.values())
            stats_display["downloaded"] = stats["downloaded"]
            base_total = stats["downloaded"] + stats["from_cache"]
            stats["total_documents"] = base_total
            stats_display["total_documents"] = base_total
            stats_display["inflight_documents"] = in_flight

        def _ingest_cached_result(cached_result: dict):
            if not cached_result:
                return
            results.append(cached_result)
            stats["processed"] += 1
            stats["successful"] += 1
            cached_count = int(cached_result.get("documents_from_cache", 0))
            stats["from_cache"] += cached_count
            stats["total_documents"] += cached_count
            stats["items"].append(cached_result)
            _refresh_display_stats()

        def _company_total_with_active(active_extra: int = 0) -> int:
            base = max(last_planner_seen, stats["processed"])
            hint = company_target_hint or 0
            if base == 0 and hint:
                base = hint
            if active_extra:
                base = max(base, stats["processed"] + active_extra)
            return max(base, 1)

        _refresh_display_stats()
        _render_progress_summary(prog, summary_ph, stats_display, _company_total_with_active())
        if SHOW_TELEMETRY:
            _render_upload_telemetry(telemetry_ph, upload_telemetry)
        last_telemetry_render = time.time()

        while True:
            events = _drain_progress(progress_q, log_ph)
            if events:
                _apply_progress_events(events)
                _refresh_display_stats()
                if SHOW_TELEMETRY:
                    _render_upload_telemetry(telemetry_ph, upload_telemetry)
                last_telemetry_render = time.time()
            if _is_cancelled("gazette"):
                cancelled = True

            # Pull cached companies emitted by planner (no executor work needed)
            while True:
                try:
                    cached_result = cached_results_queue.get_nowait()
                except Empty:
                    break
                _ingest_cached_result(cached_result)
                _render_progress_summary(
                    prog,
                    summary_ph,
                    stats_display,
                    _company_total_with_active(),
                )
                if SHOW_TELEMETRY:
                    _render_upload_telemetry(telemetry_ph, upload_telemetry)
                last_telemetry_render = time.time()

            # Handle completed futures
            completed: List[tuple[Any, Dict]] = []
            with future_lock:
                done = [f for f in list(active_futures) if f.done()]
                for fut in done:
                    active_futures.remove(fut)
                    comp = future_to_company.pop(fut, {})
                    _get_gazette_futures().discard(fut)
                    completed.append((fut, comp))
            for fut, comp in completed:
                try:
                    res = fut.result()
                    res.setdefault("documents_downloaded_fresh", int(res.get("documents_downloaded", 0)))
                except Exception as e:
                    res = {
                        "success": False,
                        "errors": [str(e)],
                        "company_number": comp.get("company_number", "?"),
                        "company_name": comp.get("company_name", "?"),
                        "documents_downloaded": 0,
                        "documents_downloaded_fresh": 0,
                        "documents_from_cache": 0,
                        "folder_path": "",
                        "procedure_folder": pipeline._classify_direct(
                            comp.get("notice_type", "CVL")
                        ),
                    }
                events = _drain_progress(progress_q, log_ph)
                if events:
                    _apply_progress_events(events)

                results.append(res)
                stats["processed"] += 1
                stats["successful"] += 1 if res.get("success") else 0
                downloaded_count = int(res.get("documents_downloaded", 0))
                cached_count = int(res.get("documents_from_cache", 0))
                stats["downloaded"] += downloaded_count
                stats["from_cache"] += cached_count
                stats["total_documents"] += downloaded_count + cached_count
                stats["errors"] += len(res.get("errors") or [])
                stats["items"].append(res)
                company_id = res.get("company_number")
                if company_id and company_id in inflight_downloads:
                    inflight_downloads.pop(company_id, None)
                _refresh_display_stats()

            with future_lock:
                active_count = len(active_futures)
            planner_seen = _planner_seen_total()
            last_planner_seen = max(last_planner_seen, planner_seen)
            seen_total = max(planner_seen, stats["processed"])
            estimated_total = max(seen_total, stats["processed"] + active_count)
            if not producer_finished.is_set():
                estimated_total = max(estimated_total, seen_total + active_count + 1)
            if not planner_finished.is_set():
                estimated_total = max(estimated_total, seen_total + active_count + 1)
            stream_total = None
            if stream_stats_holder.get("value"):
                stream_total = stream_stats_holder["value"].get("total_companies")
                if stream_total and planner_seen == 0 and stats["processed"] == 0:
                    company_target_hint = max(company_target_hint, int(stream_total) or 0)
            display_total = _company_total_with_active(active_count)
            total_estimate = max(total_estimate, display_total)
            _render_progress_summary(prog, summary_ph, stats_display, display_total)
            if SHOW_TELEMETRY:
                _render_upload_telemetry(telemetry_ph, upload_telemetry)
            last_telemetry_render = time.time()

            if cancelled:
                with future_lock:
                    to_cancel = list(active_futures)
                for fut in to_cancel:
                    fut.cancel()
                break

            with future_lock:
                no_active = not active_futures
            if producer_finished.is_set() and planner_finished.is_set() and no_active:
                break

            now_tick = time.time()
            if now_tick - last_telemetry_render > 1.0:
                if SHOW_TELEMETRY:
                    _render_upload_telemetry(telemetry_ph, upload_telemetry)
                last_telemetry_render = now_tick
            time.sleep(0.05)

        producer_thread.join(timeout=0.1)
        planner_thread.join(timeout=0.1)
        events = _drain_progress(progress_q, log_ph)
        if events:
            _apply_progress_events(events)
        _refresh_display_stats()
        if SHOW_TELEMETRY:
            _render_upload_telemetry(telemetry_ph, upload_telemetry)
        last_telemetry_render = time.time()
        while True:
            try:
                cached_result = cached_results_queue.get_nowait()
            except Empty:
                break
            _ingest_cached_result(cached_result)
        _refresh_display_stats()

        pool = executor_holder["pool"]
        if pool:
            try:
                pool.shutdown(wait=False)
            except Exception:
                pass
        st.session_state["gazette_executor"] = None

        if stream_error_holder and stream_error_holder.get("value") and stats["processed"] == 0 and not cancelled:
            st.error(f"Gazette search failed: {stream_error_holder['value']}")
            _finish_run("gazette", cancelled=True)
            if hasattr(pipeline, "_forced_folder"):
                delattr(pipeline, "_forced_folder")
            st.rerun()

        if not cancelled:
            with st.spinner("☁️ finalising uploads to Drive…"):
                try:
                    pipeline.wait_for_uploads(timeout=None)
                except Exception:
                    pass
                try:
                    st.write("Finalising uploads…")
                    pipeline.wait_for_uploads(timeout=None)
                    pending_batches = 0
                    try:
                        with pipeline._batch_lock:
                            pending_batches = len(getattr(pipeline, "_batch_specs", []))
                    except Exception:
                        pass
                    st.caption(f"Pending small-file batch items: {pending_batches}")
                except Exception:
                    pass
                try:
                    pipeline.flush_and_stop_upload_workers()
                except Exception:
                    pass

        seen_final = max(_planner_seen_total(), last_planner_seen)
        final_total = max(seen_final, stats["processed"], company_target_hint or 0, 1)
        _refresh_display_stats()
        _render_progress_summary(prog, summary_ph, stats_display, final_total)

        if cancelled:
            inflight_downloads.clear()
            _refresh_display_stats()
            st.warning("Run cancelled. Summary includes completed items only.")
        else:
            st.success("🎉 Download Complete!")

        run_config = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "categories": ", ".join(selected_labels) if selected_labels else "Gazette extraction",
        }
        _save_results("gazette", stats, stats["items"], run_config=run_config)
        _render_results_summary("gazette", stats, stats["items"])
        _log_download_rate("gazette", stats.get("downloaded", 0), stats.get("from_cache", 0))
        _refresh_display_stats()
        _render_progress_summary(prog, summary_ph, stats_display, final_total)

        # Clear forced folder to avoid leaking to other tabs/runs
        if hasattr(pipeline, "_forced_folder"):
            delattr(pipeline, "_forced_folder")

        _finish_run("gazette", cancelled=cancelled)
        time.sleep(0.1)
        st.rerun()

    # --------------------------
    # PREVIOUS RESULTS
    # --------------------------
    if st.session_state["gazette_run_state"] in ("idle", "completed", "cancelled"):
        last_stats, last_results, last_timestamp = _get_last_results("gazette")
        if last_stats and last_timestamp:
            uk_time = datetime.fromtimestamp(last_timestamp, tz=UK_TZ)
            run_time = uk_time.strftime("%d/%m/%Y %H:%M:%S %Z")
            st.markdown(f"### 📊 Last Run Results ({run_time})")
            _render_results_summary("gazette", last_stats, last_results)


# --------------------------
# TAB 2: Search by Company Number (Updated labels)
# --------------------------
with tabs[1]:
    _ensure_run_state("research")

    st.markdown("## Find and download cases using company number")

    render_control_buttons("research")

    research_locked = st.session_state["research_run_state"] == "running"

    nums_text = st.text_area(
        "Enter Company Number (one per row)",
        value="",
        height=100,
        placeholder="12345678\nSC123456",
        disabled=research_locked,
    )
    raw_numbers = sanitize_company_numbers(nums_text)
    dedup_numbers: List[str] = []
    seen_numbers: set[str] = set()
    for n in raw_numbers:
        if n and n not in seen_numbers:
            seen_numbers.add(n)
            dedup_numbers.append(n)
    numbers = dedup_numbers
    companies_count = len(numbers)

    if companies_count == 0 and nums_text.strip():
        st.warning("No valid company numbers detected. Use 8 digits or SC/NI + 6 digits (one per line).")

    if st.session_state["research_run_state"] == "idle":
        if st.button(
            "DOWNLOAD",
            type="primary",
            width='stretch',
            disabled=(companies_count == 0),
            key="download_research"
        ):
            _start_run("research")
            st.rerun()


    if st.session_state["research_run_state"] == "running":
        cancel_event = st.session_state["research_cancel_event"]

        st.markdown("### 📊 Research Progress")
        prog, summary_ph = _progress_widgets()
        telemetry_ph = st.empty()
        log_ph = st.empty()

        pipeline = get_pipeline(
            base_folder=st.secrets.get("BASE_FOLDER", "InsolvencyDocuments_Cached"),
            force_refresh=force_companies,
            force_redownload=force_docs,
        )
        progress_q: Queue = Queue(maxsize=2048)
        pipeline.set_progress_emitter(progress_q.put)

        if _is_cancelled("research"):
            st.warning("Run cancelled before starting.")
            _finish_run("research", cancelled=True)
            st.rerun()

        results = []
        total = len(numbers)
        stats = {"processed": 0, "successful": 0, "downloaded": 0, "from_cache": 0, "total_documents": 0, "errors": 0}
        stats_display = stats.copy()
        inflight_downloads = defaultdict(int)

        upload_telemetry = _init_upload_telemetry_state()

        def _research_apply_events(events: List[dict]):
            for evt in events:
                if evt.get("kind") == "documents_uploaded":
                    company = evt.get("company_number")
                    count = int(evt.get("count", 0) or 0)
                    if company and count:
                        inflight_downloads[company] += count
                elif evt.get("kind") == "document_progress":
                    company = evt.get("company_number")
                    fresh = int(evt.get("fresh", 0) or 0)
                    cached = int(evt.get("cached", 0) or 0)
                    delta = fresh + cached
                    if company and delta:
                        inflight_downloads[company] += delta
                elif evt.get("kind") in ("upload_progress", "rate_limit"):
                    _update_upload_telemetry_state(upload_telemetry, evt)

        def _research_refresh_display():
            stats_display.update(
                {
                    "processed": stats["processed"],
                    "successful": stats["successful"],
                    "from_cache": stats["from_cache"],
                    "errors": stats["errors"],
                }
            )
            inflight_total = sum(inflight_downloads.values())
            stats_display["downloaded"] = stats["downloaded"]
            base_total = stats["downloaded"] + stats["from_cache"]
            stats["total_documents"] = base_total
            stats_display["total_documents"] = base_total
            stats_display["inflight_documents"] = inflight_total

        cancelled = False
        _research_refresh_display()
        _render_progress_summary(prog, summary_ph, stats_display, total or 1)
        if SHOW_TELEMETRY:
            _render_upload_telemetry(telemetry_ph, upload_telemetry)
        last_telemetry_render = time.time()

        worker_count = max(1, min(total or 1, MAX_CONCURRENCY_DEFAULT))
        executor: ThreadPoolExecutor | None = ThreadPoolExecutor(max_workers=worker_count) if total else None
        future_lock = threading.Lock()
        future_to_number: Dict[Any, str] = {}
        active_futures: set[Any] = set()

        def _submit_company_number(num: str):
            if not executor or _is_cancelled("research"):
                return
            fut = executor.submit(
                pipeline.download_company_by_number,
                num,
                max_documents=int(MAX_DOCS_DEFAULT),
                research_root="Research",
                cancel_event=cancel_event,
            )
            with future_lock:
                future_to_number[fut] = num
                active_futures.add(fut)

        for num in numbers:
            _submit_company_number(num)

        try:
            if executor:
                with st.spinner(f"🔍 Processing {total} companies..."):
                    while stats["processed"] < total:
                        events = _drain_progress(progress_q, log_ph)
                        if events:
                            _research_apply_events(events)
                            _research_refresh_display()
                            _render_progress_summary(prog, summary_ph, stats_display, total)
                            if SHOW_TELEMETRY:
                                _render_upload_telemetry(telemetry_ph, upload_telemetry)
                            last_telemetry_render = time.time()

                        completed: List[tuple[Any, str]] = []
                        with future_lock:
                            done = [f for f in list(active_futures) if f.done()]
                            for fut in done:
                                active_futures.remove(fut)
                                num = future_to_number.pop(fut, "?")
                                completed.append((fut, num))

                        if not completed and not active_futures and stats["processed"] >= total:
                            break

                        for fut, num in completed:
                            try:
                                res = fut.result()
                            except Exception as e:
                                res = {
                                    "success": False,
                                    "company_number": num,
                                    "company_name": num,
                                    "documents_downloaded": 0,
                                    "documents_from_cache": 0,
                                    "errors": [str(e)],
                                }
                            results.append(res)
                            stats["processed"] += 1
                            stats["successful"] += 1 if res.get("success") else 0
                            downloaded_count = int(res.get("documents_downloaded", 0))
                            cached_count = int(res.get("documents_from_cache", 0))
                            stats["downloaded"] += downloaded_count
                            stats["from_cache"] += cached_count
                            stats["total_documents"] += downloaded_count + cached_count
                            stats["errors"] += len(res.get("errors") or [])
                            inflight_downloads.pop(res.get("company_number"), None)
                            _research_refresh_display()
                            _render_progress_summary(prog, summary_ph, stats_display, total)
                            if SHOW_TELEMETRY:
                                _render_upload_telemetry(telemetry_ph, upload_telemetry)

                        if _is_cancelled("research"):
                            cancelled = True
                            with future_lock:
                                for fut in list(active_futures):
                                    fut.cancel()
                            if not active_futures:
                                break

                        if not active_futures and stats["processed"] >= total:
                            break

                        now_tick = time.time()
                        if now_tick - last_telemetry_render > 1.0:
                            if SHOW_TELEMETRY:
                                _render_upload_telemetry(telemetry_ph, upload_telemetry)
                            last_telemetry_render = now_tick
                        time.sleep(0.05)
            else:
                st.info("No valid company numbers to process.")

            if not cancelled:
                with st.spinner("☁️ finalising uploads to Drive…"):
                    try:
                        pipeline.wait_for_uploads(timeout=None)
                    except Exception:
                        pass
                    try:
                        st.write("Finalising uploads…")
                        pipeline.wait_for_uploads(timeout=None)
                        pending_batches = 0
                        try:
                            with pipeline._batch_lock:
                                pending_batches = len(getattr(pipeline, "_batch_specs", []))
                        except Exception:
                            pass
                        st.caption(f"Pending small-file batch items: {pending_batches}")
                    except Exception:
                        pass
                    try:
                        pipeline.flush_and_stop_upload_workers()
                    except Exception:
                        pass

        except Exception as e:
            st.error(f"Processing error: {e}")
            cancelled = True
        finally:
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)

        events = _drain_progress(progress_q, log_ph)
        if events:
            _research_apply_events(events)
        _research_refresh_display()
        if SHOW_TELEMETRY:
            _render_upload_telemetry(telemetry_ph, upload_telemetry)

        if cancelled:
            inflight_downloads.clear()
            _research_refresh_display()
            st.warning("Run cancelled. Summary includes completed items only.")
        else:
            st.success("🎉 Download Complete!")

        preview = ", ".join(numbers[:8])
        if len(numbers) > 8:
            preview += f", +{len(numbers) - 8} more"
        if not preview:
            preview = "no company numbers"
        run_config = {
            "start_date": "N/A",
            "end_date": "N/A",
            "categories": f"Research (company numbers: {companies_count}) • {preview}",
        }
        _save_results("research", stats, results, run_config=run_config)
        _render_results_summary("research", stats, results)
        _research_refresh_display()
        _render_progress_summary(prog, summary_ph, stats_display, total)
        if SHOW_TELEMETRY:
            _render_upload_telemetry(telemetry_ph, upload_telemetry)

        # If nothing downloaded, show actionable tip
        if stats.get("downloaded", 0) == 0:
            show_zero_results_tip(
                "No downloadable documents found for the supplied company numbers.",
                suggestions=[
                    "Confirm the numbers on Companies House",
                    "Try fewer companies to rule out rate limiting",
                    "Retry without forcing re-download",
                ],
            )
        _log_download_rate("research", stats.get("downloaded", 0), stats.get("from_cache", 0))
        _finish_run("research", cancelled=cancelled)
        time.sleep(0.1)
        st.rerun()

    if st.session_state["research_run_state"] in ("idle", "completed", "cancelled"):
        last_stats, last_results, last_timestamp = _get_last_results("research")
        if last_stats and last_timestamp:
            uk_time = datetime.fromtimestamp(last_timestamp, tz=UK_TZ)
            run_time = uk_time.strftime("%d/%m/%Y %H:%M:%S %Z")
            st.markdown(f"### 📊 Last Run Results ({run_time})")
            
            cols = st.columns(4)
            with cols[0]:
                st.metric("Companies", last_stats.get("processed", 0))
            with cols[1]:
                st.metric("Successful", last_stats.get("successful", 0))
            with cols[2]:
                total_docs = last_stats.get("total_documents", 0)
                if not total_docs:
                    total_docs = last_stats.get("downloaded", 0) + last_stats.get("from_cache", 0)
                st.metric("Documents", total_docs)
            with cols[3]:
                st.metric("From Cache", last_stats.get("from_cache", 0))
            
            if last_results and len([r for r in last_results if r.get("success")]) > 0:
                with st.expander("📋 Previous Search Details", expanded=False):
                    import pandas as pd
                    df_data = []
                    for res in last_results[:20]:
                        total_docs_item = int(res.get("documents_downloaded", 0)) + int(res.get("documents_from_cache", 0))
                        df_data.append({
                            "Company Number": res.get("company_number", "N/A"),
                            "Company Name": res.get("company_name", "N/A")[:50],
                            "Documents": total_docs_item,
                            "From Cache": res.get("documents_from_cache", 0),
                            "Status": "✅ Success" if res.get("success") else "❌ Failed",
                        })
                    if df_data:
                        df = pd.DataFrame(df_data)
                        st.dataframe(df, width='stretch')
                    if len(last_results) > 20:
                        st.info(f"... and {len(last_results) - 20} more companies processed")


# =========================
# TAB 3: CVA by Commencement Date (minimal UI)
# =========================

CVA_SOURCE_URL = (
    "https://find-and-update.company-information.service.gov.uk/advanced-search/"
    "get-results?companyNameIncludes=&companyNameExcludes=&registeredOfficeAddress=&"
    "incorporationFromDay=&incorporationFromMonth=&incorporationFromYear=&"
    "incorporationToDay=&incorporationToMonth=&incorporationToYear=&"
    "status=voluntary-arrangement&sicCodes=&dissolvedFromDay=&dissolvedFromMonth=&"
    "dissolvedFromYear=&dissolvedToDay=&dissolvedToMonth=&dissolvedToYear="
)

with tabs[2]:
    _ensure_run_state("cva")
    state = st.session_state["cva_run_state"]  # "idle" | "running" | "completed" | "cancelled"

    st.markdown("## Find and download CVA cases using commencement date")

    render_control_buttons("cva")  # your existing STOP / NEW SEARCH bar

    # --- Date inputs (only UI we expose) ---
    c1, c2 = st.columns(2)
    today = date.today()
    min_iso, max_iso = get_date_bounds()

    def _iso_to_date(iso_str: str | None, fallback: date) -> date:
        if not iso_str:
            return fallback
        try:
            return datetime.strptime(iso_str, "%Y-%m-%d").date()
        except Exception:
            return fallback

    min_available = _iso_to_date(min_iso, date(1950, 1, 1))
    max_available = today
    if min_available > max_available:
        min_available = date(1950, 1, 1)
    default_start = min_available
    default_end = max_available

    with c1:
        start_d = st.date_input(
            "Start Date",
            value=default_start,
            format="DD/MM/YYYY",
            min_value=min_available,
            max_value=max_available,
            disabled=(state != "idle"),
            key="cva_start",
        )
    with c2:
        end_d = st.date_input(
            "End Date",
            value=default_end,
            format="DD/MM/YYYY",
            min_value=start_d,
            max_value=max_available,
            disabled=(state != "idle"),
            key="cva_end",
        )

    if not min_iso or not max_iso:
        st.info("CVA index is empty. A refresh will run automatically when you start a download.")
    else:
        earliest = min_available
        # st.caption(
        #     f"CVA meeting dates currently indexed from {earliest:%d %b %Y} through today ({today:%d %b %Y})."
        # )

    # ---- Banner rules (never show "complete" while running) ----
    # if state == "completed":
    #     st.success("Download complete. Click **NEW SEARCH** to run another.", icon="✅")

    # ---- Button visibility: only when idle ----
    if state == "idle":
        if st.button("DOWNLOAD", type="primary", width='stretch',
                     disabled=(start_d > end_d), key="cva_download"):
            _start_run("cva")
            st.session_state["cva_task"] = {"op": "download", "start": start_d, "end": end_d}
            st.rerun()

    # ---- Auto-refresh the CVA index if stale (runs only when idle) ----
    if state == "idle":
        if CStore.needs_bulk_refresh() and not st.session_state.get("cva_refresh_kicked"):
            st.session_state["cva_refresh_kicked"] = True
            _start_run("cva")
            st.session_state["cva_task"] = {"op": "refresh"}
            st.rerun()


    # -------------------
    # RUNNING
    # -------------------
    if state == "running":
        task = st.session_state.get("cva_task", {}) or {}

        st.markdown("### 📊 Extraction Progress")
        prog, summary_ph = _progress_widgets()
        log_ph = st.empty()
        cancel_event = st.session_state.get("cva_cancel_event")

        if task.get("op") == "refresh":
            stats = {"processed": 0, "successful": 0, "downloaded": 0, "from_cache": 0, "total_documents": 0}

            def _refresh_progress(done: int, total: int, label: str):
                stats["processed"] = done
                stats["successful"] = done
                _render_progress_summary(
                    prog,
                    summary_ph,
                    stats,
                    max(total, 1),
                    label="Companies indexed",
                    show_documents=False,
                )
                if label and label not in ("starting", ""):
                    with log_ph:
                        st.write(f"📇 {label}")

            try:
                refresh_index(force=True, cancel_event=cancel_event, progress_cb=_refresh_progress)
            except Exception as exc:
                st.error(f"CVA refresh failed: {exc}")

            _finish_run("cva", cancelled=bool(cancel_event and cancel_event.is_set()))
            st.rerun()

        elif task.get("op") == "download":
            start_iso = task["start"].strftime("%Y-%m-%d")
            end_iso = task["end"].strftime("%Y-%m-%d")
            hits = CStore.query_between(start_iso, end_iso)
            total = len(hits)
            if total == 0:
                st.warning("No CVA companies found in that date range.")
                _finish_run("cva", cancelled=False)
                st.rerun()

            pipeline = get_pipeline(
                base_folder=st.secrets.get("BASE_FOLDER", "InsolvencyDocuments_Cached"),
                force_refresh=force_companies,
                force_redownload=force_docs,
            )
            pipeline._forced_folder = "CVA"

            progress_q: Queue = Queue(maxsize=2048)
            pipeline.set_progress_emitter(progress_q.put)

            stats = {
                "processed": 0,
                "successful": 0,
                "errors": 0,
                "downloaded": 0,
                "from_cache": 0,
                "total_documents": 0,
            }
            stats_display = stats.copy()
            inflight_downloads = defaultdict(int)
            results = []

            def _cva_apply_events(events: List[dict]):
                for evt in events:
                    if evt.get("kind") == "documents_uploaded":
                        company = evt.get("company_number")
                        count = int(evt.get("count", 0) or 0)
                        if company and count:
                            inflight_downloads[company] += count
                    elif evt.get("kind") == "document_progress":
                        company = evt.get("company_number")
                        fresh = int(evt.get("fresh", 0) or 0)
                        cached = int(evt.get("cached", 0) or 0)
                        delta = fresh + cached
                        if company and delta:
                            inflight_downloads[company] += delta

            def _cva_refresh_display():
                stats_display.update(
                    {
                        "processed": stats["processed"],
                        "successful": stats["successful"],
                        "from_cache": stats["from_cache"],
                        "errors": stats["errors"],
                        "downloaded": stats["downloaded"],
                        "total_documents": stats["downloaded"] + stats["from_cache"],
                        "inflight_documents": sum(inflight_downloads.values()),
                    }
                )

            _cva_refresh_display()
            _render_progress_summary(prog, summary_ph, stats_display, max(total, 1))

            with ThreadPoolExecutor(max_workers=max(1, min(total, MAX_CONCURRENCY_DEFAULT))) as ex:
                futures = {
                    ex.submit(
                        pipeline.download_company_by_number,
                        row["company_number"],
                        max_documents=int(MAX_DOCS_DEFAULT),
                        research_root="CVA",
                        cancel_event=cancel_event,
                    ): row
                    for row in hits
                }

                pending = set(futures.keys())
                cancelled_run = False
                while pending:
                    events = _drain_progress(progress_q, log_ph)
                    if events:
                        _cva_apply_events(events)
                        _cva_refresh_display()
                        _render_progress_summary(prog, summary_ph, stats_display, max(total, 1))

                    done, pending = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                    if cancel_event and cancel_event.is_set():
                        cancelled_run = True
                        for fut in pending:
                            fut.cancel()
                        break

                    for fut in done:
                        row = futures[fut]
                        name = clean_company_name(row.get("company_name")) or row["company_number"]
                        try:
                            res = fut.result()
                            res.setdefault("documents_downloaded_fresh", int(res.get("documents_downloaded", 0)))
                            ok = bool(res.get("success", True))
                            docs = int(res.get("documents_downloaded", 0))
                            cache = int(res.get("documents_from_cache", 0))
                        except Exception as exc:
                            ok, docs, cache = False, 0, 0
                            with log_ph:
                                st.error(f"{name}: {exc}")

                        stats["processed"] += 1
                        stats["successful"] += 1 if ok else 0
                        stats["downloaded"] += docs
                        stats["from_cache"] += cache
                        stats["total_documents"] += docs + cache
                        if not ok:
                            stats["errors"] += 1
                        inflight_downloads.pop(row["company_number"], None)
                        _cva_refresh_display()
                        _render_progress_summary(prog, summary_ph, stats_display, max(total, 1))

                        results.append(
                            {
                                "company_number": row["company_number"],
                                "company_name": name,
                                "documents_downloaded": docs,
                                "documents_downloaded_fresh": docs,
                                "documents_from_cache": cache,
                                "success": ok,
                                "procedure_folder": res.get("procedure_folder", ""),
                                "folder_path": res.get("folder_path", ""),
                                "notice_type": "CVA",
                            }
                        )

                if not cancelled_run:
                    with st.spinner("☁️ finalising uploads to Drive…"):
                        try:
                            pipeline.wait_for_uploads(timeout=None)
                        except Exception:
                            pass
                        try:
                            st.write("Finalising uploads…")
                            pipeline.wait_for_uploads(timeout=None)
                            pending_batches = 0
                            try:
                                with pipeline._batch_lock:
                                    pending_batches = len(getattr(pipeline, "_batch_specs", []))
                            except Exception:
                                pass
                            st.caption(f"Pending small-file batch items: {pending_batches}")
                        except Exception:
                            pass
                        try:
                            pipeline.flush_and_stop_upload_workers()
                        except Exception:
                            pass

                if hasattr(pipeline, "_forced_folder"):
                    delattr(pipeline, "_forced_folder")

                run_config = {
                    "start_date": start_iso,
                    "end_date": end_iso,
                    "categories": "CVA extraction",
                }
                _save_results("cva", stats, results, run_config=run_config)
                _render_results_summary("cva", stats, results)
                _log_download_rate("cva", stats.get("downloaded", 0), stats.get("from_cache", 0))
                _cva_refresh_display()
                _render_progress_summary(prog, summary_ph, stats_display, max(total, 1))
                _finish_run("cva", cancelled=cancelled_run)
                st.rerun()

    # -------------------
    # PREVIOUS RESULTS (show when not running)
    # -------------------
    if state in ("idle", "completed", "cancelled"):
        last_stats, last_results, last_ts = _get_last_results("cva")
        if last_stats and last_ts:
            uk_time = datetime.fromtimestamp(last_ts, tz=UK_TZ)
            run_time = uk_time.strftime("%d/%m/%Y %H:%M:%S %Z")
            st.markdown(f"### 📊 Last CVA Run Results ({run_time})")
            _render_results_summary("cva", last_stats, last_results)



with tabs[3]:
    _ensure_run_state("mora")
    state = st.session_state["mora_run_state"]

    st.markdown("## Find and download Moratorium cases (England & Wales)")
    render_control_buttons("mora")

    refresh_days = int(st.secrets.get("MORA_REFRESH_DAYS", "30"))
    ensure_mora_index_fresh(refresh_days)

    min_month, max_month = get_month_bounds()

    def _month_str_to_date(month_str: Optional[str]) -> Optional[date]:
        if not month_str:
            return None
        try:
            year, month = map(int, month_str.split("-"))
            return date(year, month, 1)
        except Exception:
            return None

    today = date.today()
    min_date = _month_str_to_date(min_month) or today.replace(day=1) - timedelta(days=365)
    max_date = _month_str_to_date(max_month) or today
    if min_date > max_date:
        min_date = max_date

    c1, c2 = st.columns(2)
    with c1:
        mora_start = st.date_input(
            "Start Date",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            disabled=(state != "idle"),
            key="mora_start_date",
            format="DD/MM/YYYY",
        )
    with c2:
        mora_end = st.date_input(
            "End Date",
            value=max_date,
            min_value=mora_start,
            max_value=max_date,
            disabled=(state != "idle"),
            key="mora_end_date",
            format="DD/MM/YYYY",
        )

    if min_month and max_month:
        st.caption(
            f"Moratorium cases indexed from {min_date:%d %b %Y} through {max_date:%d %b %Y}. "
            f"Dataset refreshes every {refresh_days} days."
        )
    else:
        st.caption("Moratorium index is empty. A refresh will run automatically when data becomes available.")

    def _date_to_month(d: date) -> str:
        return d.replace(day=1).strftime("%Y-%m")

    if state == "idle":
        disabled = (mora_start > mora_end) or not (min_month and max_month)
        if st.button(
            "DOWNLOAD",
            type="primary",
            width="stretch",
            disabled=disabled,
            key="mora_download",
        ):
            _start_run("mora")
            st.session_state["mora_task"] = {
                "op": "download",
                "start": mora_start.isoformat(),
                "end": mora_end.isoformat(),
            }
            st.rerun()

    if state == "running":
        task = st.session_state.get("mora_task") or {}
        if task.get("op") != "download":
            st.error("Unknown Moratorium task. Please restart the download.")
            _finish_run("mora", cancelled=True)
            st.rerun()

        try:
            start_dt = datetime.fromisoformat(task["start"]).date().replace(day=1)
            end_dt = datetime.fromisoformat(task["end"]).date().replace(day=1)
        except Exception:
            st.error("Invalid date range provided. Please start a new download.")
            _finish_run("mora", cancelled=True)
            st.rerun()

        start_month = _date_to_month(start_dt)
        end_month = _date_to_month(end_dt)
        if end_month < start_month:
            start_month, end_month = end_month, start_month

        st.markdown("### 📊 Extraction Progress")
        prog, summary_ph = _progress_widgets()
        telemetry_ph = st.empty()
        log_ph = st.empty()

        with st.spinner("Filtering moratorium index…"):
            picks = query_month_range(start_month, end_month)

        unique_companies: List[Dict[str, Any]] = []
        seen_numbers: set[str] = set()
        for pick in picks:
            num = (pick.get("company_number") or "").strip()
            if not num or num in seen_numbers:
                continue
            if num.isdigit() and len(num) == 7:
                num = num.zfill(8)
            seen_numbers.add(num)
            unique_companies.append(
                {
                    "company_number": num,
                    "company_name": (pick.get("company_name") or num).strip(),
                    "start_month": pick.get("start_month"),
                }
            )

        total = len(unique_companies)
        if total == 0:
            st.warning("No moratorium cases for the selected date range.")
            _finish_run("mora", cancelled=False)
            st.rerun()

        pipeline = get_pipeline(
            base_folder=st.secrets.get("BASE_FOLDER", "InsolvencyDocuments_Cached"),
            force_refresh=st.session_state.get("force_companies", False),
            force_redownload=st.session_state.get("force_docs", False),
        )
        pipeline._forced_folder = "Moratorium"

        progress_q: Queue = Queue(maxsize=2048)
        pipeline.set_progress_emitter(progress_q.put)

        stats = {
            "processed": 0,
            "successful": 0,
            "errors": 0,
            "downloaded": 0,
            "from_cache": 0,
            "total_documents": 0,
        }
        stats_display = stats.copy()
        inflight_downloads = defaultdict(int)
        results: List[Dict[str, Any]] = []
        cancel_event = st.session_state.get("mora_cancel_event")
        upload_telemetry = _init_upload_telemetry_state()

        def _mora_apply_events(events: List[dict]):
            for evt in events:
                if evt.get("kind") == "documents_uploaded":
                    company = evt.get("company_number")
                    count = int(evt.get("count", 0) or 0)
                    if company and count:
                        inflight_downloads[company] += count
                elif evt.get("kind") == "document_progress":
                    company = evt.get("company_number")
                    fresh = int(evt.get("fresh", 0) or 0)
                    cached = int(evt.get("cached", 0) or 0)
                    delta = fresh + cached
                    if company and delta:
                        inflight_downloads[company] += delta
                elif evt.get("kind") in ("upload_progress", "rate_limit"):
                    _update_upload_telemetry_state(upload_telemetry, evt)

        def _mora_refresh_display():
            stats_display.update(
                {
                    "processed": stats["processed"],
                    "successful": stats["successful"],
                    "from_cache": stats["from_cache"],
                    "errors": stats["errors"],
                    "downloaded": stats["downloaded"],
                    "total_documents": stats["downloaded"] + stats["from_cache"],
                    "inflight_documents": sum(inflight_downloads.values()),
                }
            )

        _mora_refresh_display()
        _render_progress_summary(prog, summary_ph, stats_display, total)
        if SHOW_TELEMETRY:
            _render_upload_telemetry(telemetry_ph, upload_telemetry)

        cancelled_run = False
        try:
            with ThreadPoolExecutor(max_workers=max(1, min(total, MAX_CONCURRENCY_DEFAULT))) as ex:
                futures = {
                    ex.submit(
                        pipeline.download_company_by_number,
                        row["company_number"],
                        max_documents=int(MAX_DOCS_DEFAULT),
                        research_root="Moratorium",
                        cancel_event=cancel_event,
                    ): row
                    for row in unique_companies
                }
                pending = set(futures.keys())
                while pending:
                    events = _drain_progress(progress_q, log_ph)
                    if events:
                        _mora_apply_events(events)
                        _mora_refresh_display()
                        _render_progress_summary(prog, summary_ph, stats_display, total)
                        if SHOW_TELEMETRY:
                            _render_upload_telemetry(telemetry_ph, upload_telemetry)
                    done, pending = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                    if cancel_event and cancel_event.is_set():
                        cancelled_run = True
                        for fut in pending:
                            fut.cancel()
                        break
                    for fut in done:
                        row = futures[fut]
                        try:
                            res = fut.result()
                            res.setdefault("documents_downloaded_fresh", int(res.get("documents_downloaded", 0)))
                            ok = bool(res.get("success", True))
                            docs = int(res.get("documents_downloaded", 0))
                            cache = int(res.get("documents_from_cache", 0))
                        except Exception as exc:
                            ok, docs, cache = False, 0, 0
                            with log_ph:
                                st.error(f"{row['company_number']}: {exc}")
                        stats["processed"] += 1
                        stats["successful"] += 1 if ok else 0
                        stats["downloaded"] += docs
                        stats["from_cache"] += cache
                        stats["total_documents"] += docs + cache
                        if not ok:
                            stats["errors"] += 1
                        inflight_downloads.pop(row["company_number"], None)
                        _mora_refresh_display()
                        _render_progress_summary(prog, summary_ph, stats_display, total)
                        results.append(
                            {
                                "company_number": row["company_number"],
                                "company_name": row["company_name"],
                                "start_month": row.get("start_month", ""),
                                "documents_downloaded": docs,
                                "documents_downloaded_fresh": docs,
                                "documents_from_cache": cache,
                                "success": ok,
                                "procedure_folder": res.get("procedure_folder", ""),
                                "folder_path": res.get("folder_path", ""),
                                "notice_type": "Moratorium",
                            }
                        )
        finally:
            if hasattr(pipeline, "_forced_folder"):
                delattr(pipeline, "_forced_folder")

        if not cancelled_run:
            with st.spinner("☁️ finalising uploads to Drive…"):
                try:
                    pipeline.wait_for_uploads(timeout=None)
                except Exception:
                    pass
                try:
                    pipeline.flush_and_stop_upload_workers()
                except Exception:
                    pass

        if results:
            run_config = {
                "start_date": task.get("start"),
                "end_date": task.get("end"),
                "categories": "Moratorium",
            }
            _save_results("mora", stats, results, run_config=run_config)
            _render_results_summary("mora", stats, results)
            _log_download_rate("mora", stats.get("downloaded", 0), stats.get("from_cache", 0))

        _finish_run("mora", cancelled=cancelled_run)
        st.rerun()

    if state in ("idle", "completed", "cancelled"):
        last_stats, last_results, last_ts = _get_last_results("mora")
        if last_stats and last_ts:
            uk_time = datetime.fromtimestamp(last_ts, tz=UK_TZ)
            run_time = uk_time.strftime("%d/%m/%Y %H:%M:%S %Z")
            st.markdown(f"### 📊 Last Moratorium Run Results ({run_time})")
            _render_results_summary("mora", last_stats, last_results)
st.markdown(f"""
<div class="footer">
  <div class="footer-inner">
    <div class="footer-text">
      <strong>Corporate Insolvency Document Extraction</strong><br>
      Get Support: <a href="mailto:research@akinbc.com">research@akinbc.com</a><br>
      <a href="https://www.akinbc.com" target="_blank">www.akinbc.com</a><br>
      © 2025 AKIN BUSINESS CONSTRUCTS
    </div>
  </div>
  {f'<img src="data:image/png;base64,{logo_base64}" class="footer-logo" alt="AKIN Business Constructs"/>' if logo_base64 else ""}
</div>
""", unsafe_allow_html=True)
