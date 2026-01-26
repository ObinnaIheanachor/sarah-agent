# moratorium_fetch.py (replace load_moratorium_sheet and helpers)

import io, hashlib, time
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import streamlit as st
import requests

from moratorium_store import upsert_rows

SHEET_FILE_ID = st.secrets.get("MORA_SHEET_FILE_ID", "")        # required
SHEET_GID     = st.secrets.get("MORA_SHEET_GID", None)          # optional (tab gid)

# Reuse existing OAuth credentials if present
CLIENT_SECRET_PATH = st.secrets.get("GOOGLE_CLIENT_SECRET_PATH") or "secrets/client_secret.json"
TOKEN_PATH         = st.secrets.get("GOOGLE_TOKEN_PATH") or "token.json"

# Optional service-account JSON (as a dict in secrets)
GCP_SA_JSON   = st.secrets.get("gcp_service_account", None)     # dict or None

# Scopes needed for read-only access (add both; Drive export needs Drive scope)
OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

def _month_norm(v: str) -> Optional[str]:
    if not v: return None
    s = str(v).strip()
    for fmt in ("%Y-%m", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m")
        except Exception:
            pass
    return None

def _hash_row(d: Dict[str, Any]) -> str:
    return hashlib.md5(("|".join(str(d.get(k,"")) for k in ("company_number","company_name","start_month"))).encode()).hexdigest()

def _read_sheet_via_drive_oauth(file_id: str, gid: Optional[str]) -> pd.DataFrame:
    """Export the Google Sheet as CSV using the user's OAuth token (works for Shared Drives)."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    creds = Credentials.from_authorized_user_file(TOKEN_PATH, scopes=OAUTH_SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    # Export as CSV. (The export returns the active sheet; gid selection
    # isn’t directly supported. If you need a specific tab, prefer Sheets API below.)
    # For strict tab selection, we’ll prefer the Sheets API path next.
    if not gid:
        resp = drive.files().export(
            fileId=file_id,
            mimeType="text/csv",
        ).execute()
        return pd.read_csv(io.BytesIO(resp))

    # If a specific gid is required, use the Sheets API to read values from that tab
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    # GID → sheet name lookup
    meta = sheets.spreadsheets().get(spreadsheetId=file_id).execute()
    sheet_name = None
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if str(props.get("sheetId")) == str(gid):
            sheet_name = props.get("title")
            break
    # Fallback: first sheet if gid not matched
    range_a1 = f"'{sheet_name}'!A:Z" if sheet_name else "A:Z"
    vals = sheets.spreadsheets().values().get(
        spreadsheetId=file_id,
        range=range_a1,
        majorDimension="ROWS",
    ).execute().get("values", [])

    if not vals:
        return pd.DataFrame()

    header, *rows = vals
    df = pd.DataFrame(rows, columns=header)
    return df

def _read_sheet_via_service_account(file_id: str, gid: Optional[str]) -> Optional[pd.DataFrame]:
    """If a service account is supplied and has access to the sheet, use gspread."""
    if not GCP_SA_JSON:
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(file_id)
        ws = sh.get_worksheet_by_id(int(gid)) if gid else sh.sheet1
        rows = ws.get_all_records()
        return pd.DataFrame(rows)
    except Exception:
        return None  # fall back

def _read_sheet_via_public_export(file_id: str, gid: Optional[str]) -> pd.DataFrame:
    """Unauthenticated CSV export (requires sheet to be public or at least accessible without login)."""
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
    if gid is not None:
        url += f"&gid={gid}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content))

def load_moratorium_sheet() -> pd.DataFrame:
    """
    Order: OAuth (reused token.json) → Service Account → public CSV export.
    """
    # 1) OAuth
    try:
        return _read_sheet_via_drive_oauth(SHEET_FILE_ID, SHEET_GID)
    except Exception:
        pass

    # 2) Service Account (if provided)
    df = _read_sheet_via_service_account(SHEET_FILE_ID, SHEET_GID)
    if df is not None:
        return df

    # 3) Public export (will 401/403 if not shared)
    return _read_sheet_via_public_export(SHEET_FILE_ID, SHEET_GID)

def refresh_moratorium_index(note_etag: Optional[str] = None) -> int:
    df = load_moratorium_sheet()
    # Defensive column handling
    cols = {c.lower().strip().replace(" ", "_"): c for c in df.columns}
    cn_col = cols.get("company_number") or "company_number"
    nm_col = cols.get("company_name") or "company_name"
    st_col = cols.get("insolvency_start_date") or cols.get("start_month") or "insolvency_start_date"

    df = df.rename(columns={cn_col:"company_number", nm_col:"company_name", st_col:"insolvency_start_date"})

    # Exclude NI if register_location present
    lc_map = {c.lower(): c for c in df.columns}
    if "register_location" in lc_map:
        rl = lc_map["register_location"]
        df = df[df[rl].astype(str).str.contains("England/Wales", case=False, na=False)]

    # Normalize to YYYY-MM
    df["start_month"] = df["insolvency_start_date"].apply(lambda x: _month_norm(str(x)))
    df = df.dropna(subset=["company_number","start_month"])

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        item = {
            "company_number": str(r["company_number"]).strip(),
            "company_name":   str(r.get("company_name") or "").strip(),
            "start_month":    r["start_month"],
        }
        item["source_last_hash"] = _hash_row(item)
        if note_etag:
            item["source_etag"] = note_etag
        rows.append(item)

    if rows:
        upsert_rows(rows)
    return len(rows)
