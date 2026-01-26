# moratorium_store.py
import sqlite3, time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import date   # <-- needed for age calc
import streamlit as st

DB_PATH = Path("cache/moratorium_index.sqlite3")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS moratorium_index (
      company_number     TEXT PRIMARY KEY,
      company_name       TEXT,
      start_month        TEXT,   -- 'YYYY-MM'
      source_etag        TEXT,   -- optional, to avoid re-pulling the same sheet
      source_last_hash   TEXT,   -- optional checksum of the current sheet row
      last_checked       INTEGER
    )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_mora_month ON moratorium_index(start_month)")
    return con

def upsert_rows(rows: List[Dict[str, Any]]):
    now = int(time.time())
    with _conn() as con:
        con.executemany("""
          INSERT INTO moratorium_index(company_number, company_name, start_month, source_etag, source_last_hash, last_checked)
          VALUES(?,?,?,?,?,?)
          ON CONFLICT(company_number) DO UPDATE SET
            company_name       = excluded.company_name,
            start_month        = excluded.start_month,
            source_etag        = COALESCE(excluded.source_etag, moratorium_index.source_etag),
            source_last_hash   = COALESCE(excluded.source_last_hash, moratorium_index.source_last_hash),
            last_checked       = ?
        """, [
            (
                r["company_number"],
                r.get("company_name",""),
                r["start_month"],
                r.get("source_etag"),
                r.get("source_last_hash"),
                now,  # insert last_checked
                now   # update last_checked
            )
            for r in rows
        ])

def query_month_range(start_month: str, end_month: str) -> List[Dict[str, Any]]:
    """start_month/end_month are 'YYYY-MM' (inclusive)."""
    with _conn() as con:
        cur = con.execute("""
          SELECT company_number, company_name, start_month
          FROM moratorium_index
          WHERE start_month >= ? AND start_month <= ?
          ORDER BY start_month, company_name
        """, (start_month, end_month))
        keys = ["company_number","company_name","start_month"]
        return [dict(zip(keys, r)) for r in cur.fetchall()]

def last_refresh_epoch() -> Optional[int]:
    with _conn() as con:
        cur = con.execute("SELECT MAX(last_checked) FROM moratorium_index")
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else None

def get_month_bounds() -> tuple[Optional[str], Optional[str]]:
    with _conn() as con:
        cur = con.execute(
            "SELECT MIN(start_month), MAX(start_month) FROM moratorium_index WHERE start_month IS NOT NULL AND TRIM(start_month) <> ''"
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]

def ensure_mora_index_fresh(refresh_days: int | None = None) -> int:
    """
    Ensures the moratorium index is refreshed if it's missing or older than N days.
    Returns number of rows upserted when a refresh occurs, else 0.
    Runs synchronously (spinner only), no run-state banners/buttons.
    """
    from moratorium_fetch import refresh_moratorium_index  # <-- call the real refresher

    refresh_days = refresh_days or int(st.secrets.get("MORA_REFRESH_DAYS", "30"))
    last_epoch = last_refresh_epoch()
    needs = (last_epoch is None)
    if not needs:
        age_days = (date.today() - date.fromtimestamp(last_epoch)).days
        needs = age_days >= refresh_days

    if not needs:
        return 0

    with st.spinner("Updating moratorium index from Google Sheet…"):
        count = refresh_moratorium_index()

    st.session_state["mora_last_refresh_epoch"] = int(time.time())
    return count
