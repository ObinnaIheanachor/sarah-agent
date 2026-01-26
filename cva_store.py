# cva_store.py
from __future__ import annotations
import sqlite3, time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from utils.text_sanitize import clean_company_name

DB_PATH = Path("cache/cva_index.sqlite3")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS cva_index (
      company_number   TEXT PRIMARY KEY,
      company_name     TEXT,
      commencement_date TEXT,   -- ISO 'YYYY-MM-DD'
      date_source      TEXT,    -- 'api:first' | 'html:first' | 'html:last'
      last_checked     INTEGER,
      ch_etag          TEXT
    )
    """)
    # migrate silently if coming from older version
    try: con.execute("ALTER TABLE cva_index ADD COLUMN date_source TEXT")
    except sqlite3.OperationalError: pass
    try: con.execute("ALTER TABLE cva_index ADD COLUMN ch_etag TEXT")
    except sqlite3.OperationalError: pass

    con.execute("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)""")
    return con

# -------- basic CRUD --------

def upsert_row(row: Dict[str, Any]) -> None:
    row["company_name"] = clean_company_name(row.get("company_name"))
    now = int(time.time())
    with _conn() as con:
        con.execute("""
          INSERT INTO cva_index(company_number, company_name, commencement_date, date_source, last_checked, ch_etag)
          VALUES(?,?,?,?,?,?)
          ON CONFLICT(company_number) DO UPDATE SET
            company_name       = COALESCE(excluded.company_name, cva_index.company_name),
            commencement_date  = COALESCE(excluded.commencement_date, cva_index.commencement_date),
            date_source        = COALESCE(excluded.date_source, cva_index.date_source),
            last_checked       = ?,
            ch_etag            = COALESCE(excluded.ch_etag, cva_index.ch_etag)
        """, (
            row["company_number"], row.get("company_name"),
            row.get("commencement_date"), row.get("date_source"),
            now, row.get("ch_etag"),
            now
        ))

def get_row(company_number: str) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        cur = con.execute("""
          SELECT company_number, company_name, commencement_date, date_source, last_checked, ch_etag
          FROM cva_index WHERE company_number=?
        """, (company_number,))
        r = cur.fetchone()
        if not r: return None
        keys = ["company_number", "company_name", "commencement_date", "date_source", "last_checked", "ch_etag"]
        return dict(zip(keys, r))

def query_between(start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    with _conn() as con:
        cur = con.execute("""
          SELECT company_number, company_name, commencement_date
          FROM cva_index
          WHERE commencement_date IS NOT NULL
            AND commencement_date BETWEEN ? AND ?
          ORDER BY commencement_date
        """, (start_iso, end_iso))
        return [dict(zip(["company_number","company_name","commencement_date"], r)) for r in cur.fetchall()]

REFRESH_INTERVAL_DAYS = 30
REFRESH_SECS = REFRESH_INTERVAL_DAYS * 24 * 60 * 60

def _get_meta(key: str) -> Optional[str]:
    with _conn() as con:
        cur = con.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def _set_meta(key: str, value: str) -> None:
    with _conn() as con:
        con.execute(
            """
            INSERT INTO meta(key,value) VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )

def get_last_full_refresh() -> int:
    val = _get_meta("last_full_refresh") or _get_meta("last_bulk_refresh")
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0

def set_last_full_refresh(ts: int | None = None) -> None:
    ts = ts or int(time.time())
    _set_meta("last_full_refresh", str(ts))

def set_last_bulk_refresh(ts: int | None = None) -> None:
    # backward-compatible wrapper
    set_last_full_refresh(ts)

def needs_bulk_refresh() -> bool:
    last = get_last_full_refresh()
    if not last:
        return True
    return (time.time() - last) > REFRESH_SECS

def get_date_bounds() -> Tuple[Optional[str], Optional[str]]:
    with _conn() as con:
        cur = con.execute(
            """
            SELECT MIN(commencement_date), MAX(commencement_date)
            FROM cva_index
            WHERE commencement_date IS NOT NULL AND TRIM(commencement_date) <> ''
            """
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]
