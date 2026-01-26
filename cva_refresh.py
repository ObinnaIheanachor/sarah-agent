from __future__ import annotations

import csv
import io
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qsl

from cva_store import (
    upsert_row,
    get_row,
    set_last_full_refresh,
    get_last_full_refresh,
)
from cva_fetch import fetch_commencement_date

ADV_SEARCH_URL = (
    "https://find-and-update.company-information.service.gov.uk/advanced-search/"
    "get-results?companyNameIncludes=&companyNameExcludes=&registeredOfficeAddress="
    "&incorporationFromDay=&incorporationFromMonth=&incorporationFromYear="
    "&incorporationToDay=&incorporationToMonth=&incorporationToYear="
    "&status=voluntary-arrangement&sicCodes=&dissolvedFromDay=&dissolvedFromMonth="
    "&dissolvedFromYear=&dissolvedToDay=&dissolvedToMonth=&dissolvedToYear="
)
EXPORT_URL = "https://find-and-update.company-information.service.gov.uk/advanced-search/export"
ADV_SEARCH_PARAMS = dict(parse_qsl(urlparse(ADV_SEARCH_URL).query, keep_blank_values=True))

REFRESH_INTERVAL_SECS = 30 * 24 * 3600
MAX_FETCH_WORKERS = 8
MAX_RESULT_PAGES = 500


def _session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; SARAH-CVA/1.0)",
            "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": ADV_SEARCH_URL,
        }
    )
    return sess


def _parse_csv_bytes(payload: bytes) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    buf = io.StringIO(payload.decode("utf-8", errors="ignore"))
    rdr = csv.DictReader(buf)
    for row in rdr:
        name = (row.get("Company name") or row.get("company name") or row.get("Company Name") or "").strip()
        num = (row.get("Company number") or row.get("company number") or row.get("Company Number") or "").strip()
        if not num:
            joined = " ".join([str(v) for v in row.values() if v])
            m = re.search(r"/company/([A-Z0-9]{6,10})", joined, re.I)
            if m:
                num = m.group(1)
        if num:
            rows.append({"company_number": num.upper().strip(), "company_name": name})
    return rows


def _parse_html_list(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, str]] = []
    for link in soup.select('a[href*="/company/"]'):
        m = re.search(r"/company/([A-Z0-9]{6,10})", link.get("href", ""), re.I)
        if not m:
            continue
        num = m.group(1).upper().strip()
        name = link.get_text(strip=True)
        rows.append({"company_number": num, "company_name": name})
    return rows


def _paginate_results(sess: requests.Session, first_page_html: str | None = None) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    seen: set[str] = set()
    page = 1
    html = first_page_html
    while page <= MAX_RESULT_PAGES:
        if html is None:
            try:
                resp = sess.get(f"{ADV_SEARCH_URL}&page={page}", timeout=45)
            except requests.RequestException:
                break
            if resp.status_code != 200:
                break
            html = resp.text
        rows = _parse_html_list(html)
        new_rows = []
        for row in rows:
            num = row.get("company_number")
            if not num or num in seen:
                continue
            seen.add(num)
            new_rows.append(row)
        if not new_rows:
            break
        collected.extend(new_rows)
        page += 1
        html = None
    return collected


def _download_company_list() -> List[Dict[str, str]]:
    sess = _session()
    try:
        sess.get(ADV_SEARCH_URL, timeout=45)
    except requests.RequestException:
        pass

    payload = dict(ADV_SEARCH_PARAMS)
    payload.pop("page", None)
    payload.pop("download", None)
    payload["download"] = "1"

    try:
        resp = sess.post(
            EXPORT_URL,
            data=payload,
            timeout=60,
            headers={"Accept": "text/csv"},
        )
        if resp.status_code == 200 and b"company number" in resp.content[:100].lower():
            rows = _parse_csv_bytes(resp.content)
            if rows:
                return rows
    except requests.RequestException:
        pass

    # Fallback: try download=1 via GET
    try:
        resp = sess.get(ADV_SEARCH_URL + "&download=1", headers={"Accept": "text/csv"}, timeout=60)
        if resp.status_code == 200 and b"company number" in resp.content[:100].lower():
            rows = _parse_csv_bytes(resp.content)
            if rows:
                return rows
    except requests.RequestException:
        pass

    # Pagination fallback (slow but ensures coverage)
    return _paginate_results(sess)


def refresh_index(force: bool = False, cancel_event=None, progress_cb: Optional[Callable[[int, int, str], None]] = None) -> int:
    """Refresh the cached CVA index. Returns number of companies processed."""
    now = int(time.time())
    last = get_last_full_refresh()
    if not force and last and (now - last) < REFRESH_INTERVAL_SECS:
        return 0

    rows = _download_company_list()
    if not rows:
        return 0

    # Deduplicate while preserving order
    deduped: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        num = row.get("company_number", "").strip().upper()
        if not num or num in seen:
            continue
        seen.add(num)
        deduped.append({"company_number": num, "company_name": row.get("company_name", "").strip()})

    total = len(deduped)
    processed = 0

    def _emit(status: str = ""):
        if progress_cb:
            progress_cb(processed, total, status)

    _emit("starting")

    def _needs_fetch(number: str) -> bool:
        cached = get_row(number)
        if not cached:
            return True
        return not cached.get("commencement_date")

    work_items = [row for row in deduped if force or _needs_fetch(row["company_number"])]

    if not work_items:
        set_last_full_refresh(int(time.time()))
        return 0

    cancelled = False
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
        futures = {pool.submit(fetch_commencement_date, row["company_number"]): row for row in work_items}
        for fut in as_completed(futures):
            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                break
            row = futures[fut]
            num = row["company_number"]
            name = row.get("company_name", "")
            try:
                date_str, source = fut.result()
            except Exception:
                date_str, source = None, None
            upsert_row(
                {
                    "company_number": num,
                    "company_name": name,
                    "commencement_date": date_str,
                    "date_source": source,
                }
            )
            processed += 1
            _emit(num)
    if not cancelled:
        set_last_full_refresh(int(time.time()))
    return processed
