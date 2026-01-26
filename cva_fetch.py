# cva_fetch.py
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple

import requests
import streamlit as st
from bs4 import BeautifulSoup

CH_API_KEY = st.secrets.get("COMPANIES_HOUSE_API_KEY")
CH_BASE = "https://api.company-information.service.gov.uk"
CH_UI = "https://find-and-update.company-information.service.gov.uk/company"


def _to_iso(dstr: str) -> str:
    return datetime.strptime(dstr.strip(), "%d %B %Y").strftime("%Y-%m-%d")


def _extract_cva_meeting_date(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    dd = soup.find("dd", id=re.compile(r"^voluntary-arrangement-started-on_date_\d+$"))
    if dd and dd.text.strip():
        try:
            return _to_iso(dd.text.strip())
        except Exception:
            pass
    label = soup.find(string=lambda s: s and "Voluntary arrangement started on" in s)
    if label:
        val = label.find_next("dd")
        if val and val.text.strip():
            try:
                return _to_iso(val.text.strip())
            except Exception:
                pass
    fallback = soup.find(string=lambda s: s and "Date of meeting to approve CVA" in s)
    if fallback:
        val = fallback.find_next("dd")
        if val and val.text.strip():
            try:
                return _to_iso(val.text.strip())
            except Exception:
                pass
    first = soup.find(string=lambda s: s and "First statement date" in s)
    if first:
        val = first.find_next("strong")
        if val and val.text.strip():
            try:
                return _to_iso(val.text.strip())
            except Exception:
                pass
    last = soup.find(string=lambda s: s and "Last statement dated" in s)
    if last:
        val = last.find_next("strong")
        if val and val.text.strip():
            try:
                return _to_iso(val.text.strip())
            except Exception:
                pass
    return None


def fetch_commencement_date_html(company_number: str) -> Tuple[Optional[str], Optional[str]]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; CVA-Fetch/1.0)"}
    insolvency_url = f"{CH_UI}/{company_number}/insolvency"
    try:
        r = requests.get(insolvency_url, headers=headers, timeout=20)
    except requests.RequestException:
        r = None
    if r and r.status_code == 200:
        date_val = _extract_cva_meeting_date(r.text)
        if date_val:
            return date_val, "html:cva_started"
    # fall back to overview page for legacy cases
    overview_url = f"{CH_UI}/{company_number}"
    try:
        r2 = requests.get(overview_url, headers=headers, timeout=20)
    except requests.RequestException:
        r2 = None
    if r2 and r2.status_code == 200:
        date_val = _extract_cva_meeting_date(r2.text)
        if date_val:
            return date_val, "html:overview"
    return None, None


def fetch_commencement_date_api(company_number: str) -> Tuple[Optional[str], Optional[str]]:
    if not CH_API_KEY:
        return None, None
    url = f"{CH_BASE}/company/{company_number}/insolvency"
    try:
        r = requests.get(url, auth=(CH_API_KEY, ""), timeout=20)
    except requests.RequestException:
        return None, None
    if r.status_code != 200:
        return None, None
    data = r.json()
    for case in data.get("cases", []):
        if str(case.get("type", "")).upper() == "CVA":
            start = case.get("case_start_date") or case.get("commencement_date")
            if start:
                return start[:10], "api:cva_start"
    return None, None


def fetch_commencement_date(company_number: str) -> Tuple[Optional[str], Optional[str]]:
    # Prefer HTML (more accurate historic data), then fallback to API
    html_date, html_tag = fetch_commencement_date_html(company_number)
    if html_date:
        return html_date, html_tag
    return fetch_commencement_date_api(company_number)


__all__ = [
    "fetch_commencement_date",
    "fetch_commencement_date_html",
    "fetch_commencement_date_api",
    "_extract_cva_meeting_date",
]
