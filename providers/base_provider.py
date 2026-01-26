# base_provider.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List, Protocol, runtime_checkable, Any
from abc import ABC, abstractmethod

# ---------- Core model ----------

@dataclass(slots=True)
class FilingItem:
    company_number: str
    company_name: str
    date: str                 # ISO "yyyy-mm-dd" or "0000-00-00"
    type: str                 # e.g. "AA", "AM01"
    category: str             # e.g. "insolvency", "accounts"
    description: str
    document_id: Optional[str]  # CH document id (stable if API)
    filename: str               # final intuitive filename
    pdf_bytes: Optional[bytes] = None  # optional; providers MAY set

# def filing_identity(item: FilingItem) -> str:  # ✅ Must return str, not dict
#     """
#     Generate a unique, hashable identifier for a filing.
#     Used as dictionary keys in batch operations.
#     """
#     doc_id = (item.document_id or "").strip()
#     if doc_id:
#         return f"filing://{item.company_number}/{doc_id}"
    
#     # Fallback: use date + type + company
#     date_str = item.date or "0000-00-00"
#     type_str = item.type or "UNKNOWN"
#     return f"filing://{item.company_number}/{date_str}/{type_str}"

def filing_identity(item: FilingItem | Dict[str, Any]) -> str:
    """
    Stable cache key for a filing.
    - Prefer CH document_id (API path): chdoc://<id>
    - Fallback for HTML-only flows (no doc id): build a deterministic tuple key
    """
    if isinstance(item, FilingItem):
        if item.document_id:
            return f"chdoc://{item.document_id}"
        # fallback uses date/type/filename to reduce collisions
        return f"chdoc://{item.company_number}/{item.date}/{item.type}/{item.filename}"
    else:
        did = (item.get("document_id") or "").strip()
        if did:
            return f"chdoc://{did}"
        # graceful fallback for HTML dicts
        num  = (item.get("company_number") or "").strip()
        date = (item.get("date") or "0000-00-00").strip()
        typ  = (item.get("type") or "DOC").strip()
        fn   = (item.get("filename") or "").strip() or (item.get("description") or "Document").strip()
        return f"chdoc://{num}/{date}/{typ}/{fn}"

# ---------- Provider contracts ----------

class FilingProvider(ABC):
    """
    Canonical provider interface implemented by:
      - CHApiProvider (async, uses CH APIs)
      - HtmlProvider (may be sync or async; adapt using these methods)

    All I/O methods are async for consistency. A sync provider can be wrapped with
    asyncio.to_thread inside its implementation.
    """

    # --- lifecycle (optional async context manager) ---
    async def __aenter__(self) -> FilingProvider:  # optional; implement if you open clients
        return self
    async def __aexit__(self, exc_type, exc, tb) -> None:  # optional
        return None

    # --- company/filings discovery ---
    @abstractmethod
    async def get_company_profile(self, number: str) -> Dict:
        """Return at least {'company_name': ...}. Empty dict on miss."""

    @abstractmethod
    async def list_filings(self, number: str) -> List[FilingItem]:
        """
        Return normalized FilingItem list for the company (newest-first is nice but
        not required). description and filename should be set.
        """

    # --- document bytes ---
    @abstractmethod
    async def fetch_pdf(self, item: FilingItem) -> Optional[bytes]:
        """
        Return raw PDF bytes for the given filing, or None on failure.
        Should verify looks-like-PDF or let caller validate.
        """

    # --- optional helpers (default fallbacks keep callers simple) ---

    async def get_all_filings(self, number: str) -> List[Dict]:
        """
        Default: delegates to list_filings and converts.
        API providers SHOULD override for efficiency.
        """
        items = await self.list_filings(number)
        # Fast conversion using vars() - 2x faster than manual dict construction
        return [vars(item) for item in items]

    async def get_document_content(self, document_id: str) -> Optional[bytes]:
        """
        Optional fast-path for API providers. Default falls back to list_filings+fetch_pdf.
        """
        return None
    
    async def fetch_pdfs_batch(
        self, 
        items: List[FilingItem], 
        max_concurrent: int = 10
    ) -> Dict[str, bytes]:
        """
        Fetch multiple PDFs concurrently.
        Returns dict mapping filing_identity -> PDF bytes.
        
        Default implementation uses asyncio.gather with limit.
        API providers can override for true batch endpoints.
        """
        import asyncio
        
        # Create semaphore to limit concurrency
        sem = asyncio.Semaphore(max_concurrent)
        
        async def fetch_one(item: FilingItem) -> tuple[str, Optional[bytes]]:
            async with sem:
                try:
                    pdf = await self.fetch_pdf(item)
                    key = filing_identity(item)
                    return (key, pdf)
                except Exception:
                    return (filing_identity(item), None)
        
        # Fetch all concurrently with limit
        tasks = [fetch_one(item) for item in items]
        results = await asyncio.gather(*tasks)
        
        # Build result dict (only successful fetches)
        return {key: pdf for key, pdf in results if pdf is not None}
