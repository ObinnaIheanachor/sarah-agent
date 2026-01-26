# drive_client.py
from __future__ import annotations

import io
import json
import os
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple, Callable
import threading


from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaFileUpload, BatchHttpRequest
from googleapiclient.errors import HttpError

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# --------------------------------------------------------------------------------------
# Public scopes (kept for callers)
# --------------------------------------------------------------------------------------
DRIVE_SCOPE_FILE = "https://www.googleapis.com/auth/drive.file"
DRIVE_SCOPE_FULL = "https://www.googleapis.com/auth/drive"

# --------------------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------------------
def _backoff_sleep(attempt: int, base: float = 0.5, cap: float = 16.0) -> None:
    """Exponential backoff with jitter."""
    delay = min(cap, base * (2 ** attempt))
    # small jitter
    delay *= (0.8 + 0.4 * os.urandom(1)[0] / 255.0)
    time.sleep(delay)

def _is_retryable_http_error(e: HttpError) -> bool:
    try:
        status = int(e.resp.status)
    except Exception:
        return False
    return status in (429, 500, 502, 503, 504)

# --------------------------------------------------------------------------------------
# Data structure for batch uploads
# --------------------------------------------------------------------------------------
@dataclass
class CreateSpec:
    """One 'files.create' operation."""
    name: str
    parent_id: str
    mime_type: str = "application/pdf"
    media: Optional[MediaIoBaseUpload] = None  # If None, metadata-only file gets created
    fields: str = "id,name,parents"

# --------------------------------------------------------------------------------------
# Google Drive Client
# --------------------------------------------------------------------------------------
class GoogleDriveClient:
    """
    Backward-compatible client with:
      - ensure_path(root_id, parts)
      - upload_pdf_stream(data, name, parent_id)
      - upload_pdf(local_path, parent_id, delete_local_after=False)

    New high-performance APIs:
      - upload_bytes_resumable(...)
      - upload_file_resumable(...)
      - upload_batch_create(specs, batch_size=100)
    """

    def __init__(
        self,
        client_secret_path: str,
        token_path: str,
        scopes: List[str],
        user_agent: str = "wc-insolvency/drive-client",
        cache_discovery: bool = False,
        chunk_size_bytes: int = 8 * 1024 * 1024,  # 8 MB chunks (safe default)
    ):
        self.client_secret_path = client_secret_path
        self.token_path = token_path
        self.scopes = scopes or [DRIVE_SCOPE_FILE]
        self.user_agent = user_agent
        self.chunk_size = int(os.getenv("DRIVE_CHUNK_SIZE", chunk_size_bytes))
        self._cache_discovery = cache_discovery
        self._service = self._build_service(cache_discovery=cache_discovery)
        # simple in-process cache for folders
        self._folder_cache: Dict[Tuple[str, str], str] = {}
        self._tls = threading.local()       # ← thread-local holder
        self._batch_exec_lock = threading.RLock()

    # drive_client.py (top-level of GoogleDriveClient)

    def upload_pdf(self, local_path: str, parent_id: str, delete_local_after: bool = True) -> str | None:
        """
        Upload a PDF from disk to Google Drive.
        Uses the file's basename as the Drive file name.
        """
        try:
            svc = self._get_service()
            if svc is None:
                return None

            media = MediaFileUpload(local_path, mimetype="application/pdf", resumable=False)
            metadata = {
                "name": os.path.basename(local_path),
                "parents": [parent_id],
                "mimeType": "application/pdf",
            }
            request = svc.files().create(
                body=metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            resp = request.execute()
            fid = resp.get("id")

            if fid and delete_local_after:
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            return fid
        except Exception as e:
            print(f"Drive upload_pdf error: {e}")
            return None


    def _get_service(self):
        """Return a Drive resource bound to the current thread."""
        svc = getattr(self._tls, "svc", None)
        if svc is None:
            self._tls.svc = self._build_service(cache_discovery=self._cache_discovery)
            svc = self._tls.svc
            # optional: keep a process-level reference for legacy code paths
            self._service = svc
        return svc

    @property
    def service(self):
        # unify on thread-local
        return self._get_service()


    # --------------------------- Auth/Service ---------------------------------
    def _build_service(self, cache_discovery: bool):
        creds = None
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.scopes)
        # Refresh / run local flow if needed
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.client_secret_path, self.scopes)
                creds = flow.run_local_server(port=0)
            with open(self.token_path, "w") as f:
                f.write(creds.to_json())

        return build(
            "drive",
            "v3",
            credentials=creds,
            cache_discovery=cache_discovery,
        )

    @property
    def service(self):
        return self._service

    # --------------------------- Paths / Folders -------------------------------
    def ensure_path(self, root_folder_id: str, parts: List[str]) -> Optional[str]:
        """
        Ensure nested folders exist under root_folder_id, returning the final folderId.
        Uses an in-memory cache so repeated calls are cheap.
        """
        parent = root_folder_id
        for name in parts:
            key = (parent, name)
            cached = self._folder_cache.get(key)
            if cached:
                parent = cached
                continue
            folder_id = self._get_or_create_folder(parent, name)
            if not folder_id:
                return None
            self._folder_cache[key] = folder_id
            parent = folder_id
        return parent
    
    def _get_or_create_folder(self, parent_id: str, name: str) -> Optional[str]:
        # Properly escape single quotes for the Drive query
        safe_name = name.replace("'", "\\'")
        q = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='{safe_name}' "
            f"and '{parent_id}' in parents "
            f"and trashed=false"
        )

        # 1) Try to find an existing folder
        try:
            svc = self._get_service()
            if svc is None:            # One more attempt to build; if still None, fail gracefully
                self._service = self._build_service(cache_discovery=False)
                svc = self._get_service()
                if svc is None:
                    return None
            # Then, try to find existing
            res = svc.files().list(
                q=q,
                spaces="drive",
                fields="files(id,name,parents)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                corpora="user",
                pageSize=10,
            ).execute()  # ← no extra kwargs here
            files = res.get("files", []) if isinstance(res, dict) else []
            if files:
                return files[0]["id"]
        except HttpError as e:
            # Retry a couple of times on transient errors
            if _is_retryable_http_error(e):
                _backoff_sleep(1)
                try:
                    res = svc.files().list(
                        q=q,
                        spaces="drive",
                        fields="files(id,name,parents)",
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        corpora="user",
                        pageSize=10,
                    ).execute()
                    files = res.get("files", []) if isinstance(res, dict) else []
                    if files:
                        return files[0]["id"]
                except Exception:
                    pass
            # non-retryable -> fall through and try to create

        # 2) Create the folder if missing
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        try:
            created = svc.files().create(
                body=file_metadata,
                fields="id,name,parents",
                supportsAllDrives=True,
            ).execute(num_retries=3)  # num_retries is allowed
            return created.get("id")
        except HttpError as e:
            print(f"Drive create folder error: {e}")
            return None

    # --------------------------- Uploads (legacy APIs) --------------------------        
    def upload_pdf_stream(self, data: bytes, name: str, parent_id: str) -> str | None:
        """
        Upload a small-ish PDF already in memory.
        Uses simple upload (faster than resumable for sub-MB files).
        Returns fileId or None.
        """
        try:
            media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/pdf", resumable=False)
            metadata = {
                "name": name,
                "parents": [parent_id],
                "mimeType": "application/pdf",
            }
            svc = self._get_service()
            if svc is None:
              return None

            request = svc.files().create(
                body=metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,   # harmless if not using shared drives
            )
            resp = request.execute()     # ← no body= here
            return resp.get("id")
        except Exception as e:
            print(f"Drive upload_pdf_stream error: {e}")
            return None

    # --------------------------- Uploads (new APIs) ----------------------------
    def upload_bytes_resumable(
        self,
        data: bytes,
        name: str,
        parent_id: str,
        mime_type: str = "application/pdf",
        chunk_size: Optional[int] = None,
    ) -> Optional[str]:
        """Explicit resumable upload from bytes."""
        if not data:
            return None
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, chunksize=(chunk_size or self.chunk_size), resumable=True)
        return self._upload_media_resumable(name=name, parent_id=parent_id, media=media)

    def upload_file_resumable(
        self,
        path: str,
        name: Optional[str],
        parent_id: str,
        mime_type: str = "application/pdf",
        chunk_size: Optional[int] = None,
    ) -> Optional[str]:
        """Explicit resumable upload from a file path."""
        media = MediaFileUpload(path, mimetype=mime_type, chunksize=(chunk_size or self.chunk_size), resumable=True)
        return self._upload_media_resumable(name=(name or os.path.basename(path)), parent_id=parent_id, media=media)

    def _upload_media_resumable(self, name: str, parent_id: str, media) -> Optional[str]:
        """Core resumable loop with robust retry/backoff on chunk boundaries."""
        meta = {"name": name, "parents": [parent_id]}
        svc = self._get_service()
        if svc is None:
            return None
        req = svc.files().create(
            body=meta,
            media_body=media,
            fields="id,name,parents",
        )
        attempt = 0
        while True:
            try:
                status, resp = req.next_chunk()
                if resp and resp.get("id"):
                    return resp["id"]
                # If status is not None, upload still in progress; loop continues.
            except HttpError as e:
                if _is_retryable_http_error(e) and attempt < 6:
                    attempt += 1
                    _backoff_sleep(attempt)
                    continue
                return None
            except Exception:
                # Network/reset etc. Don’t loop forever.
                if attempt < 3:
                    attempt += 1
                    _backoff_sleep(attempt)
                    continue
                return None

    # --------------------------- Batch operations ------------------------------
    def upload_batch_create(self, specs, batch_size=100, on_progress=None):
        from googleapiclient.http import BatchHttpRequest
        results = [None] * len(specs)
        total = len(specs)
        sent = 0

        def _progress():
            nonlocal sent
            sent += 1
            if on_progress:
                try: on_progress(sent, total)
                except: pass

        # 1) metadata-only through batch
        meta_idxs = [i for i,s in enumerate(specs) if s.media is None]
        start = 0
        while start < len(meta_idxs):
            end = min(start + batch_size, len(meta_idxs))
            batch = BatchHttpRequest()
            for j in meta_idxs[start:end]:
                spec = specs[j]
                body = {"name": spec.name, "parents": [spec.parent_id], "mimeType": spec.mime_type}
                svc = self._get_service()
                if svc is None:
                    return None
                req = svc.files().create(body=body, fields=spec.fields, supportsAllDrives=True)
                def make_cb(ix=j):
                    def _cb(_rid, resp, exc):
                        results[ix] = None if exc else resp
                    return _cb
                batch.add(req, callback=make_cb())
            attempt = 0
            while True:
                try:
                    with self._batch_exec_lock:
                        batch.execute()
                    break
                except HttpError as e:
                    if getattr(e, "resp", None) and int(e.resp.status) in (429,500,502,503,504) and attempt < 4:
                        attempt += 1
                        _backoff_sleep(attempt)
                        continue
                    for j in meta_idxs[start:end]:
                        if results[j] is None:
                            results[j] = None
                    break
            for _ in meta_idxs[start:end]:
                _progress()
            start = end

        # 2) media specs individually (NO batch)
        media_idxs = [i for i,s in enumerate(specs) if s.media is not None]
        for j in media_idxs:
            spec = specs[j]
            try:
                svc = self._get_service()
                if svc is None:
                    return None
                req = svc.files().create(
                    body={"name": spec.name, "parents": [spec.parent_id], "mimeType": spec.mime_type},
                    media_body=spec.media,
                    fields=spec.fields,
                    supportsAllDrives=True,
                )
                results[j] = req.execute(num_retries=3)
            except HttpError as e:
                if getattr(e, "resp", None) and int(e.resp.status) in (429,500,502,503,504):
                    _backoff_sleep(1)
                    try:
                        svc = self._get_service()
                        if svc is None:
                            return None
                        results[j] = svc.files().create(
                            body={"name": spec.name, "parents": [spec.parent_id], "mimeType": spec.mime_type},
                            media_body=spec.media,
                            fields=spec.fields,
                            supportsAllDrives=True,
                        ).execute(num_retries=3)
                    except Exception:
                        results[j] = None
                else:
                    results[j] = None
            except Exception:
                results[j] = None
            finally:
                _progress()

        return results