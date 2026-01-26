Insolvency Agent (SARAH CASE)
=============================

Streamlit application for corporate insolvency document extraction and reporting. It pulls data from The Gazette and Companies House, optionally enriches results with CVA and moratorium data, caches results for speed, generates CSV/Excel/PDF reports, and can upload downloaded documents to Google Drive.

Features
--------
- Streamlit UI for search, extraction, and report generation.
- Gazette + Companies House pipeline with caching and rate limiting.
- Optional CVA and moratorium enrichment.
- CSV, Excel, and PDF run reports.
- Google Drive upload support (OAuth).

Quick start
-----------
```bash
pip install -r requirements.txt
streamlit run app.py
```

If you use a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

Configuration
-------------
Configuration is resolved in order: environment variables, then Streamlit secrets (`secrets.toml`), then defaults. See `settings.py` for full details.

Common keys
-----------
- `CH_API_KEY` or `CH_API_KEYS`: Companies House API key(s) (comma-separated for rotation).
- `DOC_SOURCE`: `auto`, `api`, or `html`.
- `MAX_DOCUMENTS_PER_COMPANY`: Cap per company.
- `MAX_GAZETTE_PAGES`: Gazette page cap.
- `MAX_CONCURRENT_COMPANIES`: Concurrency cap.
- `DRIVE_ROOT_ID`: Optional Drive folder id.
- `DRIVE_SCOPE`: `file` or `full` (Drive scope).
- `DRIVE_UPLOAD_WORKERS`: Upload worker count.
- `MORA_SHEET_FILE_ID`, `MORA_SHEET_GID`: Moratorium sheet source.

Example `secrets.toml`
----------------------
```toml
CH_API_KEY = "your-key"
DOC_SOURCE = "auto"
MAX_GAZETTE_PAGES = 250
DRIVE_SCOPE = "file"
```

Google Drive setup (optional)
-----------------------------
1) Create OAuth credentials in Google Cloud Console (Desktop application).
2) Place the file at `secrets/client_secret.json`.
3) Run the app or `get_sheets_token.py` to generate `token.json`.

Run tests
---------
```bash
pytest
```

Repository layout (key files)
-----------------------------
- `app.py`: Main Streamlit UI and orchestration.
- `wc_async_core.py`: Async networking primitives.
- `wc_insolvency_caached.py`: Cached pipeline used by `app.py` (note filename typo).
- `providers/`: Companies House provider implementations.
- `cva_fetch.py`, `cva_refresh.py`, `cva_store.py`: CVA indexing.
- `moratorium_fetch.py`, `moratorium_store.py`: Moratorium indexing.
- `report_generators.py`: CSV/Excel/PDF run reports.
- `drive_client.py`: Google Drive integration.
- `settings.py`: Config helper.
- `taxonomy.py`: Gazette categories and labels.

Runtime artifacts
-----------------
These are generated at runtime and should not be committed:
- `cache/`, `downloads/`, `CH_Downloads/`, `tmp_cache_test/`, `InsolvencyDocuments_Cached/`
- `token.json` (OAuth token cache)

Documentation
-------------
- `CLIENT_CODEBASE_GUIDE.md`: Detailed file-by-file guide.
- `DEPLOYMENT.md`: Deployment and AWS notes.
- `docs/user_manual.md`: End-user manual.
- `docs/technical_spec.md`: Technical specification.

Security notes
--------------
- Do not commit `secrets.toml`, `secrets/client_secret.json`, or `token.json`.
- Treat API keys and OAuth credentials as sensitive.
