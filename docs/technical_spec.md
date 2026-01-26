🏛️ AkinBC Insolvency Agent – Technical Specification  
Automated UK Corporate Insolvency Document Extraction (Streamlit)

Snapshot placements (for clarity):
- [Snapshot: Gazette progress + metrics + telemetry]
- [Snapshot: Research (company number) progress + metrics]
- [Snapshot: CVA Last Run Results]
- [Snapshot: Moratorium progress summary]
- [Snapshot: Export buttons (CSV/XLSX/PDF)]

At-a-glance prerequisites
- Runtime: Python + Streamlit stack (install `requirements.txt`); single Streamlit process.
- Network: outbound HTTPS to The Gazette, Companies House (API/HTML), Google Drive (if enabled).
- Secrets: `.streamlit/secrets.toml` (BASE_FOLDER, optional DRIVE_ROOT_ID, optional Google client secret path/override), `token.json` (created on first Drive OAuth), optional env for CH API and performance tuning.
- Storage: local base folder (default `InsolvencyDocuments_Cached`); optional Google Drive root; local cache DB `insolvency_cache.db`.

1) Runtime & UI Shell
- Framework: Streamlit (`app.py`); wide layout; SARAH theme CSS; header + sidebar + four tabs.
- State model: per-tab session keys (`<prefix>_run_state`, run_id, cancel_event, executor, futures, cleanup thread). Helpers: `_ensure_run_state`, `_start_run`, `_request_stop`, `_finish_run`, `_reset_run`, `_save_results`.
- Controls: `DOWNLOAD` (starts run when inputs valid), `STOP` (graceful cancel), `NEW SEARCH` (reset). Export buttons hidden during runs.
- Progress rendering: `_progress_widgets`, `_render_progress_summary`, `_render_upload_telemetry`; events drained via `_drain_progress`.

2) Tabs & Pipelines
- Gazette (Search by Procedure):
  - Source: The Gazette via `CachedGazetteCompaniesHousePipeline.gazette_scraper.search_insolvencies_streaming`.
  - Inputs: procedures from `taxonomy.PROCEDURES` (enabled_in_gazette), Start/End dates; optional notice filters from selected procedures.
  - Flow: preflight count (page 1) → producer streams companies → planner dedupes/applies filters → cache short-circuit via `_make_cached_result` → concurrent downloads.
  - Forced folder: when exactly one procedure selected, folder = that procedure label.
  - Limits: `MAX_GAZETTE_PAGES`, `MAX_DOCUMENTS_PER_COMPANY`, `MAX_CONCURRENT_COMPANIES`.
- Company Research (Search by Company Number):
  - Source: Companies House (API/HTML) via `pipeline.download_company_by_number`.
  - Inputs: list of numbers; `sanitize_company_numbers` validates/dedupes (8-digit or SC/NI+6; 7-digit padded).
  - Flow: per-number futures; research root “Research”.
- CVA:
  - Source: `cva_store` index; queries by commencement date (`query_between`); auto-refresh via `refresh_index` when stale/empty.
  - Flow: forced folder `CVA`; research root `CVA`; downloads by number; uses cache; parallel futures.
- Moratorium:
  - Source: `moratorium_store`; freshness via `ensure_mora_index_fresh`; filters by month range (`query_month_range`).
  - Flow: unique company list (pads 7-digit to 8); forced folder `Moratorium`; research root `Moratorium`; parallel futures.

3) Pipeline & Infrastructure
- Core: `wc_insolvency_caached.py` with config from `create_optimized_cache_config`.
- Concurrency: thread pools for planning/downloading; HTTP inflight via `HTTP_MAX_INFLIGHT`; per-company `DOC_CONCURRENCY_PER_COMPANY`; company-level `MAX_CONCURRENT_COMPANIES`.
- Caching: company/document cache; `DOWNLOAD_ALL_DOCS` toggle; `_make_cached_result` returns cached results when valid; limits bounded by `MAX_DOCUMENTS_PER_COMPANY`.
- Drive integration: optional `GoogleDriveClient`; settings `DRIVE_ROOT_ID`, `DRIVE_ONLY`, `DELETE_LOCAL_AFTER`, `DRIVE_UPLOAD_CONCURRENCY`; finalization via `wait_for_uploads` and `flush_and_stop_upload_workers`.
- Progress events: `document_progress`, `documents_uploaded`, `upload_progress`, `rate_limit`; surfaced in telemetry.

4) Configuration (`settings.py`)
- Resolver: `cfg(key, default, cast, env=...)` checks ENV → `st.secrets` → default; memoized; `refresh_cfg()` clears cache.
- Legacy attributes via `__getattr__` (e.g., `CH_API_KEY`, `DOC_SOURCE`, `MAX_GAZETTE_PAGES`, concurrency/QPS defaults).
- Notable keys: CH API keys/RPS/concurrency (`CH_API_KEY`, `CH_API_KEYS`, `CH_QPS`, `CH_BURST`, `CH_TIMEOUT`), Gazette QPS/burst/timeout (`GAZETTE_QPS`, `GAZETTE_BURST`, `GAZETTE_TIMEOUT`), Drive (`DRIVE_ROOT_ID`, `DRIVE_ONLY`, `DELETE_LOCAL_AFTER`, `DRIVE_UPLOAD_CONCURRENCY`, `DRIVE_BATCH_SIZE`, `DRIVE_SCOPE`), download limits (`MAX_DOCUMENTS_PER_COMPANY`, `MAX_GAZETTE_PAGES`), HTTP (`HTTP_MAX_INFLIGHT`, `HTTP_MAX_KEEPALIVE`, `HTTP_TOKEN_RATE/BURST`), async toggle (`ASYNC_NET`).

5) Providers & Data Sources
- Gazette HTML scraping (categories from `taxonomy.PROCEDURES` with `gazette_codes` and optional `notice_filters`).
- Companies House provider: `get_provider()` chooses API when `DOC_SOURCE` in (`api`, `auto`) and `CH_API_KEY` present; otherwise HTML fallback.
- CVA data: `cva_store` index (refreshable).
- Moratorium data: `moratorium_store` month index.

6) Storage & Paths
- Base download folder: `BASE_FOLDER` (secret; default `InsolvencyDocuments_Cached`).
- Cache DB: `insolvency_cache.db`; temp/cache under `cache/`.
- Tokens/creds: Google OAuth token at `token.json`; client secret under `secrets/` (configurable via `SECRETS_DIR`); `.streamlit/secrets.toml` for runtime secrets.
- Foldering: per procedure label or forced folders (`CVA`, `Moratorium`); per-company subfolders; Drive paths normalized in reports when `gdrive://`.

7) Reports (`report_generators.py`)
- Formats: CSV, Excel, PDF; generated after runs via `_prepare_report_assets`.
- Stats: total docs, fresh vs cache, cache efficiency, docs/min, companies processed/successful, processing time.
- Per-company rows: totals, fresh, cache, status, folder path (Drive link normalized).
- Failure tolerance: generation wrapped in try/except; missing deps (e.g., openpyxl) result in skipped format.

8) Validation & Guards
- Date validation: future dates blocked; End must be after Start; Gazette date inputs constrained.
- Input sanitation: company numbers validated/deduped; notice filters applied when provided.
- Zero-result handling: Gazette preflight; guidance when no docs downloaded or no matches.
- Cancellation: STOP sets cancel_event, cancels futures, shuts executors; partial results preserved.

9) Telemetry, Logging & Error Handling
- UI telemetry: Drive queue status, rate-limit notices, cancellation warnings, errors counted in stats.
- Console: `_log_download_rate` and pipeline logs in stdout/stderr.
- Resilience: Drive failures do not block downloads (local completes); report generation guarded; cancellation is graceful.
- Incident capture: tab/task, inputs (dates/procedures/company numbers), exact error text, timestamp, browser, Drive enabled?, concurrency/env overrides, Force refresh/re-download toggles.

10) Performance Levers
- Env/secrets: `MAX_CONCURRENT_COMPANIES`, `DOC_CONCURRENCY_PER_COMPANY`, `MAX_GAZETTE_PAGES`, `HTTP_MAX_INFLIGHT`, Gazette `GAZETTE_QPS/GAZETTE_BURST/GAZETTE_TIMEOUT`, CH `CH_QPS/CH_BURST/CH_TIMEOUT`, `DOWNLOAD_ALL_DOCS`, `DRIVE_UPLOAD_CONCURRENCY`, `DRIVE_BATCH_SIZE`.
- UI toggles: “Force refresh companies” (bypass cached company data), “Force re-download documents” (ignore document cache).
- Usage guidance: short Gazette ranges (1–7 days), single procedure for accuracy, batch company-number inputs, avoid forcing refresh unless required.

11) Security & Access
- Secrets location: `.streamlit/secrets.toml`; Google client secret under `secrets/` (or `SECRETS_DIR`); OAuth token persisted at `token.json` (restrict file permissions).
- Drive scopes: `DRIVE_SCOPE` (`file` default; `full` if configured); `DRIVE_ONLY`/`DELETE_LOCAL_AFTER` govern local retention vs Drive-only.
- Data residency: documents stored under `BASE_FOLDER`; optional mirroring to Drive; no external DB besides local cache.
- Keys: prefer env/secret injection for CH API keys; rotate via `CH_API_KEYS`; avoid embedding in code.

12) Deployment & Ops
- Local/dev run: `streamlit run app.py`.
- Configuration: set env vars or `.streamlit/secrets.toml`; call `refresh_cfg()` if secrets/env change mid-session.
- Dependencies: install from `requirements.txt`; PDF/Excel exports need reportlab/openpyxl (listed).
- Network prerequisites: outbound HTTPS to Gazette/CH/Drive (if enabled).
- Monitoring: UI telemetry + console logs; reports provide per-run evidence and audit trail.
