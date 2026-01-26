🏛️ AkinBC Insolvency Agent – User Manual
Automated UK Corporate Insolvency Document Extraction

Snapshot placements (replace with actual images):
- [Snapshot: Gazette tab – procedure multiselect + date pickers + metrics row]
- [Snapshot: Company Number tab – textarea + metrics row]
- [Snapshot: CVA tab – date pickers + Last Run metrics]
- [Snapshot: Moratorium tab – date pickers + metrics]
- [Snapshot: Export buttons – CSV/Excel/PDF]

Table of Contents
- Getting Started
- Overview
- Using Gazette Search
- Using Company Research
- Using CVA
- Using Moratorium
- Understanding Results
- Tips & Best Practices
- Troubleshooting
- Sidebar Options
- Support
- Quick Reference Card

Getting Started
- Requirements: modern browser (Chrome/Firefox/Safari/Edge), internet, access provided by your admin (no local install).
- Access: open the provided URL; the app loads in the browser.

Overview
- Purpose: automatically find and download UK corporate insolvency documents.
- Features:
  - 🔍 Gazette Search: search The Gazette by insolvency procedure + date range.
  - 🏢 Company Research: fetch documents by company number.
  - 📅 CVA: download Company Voluntary Arrangements by commencement date.
  - 🧾 Moratorium: download moratorium cases by month range.
- Layout: header (app name/purpose), sidebar (force-refresh toggles, run hints), main area with tabs and results.

Using Gazette Search (Search by Procedure)
- Purpose: search The Gazette for insolvency notices in a date window and download related documents.
- Steps:
  1) Select Start Date and End Date (DD/MM/YYYY). Rules: past dates only; End must be after Start. Short ranges (1–7 days) run fastest.
  2) Choose insolvency procedures from the dropdown (e.g., Administration, CVL, Liquidation by Court, MVL, Receivership, Moratoria, Notices of Dividends, IP Applications, Other corporate insolvency/business, QDP, Re-use of Prohibited Name).
  3) Click `DOWNLOAD` (button stays disabled until dates and at least one procedure are valid).
  4) Watch progress: companies processed, successful, documents (new vs cache), Drive queue/rate-limit notes.
  5) After completion, review metrics and use the CSV/Excel/PDF export buttons.
- Tips: start with common procedures (Administration, CVL); keep the window narrow to avoid timeouts.
- Edge cases: zero preflight results → guidance to widen dates; future dates blocked; empty selection blocked; STOP keeps partial results; cache-only hits count as “From Cache.”

Using Company Research (Search by Company Number)
- Purpose: download insolvency documents directly by company number (bypasses Gazette).
- Input format: one number per line; 8-digit numeric (e.g., 02454830) or SC/NI + 6 digits (e.g., SC123456). Seven-digit numerics are auto-padded to eight.
- Steps:
  1) Paste the company numbers.
  2) Click `DOWNLOAD` (disabled if none are valid).
  3) Monitor per-company progress/logs; STOP is available.
  4) Review metrics and exports when done.
- Edge cases: invalid/empty input warned; rate limits surfaced in telemetry; zero docs → guidance tip.

Using CVA
- Purpose: download Company Voluntary Arrangement cases by commencement date.
- Steps:
  1) Select Start/End dates within the available index range.
  2) Click `DOWNLOAD`. If the CVA index is stale/empty, the app refreshes it automatically before downloading.
  3) Monitor progress; uploads to Drive finalize after downloads.
  4) Use exports when complete.
- Edge cases: empty index triggers refresh; no matches → warning; STOP allowed; folder is forced to `CVA`.

Using Moratorium
- Purpose: download moratorium cases by month range.
- Steps:
  1) Pick start and end months (via date pickers).
  2) Click `DOWNLOAD` (disabled if the index is empty or dates are invalid).
  3) Monitor progress/telemetry; STOP allowed.
  4) Use exports when complete.
- Edge cases: empty index → warning; start after end auto-swapped; seven-digit numbers are padded; folder forced to `Moratorium`.

Understanding Results
- Metrics: Companies, Successful, Documents (new), From Cache, Total, Errors.
- Progress UI: progress bar; meta chips for companies/success/docs/cache/errors; Drive queue status; rate-limit notices.
- Reports: CSV/XLSX/PDF buttons appear only after a run finishes; filenames include UK timestamp and tab prefix.
- Foldering: organized by procedure (or CVA/Moratorium) then company; Drive links appear in reports when available.

Tips & Best Practices
- Keep Gazette ranges short (1–7 days).
- Prefer single procedure selection for cleaner classification.
- Batch company-number lists instead of very large single runs.
- Re-run failed companies separately; avoid forcing re-downloads unless needed.
- Use “Force refresh companies/docs” only when you suspect stale or incomplete data.

Troubleshooting
- “Dates cannot be in the future” / “End Date must be after Start Date”: adjust dates.
- “Please select at least one procedure”: choose a procedure for Gazette.
- No results: widen the date range or change procedures; verify company numbers.
- Slow/partial downloads: shorten ranges, reduce batch size, avoid peak times; Drive rate limits may slow uploads.
- Drive unavailable: downloads still save locally; retry uploads later.
- Stopped runs: partial results are kept; click NEW SEARCH to reset.
- Invalid company numbers: use 8 digits or SC/NI + 6 digits; one per line.

Sidebar Options
- 🔄 Force refresh companies: re-fetch company data even if cached.
- 📄 Force re-download documents: re-download documents even if cached.
- Use these when you require the freshest data or suspect cache gaps; expect slower runs.

Support
- Provide when asking for help: what you tried, date range or company numbers, exact error text, browser, time.
- Contact: your administrator or research@akinbc.com.

Quick Reference Card
- Gazette checklist: Start date (past) → End date (> start) → Select procedures → DOWNLOAD → Monitor → Export.
- Company Research checklist: Enter numbers (8 digits or SC/NI+6) → DOWNLOAD → Monitor → Export.
- Common formats: 02454830, SC123456. One number per line, no extra spaces.
