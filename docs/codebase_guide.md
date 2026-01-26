# Insolvency Agent Codebase Guide

This document explains the purpose of the key code files and how they fit together. It is intended to ship alongside the code for client handover.

## Overview
The project provides a Streamlit application for corporate insolvency document extraction. It pulls data from The Gazette and Companies House, optionally enriches results with CVA and moratorium data, caches results for speed, and generates reports (CSV/Excel/PDF). It can also upload downloaded documents to Google Drive.

## Primary entry points
- `app.py`: Main Streamlit UI and orchestration. Drives the extraction workflow, progress UI, CVA/moratorium refresh, report generation, and Drive upload.

## Core pipeline and caching
- `wc_async_core.py`: Core networking primitives (rate limiting, HTTP pools, caching helpers) used by the async pipeline.
- `wc_insolvency_caached.py`: Cached pipeline used by `app.py` (note the filename typo in "caached")
- `wc_drive_upload_manager.py`: Multi-threaded Google Drive upload queue.

## Providers (Companies House access)
- `providers/base_provider.py`: Canonical provider interface (async). Defines `FilingItem` and expected provider methods.
- `providers/ch_api_provider.py`: Async Companies House API provider with rate limiting and key rotation.

## CVA and moratorium indexes
- `cva_fetch.py`: Fetches CVA commencement dates from Companies House (HTML first, API fallback).
- `cva_refresh.py`: Builds or refreshes the CVA index from the Companies House advanced search export.
- `cva_store.py`: SQLite store for the CVA index and refresh metadata.
- `moratorium_fetch.py`: Loads the moratorium sheet (OAuth, service account, or public export) and normalizes rows.
- `moratorium_store.py`: SQLite store for moratorium records and refresh helpers.

## Reporting
- `report_generators.py`: Builds CSV, Excel, and PDF run reports (summary + per-company details).

## Google auth and Drive
- `drive_client.py`: Google Drive client with resumable uploads, folder management, and batch helpers.

## Utilities
- `settings.py`: Central config helper (`cfg`) that reads env vars or Streamlit secrets with defaults.
- `taxonomy.py`: Defines insolvency procedure taxonomy, labels, and Gazette category codes.
- `utils/text_sanitize.py`: Normalizes company names and removes hidden UI artifacts.
- `cache_update.py`: One-off migration script for cached document URLs.

## Configuration and secrets
- `requirements.txt`: Python dependencies.
- `secrets.toml`: Streamlit secrets (do not share publicly).
- `secrets/client_secret.json`: Google OAuth client credentials (do not share publicly).
- `token.json`: OAuth token cache (do not share publicly).

The app reads configuration from environment variables or `secrets.toml` via `settings.cfg`. Common keys include:
- `CH_API_KEY`, `CH_API_KEYS`, `CH_API_RPS`, `CH_API_MAX_CONC`, `CH_API_TIMEOUT`
- `DOC_SOURCE` (auto/api/html), `MAX_DOCUMENTS_PER_COMPANY`, `MAX_GAZETTE_PAGES`
- `DRIVE_ROOT_ID`, `DRIVE_SCOPE`, `DRIVE_UPLOAD_WORKERS`, `DRIVE_BATCH_SIZE`
- `MORA_SHEET_FILE_ID`, `MORA_SHEET_GID`, `MORA_REFRESH_DAYS`

## Data and cache artifacts
These folders are runtime artifacts, not source code:
- `cache/`: SQLite indexes and document cache files.
- `downloads/`, `CH_Downloads/`: Downloaded PDFs.
- `tmp_cache_test/`: Test cache artifacts.

## Typical run (local)
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Notes for clients
- Avoid committing or sharing any secrets or OAuth tokens.
- If you need a clean delivery bundle, exclude `cache/`, `downloads/`, `CH_Downloads/`, and `token.json`.

## Accessing the virtual server

### SSH access
- Host: `ec2-18-202-54-178.eu-west-1.compute.amazonaws.com` (public DNS)
- Public IP: `18.202.54.178`
- Private IP: `172.31.34.108`
- Port: `<ssh-port>` (default `22`)
- User: `ubuntu`
- Authentication: `insolvency-agent-key` (EC2 key pair)

Example:
```bash
ssh -i /path/to/insolvency-agent-key.pem ubuntu@ec2-18-202-54-178.eu-west-1.compute.amazonaws.com
```

### Code location on the VM
- Project path: `/home/ubuntu/insolvency-agent`
- Start command: `streamlit run app.py`

### Environment notes
- Python version: `<python-version>`
- Virtual environment: `/home/ubuntu/insolvency-agent/akinvenv`
- Config/secrets location: `secrets.toml`, `secrets/client_secret.json`, `token.json`

### Basic run (VM)
```bash
cd /home/ubuntu/insolvency-agent
source /home/ubuntu/insolvency-agent/akinvenv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

### Security notes
- Share secrets and SSH keys only via a secure channel.
- Rotate credentials after handover if granting temporary access.
