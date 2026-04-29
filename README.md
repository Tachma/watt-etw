# Watt ETW

FastAPI + React MVP for battery fleet optimization using HEnEx-style Day-Ahead Market data.

## User Flow

Landing Page -> Battery Fleet Setup -> Import DAM CSV/XLSX -> Validate -> Optimize -> Dashboard.

## Backend

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn watt_etw.api.main:app --reload
```

The API exposes:

- `POST /api/market-data/validate`
- `POST /api/optimize`
- `POST /api/export-schedule`

## Frontend

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/api` calls to `http://127.0.0.1:8000`.

## DAM Data

CSV uploads should contain either:

- `timestamp`, `price_eur_mwh`
- or a HEnEx-like time/period column plus a market clearing price column.

Extra market columns are preserved for context. XLSX parsing supports simple workbook sheets without requiring `openpyxl`; CSV is the most reliable MVP format.
