# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

Backend (Python 3.11+):
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
uvicorn watt_etw.api.main:app --reload    # serves http://127.0.0.1:8000
pytest                                     # run all tests
pytest tests/test_optimizer.py             # one file
pytest tests/test_api.py::test_optimize_endpoint  # one test
```

Frontend (React + Vite, served on port 5173, proxies `/api` to `127.0.0.1:8000`):
```bash
cd frontend
npm install
npm run dev
npm run build
```

Forecaster training (long-running; hits external APIs):
```bash
python train_forecaster.py             # train end-to-end on cached + freshly fetched data
python train_forecaster.py --force     # rebuild parquet caches first
python train_forecaster.py --eval-only # load saved model and print holdout metrics
python train_forecaster.py --no-rae    # skip RAE per-tech RES weather (faster)
```

## Architecture

The repo holds **two parallel tracks** that share data modules but are not yet wired together:

1. **Battery optimization MVP** (live in API + UI) — `watt_etw/api/main.py`, `optimizer.py`, `battery_fleet.py`, `market_import.py`, `explanations.py`, plus `frontend/src/main.jsx`. User uploads a DAM CSV/XLSX, the backend validates it, then a heuristic optimizer produces a charge/discharge/idle schedule with KPIs.
2. **15-minute MCP price forecaster** (offline, run via `train_forecaster.py`) — `watt_etw/data/`, `features/`, `forecasting/`. Builds a feature matrix from many sources and trains one global LightGBM model. Output is in `models/price_forecaster/`. **Not yet exposed via the API.**

### Optimization MVP flow
`POST /api/market-data/validate` → `market_import.load_market_file` (handles plain CSV, generic XLSX, and the HEnEx "Results Summary" workbook with its non-tabular layout — buy/sell/coupling sheets are merged on MTU). Returns `MarketRow`s plus interval/duplicates/missing-interval warnings.

`POST /api/optimize` → `BatterySpec.from_dict` (per-battery config) → `aggregate_fleet` (capacity-weighted mean of efficiency/SOC bounds, summed power/ramp scaled by availability) → `optimize_schedule`. The optimizer is a **greedy threshold heuristic**, not an LP/MIP: 30th/70th percentile price thresholds, per-row look-ahead at remaining max/min prices, applies sqrt(round-trip-eff) on each leg, optional cycle cap. If final SOC undershoots the initial SOC, `_restore_final_soc` buys back at the cheapest interval. `explanations.explain_action` produces the user-facing reason string.

`POST /api/export-schedule` → CSV streamed back with a fixed column order.

### Forecasting pipeline (`watt_etw/forecasting/pipeline.py`)
Each upstream is **optional and degrades gracefully** — failures are caught and logged so a missing source just drops its features:
- `data/henex_parser.py` — parses HEnEx XLSX (auto-detects pre-Oct 2025 hourly vs Oct 2025+ 15-min files; picks the highest version when revisions exist). Output keyed on `(date, mtu)` 0..95.
- `data/weather_fetcher.py` — Open-Meteo, with per-(lat, lon, day) cache. `fetch_renewable_weather_features` capacity-weights across RAE assets per technology.
- `data/admie_fetcher.py` — ADMIE ISP1 day-ahead load + RES forecasts (15-min).
- `data/ttf_fetcher.py` — Dutch TTF gas (CSV in `data/external/ttf_gas/`).
- `data/carbon_fetcher.py` — EUA carbon proxy via Yahoo Finance.
- `data/rae_geoportal.py` — RAE Geoportal WFS for renewable-asset coordinates (filtered to top-N per tech by capacity in `train_forecaster.py`).

`features/feature_builder.py` joins all of the above into one frame keyed on `(date, mtu)`, broadcasting hourly inputs across 4 MTUs and daily inputs across 96. Adds 15-min lags (`mcp_lag1/4/96/192/672`), 24-hour rolling stats, calendar/holiday flags (Greek), and peak-hour helpers.

`forecasting/price_forecaster.py` — **single global LightGBM** with `mtu`/`hour_of_day`/`quarter_of_hour` exposed as features (NOT 96 per-MTU models). Train/test split is temporal (last `test_days=30` held out, never shuffled). RES weather columns are picked up dynamically by prefix (`wind_`, `solar_`, `hydro_`, `hybrid_`). Saves `model.pkl` + `meta.json`; the loader rejects models whose `meta.json` schema isn't `"15min"`.

### Data layout
- `data/2024_DAM_data/`, `data/2025_DAM_data/` — raw HEnEx XLSX summaries (one per day; multiple `_v01/_v02` revisions allowed).
- `data/external/` — TTF CSVs and cached RAE assets parquet.
- `data/processed/` — parquet caches (`prices.parquet`, `prices_15min.parquet`, `features.parquet`). Rebuilt when `--force` is passed.
- `models/price_forecaster/` — `model.pkl` + `meta.json`.

## Notes for editing

- **Holiday calendar is Greece** (`holidays.country_holidays("GR")`) and the baseline weather location is Athens (37.98, 23.73). These defaults appear in both `feature_builder.py` and `forecasting/pipeline.py`.
- The XLSX parser in `market_import.py` is a hand-rolled reader (zipfile + ElementTree) — pyproject lists `openpyxl` is **not** a dependency. The HEnEx workbook detection in `_extract_henex_summary_sheet` keys on a row whose first four MTU cells are `1,2,3,4`; preserve that contract when changing the parser.
- When the optimizer's heuristic finishes below `initial_soc_mwh`, schedule is rewritten in place at the cheapest interval — keep this invariant if refactoring (`tests/test_optimizer.py` covers it).
- `frontend/.agents/` and `frontend/skills-lock.json` are agent-tool metadata, not part of the build.
- Detailed design notes live in `docs/forecasting_and_optimization_report.md`.
