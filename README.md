# Watt ETW — Battery Fleet Optimization for the Greek Electricity Market

A full-stack web application for optimizing battery energy storage systems (BESS) operating in the Greek Day-Ahead Market (DAM). Upload market data or use ML-based price forecasts, configure a battery fleet, and get arbitrage schedules with investment economics — all in one workflow.

---

## Overview

Greece's electricity market is transitioning rapidly as renewable energy sources expand, creating growing price volatility and a pressing need for flexibility. Standalone batteries entered the Greek DAM in April 2026, and Watt ETW provides the optimization framework to operate them profitably.

The system combines two core capabilities:

1. **MILP Arbitrage Optimizer** — produces constraint-aware 15-minute charge/discharge schedules given DAM prices
2. **ML Price Forecaster** — a LightGBM model trained on historical DAM prices, weather, load, gas, and carbon data to predict next-day prices at 15-minute resolution

---

## Features

- **Fleet Configuration** — model multiple batteries with individual capacity, efficiency, ramp rate, and availability parameters
- **Market Data Import** — upload HEnEx CSV/XLSX DAM files with automatic validation and interval checking
- **MILP Optimization** — mixed-integer linear programming schedule respecting SOC bounds, ramp constraints, and round-trip efficiency
- **Greedy Heuristic** — fast threshold-based optimizer as an alternative to MILP
- **ML Forecasting** — LightGBM-based day-ahead price prediction using weather, load, TTF gas, EUA carbon, and calendar features
- **Investment Economics** — CAPEX, NPV, IRR, payback period, and year-by-year cash flow analysis
- **Bidding View** — converts optimized dispatch into buy/sell order plan with limit prices
- **CSV Export** — download schedules for further analysis

---

## Architecture

```
frontend/                  React + Vite (port 5173)
  └── src/main.jsx         Single-file UI: landing, fleet setup, results dashboard

watt_etw/                  Python backend
  ├── api/main.py          FastAPI endpoints
  ├── milp_optimizer.py    MILP arbitrage (PuLP)
  ├── optimizer.py         Greedy threshold heuristic
  ├── battery_fleet.py     Fleet aggregation logic
  ├── market_import.py     HEnEx CSV/XLSX parser
  ├── economics.py         Investment economics engine
  ├── explanations.py      User-facing action explanations
  ├── data/                Upstream data fetchers
  │   ├── henex_parser.py  HEnEx XLSX → (date, mtu) prices
  │   ├── admie_fetcher.py ADMIE ISP1 load + RES forecasts
  │   ├── weather_fetcher.py Open-Meteo per-asset weather
  │   ├── ttf_fetcher.py   Dutch TTF gas prices
  │   ├── carbon_fetcher.py EUA carbon proxy
  │   └── rae_geoportal.py RAE renewable asset coordinates
  ├── features/
  │   └── feature_builder.py  Joins all sources into one feature matrix
  └── forecasting/
      ├── pipeline.py      Orchestrates data collection + feature building
      └── price_forecaster.py  Single global LightGBM model
```

---

## Quick Start

### Backend

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
uvicorn watt_etw.api.main:app --reload
```

The API starts at `http://127.0.0.1:8000`. Interactive docs are available at `/docs`.

### Frontend

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server starts at `http://127.0.0.1:5173` and proxies `/api` requests to the backend.

---

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Health check |
| `/api/market-data/validate` | POST | Upload and validate DAM CSV/XLSX |
| `/api/optimize` | POST | Greedy heuristic optimization |
| `/api/optimize-arbitrage` | POST | MILP arbitrage optimization |
| `/api/forecast/{date}` | GET | ML price forecast for a given date |
| `/api/economics` | POST | Investment economics (NPV, IRR, payback) |
| `/api/export-schedule` | POST | Export schedule as CSV |

---

## Training the Price Forecaster

The forecaster is trained offline using historical data from multiple sources. Each source is optional and degrades gracefully if unavailable.

```bash
python train_forecaster.py             # train end-to-end
python train_forecaster.py --force     # rebuild parquet caches first
python train_forecaster.py --eval-only # load saved model, print holdout metrics
python train_forecaster.py --no-rae    # skip per-tech RES weather (faster)
```

**Output:** `models/price_forecaster/model.pkl` + `meta.json`

**Data sources used:**

| Source | Data | Resolution |
|---|---|---|
| HEnEx | DAM clearing prices | Hourly (pre-Oct 2025) / 15-min (Oct 2025+) |
| ADMIE (IPTO) | System load + RES generation forecasts | 15-min |
| Open-Meteo | Wind speed, solar irradiance, temperature, humidity | Hourly |
| RAE Geoportal | Renewable asset coordinates & capacity | Static |
| Dutch TTF | Natural gas futures | Daily |
| Yahoo Finance | EUA carbon allowances | Daily |

**Model details:**

- Single global LightGBM model (not 96 per-MTU models)
- Features: MTU index, hour/quarter of day, price lags (1/4/96/192/672 MTU), rolling stats, calendar/holiday flags (Greek), weather (capacity-weighted per technology), gas, carbon
- Temporal train/test split (last 30 days held out, never shuffled)

---

## User Flow

1. **Configure Fleet** — set up one or more batteries with capacity, min SOC, round-trip efficiency, availability, and ramp rate
2. **Optimize** — run MILP arbitrage using ML-forecasted prices (or uploaded DAM data)
3. **Results Dashboard** — view price charts, dispatch schedule, economics, bidding plan, and investment analysis
4. **Export** — download the schedule as CSV

---

## Data Layout

```
data/
  2024_DAM_data/          Raw HEnEx XLSX summaries (one per day)
  2025_DAM_data/          Raw HEnEx XLSX summaries (15-min from Oct 2025)
  external/               TTF CSVs, cached RAE assets parquet
  processed/              Parquet caches (prices, features)

models/
  price_forecaster/       model.pkl + meta.json
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11+, FastAPI, PuLP |
| ML | LightGBM, scikit-learn |
| Frontend | React 19, Vite 7, TypeScript |
| Data | Pandas, PyArrow, Requests |

---

## Testing

```bash
pytest                               # run all tests
pytest tests/test_optimizer.py       # single file
pytest tests/test_api.py::test_optimize_endpoint  # single test
```

---

## Project Context

This project was built for the **Battery Optimization in the Greek Electricity Market** challenge. The Greek DAM transitioned from hourly to 15-minute Market Time Units on 1 October 2025, and this system is designed to operate at that resolution. All forecasting defaults use Greek calendar holidays and Athens-area weather baselines.
