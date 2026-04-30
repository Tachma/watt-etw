from __future__ import annotations

import csv
import io
import logging
import math
from datetime import date
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from watt_etw.battery_fleet import BatterySpec, aggregate_fleet
from watt_etw.economics import EconomicsInputs, compute_economics
from watt_etw.market_import import filter_rows_for_date, load_market_file, rows_from_payload
from watt_etw.milp_optimizer import (
    DT,
    NUM_INTERVALS,
    NUM_QUARTERS,
    hourly_to_quarterly,
    optimize_fleet,
)
from watt_etw.optimizer import optimize_schedule

logger = logging.getLogger(__name__)

_MODEL_DIR = Path("models/price_forecaster")
_FEATURES_CACHE = Path("data/processed/features.parquet")


app = FastAPI(title="Watt ETW Battery Optimizer")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class OptimizeRequest(BaseModel):
    batteries: list[dict]
    market_rows: list[dict]
    selected_date: str | None = None


class ArbitrageRequest(BaseModel):
    """Request body for the MILP arbitrage optimizer.

    Prices can be supplied directly (96 quarter-hour or 24 hourly values) or
    omitted entirely — in that case the ML forecaster is called for `date`.
    """
    batteries: list[dict]
    # Option A: supply prices explicitly
    prices_15min: list[float] | None = None   # 96 quarter-hour prices EUR/MWh
    prices_hourly: list[float] | None = None  # 24 hourly prices EUR/MWh (repeated x4)
    # Option B: let the ML forecaster produce 96 quarter-hour prices
    date: str | None = None                   # ISO format YYYY-MM-DD


class ExportRequest(BaseModel):
    schedule: list[dict]


class EconomicsRequest(BaseModel):
    energy_capacity_mwh: float
    power_capacity_mw: float
    daily_revenue_eur: float
    daily_throughput_mwh: float

    realization_ratio: float = 0.75
    availability: float = 0.97
    operating_days_per_year: int = 365
    annual_degradation: float = 0.02

    capex_per_mwh_energy: float = 300_000.0
    capex_per_mw_power: float = 0.0
    grid_connection_eur: float = 0.0
    grant_eur: float = 0.0

    opex_fixed_pct: float = 0.02
    opex_var_eur_per_mwh: float = 2.0
    augmentation_pct: float = 0.012

    wacc: float = 0.08
    lifetime_years: int = 12
    salvage_pct: float = 0.07
    tax_rate: float = 0.24
    depreciation_years: int = 10


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/market-data/validate")
async def validate_market_data(file: UploadFile = File(...)) -> dict:
    content = await file.read()
    result = load_market_file(file.filename or "upload", content)
    return result.to_dict(include_rows=True)


@app.post("/api/optimize")
def optimize(payload: OptimizeRequest) -> dict:
    """Greedy battery schedule optimizer (existing flow — uploaded market data)."""
    try:
        specs = [BatterySpec.from_dict(item) for item in payload.batteries]
        fleet = aggregate_fleet(specs)
        rows = filter_rows_for_date(rows_from_payload(payload.market_rows), payload.selected_date)
        result = optimize_schedule(rows, fleet)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return result.to_dict()


@app.post("/api/optimize-arbitrage")
def optimize_arbitrage(payload: ArbitrageRequest) -> dict:
    """MILP battery arbitrage optimizer (faithful translation of battery_qa.mod).

    Price resolution priority:
      1. prices_15min  (96 explicit quarter-hour prices)
      2. prices_hourly (24 explicit hourly prices, repeated x4)
      3. date          (ML forecaster → 96 quarter-hour prices)
    """
    try:
        specs = [BatterySpec.from_dict(item) for item in payload.batteries]
        fleet = aggregate_fleet(specs)
        actual_prices = _actual_prices_for_date(payload.date) if payload.date else None

        if payload.prices_15min is not None:
            if len(payload.prices_15min) != NUM_INTERVALS:
                raise ValueError(
                    f"prices_15min must have {NUM_INTERVALS} values, "
                    f"got {len(payload.prices_15min)}"
                )
            prices = payload.prices_15min

        elif payload.prices_hourly is not None:
            prices = hourly_to_quarterly(payload.prices_hourly)

        elif payload.date is not None:
            prices = _forecast_for_date(payload.date)

        else:
            raise ValueError(
                "Provide prices_15min, prices_hourly, or date for ML forecast."
            )

        result = optimize_fleet(fleet, prices)

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _milp_to_frontend(result, fleet, actual_prices=actual_prices)


@app.get("/api/forecast/{target_date}")
def get_forecast(target_date: str) -> dict:
    """Return ML day-ahead price forecast for the given date (YYYY-MM-DD).

    Response includes 24 hourly prices and 96 derived quarter-hour prices.
    """
    try:
        quarterly = _forecast_for_date(target_date)
        hourly = _hourly_average_from_quarterly(quarterly)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "date": target_date,
        "hourly_eur_mwh": [round(p, 4) for p in hourly],
        "quarterly_eur_mwh": [round(p, 4) for p in quarterly],
    }


@app.post("/api/economics")
def economics(payload: EconomicsRequest) -> dict:
    """Investment economics for a battery project (CAPEX, NPV, IRR, payback).

    `daily_revenue_eur` and `daily_throughput_mwh` should come from the
    optimizer's KPIs (`expected_profit` and `total_discharged_mwh`). The
    response includes the year-by-year cash flows plus headline metrics and
    a `verdict` of `worth_it`, `marginal`, or `burning_money`.
    """
    try:
        inputs = EconomicsInputs(**payload.model_dump())
        result = compute_economics(inputs)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return result.to_dict()


@app.post("/api/export-schedule")
def export_schedule(payload: ExportRequest) -> StreamingResponse:
    output = io.StringIO()
    fieldnames = [
        "timestamp",
        "price_eur_mwh",
        "action",
        "charge_mw",
        "discharge_mw",
        "interval_hours",
        "explanation",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in payload.schedule:
        writer.writerow({field: row.get(field, "") for field in fieldnames})
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="battery_schedule.csv"'},
    )


# ---------------------------------------------------------------------------
# Response adapter  (MILP → frontend Results component format)
# ---------------------------------------------------------------------------

def _milp_to_frontend(result, fleet, actual_prices: list[float] | None = None) -> dict:
    """Convert ArbitrageResult to the shape the Results component expects."""
    discharge_revenue = 0.0
    charging_cost = 0.0
    schedule_rows = []

    for row in result.schedule:
        # Convert MWh → MW  (divide by dt = 0.25 h)
        charge_mw = round(row.charge_mwh / DT, 4)
        discharge_mw = round(row.discharge_mwh / DT, 4)

        discharge_revenue += row.lambda_eur_mwh * row.discharge_mwh
        charging_cost += row.lambda_eur_mwh * row.charge_mwh

        if row.is_discharging:
            action = "discharge"
            explanation = f"Discharging at {row.lambda_eur_mwh:.2f} EUR/MWh"
        elif row.charge_mwh > 1e-4:
            action = "charge"
            explanation = f"Charging at {row.lambda_eur_mwh:.2f} EUR/MWh"
        else:
            action = "hold"
            explanation = "Holding"

        # Build a human-readable timestamp: "HH:MM"
        minutes = (row.hour - 1) * 60 + (row.quarter - 1) * 15
        ts = f"{minutes // 60:02d}:{minutes % 60:02d}"

        interval_index = (row.hour - 1) * NUM_QUARTERS + (row.quarter - 1)
        schedule_row = {
            "timestamp": ts,
            "hour": row.hour,
            "quarter": row.quarter,
            "price_eur_mwh": row.lambda_eur_mwh,
            "action": action,
            "charge_mw": charge_mw,
            "discharge_mw": discharge_mw,
            "soc_mwh": row.soc_mwh,
            "explanation": explanation,
        }
        if actual_prices is not None and interval_index < len(actual_prices):
            schedule_row["actual_price_eur_mwh"] = round(actual_prices[interval_index], 4)

        schedule_rows.append(schedule_row)

    usable_capacity = fleet.max_soc_mwh - fleet.min_soc_mwh
    equiv_cycles = round(
        result.kpis["total_discharged_mwh"] / usable_capacity, 2
    ) if usable_capacity > 0 else 0.0

    kpis = {
        "expected_profit": round(result.revenue_eur, 2),
        "discharge_revenue": round(discharge_revenue, 2),
        "charging_cost": round(charging_cost, 2),
        "equivalent_cycles": equiv_cycles,
        "final_soc_mwh": result.kpis["final_soc_mwh"],
        **result.kpis,
    }

    return {
        "status": result.status,
        "revenue_eur": result.revenue_eur,
        "kpis": kpis,
        "schedule": schedule_rows,
    }


# ---------------------------------------------------------------------------
# ML forecaster helpers
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_forecaster():
    """Load the trained PriceForecaster once and cache it in memory."""
    from watt_etw.forecasting.price_forecaster import PriceForecaster
    fc = PriceForecaster(model_dir=str(_MODEL_DIR))
    fc.load()
    return fc


def _forecast_for_date(target_date_str: str) -> list[float]:
    """Return 96 quarter-hour predicted prices for a given date string."""
    import pandas as pd

    target = date.fromisoformat(target_date_str)

    if not _FEATURES_CACHE.exists():
        raise RuntimeError(
            "Feature cache not found. Run train_forecaster.py first."
        )
    if not _MODEL_DIR.exists():
        raise RuntimeError(
            "Model directory not found. Run train_forecaster.py first."
        )

    features = pd.read_parquet(_FEATURES_CACHE)
    features["date"] = pd.to_datetime(features["date"])
    forecaster = _load_forecaster()

    target_ts = pd.Timestamp(target)
    day_features = features[features["date"] == target_ts]

    if day_features.empty:
        # Outside training window: use the last full 96-MTU shape, override
        # known calendar/index columns, and keep lag/exogenous columns as the
        # latest available proxy.
        day_features = features.iloc[-NUM_INTERVALS:].copy()
        if len(day_features) != NUM_INTERVALS:
            raise RuntimeError(
                f"Need at least {NUM_INTERVALS} cached feature rows to forecast "
                f"{target_date_str}; found {len(features)}."
            )
        day_features = day_features.reset_index(drop=True)
        day_features["mtu"] = range(NUM_INTERVALS)
        day_features["hour"] = day_features["mtu"] // 4
        day_features["quarter"] = day_features["mtu"] % 4
        day_features["hour_of_day"] = day_features["hour"]
        day_features["quarter_of_hour"] = day_features["quarter"]
        day_features["mtu_of_day"] = day_features["mtu"]
        day_features["date"] = target_ts
        day_features["day_of_week"] = target.weekday()
        day_features["month"] = target.month
        day_features["is_weekend"] = int(target.weekday() >= 5)

    result = forecaster.predict(day_features, target)
    # predictions is dict {mtu: price}; return as ordered list [mtu0..mtu95]
    return [result.predictions.get(mtu, float("nan")) for mtu in range(NUM_INTERVALS)]


@lru_cache(maxsize=128)
def _actual_prices_for_date(target_date_str: str) -> list[float] | None:
    """Return actual cached DAM/MCP prices for a date, if all 96 MTUs exist."""
    import pandas as pd

    target = date.fromisoformat(target_date_str)
    if not _FEATURES_CACHE.exists():
        return None

    features = pd.read_parquet(_FEATURES_CACHE, columns=["date", "mtu", "mcp_eur_mwh"])
    features["date"] = pd.to_datetime(features["date"])
    day = features[features["date"] == pd.Timestamp(target)].sort_values("mtu")
    if len(day) != NUM_INTERVALS:
        return None

    prices = [float(value) for value in day["mcp_eur_mwh"].tolist()]
    if any(not math.isfinite(value) for value in prices):
        return None
    return prices


def _hourly_average_from_quarterly(prices_15min: list[float]) -> list[float]:
    """Average 96 quarter-hour prices into 24 hourly values for API display."""
    if len(prices_15min) != NUM_INTERVALS:
        raise ValueError(
            f"Expected {NUM_INTERVALS} quarter-hour prices, got {len(prices_15min)}"
        )
    return [
        sum(prices_15min[start:start + 4]) / 4
        for start in range(0, NUM_INTERVALS, 4)
    ]
