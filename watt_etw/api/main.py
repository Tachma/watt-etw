from __future__ import annotations

import csv
import io
import logging
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from watt_etw.battery_fleet import BatterySpec, aggregate_fleet
from watt_etw.market_import import filter_rows_for_date, load_market_file, rows_from_payload
from watt_etw.milp_optimizer import (
    DT,
    NUM_INTERVALS,
    hourly_to_quarterly,
    optimize_battery,
    optimize_fleet,
)
from watt_etw.optimizer import NUM_INTERVALS as _ADJ_NUM_INTERVALS, optimize_adjustments

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
    prices_hourly: list[float] | None = None  # 24 hourly prices EUR/MWh (repeated ×4)
    # Option B: let the ML forecaster produce prices
    date: str | None = None                   # ISO format YYYY-MM-DD


class ExportRequest(BaseModel):
    schedule: list[dict]


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
    try:
        specs = [BatterySpec.from_dict(item) for item in payload.batteries]
        fleet = aggregate_fleet(specs)
        rows = filter_rows_for_date(rows_from_payload(payload.market_rows), payload.selected_date)
        # Extract the 96 λ prices and max capacity before passing to the pure model
        prices = [row.price_eur_mwh for row in rows[:_ADJ_NUM_INTERVALS]]
        result = optimize_adjustments(prices, fleet.power_mw)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return result.to_dict()


@app.post("/api/optimize-arbitrage")
def optimize_arbitrage(payload: ArbitrageRequest) -> dict:
    """MILP battery arbitrage optimizer (faithful translation of battery_qa.mod).

    Price resolution priority:
      1. prices_15min  (96 explicit quarter-hour prices)
      2. prices_hourly (24 explicit hourly prices, repeated x4)
      3. date          (ML forecaster → 24 hourly prices, repeated x4)
    """
    try:
        specs = [BatterySpec.from_dict(item) for item in payload.batteries]
        fleet = aggregate_fleet(specs)

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
            prices = hourly_to_quarterly(_forecast_for_date(payload.date))

        else:
            raise ValueError(
                "Provide prices_15min, prices_hourly, or date for ML forecast."
            )

        result = optimize_fleet(fleet, prices)

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _milp_to_frontend(result, fleet)


@app.get("/api/forecast/{target_date}")
def get_forecast(target_date: str) -> dict:
    """Return ML day-ahead price forecast for the given date (YYYY-MM-DD).

    Response includes 24 hourly prices and 96 derived quarter-hour prices.
    """
    try:
        hourly = _forecast_for_date(target_date)
        quarterly = hourly_to_quarterly(hourly)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "date": target_date,
        "hourly_eur_mwh": [round(p, 4) for p in hourly],
        "quarterly_eur_mwh": [round(p, 4) for p in quarterly],
    }


# ---------------------------------------------------------------------------
# Response adapter  (MILP → frontend Results component format)
# ---------------------------------------------------------------------------

def _milp_to_frontend(result, fleet) -> dict:
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

        # Build a human-readable timestamp: "HH:MM" for hour h, quarter s
        minutes = ((row.hour - 1) * 60) + (row.quarter - 1) * 15
        ts = f"{minutes // 60:02d}:{minutes % 60:02d}"

        schedule_rows.append({
            "timestamp": ts,
            "hour": row.hour,
            "quarter": row.quarter,
            "price_eur_mwh": row.lambda_eur_mwh,
            "action": action,
            "charge_mw": charge_mw,
            "discharge_mw": discharge_mw,
            "soc_mwh": row.soc_mwh,
            "explanation": explanation,
        })

    # Equivalent full cycles = total energy discharged / usable capacity
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
        # Pass through all MILP kpis as well
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
    """Return 24 hourly predicted prices for a given date string."""
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
        # Outside training window: use last 24 rows, override calendar columns
        day_features = features.iloc[-24:].copy()
        day_features["date"] = target_ts
        day_features["day_of_week"] = target.weekday()
        day_features["month"] = target.month
        day_features["is_weekend"] = int(target.weekday() >= 5)

    result = forecaster.predict(day_features, target)
    # predictions is dict {hour: price}; return as ordered list [h0..h23]
    return [result.predictions.get(h, float("nan")) for h in range(24)]


@app.post("/api/export-schedule")
def export_schedule(payload: ExportRequest) -> StreamingResponse:
    output = io.StringIO()
    fieldnames = [
        "scenario",
        "hour",
        "price_coefficient",
        "reference_dispatch_mw",
        "adjusted_quantity_mw",
        "obj_contribution",
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
