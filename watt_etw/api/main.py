from __future__ import annotations

import csv
import io

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from watt_etw.battery_fleet import BatterySpec, aggregate_fleet
from watt_etw.market_import import filter_rows_for_date, load_market_file, rows_from_payload
from watt_etw.optimizer import optimize_schedule


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
        result = optimize_schedule(rows, fleet)
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
        "soc_mwh",
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
