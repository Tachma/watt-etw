"""Fetch hourly weather data for Athens from Open-Meteo.

Historical data (past days): uses the Open-Meteo Archive API — free, no key.
Forecast data (future days): uses the Open-Meteo Forecast API — free tier up
  to 16 days ahead, no key needed for the variables we use.

Results are cached per calendar day as JSON under data/external/weather/.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path("data/external/weather")

# Athens coordinates
_LAT = 37.98
_LON = 23.73

_HOURLY_VARS = [
    "temperature_2m",
    "shortwave_radiation",
    "wind_speed_10m",
    "cloud_cover",
    "relative_humidity_2m",
    "precipitation",
]

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def _cache_path(d: date) -> Path:
    return _CACHE_DIR / f"weather_{d.isoformat()}.json"


def _fetch_range(start: date, end: date, forecast: bool = False) -> dict[str, Any]:
    """Fetch a date range in one API call. Returns raw Open-Meteo JSON."""
    url = _FORECAST_URL if forecast else _ARCHIVE_URL
    params = {
        "latitude": _LAT,
        "longitude": _LON,
        "hourly": ",".join(_HOURLY_VARS),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "timezone": "Europe/Athens",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _split_by_day(raw: dict[str, Any]) -> dict[date, dict[str, Any]]:
    """Split a multi-day Open-Meteo response into per-day caches."""
    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])
    n = len(times)

    per_day: dict[date, dict[str, list]] = {}
    for i, ts in enumerate(times):
        d = date.fromisoformat(ts[:10])
        if d not in per_day:
            per_day[d] = {v: [] for v in _HOURLY_VARS}
            per_day[d]["time"] = []
        per_day[d]["time"].append(ts)
        for var in _HOURLY_VARS:
            vals = hourly.get(var, [None] * n)
            per_day[d][var].append(vals[i] if i < len(vals) else None)

    return per_day


def fetch(
    start_date: date,
    end_date: date,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
) -> pd.DataFrame:
    """Return hourly weather for Athens over [start_date, end_date].

    Returns DataFrame with columns:
        date (date), hour (int 0-23), temperature_2m, shortwave_radiation,
        wind_speed_10m, cloud_cover, relative_humidity_2m, precipitation
    """
    global _CACHE_DIR
    _CACHE_DIR = Path(cache_dir)
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    today = date.today()

    # Separate days into cached vs. missing
    all_days = [start_date + timedelta(days=i)
                for i in range((end_date - start_date).days + 1)]
    missing_hist: list[date] = []
    missing_fcast: list[date] = []

    for d in all_days:
        if not force and _cache_path(d).exists():
            continue
        if d <= today:
            missing_hist.append(d)
        else:
            missing_fcast.append(d)

    # Fetch missing historical in one batch (API supports multi-day)
    for batch, is_forecast in [(missing_hist, False), (missing_fcast, True)]:
        if not batch:
            continue
        batch_start, batch_end = batch[0], batch[-1]
        logger.info(
            "Fetching weather %s → %s (%s)",
            batch_start, batch_end,
            "forecast" if is_forecast else "historical",
        )
        try:
            raw = _fetch_range(batch_start, batch_end, forecast=is_forecast)
            per_day = _split_by_day(raw)
            for d, day_data in per_day.items():
                _cache_path(d).write_text(json.dumps(day_data))
        except Exception as exc:
            logger.error("Weather fetch failed for %s–%s: %s", batch_start, batch_end, exc)

    # Load everything from cache and assemble DataFrame
    records: list[dict] = []
    for d in all_days:
        cp = _cache_path(d)
        if not cp.exists():
            logger.warning("No weather data for %s — filling with NaN", d)
            for h in range(24):
                records.append({"date": d, "hour": h,
                                 **{v: None for v in _HOURLY_VARS}})
            continue

        day_data = json.loads(cp.read_text())
        for h in range(24):
            rec: dict = {"date": d, "hour": h}
            for var in _HOURLY_VARS:
                vals = day_data.get(var, [])
                rec[var] = vals[h] if h < len(vals) else None
            records.append(rec)

    df = pd.DataFrame(records)
    numeric_cols = _HOURLY_VARS
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df
