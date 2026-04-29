"""Fetch hourly weather data from Open-Meteo.

Historical data (past days): uses the Open-Meteo Archive API — free, no key.
Forecast data (future days): uses the Open-Meteo Forecast API — free tier up
  to 16 days ahead, no key needed for the variables we use.

Results are cached per calendar day as JSON under data/external/weather/.

Rate-limit handling
-------------------
The free Open-Meteo Archive API enforces a per-minute request rate.  When a
429 response is received, ``_fetch_range`` waits ``retry_delay`` seconds and
retries up to ``max_retries`` times (delay doubles on each attempt).
``fetch_for_assets`` also inserts a small inter-asset sleep so that a fleet
of 100+ assets doesn't fire requests in a tight loop.
"""
from __future__ import annotations

import json
import logging
import time
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

# RES-relevant Open-Meteo variables. These are intentionally limited to drivers
# of PV/wind output rather than the full weather catalogue.
SOLAR_WEATHER_VARS = [
    "shortwave_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "global_tilted_irradiance",
]

WIND_WEATHER_VARS = [
    "wind_speed_10m",
    "wind_speed_80m",
    "wind_speed_120m",
    "wind_direction_80m",
    "wind_direction_120m",
    "wind_gusts_10m",
]

ATMOSPHERIC_WEATHER_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "precipitation",
    "snowfall",
    "snow_depth",
    "weather_code",
]

_HOURLY_VARS = SOLAR_WEATHER_VARS + WIND_WEATHER_VARS + ATMOSPHERIC_WEATHER_VARS

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def _cache_path(d: date, lat: float = _LAT, lon: float = _LON) -> Path:
    location = f"{lat:.4f}_{lon:.4f}".replace("-", "m").replace(".", "p")
    return _CACHE_DIR / location / f"weather_{d.isoformat()}.json"


def _fetch_range(
    start: date,
    end: date,
    forecast: bool = False,
    lat: float = _LAT,
    lon: float = _LON,
    tilt: float = 30.0,
    azimuth: float = 0.0,
    max_retries: int = 4,
    retry_delay: float = 12.0,
) -> dict[str, Any]:
    """Fetch a date range in one API call. Returns raw Open-Meteo JSON.

    Retries up to *max_retries* times on HTTP 429 (Too Many Requests),
    doubling *retry_delay* on each attempt.
    """
    url = _FORECAST_URL if forecast else _ARCHIVE_URL
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(_HOURLY_VARS),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "timezone": "Europe/Athens",
        "tilt": tilt,
        "azimuth": azimuth,
    }
    delay = retry_delay
    for attempt in range(max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            if attempt < max_retries:
                logger.warning(
                    "Open-Meteo 429 for (%.4f, %.4f) — waiting %.0fs before retry %d/%d",
                    lat, lon, delay, attempt + 1, max_retries,
                )
                time.sleep(delay)
                delay *= 2
                continue
            # exhausted retries
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()
    # should never reach here
    raise RuntimeError("_fetch_range: exceeded retry limit")


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
    lat: float = _LAT,
    lon: float = _LON,
    tilt: float = 30.0,
    azimuth: float = 0.0,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
) -> pd.DataFrame:
    """Return hourly weather for one coordinate over [start_date, end_date].

    Returns DataFrame with columns:
        date (date), hour (int 0-23), temperature_2m, shortwave_radiation,
        RES-focused solar, wind, and atmospheric variables.
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
        if not force and _cache_path(d, lat, lon).exists():
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
            raw = _fetch_range(
                batch_start,
                batch_end,
                forecast=is_forecast,
                lat=lat,
                lon=lon,
                tilt=tilt,
                azimuth=azimuth,
            )
            per_day = _split_by_day(raw)
            for d, day_data in per_day.items():
                cp = _cache_path(d, lat, lon)
                cp.parent.mkdir(parents=True, exist_ok=True)
                cp.write_text(json.dumps(day_data))
        except Exception as exc:
            logger.error("Weather fetch failed for %s–%s: %s", batch_start, batch_end, exc)

    # Load everything from cache and assemble DataFrame
    records: list[dict] = []
    for d in all_days:
        cp = _cache_path(d, lat, lon)
        if not cp.exists():
            logger.warning("No weather data for %s — filling with NaN", d)
            for h in range(24):
                records.append({
                    "date": d,
                    "hour": h,
                    "latitude": lat,
                    "longitude": lon,
                    **{v: None for v in _HOURLY_VARS},
                })
            continue

        day_data = json.loads(cp.read_text())
        for h in range(24):
            rec: dict = {"date": d, "hour": h, "latitude": lat, "longitude": lon}
            for var in _HOURLY_VARS:
                vals = day_data.get(var, [])
                rec[var] = vals[h] if h < len(vals) else None
            records.append(rec)

    df = pd.DataFrame(records)
    numeric_cols = _HOURLY_VARS
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df


def fetch_for_assets(
    assets_df: pd.DataFrame,
    start_date: date,
    end_date: date,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
    tilt: float = 30.0,
    azimuth: float = 0.0,
    inter_asset_sleep: float = 2.0,
) -> pd.DataFrame:
    """Fetch hourly weather for each RAE asset coordinate.

    assets_df must include technology, latitude, and longitude. capacity_mw is
    optional and is carried through for weighted aggregation.

    Coordinates are deduplicated (rounded to 3 decimal places) so assets that
    share a grid cell share a single API call.  A small sleep of
    *inter_asset_sleep* seconds is inserted between each uncached fetch to
    stay within the Open-Meteo free-tier rate limit.
    """
    df = assets_df.reset_index(drop=True).copy()

    # Round to 3 dp (~100 m grid) for deduplication purposes.
    df["_lat_r"] = df["latitude"].astype(float).round(3)
    df["_lon_r"] = df["longitude"].astype(float).round(3)

    # Track which unique (lat, lon) pairs have already been fetched this run
    # so we can reuse the DataFrame instead of making a duplicate API call.
    coord_cache: dict[tuple[float, float], pd.DataFrame] = {}

    records: list[pd.DataFrame] = []
    for index, asset in df.iterrows():
        lat = float(asset["_lat_r"])
        lon = float(asset["_lon_r"])
        coord_key = (lat, lon)

        if coord_key in coord_cache:
            weather = coord_cache[coord_key].copy()
            needs_fetch = False
        else:
            # Check whether all days are already on disk to skip the sleep.
            cache_root = Path(cache_dir)
            all_cached = all(
                _cache_path(start_date + timedelta(days=i), lat, lon).exists()
                for i in range((end_date - start_date).days + 1)
            ) and not force

            weather = fetch(
                start_date,
                end_date,
                lat=lat,
                lon=lon,
                tilt=tilt,
                azimuth=azimuth,
                cache_dir=cache_dir,
                force=force,
            )
            coord_cache[coord_key] = weather.copy()
            needs_fetch = not all_cached

        weather["asset_id"] = index
        weather["technology"] = asset["technology"]
        weather["capacity_mw"] = asset.get("capacity_mw")
        records.append(weather)

        if needs_fetch and inter_asset_sleep > 0:
            logger.debug("Sleeping %.1fs between asset fetches", inter_asset_sleep)
            time.sleep(inter_asset_sleep)

    if not records:
        return pd.DataFrame()
    return pd.concat(records, ignore_index=True)


def fetch_renewable_weather_features(
    assets_df: pd.DataFrame,
    start_date: date,
    end_date: date,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
    tilt: float = 30.0,
    azimuth: float = 0.0,
) -> pd.DataFrame:
    """Fetch asset weather and aggregate it into RES technology features."""
    asset_weather = fetch_for_assets(
        assets_df,
        start_date,
        end_date,
        cache_dir=cache_dir,
        force=force,
        tilt=tilt,
        azimuth=azimuth,
    )
    return aggregate_by_technology(asset_weather)


def aggregate_by_technology(asset_weather_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate asset weather into technology-level hourly features.

    Capacity is used as a weight when available; otherwise each asset receives
    equal weight. The output can be merged into the main feature matrix on
    date/hour.
    """
    if asset_weather_df.empty:
        return pd.DataFrame()

    df = asset_weather_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["capacity_mw"] = pd.to_numeric(df.get("capacity_mw"), errors="coerce")
    df["_weight"] = df["capacity_mw"].where(df["capacity_mw"] > 0, 1.0)

    rows: list[dict[str, Any]] = []
    for (day, hour, technology), group in df.groupby(["date", "hour", "technology"], dropna=False):
        row: dict[str, Any] = {
            "date": day,
            "hour": hour,
            f"{technology}_asset_count": int(group["asset_id"].nunique()),
            f"{technology}_capacity_mw": group["capacity_mw"].sum(min_count=1),
        }
        weights = group["_weight"]
        for var in _HOURLY_VARS:
            if var not in group:
                continue
            values = pd.to_numeric(group[var], errors="coerce")
            valid = values.notna() & weights.notna()
            row[f"{technology}_{var}"] = (
                (values[valid] * weights[valid]).sum() / weights[valid].sum()
                if valid.any() and weights[valid].sum() > 0
                else None
            )
        rows.append(row)

    result = pd.DataFrame(rows)
    return result.groupby(["date", "hour"], as_index=False).first()
