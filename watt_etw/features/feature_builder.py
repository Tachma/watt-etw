"""Build the feature matrix by joining HENEX prices, TTF gas, EUA carbon, and weather.

`weather_df` is the single-location baseline (e.g. weather_fetcher.fetch for Athens).
`res_weather_df` is the optional technology-aggregated RES weather table from
weather_fetcher.fetch_renewable_weather_features(); when provided, its per-tech
columns (e.g. wind_wind_speed_120m, solar_global_tilted_irradiance) are merged
on date/hour alongside the baseline weather.

`carbon_df` is the optional output of carbon_fetcher.fetch (daily eua_eur_t).

Output schema (one row per date-hour):
    date, hour
    -- price history (lags & rolling stats on MCP) --
    mcp_lag1h, mcp_lag2h, mcp_lag24h, mcp_lag48h, mcp_lag168h
    mcp_rolling_mean_24h, mcp_rolling_std_24h
    -- supply mix (from HENEX) --
    sell_total_mwh, gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
    -- calendar --
    day_of_week, month, hour_of_day, is_weekend, is_holiday_gr
    -- weather (Athens baseline) --
    temperature_2m, shortwave_radiation, wind_speed_10m, cloud_cover,
    relative_humidity_2m, precipitation
    -- per-tech RES weather (if res_weather_df given) --
    wind_*, hydro_*, hybrid_*, ... (capacity-weighted by technology)
    -- gas --
    ttf_eur_mwh, ttf_lag1d, ttf_lag7d
    -- carbon (if carbon_df given) --
    eua_eur_t, eua_lag1d, eua_lag7d
    -- target --
    mcp_eur_mwh
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import holidays as _holidays_lib
    _HAS_HOLIDAYS = True
except ImportError:
    _HAS_HOLIDAYS = False
    logger.warning("holidays package not installed — is_holiday_gr will be 0")


def _greek_holidays(years: list[int]) -> set[date]:
    if not _HAS_HOLIDAYS:
        return set()
    gr = _holidays_lib.country_holidays("GR", years=years)
    return set(gr.keys())


def build(
    prices_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    ttf_df: pd.DataFrame,
    res_weather_df: pd.DataFrame | None = None,
    carbon_df: pd.DataFrame | None = None,
    cache_path: str | Path | None = "data/processed/features.parquet",
) -> pd.DataFrame:
    """Join all sources and engineer features. Returns the feature DataFrame."""
    # ------------------------------------------------------------------ #
    # 1. Normalise date types to datetime for consistent merging           #
    # ------------------------------------------------------------------ #
    prices = prices_df.copy()
    prices["date"] = pd.to_datetime(prices["date"])

    weather = weather_df.copy()
    weather["date"] = pd.to_datetime(weather["date"])

    ttf = ttf_df.copy()
    ttf["date"] = pd.to_datetime(ttf["date"])

    # ------------------------------------------------------------------ #
    # 2. Sort prices and build price-lag features                          #
    # ------------------------------------------------------------------ #
    prices = prices.sort_values(["date", "hour"]).reset_index(drop=True)

    prices["mcp_lag1h"] = prices["mcp_eur_mwh"].shift(1)
    prices["mcp_lag2h"] = prices["mcp_eur_mwh"].shift(2)
    prices["mcp_lag24h"] = prices["mcp_eur_mwh"].shift(24)
    prices["mcp_lag48h"] = prices["mcp_eur_mwh"].shift(48)
    prices["mcp_lag168h"] = prices["mcp_eur_mwh"].shift(168)

    prices["mcp_rolling_mean_24h"] = (
        prices["mcp_eur_mwh"].shift(1).rolling(24, min_periods=12).mean()
    )
    prices["mcp_rolling_std_24h"] = (
        prices["mcp_eur_mwh"].shift(1).rolling(24, min_periods=12).std()
    )

    # ------------------------------------------------------------------ #
    # 3. Calendar features                                                 #
    # ------------------------------------------------------------------ #
    years = sorted(prices["date"].dt.year.unique().tolist())
    gr_holidays = _greek_holidays(years)

    prices["day_of_week"] = prices["date"].dt.dayofweek
    prices["month"] = prices["date"].dt.month
    prices["hour_of_day"] = prices["hour"]
    prices["is_weekend"] = (prices["day_of_week"] >= 5).astype(int)
    prices["is_holiday_gr"] = prices["date"].dt.date.isin(gr_holidays).astype(int)

    # ------------------------------------------------------------------ #
    # 4. TTF gas lags (daily → broadcast to all hours of that day)        #
    # ------------------------------------------------------------------ #
    ttf = ttf.sort_values("date").reset_index(drop=True)
    ttf["ttf_lag1d"] = ttf["ttf_eur_mwh"].shift(1)
    ttf["ttf_lag7d"] = ttf["ttf_eur_mwh"].shift(7)

    # ------------------------------------------------------------------ #
    # 5. Merge prices with weather + TTF                                   #
    # ------------------------------------------------------------------ #
    df = prices.merge(weather, on=["date", "hour"], how="left")

    if res_weather_df is not None and not res_weather_df.empty:
        res = res_weather_df.copy()
        res["date"] = pd.to_datetime(res["date"])
        # Avoid clashing column names with the baseline weather merge.
        overlap = (set(res.columns) - {"date", "hour"}) & set(df.columns)
        if overlap:
            logger.warning(
                "res_weather_df shares %d cols with baseline weather; dropping from RES side: %s",
                len(overlap), sorted(overlap),
            )
            res = res.drop(columns=list(overlap))
        df = df.merge(res, on=["date", "hour"], how="left")

    df = df.merge(ttf, on="date", how="left")

    # ------------------------------------------------------------------ #
    # 6. Carbon lags + merge                                               #
    # ------------------------------------------------------------------ #
    if carbon_df is not None and not carbon_df.empty:
        carbon = carbon_df.copy()
        carbon["date"] = pd.to_datetime(carbon["date"])
        carbon = carbon.sort_values("date").reset_index(drop=True)
        carbon["eua_lag1d"] = carbon["eua_eur_t"].shift(1)
        carbon["eua_lag7d"] = carbon["eua_eur_t"].shift(7)
        df = df.merge(carbon, on="date", how="left")

    # ------------------------------------------------------------------ #
    # 7. Reorder                                                           #
    # ------------------------------------------------------------------ #
    col_order = [
        "date", "hour",
        "mcp_lag1h", "mcp_lag2h", "mcp_lag24h", "mcp_lag48h", "mcp_lag168h",
        "mcp_rolling_mean_24h", "mcp_rolling_std_24h",
        "sell_total_mwh", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh", "imports_mwh",
        "day_of_week", "month", "hour_of_day", "is_weekend", "is_holiday_gr",
        "temperature_2m", "shortwave_radiation", "wind_speed_10m",
        "cloud_cover", "relative_humidity_2m", "precipitation",
        "ttf_eur_mwh", "ttf_lag1d", "ttf_lag7d",
        "eua_eur_t", "eua_lag1d", "eua_lag7d",
        "mcp_eur_mwh",
    ]
    existing = [c for c in col_order if c in df.columns]
    extra = [c for c in df.columns if c not in col_order]
    df = df[existing + extra]

    logger.info(
        "Feature matrix: %d rows × %d cols, date range %s → %s",
        len(df), len(df.columns),
        df["date"].min().date(), df["date"].max().date(),
    )

    if cache_path is not None:
        p = Path(cache_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False)
        logger.info("Saved features to %s", p)

    return df


def load_or_build(
    prices_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    ttf_df: pd.DataFrame,
    res_weather_df: pd.DataFrame | None = None,
    carbon_df: pd.DataFrame | None = None,
    cache_path: str | Path = "data/processed/features.parquet",
    force: bool = False,
) -> pd.DataFrame:
    """Return cached features if available, otherwise build and cache."""
    cache_path = Path(cache_path)
    if cache_path.exists() and not force:
        logger.info("Loading features from cache: %s", cache_path)
        return pd.read_parquet(cache_path)
    return build(
        prices_df,
        weather_df,
        ttf_df,
        res_weather_df=res_weather_df,
        carbon_df=carbon_df,
        cache_path=cache_path,
    )
