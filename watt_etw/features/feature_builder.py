"""Build the feature matrix by joining HENEX prices, TTF gas, and weather.

Output schema (one row per date-hour):
    date, hour
    -- price history (lags & rolling stats on MCP) --
    mcp_lag1h, mcp_lag2h, mcp_lag24h, mcp_lag48h, mcp_lag168h
    mcp_rolling_mean_24h, mcp_rolling_std_24h
    -- supply mix (from HENEX) --
    sell_total_mwh, gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
    -- calendar --
    day_of_week, month, hour_of_day, is_weekend, is_holiday_gr
    -- weather (Athens) --
    temperature_2m, shortwave_radiation, wind_speed_10m, cloud_cover,
    relative_humidity_2m, precipitation
    -- gas --
    ttf_eur_mwh, ttf_lag1d, ttf_lag7d
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
    cache_path: str | Path | None = "data/processed/features.parquet",
) -> pd.DataFrame:
    """Join all sources and engineer features.  Returns the feature DataFrame.

    Args:
        prices_df:  Output of henex_parser.parse_all / load_or_parse.
        weather_df: Output of weather_fetcher.fetch.
        ttf_df:     Output of ttf_fetcher.fetch.
        cache_path: If given, write the result to parquet at this path.
    """
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

    # Flat index for lag arithmetic
    prices["_idx"] = prices.index
    mcp = prices["mcp_eur_mwh"].values

    def _lag(n: int) -> pd.Series:
        return prices["mcp_eur_mwh"].shift(n)

    prices["mcp_lag1h"] = _lag(1)
    prices["mcp_lag2h"] = _lag(2)
    prices["mcp_lag24h"] = _lag(24)
    prices["mcp_lag48h"] = _lag(48)
    prices["mcp_lag168h"] = _lag(168)  # one week back

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

    prices["day_of_week"] = prices["date"].dt.dayofweek   # 0=Mon
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
    # 5. Merge                                                             #
    # ------------------------------------------------------------------ #
    df = prices.merge(weather, on=["date", "hour"], how="left")
    df = df.merge(ttf, on="date", how="left")

    # ------------------------------------------------------------------ #
    # 6. Drop helper columns and reorder                                   #
    # ------------------------------------------------------------------ #
    df = df.drop(columns=["_idx"], errors="ignore")

    col_order = [
        "date", "hour",
        # lags
        "mcp_lag1h", "mcp_lag2h", "mcp_lag24h", "mcp_lag48h", "mcp_lag168h",
        "mcp_rolling_mean_24h", "mcp_rolling_std_24h",
        # supply mix
        "sell_total_mwh", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh", "imports_mwh",
        # calendar
        "day_of_week", "month", "hour_of_day", "is_weekend", "is_holiday_gr",
        # weather
        "temperature_2m", "shortwave_radiation", "wind_speed_10m",
        "cloud_cover", "relative_humidity_2m", "precipitation",
        # gas
        "ttf_eur_mwh", "ttf_lag1d", "ttf_lag7d",
        # target
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
    cache_path: str | Path = "data/processed/features.parquet",
    force: bool = False,
) -> pd.DataFrame:
    """Return cached features if available, otherwise build and cache."""
    cache_path = Path(cache_path)
    if cache_path.exists() and not force:
        logger.info("Loading features from cache: %s", cache_path)
        return pd.read_parquet(cache_path)
    return build(prices_df, weather_df, ttf_df, cache_path=cache_path)
