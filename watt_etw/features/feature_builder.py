"""Build the feature matrix at 15-minute resolution.

Joins HENEX prices, Athens-baseline weather, per-tech RES weather, ADMIE
ISP1 demand & RES forecasts, TTF gas, and EUA carbon prices into one
DataFrame keyed on (date, mtu) where mtu ∈ 0..95.

Hourly inputs (Athens weather, RES per-tech weather, hourly ADMIE) are
broadcast across the 4 MTUs of the corresponding hour. Daily inputs (TTF,
EUA) are broadcast across all 96 MTUs of the day.

15-min output schema (one row per date × mtu):
    date, mtu, hour, quarter
    -- price history (lags & rolling stats on MCP, in 15-min steps) --
    mcp_lag1, mcp_lag4, mcp_lag96, mcp_lag192, mcp_lag672
    mcp_rolling_mean_96, mcp_rolling_std_96
    -- supply mix (from HENEX) --
    sell_total_mwh, gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
    -- calendar --
    day_of_week, month, hour_of_day, quarter_of_hour, mtu_of_day,
    is_weekend, is_holiday_gr
    -- weather (Athens baseline) --
    temperature_2m, shortwave_radiation, wind_speed_10m, cloud_cover,
    relative_humidity_2m, precipitation, ...
    -- per-tech RES weather (if res_weather_df given) --
    wind_*, solar_*, hydro_*, hybrid_*, ... (capacity-weighted by tech)
    -- gas --
    ttf_eur_mwh, ttf_lag1d, ttf_lag7d
    -- carbon --
    eua_eur_t, eua_lag1d, eua_lag7d
    -- ADMIE ISP1 day-ahead forecasts --
    load_forecast_mw, res_forecast_mw, load_res_ratio, net_load_forecast_mw
    -- peak-hour helpers --
    temp_dev_from_climatology, net_load_vs_daily_max, mcp_range_24h
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


# 15-min lag definitions (steps × 15 minutes)
_LAG_STEPS = {
    "mcp_lag1":   1,    # 15 minutes
    "mcp_lag4":   4,    # 1 hour
    "mcp_lag96":  96,   # 1 day
    "mcp_lag192": 192,  # 2 days
    "mcp_lag672": 672,  # 1 week
}
_ROLL_WINDOW = 96  # 24 hours of 15-min steps


def _greek_holidays(years: list[int]) -> set[date]:
    if not _HAS_HOLIDAYS:
        return set()
    gr = _holidays_lib.country_holidays("GR", years=years)
    return set(gr.keys())


def _ensure_mtu(prices: pd.DataFrame) -> pd.DataFrame:
    """Make sure prices_df is keyed on (date, mtu) at 15-min resolution.

    Accepts either:
      - already-15-min: must contain `mtu` (and optional hour/quarter).
      - hourly:         must contain `hour`, no mtu — gets broadcast 4×.
    """
    if "mtu" in prices.columns:
        out = prices.copy()
    elif "hour" in prices.columns:
        # Broadcast hourly rows to 4 MTUs each.
        rows: list[pd.DataFrame] = []
        for q in range(4):
            tmp = prices.copy()
            tmp["mtu"] = tmp["hour"] * 4 + q
            tmp["quarter"] = q
            rows.append(tmp)
        out = pd.concat(rows, ignore_index=True)
    else:
        raise ValueError("prices_df must contain either `mtu` or `hour`")

    if "hour" not in out.columns:
        out["hour"] = (out["mtu"] // 4).astype(int)
    if "quarter" not in out.columns:
        out["quarter"] = (out["mtu"] % 4).astype(int)
    return out


def _broadcast_hourly_to_mtu(df_h: pd.DataFrame) -> pd.DataFrame:
    """Replicate hourly rows to (date, mtu) with 4 quarters per hour.

    `df_h` must contain `date` and `hour`. All other columns are repeated
    across the 4 MTUs of that hour.
    """
    rows: list[pd.DataFrame] = []
    for q in range(4):
        tmp = df_h.copy()
        tmp["mtu"] = tmp["hour"] * 4 + q
        rows.append(tmp.drop(columns=["hour"]))
    return pd.concat(rows, ignore_index=True)


def _add_peak_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive features that help the model during peak/scarcity hours.

    - temp_dev_from_climatology: temperature_2m minus the (month, hour) mean
      across the dataset. Negative ⇒ colder than typical, a heating-demand
      proxy that flags evening cold snaps the model otherwise misses.
    - net_load_vs_daily_max: net_load / max(net_load) on the same date. 1.0
      at the day's peak, helps the model condition on "we're at peak now".
    - mcp_range_24h: trailing 24-hour MCP max minus min, shifted by 1 step
      to avoid leakage. A volatility-regime indicator — wide spread days
      tend to have steeper evening peaks.
    """
    out = df

    if "temperature_2m" in out.columns:
        clim = out.groupby(["month", "hour"])["temperature_2m"].transform("mean")
        out["temp_dev_from_climatology"] = out["temperature_2m"] - clim

    if "net_load_forecast_mw" in out.columns:
        daily_max = out.groupby("date")["net_load_forecast_mw"].transform("max")
        out["net_load_vs_daily_max"] = (
            out["net_load_forecast_mw"] / daily_max.replace(0, float("nan"))
        )

    if "mcp_eur_mwh" in out.columns:
        out = out.sort_values(["date", "mtu"]).reset_index(drop=True)
        shifted = out["mcp_eur_mwh"].shift(1)
        out["mcp_range_24h"] = (
            shifted.rolling(96, min_periods=48).max()
            - shifted.rolling(96, min_periods=48).min()
        )

    return out


def build(
    prices_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    ttf_df: pd.DataFrame,
    admie_df: pd.DataFrame | None = None,
    res_weather_df: pd.DataFrame | None = None,
    carbon_df: pd.DataFrame | None = None,
    cache_path: str | Path | None = "data/processed/features.parquet",
) -> pd.DataFrame:
    """Join all sources at 15-min resolution and engineer features.

    Args:
        prices_df:      henex_parser output. May be 15-min (date, mtu) or
                        hourly (date, hour) — hourly is broadcast 4× per hour.
        weather_df:     Hourly Open-Meteo baseline (date, hour, ...).
        ttf_df:         Daily TTF (date, ttf_eur_mwh).
        admie_df:       ADMIE ISP1 forecasts. May be 15-min or hourly.
        res_weather_df: Per-tech RES weather, hourly (date, hour, wind_*, ...).
        carbon_df:      Daily EUA (date, eua_eur_t).
        cache_path:     If given, write the result to parquet at this path.
    """
    prices = _ensure_mtu(prices_df)
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values(["date", "mtu"]).reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # 1. Price-lag features (15-min steps)                                 #
    # ------------------------------------------------------------------ #
    for col, steps in _LAG_STEPS.items():
        prices[col] = prices["mcp_eur_mwh"].shift(steps)

    prices["mcp_rolling_mean_96"] = (
        prices["mcp_eur_mwh"].shift(1).rolling(_ROLL_WINDOW, min_periods=48).mean()
    )
    prices["mcp_rolling_std_96"] = (
        prices["mcp_eur_mwh"].shift(1).rolling(_ROLL_WINDOW, min_periods=48).std()
    )

    # ------------------------------------------------------------------ #
    # 2. Calendar features                                                 #
    # ------------------------------------------------------------------ #
    years = sorted(prices["date"].dt.year.unique().tolist())
    gr_holidays = _greek_holidays(years)

    prices["day_of_week"] = prices["date"].dt.dayofweek
    prices["month"] = prices["date"].dt.month
    prices["hour_of_day"] = prices["hour"]
    prices["quarter_of_hour"] = prices["quarter"]
    prices["mtu_of_day"] = prices["mtu"]
    prices["is_weekend"] = (prices["day_of_week"] >= 5).astype(int)
    prices["is_holiday_gr"] = prices["date"].dt.date.isin(gr_holidays).astype(int)

    # ------------------------------------------------------------------ #
    # 3. Weather (hourly → broadcast to 4 MTUs per hour)                   #
    # ------------------------------------------------------------------ #
    weather = weather_df.copy()
    weather["date"] = pd.to_datetime(weather["date"])
    weather_mtu = _broadcast_hourly_to_mtu(weather)

    df = prices.merge(weather_mtu, on=["date", "mtu"], how="left")

    if res_weather_df is not None and not res_weather_df.empty:
        res = res_weather_df.copy()
        res["date"] = pd.to_datetime(res["date"])
        overlap = (set(res.columns) - {"date", "hour"}) & set(df.columns)
        if overlap:
            logger.warning(
                "res_weather_df shares %d cols with baseline weather; dropping from RES side: %s",
                len(overlap), sorted(overlap),
            )
            res = res.drop(columns=list(overlap))
        res_mtu = _broadcast_hourly_to_mtu(res)
        df = df.merge(res_mtu, on=["date", "mtu"], how="left")

    # ------------------------------------------------------------------ #
    # 4. TTF gas (daily → broadcast to all 96 MTUs of the day)             #
    # ------------------------------------------------------------------ #
    ttf = ttf_df.copy()
    ttf["date"] = pd.to_datetime(ttf["date"])
    ttf = ttf.sort_values("date").reset_index(drop=True)
    ttf["ttf_lag1d"] = ttf["ttf_eur_mwh"].shift(1)
    ttf["ttf_lag7d"] = ttf["ttf_eur_mwh"].shift(7)
    df = df.merge(ttf, on="date", how="left")

    # ------------------------------------------------------------------ #
    # 5. ADMIE ISP1 forecasts                                              #
    # ------------------------------------------------------------------ #
    if admie_df is not None and not admie_df.empty:
        admie = admie_df.copy()
        admie["date"] = pd.to_datetime(admie["date"])
        admie["load_forecast_mw"] = pd.to_numeric(admie["load_forecast_mw"], errors="coerce")
        admie["res_forecast_mw"] = pd.to_numeric(admie["res_forecast_mw"], errors="coerce")

        if "mtu" in admie.columns:
            admie_mtu = admie[["date", "mtu", "load_forecast_mw", "res_forecast_mw"]]
        else:
            # Hourly ADMIE — broadcast to 4 MTUs.
            tmp = admie[["date", "hour", "load_forecast_mw", "res_forecast_mw"]]
            admie_mtu = _broadcast_hourly_to_mtu(tmp)

        df = df.merge(admie_mtu, on=["date", "mtu"], how="left")
        df["net_load_forecast_mw"] = df["load_forecast_mw"] - df["res_forecast_mw"]
        df["load_res_ratio"] = (
            df["res_forecast_mw"] / df["load_forecast_mw"].replace(0, float("nan"))
        )

    # ------------------------------------------------------------------ #
    # 6. Carbon (daily → broadcast)                                        #
    # ------------------------------------------------------------------ #
    if carbon_df is not None and not carbon_df.empty:
        carbon = carbon_df.copy()
        carbon["date"] = pd.to_datetime(carbon["date"])
        carbon = carbon.sort_values("date").reset_index(drop=True)
        carbon["eua_lag1d"] = carbon["eua_eur_t"].shift(1)
        carbon["eua_lag7d"] = carbon["eua_eur_t"].shift(7)
        df = df.merge(carbon, on="date", how="left")

    # ------------------------------------------------------------------ #
    # 6b. Peak-hour helper features                                        #
    # ------------------------------------------------------------------ #
    df = _add_peak_features(df)

    # ------------------------------------------------------------------ #
    # 7. Reorder                                                           #
    # ------------------------------------------------------------------ #
    col_order = [
        "date", "mtu", "hour", "quarter",
        *_LAG_STEPS.keys(),
        "mcp_rolling_mean_96", "mcp_rolling_std_96",
        "sell_total_mwh", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh", "imports_mwh",
        "day_of_week", "month", "hour_of_day", "quarter_of_hour", "mtu_of_day",
        "is_weekend", "is_holiday_gr",
        "temperature_2m", "shortwave_radiation", "wind_speed_10m",
        "cloud_cover", "relative_humidity_2m", "precipitation",
        "ttf_eur_mwh", "ttf_lag1d", "ttf_lag7d",
        "eua_eur_t", "eua_lag1d", "eua_lag7d",
        "load_forecast_mw", "res_forecast_mw",
        "net_load_forecast_mw", "load_res_ratio",
        "temp_dev_from_climatology", "net_load_vs_daily_max", "mcp_range_24h",
        "mcp_eur_mwh",
    ]
    existing = [c for c in col_order if c in df.columns]
    extra = [c for c in df.columns if c not in col_order]
    df = df[existing + extra]

    df = df.sort_values(["date", "mtu"]).reset_index(drop=True)

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
    admie_df: pd.DataFrame | None = None,
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
        admie_df=admie_df,
        res_weather_df=res_weather_df,
        carbon_df=carbon_df,
        cache_path=cache_path,
    )
