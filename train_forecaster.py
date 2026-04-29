"""Train the day-ahead 15-minute MCP price forecaster.

Usage:
    python train_forecaster.py              # train on all available data
    python train_forecaster.py --force      # rebuild parquet caches first
    python train_forecaster.py --eval-only  # load saved model and print metrics
    python train_forecaster.py --no-rae     # skip RAE-matched RES weather
                                              (faster; uses Athens weather only)

The script reads:
  data/2024_DAM_data/ + data/2025_DAM_data/   HENEX XLSX files (15-min normalised)
  data/external/ttf_gas/                       Dutch TTF CSV
  RAE Geoportal (live)                         renewable asset coordinates
  Open-Meteo                                   weather, cached per (lat, lon, day)
  ADMIE File Download API                      ISP1 load + RES forecasts (15-min)
  Yahoo Finance                                EUA carbon proxy (daily)

Outputs:
  data/processed/prices_15min.parquet
  data/processed/features.parquet
  data/external/rae/assets.parquet
  models/price_forecaster/model.pkl + meta.json
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

# Data directories — adjust if your layout differs
HENEX_DIRS = ["data/2024_DAM_data", "data/2025_DAM_data"]
PRICES_CACHE = "data/processed/prices.parquet"   # parser appends "_15min"
FEATURES_CACHE = "data/processed/features.parquet"
MODEL_DIR = "models/price_forecaster"
RAE_ASSETS_CACHE = Path("data/external/rae/assets.parquet")

# Cap how many RAE assets per technology actually drive weather fetches.
# More assets ⇒ more Open-Meteo calls (one per unique lat/lon × day range).
# Top-by-capacity gives a representative weighted aggregate without thrashing
# the API. Set to None to use them all.
RAE_TOP_PER_TECH = 25


# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def _load_or_fetch_rae_assets(force: bool) -> pd.DataFrame:
    """Cache the RAE asset list to parquet. Returns empty DF if WFS fails."""
    from watt_etw.data import rae_geoportal

    if RAE_ASSETS_CACHE.exists() and not force:
        logger.info("Loading RAE assets from cache: %s", RAE_ASSETS_CACHE)
        return pd.read_parquet(RAE_ASSETS_CACHE)

    try:
        logger.info("Fetching RAE renewable asset coordinates from WFS …")
        assets = rae_geoportal.fetch_assets()
        df = rae_geoportal.assets_to_frame(assets)
    except Exception as exc:
        logger.warning("RAE fetch failed (%s) — proceeding without per-tech weather", exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("RAE returned no assets — proceeding without per-tech weather")
        return df

    # Drop rows with missing coordinates; coerce capacity_mw to numeric.
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")

    RAE_ASSETS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    # `properties` is dict-typed and not parquet-friendly — drop for cache.
    df_to_cache = df.drop(columns=["properties"], errors="ignore")
    df_to_cache.to_parquet(RAE_ASSETS_CACHE, index=False)
    logger.info("Cached %d RAE assets to %s", len(df_to_cache), RAE_ASSETS_CACHE)
    return df_to_cache


def _top_by_capacity(assets_df: pd.DataFrame, top_n: int | None) -> pd.DataFrame:
    """Per technology, keep the top-N assets by capacity (NaN capacity goes last)."""
    if top_n is None or assets_df.empty:
        return assets_df
    out = (
        assets_df.sort_values("capacity_mw", ascending=False, na_position="last")
        .groupby("technology", group_keys=False)
        .head(top_n)
    )
    logger.info(
        "RAE asset cap: %d → %d (top %d per technology)",
        len(assets_df), len(out), top_n,
    )
    return out


# --------------------------------------------------------------------------- #
# Main                                                                          #
# --------------------------------------------------------------------------- #

def run(force: bool = False, eval_only: bool = False, use_rae: bool = True) -> None:
    from watt_etw.data.henex_parser import load_or_parse
    from watt_etw.data.ttf_fetcher import load as load_ttf
    from watt_etw.data.weather_fetcher import (
        fetch as fetch_weather,
        fetch_renewable_weather_features,
    )
    from watt_etw.data.admie_fetcher import fetch as fetch_admie
    from watt_etw.data.carbon_fetcher import fetch as fetch_carbon
    from watt_etw.features.feature_builder import load_or_build
    from watt_etw.forecasting.price_forecaster import PriceForecaster

    # ------------------------------------------------------------------ #
    # 1. Load or rebuild features                                          #
    # ------------------------------------------------------------------ #
    if eval_only and Path(FEATURES_CACHE).exists():
        logger.info("Loading features from cache (eval-only mode)")
        features = pd.read_parquet(FEATURES_CACHE)
    else:
        logger.info("Step 1/7 — Parsing HENEX files (15-min)")
        prices = load_or_parse(
            *HENEX_DIRS,
            cache_path=PRICES_CACHE,
            force=force,
            resolution="15min",
        )

        start = prices["date"].min().date()
        end = prices["date"].max().date()
        logger.info(
            "Price data: %s → %s (%d days, %d rows)",
            start, end, prices["date"].nunique(), len(prices),
        )

        logger.info("Step 2/7 — Loading TTF gas prices")
        ttf = load_ttf(start, end)

        logger.info("Step 3/7 — Fetching Athens baseline weather")
        weather = fetch_weather(start, end)

        logger.info("Step 4/7 — Fetching ADMIE ISP1 load & RES forecasts (15-min)")
        admie = fetch_admie(start, end, resolution="15min")

        logger.info("Step 5/7 — Fetching EUA carbon proxy")
        try:
            carbon = fetch_carbon(start, end)
        except Exception as exc:
            logger.warning("Carbon fetch failed (%s) — proceeding without EUA", exc)
            carbon = pd.DataFrame()

        if use_rae:
            logger.info("Step 6/7 — Fetching RAE assets + per-tech RES weather")
            assets = _load_or_fetch_rae_assets(force=force)
            assets = _top_by_capacity(assets, RAE_TOP_PER_TECH)
            if assets.empty:
                res_weather = pd.DataFrame()
            else:
                try:
                    res_weather = fetch_renewable_weather_features(assets, start, end)
                except Exception as exc:
                    logger.warning(
                        "RES weather fetch failed (%s) — proceeding without per-tech features",
                        exc,
                    )
                    res_weather = pd.DataFrame()
        else:
            logger.info("Step 6/7 — Skipping RAE per-tech weather (--no-rae)")
            res_weather = pd.DataFrame()

        logger.info("Step 7/7 — Building feature matrix")
        features = load_or_build(
            prices, weather, ttf,
            admie_df=admie,
            res_weather_df=res_weather if not res_weather.empty else None,
            carbon_df=carbon if not carbon.empty else None,
            cache_path=FEATURES_CACHE,
            force=force,
        )

    logger.info(
        "Feature matrix: %d rows × %d cols",
        len(features), len(features.columns),
    )

    # ------------------------------------------------------------------ #
    # 2. Train or evaluate                                                 #
    # ------------------------------------------------------------------ #
    forecaster = PriceForecaster(model_dir=MODEL_DIR, test_days=30)

    if eval_only:
        logger.info("Loading saved model from %s", MODEL_DIR)
        forecaster.load()
        eval_df = forecaster.evaluate(features)
        _print_eval(eval_df)
    else:
        logger.info("Training global LightGBM forecaster …")
        metrics = forecaster.train(features)
        forecaster.save()
        _print_train_metrics(metrics, MODEL_DIR)


def _print_train_metrics(metrics, model_dir: str) -> None:
    print("\n-- Holdout test metrics (last 30 days) --")
    print(f"MAE  : {metrics.mae:.2f} EUR/MWh")
    print(f"RMSE : {metrics.rmse:.2f} EUR/MWh")
    print(f"MAPE : {metrics.mape:.1f}%  (prices ≥ 5 EUR/MWh)")
    print(f"N    : {metrics.n_samples} rows")
    print(f"\nModel saved to: {model_dir}/")


def _print_eval(eval_df: pd.DataFrame) -> None:
    if eval_df.empty:
        print("No rows in evaluation window.")
        return
    mae = eval_df["abs_error"].mean()
    rmse = (eval_df["error"] ** 2).mean() ** 0.5
    by_hour = (
        eval_df.assign(hour=eval_df["mtu"] // 4)
        .groupby("hour")
        .agg(mae=("abs_error", "mean"),
             rmse=("error", lambda x: (x ** 2).mean() ** 0.5))
        .round(2)
    )
    print("\n-- Per-hour MAE / RMSE on the holdout window --")
    print(by_hour.to_string())
    print(f"\nOverall MAE : {mae:.2f} EUR/MWh")
    print(f"Overall RMSE: {rmse:.2f} EUR/MWh")
    print(f"N rows      : {len(eval_df)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train the 15-min MCP price forecaster")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild parquet caches even if they exist")
    parser.add_argument("--eval-only", action="store_true",
                        help="Skip training; load saved model and print metrics")
    parser.add_argument("--no-rae", action="store_true",
                        help="Skip RAE per-tech RES weather (faster, less detail)")
    args = parser.parse_args()
    run(force=args.force, eval_only=args.eval_only, use_rae=not args.no_rae)
