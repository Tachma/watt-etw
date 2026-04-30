"""Predict DAM prices for a single day using the trained model.

Usage:
    python predict_day.py 2026-04-29
    python predict_day.py 2026-04-29 --no-rae

The script fetches fresh features for the target date (and the 7 prior days
needed for lag features), runs the saved model, then prints predicted vs
actual MCP per 15-min interval.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

MODEL_DIR        = "models/price_forecaster"
HENEX_2026_DIR   = "data/2026_DAM_data"
RAE_ASSETS_CACHE = Path("data/external/rae/assets.parquet")

# We need 7 days of history before the target for lag features (mcp_lag672 = 1 week)
LAG_DAYS = 7


def run(target_date: date, use_rae: bool = True) -> None:
    from watt_etw.data.henex_parser import parse_dirs
    from watt_etw.data.ttf_fetcher import load as load_ttf
    from watt_etw.data.weather_fetcher import fetch as fetch_weather, fetch_renewable_weather_features
    from watt_etw.data.admie_fetcher import fetch as fetch_admie
    from watt_etw.data.carbon_fetcher import fetch as fetch_carbon
    from watt_etw.features.feature_builder import build
    from watt_etw.forecasting.price_forecaster import PriceForecaster

    # We need prices from lag_start to target_date (for lag features to be valid)
    lag_start = target_date - timedelta(days=LAG_DAYS)
    logger.info("Target date : %s", target_date)
    logger.info("Fetching features for %s → %s (7-day lag window)", lag_start, target_date)

    # ------------------------------------------------------------------
    # 1. Parse 2026 HEnEx prices (target day + lag window)
    # ------------------------------------------------------------------
    logger.info("Parsing 2026 HEnEx prices …")
    prices_all = parse_dirs(
        HENEX_2026_DIR,
        resolution="15min",
    )
    prices_all["date"] = pd.to_datetime(prices_all["date"])
    prices = prices_all[
        (prices_all["date"].dt.date >= lag_start) &
        (prices_all["date"].dt.date <= target_date)
    ].copy()

    if prices[prices["date"].dt.date == target_date].empty:
        logger.error("No price data found for %s in %s", target_date, HENEX_2026_DIR)
        sys.exit(1)

    logger.info("Loaded %d price rows (%s → %s)", len(prices),
                prices["date"].min().date(), prices["date"].max().date())

    # ------------------------------------------------------------------
    # 2. Fetch supporting features for the same window
    # ------------------------------------------------------------------
    logger.info("Fetching TTF gas prices …")
    ttf = load_ttf(lag_start, target_date)

    logger.info("Fetching Athens baseline weather …")
    weather = fetch_weather(lag_start, target_date)

    logger.info("Fetching ADMIE ISP1 load & RES forecasts …")
    admie = fetch_admie(lag_start, target_date, resolution="15min")

    logger.info("Fetching EUA carbon prices …")
    try:
        carbon = fetch_carbon(lag_start, target_date)
    except Exception as exc:
        logger.warning("Carbon fetch failed (%s) — skipping EUA features", exc)
        carbon = pd.DataFrame()

    RAE_TOP_PER_TECH = 10  # match train_forecaster.py cap

    res_weather = pd.DataFrame()
    if use_rae and RAE_ASSETS_CACHE.exists():
        logger.info("Loading RAE assets from cache …")
        assets = pd.read_parquet(RAE_ASSETS_CACHE)
        if not assets.empty:
            # Cap to top-N per technology (same as training) to avoid
            # fetching hundreds of Open-Meteo locations on every run.
            assets["capacity_mw"] = pd.to_numeric(assets["capacity_mw"], errors="coerce")
            assets = (
                assets
                .sort_values("capacity_mw", ascending=False, na_position="last")
                .groupby("technology", group_keys=False)
                .head(RAE_TOP_PER_TECH)
            )
            logger.info("Using top-%d RAE assets per technology (%d total)",
                        RAE_TOP_PER_TECH, len(assets))
            try:
                res_weather = fetch_renewable_weather_features(assets, lag_start, target_date)
            except Exception as exc:
                logger.warning("RES weather fetch failed (%s) — skipping per-tech features", exc)
    elif use_rae:
        logger.warning("RAE asset cache not found at %s — skipping per-tech weather", RAE_ASSETS_CACHE)

    # ------------------------------------------------------------------
    # 3. Build feature matrix (no caching — this is test data)
    # ------------------------------------------------------------------
    logger.info("Building feature matrix …")
    features = build(
        prices_df=prices,
        weather_df=weather,
        ttf_df=ttf,
        admie_df=admie if not admie.empty else None,
        res_weather_df=res_weather if not res_weather.empty else None,
        carbon_df=carbon if not carbon.empty else None,
        cache_path=None,   # never cache test features
    )

    # ------------------------------------------------------------------
    # 4. Load model and predict
    # ------------------------------------------------------------------
    logger.info("Loading model from %s …", MODEL_DIR)
    forecaster = PriceForecaster(model_dir=MODEL_DIR)
    forecaster.load()

    logger.info("Predicting for %s …", target_date)
    result = forecaster.predict(features, target_date)

    # ------------------------------------------------------------------
    # 5. Compare predicted vs actual
    # ------------------------------------------------------------------
    actuals = (
        features[features["date"].dt.date == target_date]
        .sort_values("mtu")[["mtu", "mcp_eur_mwh"]]
        .set_index("mtu")["mcp_eur_mwh"]
        .to_dict()
    )

    rows = []
    for mtu in range(96):
        hour    = mtu // 4
        quarter = mtu % 4
        pred    = result.predictions.get(mtu, float("nan"))
        actual  = actuals.get(mtu, float("nan"))
        error   = pred - actual
        rows.append({
            "mtu":        mtu,
            "time":       f"{hour:02d}:{quarter*15:02d}",
            "actual":     round(actual, 2),
            "predicted":  round(pred, 2),
            "error":      round(error, 2),
            "abs_error":  round(abs(error), 2),
        })

    df_out = pd.DataFrame(rows)

    print(f"\n{'='*60}")
    print(f"  Predictions for {target_date}  (EUR/MWh)")
    print(f"{'='*60}")
    print(df_out.to_string(index=False))

    mae  = df_out["abs_error"].mean()
    rmse = (df_out["error"] ** 2).mean() ** 0.5
    print(f"\n  MAE  : {mae:.2f} EUR/MWh")
    print(f"  RMSE : {rmse:.2f} EUR/MWh")
    print(f"  N    : {len(df_out)} intervals")

    # Top 10 most important features for this prediction
    if result.feature_importance:
        print(f"\n  Top 10 feature importances:")
        for feat, imp in list(result.feature_importance.items())[:10]:
            print(f"    {feat:<40s} {imp:.0f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Predict DAM prices for one day")
    parser.add_argument("date", help="Target date in YYYY-MM-DD format (must be in 2026_DAM_data)")
    parser.add_argument("--no-rae", action="store_true", help="Skip per-tech RES weather")
    args = parser.parse_args()

    try:
        target = date.fromisoformat(args.date)
    except ValueError:
        print(f"Invalid date: {args.date}. Use YYYY-MM-DD format.")
        sys.exit(1)

    run(target, use_rae=not args.no_rae)
