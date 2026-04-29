"""Train the day-ahead MCP price forecaster.

Usage:
    python train_forecaster.py              # train on all available data
    python train_forecaster.py --force      # rebuild parquet caches first
    python train_forecaster.py --eval-only  # load saved models and print metrics

The script reads:
  data/2024_DAM_data/  + data/2025_DAM_data/   HENEX XLSX files
  data/external/ttf_gas/                        Dutch TTF CSV
  (weather is fetched from Open-Meteo and cached per day)

Outputs:
  data/processed/prices.parquet
  data/processed/features.parquet
  models/price_forecaster/hour_00.pkl … hour_23.pkl + meta.json
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
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
PRICES_CACHE = "data/processed/prices.parquet"
FEATURES_CACHE = "data/processed/features.parquet"
MODEL_DIR = "models/price_forecaster"


def run(force: bool = False, eval_only: bool = False) -> None:
    from watt_etw.data.henex_parser import load_or_parse
    from watt_etw.data.ttf_fetcher import load as load_ttf
    from watt_etw.data.weather_fetcher import fetch as fetch_weather
    from watt_etw.data.admie_fetcher import fetch as fetch_admie
    from watt_etw.features.feature_builder import load_or_build
    from watt_etw.forecasting.price_forecaster import PriceForecaster

    # ------------------------------------------------------------------ #
    # 1. Load features                                                     #
    # ------------------------------------------------------------------ #
    if eval_only and Path(FEATURES_CACHE).exists():
        logger.info("Loading features from cache (eval-only mode)")
        features = pd.read_parquet(FEATURES_CACHE)
    else:
        logger.info("Step 1/5 — Parsing HENEX files")
        prices = load_or_parse(
            *HENEX_DIRS,
            cache_path=PRICES_CACHE,
            force=force,
        )

        # Infer date range from parsed data
        start = prices["date"].min().date()
        end = prices["date"].max().date()
        logger.info("Price data: %s → %s (%d days)", start, end, prices["date"].nunique())

        logger.info("Step 2/5 — Loading TTF gas prices")
        ttf = load_ttf(start, end)

        logger.info("Step 3/5 — Fetching weather data (cached per day)")
        weather = fetch_weather(start, end)

        logger.info("Step 4/5 — Fetching ADMIE ISP1 load & RES forecasts")
        admie = fetch_admie(start, end)

        logger.info("Step 5/5 — Building feature matrix")
        features = load_or_build(
            prices, weather, ttf,
            admie_df=admie,
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
        logger.info("Loading saved models from %s", MODEL_DIR)
        forecaster.load()
        eval_df = forecaster.evaluate(features)
        _print_metrics(eval_df)
    else:
        logger.info("Training 24 per-hour LightGBM models …")
        metrics = forecaster.train(features)
        forecaster.save()
        _print_metrics_from_train(metrics)


def _print_metrics_from_train(metrics: dict) -> None:
    import pandas as pd
    rows = [{"hour": h, **m.to_dict()} for h, m in metrics.items()]
    df = pd.DataFrame(rows).set_index("hour")
    print("\n-- Per-hour test metrics (last 30 days) --")
    print(df[["mae", "rmse", "mape"]].round(2).to_string())
    valid_mae = df["mae"].dropna()
    valid_mape = df["mape"].dropna()
    valid_mape = valid_mape[valid_mape < 1000]  # ignore near-zero price artefacts
    print(f"\nOverall MAE : {valid_mae.mean():.2f} EUR/MWh")
    print(f"Overall MAPE: {valid_mape.mean():.1f}%  (prices >=5 EUR/MWh only)")
    print(f"Models saved to: {MODEL_DIR}/")


def _print_metrics(eval_df: pd.DataFrame) -> None:
    summary = (
        eval_df.groupby("hour")
        .agg(mae=("abs_error", "mean"), rmse=("error", lambda x: (x**2).mean()**0.5))
        .round(2)
    )
    print("\n-- Per-hour evaluation metrics --")
    print(summary.to_string())
    print(f"\nOverall MAE: {eval_df['abs_error'].mean():.2f} EUR/MWh")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train the MCP price forecaster")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild parquet caches even if they exist")
    parser.add_argument("--eval-only", action="store_true",
                        help="Skip training; load saved models and print metrics")
    args = parser.parse_args()
    run(force=args.force, eval_only=args.eval_only)
