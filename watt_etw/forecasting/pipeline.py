"""End-to-end feature pipeline: data sources → feature matrix → trained forecaster.

This wires together every data module so the forecaster sees:
    - HENEX historical prices and supply mix
    - Baseline weather (Athens by default)
    - Per-technology RES weather, capacity-weighted from RAE Geoportal assets
    - TTF gas
    - EUA carbon proxy

Each step is optional via toggles so the pipeline degrades gracefully when
an upstream source is unavailable (e.g. RAE WFS down, no internet for
yfinance, holidays package missing).

Typical use:

    from datetime import date
    from watt_etw.forecasting.pipeline import build_feature_matrix

    features = build_feature_matrix(
        henex_dir="data/raw/2025_DAM_data",
        start=date(2024, 1, 1),
        end=date(2025, 12, 31),
    )

    from watt_etw.forecasting.price_forecaster import PriceForecaster
    forecaster = PriceForecaster()
    forecaster.train(features)
    forecaster.save()
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from watt_etw.data import (
    admie_fetcher,
    carbon_fetcher,
    henex_parser,
    rae_geoportal,
    ttf_fetcher,
    weather_fetcher,
)
from watt_etw.features import feature_builder

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    henex_dir: str | Path
    start: date
    end: date
    baseline_lat: float = 37.98
    baseline_lon: float = 23.73
    include_res_weather: bool = True
    include_carbon: bool = True
    include_admie: bool = True
    res_layers: dict[str, str] | None = None
    res_assets_per_layer: int | None = 50
    eua_ticker: str | None = None
    features_cache: str | Path | None = "data/processed/features.parquet"


def fetch_renewable_assets(
    layers: dict[str, str] | None = None,
    limit_per_layer: int | None = 50,
) -> pd.DataFrame:
    """Fetch RAE Geoportal assets and return a DataFrame ready for weather matching.

    Returns an empty DataFrame if the WFS is unreachable so the pipeline can
    continue with baseline-only weather.
    """
    try:
        assets = rae_geoportal.fetch_assets(
            layers=layers,
            limit_per_layer=limit_per_layer,
        )
    except Exception as exc:
        logger.warning("RAE asset fetch failed: %s — falling back to baseline weather only", exc)
        return pd.DataFrame()
    if not assets:
        logger.info("No RAE assets returned; skipping per-tech RES weather")
        return pd.DataFrame()
    return rae_geoportal.assets_to_frame(assets)


def build_feature_matrix(
    henex_dir: str | Path,
    start: date,
    end: date,
    *,
    baseline_lat: float = 37.98,
    baseline_lon: float = 23.73,
    include_res_weather: bool = True,
    include_carbon: bool = True,
    include_admie: bool = True,
    res_layers: dict[str, str] | None = None,
    res_assets_per_layer: int | None = 50,
    eua_ticker: str | None = None,
    features_cache: str | Path | None = "data/processed/features.parquet",
) -> pd.DataFrame:
    """Run the full data → feature pipeline and return the feature matrix."""
    logger.info("Pipeline start: %s → %s, henex=%s", start, end, henex_dir)

    prices_df = henex_parser.load_or_parse(henex_dir, resolution="15min")
    logger.info("HENEX prices (15-min): %d rows", len(prices_df))

    baseline_weather = weather_fetcher.fetch(
        start_date=start,
        end_date=end,
        lat=baseline_lat,
        lon=baseline_lon,
    )

    res_weather: pd.DataFrame | None = None
    if include_res_weather:
        assets_df = fetch_renewable_assets(
            layers=res_layers,
            limit_per_layer=res_assets_per_layer,
        )
        if not assets_df.empty:
            try:
                res_weather = weather_fetcher.fetch_renewable_weather_features(
                    assets_df=assets_df,
                    start_date=start,
                    end_date=end,
                )
                logger.info(
                    "RES weather: %d assets across %d technologies",
                    len(assets_df),
                    assets_df["technology"].nunique() if "technology" in assets_df else 0,
                )
            except Exception as exc:
                logger.warning("RES weather fetch failed: %s", exc)
                res_weather = None

    ttf_df = ttf_fetcher.load(start, end)

    admie_df: pd.DataFrame | None = None
    if include_admie:
        try:
            admie_df = admie_fetcher.fetch(start_date=start, end_date=end, resolution="15min")
            if admie_df.empty:
                logger.warning("ADMIE fetch returned empty; load/RES forecast features will be missing")
                admie_df = None
        except Exception as exc:
            logger.warning("ADMIE fetch failed: %s", exc)
            admie_df = None

    carbon_df: pd.DataFrame | None = None
    if include_carbon:
        try:
            carbon_df = carbon_fetcher.fetch(
                start_date=start,
                end_date=end,
                ticker=eua_ticker,
            )
            if carbon_df.empty:
                logger.warning("Carbon fetch returned empty; EUA features will be missing")
                carbon_df = None
        except Exception as exc:
            logger.warning("Carbon fetch failed: %s", exc)
            carbon_df = None

    return feature_builder.build(
        prices_df=prices_df,
        weather_df=baseline_weather,
        ttf_df=ttf_df,
        admie_df=admie_df,
        res_weather_df=res_weather,
        carbon_df=carbon_df,
        cache_path=features_cache,
    )


def run(config: PipelineConfig) -> pd.DataFrame:
    """Convenience wrapper that builds features from a PipelineConfig."""
    return build_feature_matrix(
        henex_dir=config.henex_dir,
        start=config.start,
        end=config.end,
        baseline_lat=config.baseline_lat,
        baseline_lon=config.baseline_lon,
        include_res_weather=config.include_res_weather,
        include_carbon=config.include_carbon,
        include_admie=config.include_admie,
        res_layers=config.res_layers,
        res_assets_per_layer=config.res_assets_per_layer,
        eua_ticker=config.eua_ticker,
        features_cache=config.features_cache,
    )
