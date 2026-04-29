from __future__ import annotations

from datetime import date, timedelta

import pytest

pd = pytest.importorskip("pandas")

from watt_etw.features import feature_builder


def _prices_frame(days: int = 10) -> pd.DataFrame:
    rows = []
    start = date(2025, 1, 1)
    for d in range(days):
        for h in range(24):
            rows.append({
                "date": start + timedelta(days=d),
                "hour": h,
                "mcp_eur_mwh": 80.0 + h + d,
                "sell_total_mwh": 5000 + 10 * h,
                "gas_mwh": 1500,
                "hydro_mwh": 800,
                "res_mwh": 2000,
                "lignite_mwh": 400,
                "imports_mwh": 300,
            })
    return pd.DataFrame(rows)


def _baseline_weather(days: int = 10) -> pd.DataFrame:
    rows = []
    start = date(2025, 1, 1)
    for d in range(days):
        for h in range(24):
            rows.append({
                "date": start + timedelta(days=d),
                "hour": h,
                "temperature_2m": 10.0 + h * 0.1,
                "shortwave_radiation": max(0.0, (h - 6) * 80.0),
                "wind_speed_10m": 5.0,
                "cloud_cover": 30.0,
                "relative_humidity_2m": 60.0,
                "precipitation": 0.0,
            })
    return pd.DataFrame(rows)


def _ttf_frame(days: int = 10) -> pd.DataFrame:
    start = date(2025, 1, 1)
    return pd.DataFrame({
        "date": [start + timedelta(days=d) for d in range(days)],
        "ttf_eur_mwh": [40.0 + d * 0.5 for d in range(days)],
    })


def _carbon_frame(days: int = 10) -> pd.DataFrame:
    start = date(2025, 1, 1)
    return pd.DataFrame({
        "date": [start + timedelta(days=d) for d in range(days)],
        "eua_eur_t": [70.0 + d for d in range(days)],
    })


def _res_weather_frame(days: int = 10) -> pd.DataFrame:
    rows = []
    start = date(2025, 1, 1)
    for d in range(days):
        for h in range(24):
            rows.append({
                "date": start + timedelta(days=d),
                "hour": h,
                "wind_asset_count": 4,
                "wind_capacity_mw": 120.0,
                "wind_wind_speed_120m": 9.0 + h * 0.05,
                "solar_asset_count": 3,
                "solar_capacity_mw": 90.0,
                "solar_global_tilted_irradiance": max(0.0, (h - 6) * 70.0),
            })
    return pd.DataFrame(rows)


def test_build_includes_carbon_lags(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        carbon_df=_carbon_frame(),
        cache_path=tmp_path / "features.parquet",
    )

    assert {"eua_eur_t", "eua_lag1d", "eua_lag7d"}.issubset(df.columns)
    # eua_eur_t broadcast to every hour of a given day
    day_two = df[df["date"] == pd.Timestamp("2025-01-02")]
    assert (day_two["eua_eur_t"] == 71.0).all()
    # 1-day lag on day 2 = day 1's price
    assert (day_two["eua_lag1d"] == 70.0).all()


def test_build_merges_res_weather_alongside_baseline(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        res_weather_df=_res_weather_frame(),
        cache_path=tmp_path / "features.parquet",
    )

    # Per-tech RES columns survive the merge
    assert "wind_wind_speed_120m" in df.columns
    assert "solar_global_tilted_irradiance" in df.columns
    # Baseline weather still present
    assert "temperature_2m" in df.columns


def test_build_drops_overlapping_res_columns(tmp_path):
    res = _res_weather_frame()
    res["temperature_2m"] = 99.0  # would clash with baseline weather
    df = feature_builder.build(
        prices_df=_prices_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        res_weather_df=res,
        cache_path=tmp_path / "features.parquet",
    )

    # Baseline temperature wins; the clash gets dropped from the RES side
    assert df["temperature_2m"].max() < 90.0


def test_build_without_optional_inputs_still_works(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        cache_path=tmp_path / "features.parquet",
    )
    assert "mcp_eur_mwh" in df.columns
    assert "eua_eur_t" not in df.columns
