from __future__ import annotations

from datetime import date, timedelta

import pytest

pd = pytest.importorskip("pandas")

from watt_etw.features import feature_builder


def _prices_15min_frame(days: int = 10) -> pd.DataFrame:
    rows = []
    start = date(2025, 1, 1)
    for d in range(days):
        for mtu in range(96):
            rows.append({
                "date": start + timedelta(days=d),
                "mtu": mtu,
                "hour": mtu // 4,
                "quarter": mtu % 4,
                "mcp_eur_mwh": 80.0 + (mtu / 4) + d,
                "sell_total_mwh": 5000 + 10 * (mtu // 4),
                "gas_mwh": 1500,
                "hydro_mwh": 800,
                "res_mwh": 2000,
                "lignite_mwh": 400,
                "imports_mwh": 300,
            })
    return pd.DataFrame(rows)


def _prices_hourly_frame(days: int = 10) -> pd.DataFrame:
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


# --------------------------------------------------------------------------- #
# Schema tests                                                                  #
# --------------------------------------------------------------------------- #

def test_build_emits_15min_schema(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_15min_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        cache_path=tmp_path / "features.parquet",
    )
    assert {"date", "mtu", "hour", "quarter", "mcp_eur_mwh"}.issubset(df.columns)
    # 96 rows per day × 10 days
    assert len(df) == 96 * 10
    # mtu should range 0..95 within each day
    day_one = df[df["date"] == pd.Timestamp("2025-01-01")]
    assert day_one["mtu"].min() == 0
    assert day_one["mtu"].max() == 95


def test_build_accepts_hourly_prices_and_broadcasts(tmp_path):
    """Hourly prices_df should be expanded to 4 MTUs per hour."""
    df = feature_builder.build(
        prices_df=_prices_hourly_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        cache_path=tmp_path / "features.parquet",
    )
    assert "mtu" in df.columns
    assert len(df) == 96 * 10
    # Within hour 5 of day 1, the 4 MTUs share the same source MCP.
    day_one_hour_five = df[(df["date"] == pd.Timestamp("2025-01-01"))
                            & (df["hour"] == 5)]
    assert len(day_one_hour_five) == 4
    assert day_one_hour_five["mcp_eur_mwh"].nunique() == 1


def test_build_includes_carbon_lags(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_15min_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        carbon_df=_carbon_frame(),
        cache_path=tmp_path / "features.parquet",
    )

    assert {"eua_eur_t", "eua_lag1d", "eua_lag7d"}.issubset(df.columns)
    day_two = df[df["date"] == pd.Timestamp("2025-01-02")]
    assert (day_two["eua_eur_t"] == 71.0).all()
    assert (day_two["eua_lag1d"] == 70.0).all()


def test_build_merges_res_weather_alongside_baseline(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_15min_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        res_weather_df=_res_weather_frame(),
        cache_path=tmp_path / "features.parquet",
    )

    assert "wind_wind_speed_120m" in df.columns
    assert "solar_global_tilted_irradiance" in df.columns
    assert "temperature_2m" in df.columns


def test_build_drops_overlapping_res_columns(tmp_path):
    res = _res_weather_frame()
    res["temperature_2m"] = 99.0
    df = feature_builder.build(
        prices_df=_prices_15min_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        res_weather_df=res,
        cache_path=tmp_path / "features.parquet",
    )
    assert df["temperature_2m"].max() < 90.0


def test_build_lags_use_15min_steps(tmp_path):
    """lag1 should equal the previous 15-min MCP, lag4 the previous hour, etc."""
    prices = _prices_15min_frame(days=8)
    df = feature_builder.build(
        prices_df=prices,
        weather_df=_baseline_weather(days=8),
        ttf_df=_ttf_frame(days=8),
        cache_path=tmp_path / "features.parquet",
    )

    # Pick a row well past the longest lag (672 steps = 7 days).
    target = df[(df["date"] == pd.Timestamp("2025-01-08")) & (df["mtu"] == 5)].iloc[0]
    same_day = df[df["date"] == pd.Timestamp("2025-01-08")].set_index("mtu")
    assert target["mcp_lag1"] == pytest.approx(same_day.loc[4, "mcp_eur_mwh"])
    assert target["mcp_lag4"] == pytest.approx(same_day.loc[1, "mcp_eur_mwh"])
    # lag96 = same MTU one day ago
    yesterday = df[df["date"] == pd.Timestamp("2025-01-07")].set_index("mtu")
    assert target["mcp_lag96"] == pytest.approx(yesterday.loc[5, "mcp_eur_mwh"])
    # lag672 = same MTU one week ago
    week_ago = df[df["date"] == pd.Timestamp("2025-01-01")].set_index("mtu")
    assert target["mcp_lag672"] == pytest.approx(week_ago.loc[5, "mcp_eur_mwh"])


def test_build_without_optional_inputs_still_works(tmp_path):
    df = feature_builder.build(
        prices_df=_prices_15min_frame(),
        weather_df=_baseline_weather(),
        ttf_df=_ttf_frame(),
        cache_path=tmp_path / "features.parquet",
    )
    assert "mcp_eur_mwh" in df.columns
    assert "eua_eur_t" not in df.columns
    assert "load_forecast_mw" not in df.columns
