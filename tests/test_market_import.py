from watt_etw.market_import import load_market_file
from pathlib import Path

import pytest


def test_accepts_csv_with_timestamp_and_price():
    content = (
        "timestamp,price_eur_mwh,producer\n"
        "2026-04-01 00:00,42.5,Unit A\n"
        "2026-04-01 00:15,40.0,Unit B\n"
    ).encode()

    result = load_market_file("dam.csv", content)

    assert result.valid
    assert result.interval_minutes == 15
    assert result.detected_dates == ["2026-04-01"]
    assert result.rows[0].extra["producer"] == "Unit A"


def test_accepts_negative_prices():
    content = (
        "timestamp,price_eur_mwh\n"
        "2026-04-01 00:00,-0.01\n"
        "2026-04-01 00:15,40.0\n"
    ).encode()

    result = load_market_file("dam.csv", content)

    assert result.valid
    assert result.price_summary["min"] == -0.01
    assert result.rows[0].price_eur_mwh == -0.01


def test_accepts_csv_with_date_period_and_price():
    content = (
        "date,period,market clearing price eur/mwh\n"
        "2026-04-01,1,10\n"
        "2026-04-01,2,20\n"
        "2026-04-01,3,30\n"
    ).encode()

    result = load_market_file("dam.csv", content)

    assert result.valid
    assert result.interval_minutes == 15
    assert result.rows[1].timestamp.isoformat() == "2026-04-01T00:15:00"


def test_rejects_missing_price_column():
    content = "timestamp,value\n2026-04-01 00:00,10\n".encode()

    result = load_market_file("bad.csv", content)

    assert not result.valid
    assert "price" in result.errors[0].lower()


def test_detects_missing_intervals():
    content = (
        "timestamp,price_eur_mwh\n"
        "2026-04-01 00:00,10\n"
        "2026-04-01 00:15,20\n"
        "2026-04-01 00:45,30\n"
    ).encode()

    result = load_market_file("dam.csv", content)

    assert result.valid
    assert any("missing" in warning for warning in result.warnings)


def test_accepts_real_henex_results_summary_workbook_when_available():
    path = Path("/Users/tachmamac/Downloads/20260429_EL-DAM_ResultsSummary_EN_v01.xlsx")
    if not path.exists():
        pytest.skip("Real HEnEx workbook is not available on this machine")

    result = load_market_file(path.name, path.read_bytes())

    assert result.valid
    assert result.detected_dates == ["2026-04-29"]
    assert result.interval_minutes == 15
    assert result.row_count == 96
    assert result.rows[0].price_eur_mwh == 157.04
    assert result.rows[0].extra["sell_lignite"] == 799
    assert result.rows[0].extra["sell_gas"] == 2505.902
    assert result.rows[0].extra["sell_hydro"] == 325
    assert result.rows[0].extra["sell_renewables"] == 809.191
    assert result.rows[40].extra["buy_bess"] == 52


def test_accepts_hourly_henex_results_summary_from_repo_when_available():
    path = Path("data/raw/2025_DAM_data/20250429_EL-DAM_ResultsSummary_EN_v01.xlsx")
    if not path.exists():
        pytest.skip("Hourly HEnEx workbook is not available in repo data")

    result = load_market_file(path.name, path.read_bytes())

    assert result.valid
    assert result.detected_dates == ["2025-04-29"]
    assert result.interval_minutes == 60
    assert result.row_count == 24
    assert result.rows[0].timestamp.isoformat() == "2025-04-29T00:00:00"
    assert result.rows[1].timestamp.isoformat() == "2025-04-29T01:00:00"
    assert result.rows[0].price_eur_mwh == 92.32
    assert result.rows[0].extra["sell_lignite"] == 156
