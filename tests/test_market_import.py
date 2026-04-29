from watt_etw.market_import import load_market_file


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
