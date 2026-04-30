from types import SimpleNamespace

from fastapi.testclient import TestClient

from watt_etw.api import main as api_main


app = api_main.app
client = TestClient(app)


def test_validate_endpoint():
    response = client.post(
        "/api/market-data/validate",
        files={"file": ("dam.csv", b"timestamp,price_eur_mwh\n2026-04-01 00:00,10\n2026-04-01 01:00,100\n")},
    )

    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_optimize_endpoint():
    payload = {
        "batteries": [
            {
                "name": "A",
                "capacity": 20,
                "min_capacity": 2,
                "efficiency": 0.9,
                "availability": 100,
                "ramp": 10,
            }
        ],
        "market_rows": [
            {"timestamp": "2026-04-01T00:00:00", "price_eur_mwh": -0.01, "extra": {}},
            {"timestamp": "2026-04-01T01:00:00", "price_eur_mwh": 100, "extra": {}},
        ],
    }

    response = client.post("/api/optimize", json=payload)

    assert response.status_code == 200
    assert "kpis" in response.json()
    assert "schedule" in response.json()


def test_export_endpoint():
    response = client.post(
        "/api/export-schedule",
        json={"schedule": [{"timestamp": "t", "price_eur_mwh": 1, "action": "idle"}]},
    )

    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]


def test_optimize_arbitrage_date_uses_96_forecast_prices(monkeypatch):
    captured = {}

    def fake_forecast(_target_date):
        return [float(index) for index in range(api_main.NUM_INTERVALS)]

    def fake_optimize_fleet(fleet, prices):
        captured["prices"] = prices
        return SimpleNamespace(
            status="Optimal",
            revenue_eur=0.0,
            kpis={
                "total_discharged_mwh": 0.0,
                "final_soc_mwh": fleet.initial_soc_mwh,
            },
            schedule=[],
        )

    monkeypatch.setattr(api_main, "_forecast_for_date", fake_forecast)
    monkeypatch.setattr(api_main, "optimize_fleet", fake_optimize_fleet)

    response = client.post(
        "/api/optimize-arbitrage",
        json={
            "batteries": [
                {
                    "name": "A",
                    "capacity": 20,
                    "min_capacity": 2,
                    "efficiency": 0.9,
                    "availability": 100,
                    "ramp": 10,
                }
            ],
            "date": "2026-04-29",
        },
    )

    assert response.status_code == 200
    assert captured["prices"] == [float(index) for index in range(api_main.NUM_INTERVALS)]


def test_optimize_arbitrage_includes_actual_prices_for_date(monkeypatch):
    forecast_prices = [100.0 + index for index in range(api_main.NUM_INTERVALS)]
    actual_prices = [90.0 + index for index in range(api_main.NUM_INTERVALS)]

    def fake_optimize_fleet(fleet, prices):
        return SimpleNamespace(
            status="Optimal",
            revenue_eur=0.0,
            kpis={
                "total_discharged_mwh": 0.0,
                "final_soc_mwh": fleet.initial_soc_mwh,
            },
            schedule=[
                SimpleNamespace(
                    hour=1,
                    quarter=1,
                    lambda_eur_mwh=prices[0],
                    charge_mwh=0.0,
                    discharge_mwh=0.0,
                    soc_mwh=fleet.initial_soc_mwh,
                    is_discharging=0,
                )
            ],
        )

    monkeypatch.setattr(api_main, "_forecast_for_date", lambda _target_date: forecast_prices)
    monkeypatch.setattr(api_main, "_actual_prices_for_date", lambda _target_date: actual_prices)
    monkeypatch.setattr(api_main, "optimize_fleet", fake_optimize_fleet)

    response = client.post(
        "/api/optimize-arbitrage",
        json={
            "batteries": [
                {
                    "name": "A",
                    "capacity": 20,
                    "min_capacity": 2,
                    "efficiency": 0.9,
                    "availability": 100,
                    "ramp": 10,
                }
            ],
            "date": "2026-04-29",
        },
    )

    assert response.status_code == 200
    row = response.json()["schedule"][0]
    assert row["price_eur_mwh"] == forecast_prices[0]
    assert row["actual_price_eur_mwh"] == actual_prices[0]


def test_forecast_endpoint_derives_hourly_from_96_quarterly(monkeypatch):
    monkeypatch.setattr(
        api_main,
        "_forecast_for_date",
        lambda _target_date: [float(index) for index in range(api_main.NUM_INTERVALS)],
    )

    response = client.get("/api/forecast/2026-04-29")

    assert response.status_code == 200
    data = response.json()
    assert len(data["quarterly_eur_mwh"]) == api_main.NUM_INTERVALS
    assert len(data["hourly_eur_mwh"]) == 24
    assert data["hourly_eur_mwh"][0] == 1.5
    assert data["hourly_eur_mwh"][-1] == 93.5
