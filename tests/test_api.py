from fastapi.testclient import TestClient

from watt_etw.api.main import app


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
        "batteries": [{"name": "A", "capacity_mwh": 20, "power_mw": 10}],
        "market_rows": [
            {"timestamp": "2026-04-01T00:00:00", "price_eur_mwh": 10, "extra": {}},
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
