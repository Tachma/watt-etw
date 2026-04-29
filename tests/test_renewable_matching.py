from datetime import date

import pytest

pd = pytest.importorskip("pandas")

from watt_etw.data.rae_geoportal import parse_geojson_assets
from watt_etw.data.weather_fetcher import aggregate_by_technology


def test_parse_rae_geojson_assets_extracts_location_and_capacity():
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [23.73, 37.98]},
                "properties": {"POWER_MW": "12,5", "name": "Wind A"},
            }
        ],
    }

    assets = parse_geojson_assets(
        geojson,
        technology="wind",
        layer="rae_status:V_SDI_R_AIOLIKA_ALL",
    )

    assert len(assets) == 1
    assert assets[0].latitude == 37.98
    assert assets[0].longitude == 23.73
    assert assets[0].capacity_mw == 12.5


def test_aggregate_asset_weather_by_technology_uses_capacity_weights():
    weather = pd.DataFrame(
        [
            {
                "date": date(2026, 4, 1),
                "hour": 0,
                "asset_id": 0,
                "technology": "wind",
                "capacity_mw": 10,
                "temperature_2m": 20,
                "shortwave_radiation": 700,
                "direct_normal_irradiance": 800,
                "diffuse_radiation": 100,
                "global_tilted_irradiance": 760,
                "wind_speed_10m": 5,
                "wind_speed_80m": 7,
                "wind_speed_120m": 8,
                "wind_direction_80m": 20,
                "wind_direction_120m": 30,
                "wind_gusts_10m": 12,
                "cloud_cover": 20,
                "relative_humidity_2m": 50,
                "surface_pressure": 1000,
                "precipitation": 0,
            },
            {
                "date": date(2026, 4, 1),
                "hour": 0,
                "asset_id": 1,
                "technology": "wind",
                "capacity_mw": 30,
                "temperature_2m": 24,
                "shortwave_radiation": 900,
                "direct_normal_irradiance": 1000,
                "diffuse_radiation": 120,
                "global_tilted_irradiance": 960,
                "wind_speed_10m": 9,
                "wind_speed_80m": 11,
                "wind_speed_120m": 12,
                "wind_direction_80m": 40,
                "wind_direction_120m": 50,
                "wind_gusts_10m": 18,
                "cloud_cover": 40,
                "relative_humidity_2m": 60,
                "surface_pressure": 990,
                "precipitation": 0,
            },
        ]
    )

    result = aggregate_by_technology(weather)

    assert len(result) == 1
    assert result.loc[0, "wind_asset_count"] == 2
    assert result.loc[0, "wind_capacity_mw"] == 40
    assert result.loc[0, "wind_wind_speed_10m"] == pytest.approx(8)
    assert result.loc[0, "wind_wind_speed_120m"] == pytest.approx(11)
    assert result.loc[0, "wind_global_tilted_irradiance"] == pytest.approx(910)
