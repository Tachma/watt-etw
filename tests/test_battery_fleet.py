import pytest

from watt_etw.battery_fleet import BatterySpec, aggregate_fleet


def test_aggregate_multiple_batteries():
    fleet = aggregate_fleet(
        [
            BatterySpec("A", capacity_mwh=50, power_mw=25, initial_soc_pct=50),
            BatterySpec("B", capacity_mwh=25, power_mw=10, initial_soc_pct=60),
        ]
    )

    assert fleet.capacity_mwh == 75
    assert fleet.power_mw == 35
    assert round(fleet.initial_soc_mwh, 4) == 40
    assert fleet.battery_count == 2


def test_accepts_user_facing_battery_fields():
    spec = BatterySpec.from_dict(
        {
            "name": "A",
            "max_capacity": 20,
            "min_capacity": 4,
            "effieciency": 0.92,
            "availability": 95,
            "ramp": 8,
        }
    )

    fleet = aggregate_fleet([spec])

    assert fleet.capacity_mwh == 20
    assert fleet.min_soc_mwh == pytest.approx(4)
    assert fleet.round_trip_efficiency == pytest.approx(0.92)
    assert fleet.availability_pct == 95
    assert fleet.ramp_mw == pytest.approx(7.6)
    assert fleet.power_mw == pytest.approx(7.6)


def test_invalid_battery_rejected():
    with pytest.raises(ValueError, match="capacity"):
        aggregate_fleet([BatterySpec("Bad", capacity_mwh=0, power_mw=10)])
