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


def test_invalid_battery_rejected():
    with pytest.raises(ValueError, match="capacity_mwh"):
        aggregate_fleet([BatterySpec("Bad", capacity_mwh=0, power_mw=10)])
