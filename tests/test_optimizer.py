from datetime import datetime, timedelta

from watt_etw.battery_fleet import BatterySpec, aggregate_fleet
from watt_etw.market_import import MarketRow
from watt_etw.optimizer import optimize_schedule


def rows(prices):
    start = datetime(2026, 4, 1)
    return [
        MarketRow(timestamp=start + timedelta(hours=index), price_eur_mwh=price, extra={})
        for index, price in enumerate(prices)
    ]


def test_low_then_high_prices_charge_then_discharge():
    fleet = aggregate_fleet([BatterySpec("A", capacity_mwh=20, power_mw=10, degradation_cost_eur_mwh=1)])

    result = optimize_schedule(rows([10, 12, 100, 110]), fleet)
    actions = [row.action for row in result.schedule]

    assert "charge" in actions
    assert "discharge" in actions
    assert result.kpis["expected_profit"] > 0


def test_flat_prices_do_not_cycle_profitably():
    fleet = aggregate_fleet([BatterySpec("A", capacity_mwh=20, power_mw=10, degradation_cost_eur_mwh=5)])

    result = optimize_schedule(rows([50, 50, 50, 50]), fleet)

    assert {row.action for row in result.schedule} <= {"idle"}
    assert result.kpis["expected_profit"] == 0


def test_soc_bounds_and_no_simultaneous_charge_discharge():
    fleet = aggregate_fleet([BatterySpec("A", capacity_mwh=20, power_mw=10)])

    result = optimize_schedule(rows([5, 5, 200, 200]), fleet)

    for row in result.schedule:
        assert fleet.min_soc_mwh <= row.soc_mwh <= fleet.max_soc_mwh
        assert not (row.charge_mw > 0 and row.discharge_mw > 0)
