from __future__ import annotations

import pytest

from watt_etw.economics import EconomicsInputs, compute_economics


def _base_inputs(**overrides) -> EconomicsInputs:
    defaults = dict(
        energy_capacity_mwh=50.0,
        power_capacity_mw=25.0,
        daily_revenue_eur=11_000.0,
        daily_throughput_mwh=130.0,
    )
    defaults.update(overrides)
    return EconomicsInputs(**defaults)


def test_capex_includes_grid_minus_grant():
    inp = _base_inputs(grid_connection_eur=500_000, grant_eur=2_000_000)
    result = compute_economics(inp)
    expected = 50 * 300_000 + 0 + 500_000 - 2_000_000
    assert result.capex == pytest.approx(expected)


def test_year1_revenue_applies_realization_and_availability():
    inp = _base_inputs(realization_ratio=0.7, availability=0.95)
    result = compute_economics(inp)
    assert result.annual_revenue_year1 == pytest.approx(11_000 * 365 * 0.7 * 0.95)


def test_revenue_degrades_each_year():
    inp = _base_inputs(annual_degradation=0.02)
    result = compute_economics(inp)
    y1, y2, y3 = result.cash_flows[:3]
    assert y2.revenue == pytest.approx(y1.revenue * 0.98)
    assert y3.revenue == pytest.approx(y1.revenue * 0.98 ** 2)


def test_npv_matches_discounted_sum_minus_capex():
    inp = _base_inputs()
    result = compute_economics(inp)
    expected_npv = -result.capex + sum(cf.discounted for cf in result.cash_flows)
    assert result.npv == pytest.approx(expected_npv)


def test_profitable_project_yields_positive_npv_and_payback():
    inp = _base_inputs(daily_revenue_eur=15_000, capex_per_mwh_energy=250_000)
    result = compute_economics(inp)
    assert result.npv > 0
    assert result.payback_years is not None
    assert result.irr is not None and result.irr > inp.wacc
    assert result.verdict == "worth_it"


def test_unprofitable_project_returns_burning_money():
    inp = _base_inputs(daily_revenue_eur=500, capex_per_mwh_energy=400_000)
    result = compute_economics(inp)
    assert result.npv < 0
    assert result.verdict == "burning_money"


def test_irr_falls_back_to_none_when_all_flows_negative():
    inp = _base_inputs(daily_revenue_eur=0, lifetime_years=2, salvage_pct=0.0)
    result = compute_economics(inp)
    assert all(cf.net_cash_flow < 0 for cf in result.cash_flows)
    assert result.irr is None


def test_capex_must_be_positive_after_grant():
    with pytest.raises(ValueError):
        compute_economics(_base_inputs(grant_eur=100_000_000))
