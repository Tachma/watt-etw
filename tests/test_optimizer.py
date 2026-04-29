from watt_etw.optimizer import NUM_HOURS, NUM_INTERVALS, NUM_SCENARIOS, optimize_adjustments

# 96 flat prices at a given level
def _prices(value: float) -> list[float]:
    return [value] * NUM_INTERVALS


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

def test_result_has_correct_dimensions():
    result = optimize_adjustments(_prices(50.0), max_capacity=10.0)
    assert result.status == "optimized"
    assert result.kpis["num_intervals"] == NUM_INTERVALS
    assert len(result.schedule) == NUM_SCENARIOS * NUM_HOURS


def test_row_fields_present():
    row = optimize_adjustments(_prices(50.0), max_capacity=10.0).schedule[0]
    assert row.scenario == 1
    assert row.hour == 1
    assert hasattr(row, "price_coefficient")
    assert hasattr(row, "reference_dispatch_mw")
    assert hasattr(row, "adjusted_quantity_mw")
    assert hasattr(row, "obj_contribution")


# ---------------------------------------------------------------------------
# Capacity bounds: q ∈ [0.2·Cap, 0.8·Cap] when d = 0
# ---------------------------------------------------------------------------

def test_adjusted_quantity_within_bounds():
    result = optimize_adjustments(_prices(100.0), max_capacity=10.0)
    for row in result.schedule:
        assert 2.0 - 1e-9 <= row.adjusted_quantity_mw <= 8.0 + 1e-9


# ---------------------------------------------------------------------------
# Complementarity: d · q = 0
# ---------------------------------------------------------------------------

def test_complementarity_forces_q_zero_when_d_nonzero():
    d = [[0.0] * NUM_HOURS for _ in range(NUM_SCENARIOS)]
    d[0][0] = 5.0  # scenario 1, hour 1
    result = optimize_adjustments(_prices(80.0), max_capacity=10.0, reference_dispatch=d)
    cell = next(r for r in result.schedule if r.scenario == 1 and r.hour == 1)
    assert cell.adjusted_quantity_mw == 0.0


# ---------------------------------------------------------------------------
# Optimality: positive price → q = q_min; negative price → q = q_max
# ---------------------------------------------------------------------------

def test_positive_price_sets_q_to_lower_bound():
    result = optimize_adjustments(_prices(100.0), max_capacity=10.0)
    for row in result.schedule:
        assert abs(row.adjusted_quantity_mw - 2.0) < 1e-9


def test_negative_price_sets_q_to_upper_bound():
    result = optimize_adjustments(_prices(-50.0), max_capacity=10.0)
    for row in result.schedule:
        assert abs(row.adjusted_quantity_mw - 8.0) < 1e-9


# ---------------------------------------------------------------------------
# Price index mapping: prices[h * 4 + s] → a_{s+1, h+1}
# ---------------------------------------------------------------------------

def test_price_index_mapping():
    prices = list(range(NUM_INTERVALS))  # prices[i] = i
    result = optimize_adjustments(prices, max_capacity=10.0)
    for row in result.schedule:
        expected = (row.hour - 1) * NUM_SCENARIOS + (row.scenario - 1)
        assert abs(row.price_coefficient - expected) < 1e-9


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------

def test_wrong_number_of_prices_raises():
    import pytest
    with pytest.raises(ValueError, match="96"):
        optimize_adjustments([50.0] * 10, max_capacity=10.0)


def test_nonpositive_capacity_raises():
    import pytest
    with pytest.raises(ValueError, match="max_capacity"):
        optimize_adjustments(_prices(50.0), max_capacity=0.0)


# ---------------------------------------------------------------------------
# KPI keys
# ---------------------------------------------------------------------------

def test_kpis_contain_required_keys():
    kpis = optimize_adjustments(_prices(50.0), max_capacity=10.0).kpis
    required = {
        "total_objective_eur",
        "max_capacity_mw",
        "q_lower_bound_mw",
        "q_upper_bound_mw",
        "num_intervals",
        "scenario_1_objective_eur",
        "scenario_4_objective_eur",
    }
    assert required.issubset(kpis.keys())
