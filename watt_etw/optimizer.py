from __future__ import annotations

from dataclasses import asdict, dataclass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_SCENARIOS: int = 4   # s ∈ {1, 2, 3, 4}  — quarter-hours within an hour
NUM_HOURS: int = 24      # h ∈ {1, …, 24}
NUM_INTERVALS: int = NUM_SCENARIOS * NUM_HOURS  # 96 λ values per day


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AdjustmentRow:
    """Optimal solution for one (scenario, hour) cell."""
    scenario: int                 # s ∈ {1, 2, 3, 4}  — quarter within the hour
    hour: int                     # h ∈ {1, …, 24}
    price_coefficient: float      # a_{s,h} = λ_{h,s}  (EUR/MWh)
    reference_dispatch_mw: float  # d_{s,h}  — DAM committed quantity (MW)
    adjusted_quantity_mw: float   # q_{s,h}  — optimal adjustment quantity (MW)
    obj_contribution: float       # a_{s,h} · (d_{s,h} − q_{s,h})

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class AdjustmentResult:
    status: str
    kpis: dict[str, float]
    schedule: list[AdjustmentRow]

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "kpis": self.kpis,
            "schedule": [row.to_dict() for row in self.schedule],
        }


# ---------------------------------------------------------------------------
# Quantity Adjustment Model  (pure mathematics — no domain objects)
# ---------------------------------------------------------------------------

def optimize_adjustments(
    prices: list[float],
    max_capacity: float,
    reference_dispatch: list[list[float]] | None = None,
) -> AdjustmentResult:
    """
    Quantity Adjustment Model for the Greek Adjustment / Balancing Market.

    Solves:

        max   Σ_{h=1}^{24} Σ_{s=1}^{4}  a_{s,h} · (d_{s,h} − q_{s,h})

    subject to:

        (1)  d_{s,h} · q_{s,h}  = 0            ∀ (s, h)   [complementarity]
        (2)  q_{s,h}  ≤  Max_Capacity · 0.8    ∀ (s, h)   [upper op. bound]
        (3)  q_{s,h}  ≥  Max_Capacity · 0.2    ∀ (s, h)   [lower op. bound]

    Parameters
    ----------
    prices:
        96 market prices λ_{h,s} (EUR/MWh), one per 15-minute interval,
        ordered chronologically: index = (h − 1) · 4 + (s − 1).

        Example layout:
            prices[0]  = λ_{1,1}  (hour 1, quarter 1: 00:00–00:15)
            prices[1]  = λ_{1,2}  (hour 1, quarter 2: 00:15–00:30)
            prices[2]  = λ_{1,3}  (hour 1, quarter 3: 00:30–00:45)
            prices[3]  = λ_{1,4}  (hour 1, quarter 4: 00:45–01:00)
            prices[4]  = λ_{2,1}  (hour 2, quarter 1: 01:00–01:15)
            ...
            prices[95] = λ_{24,4} (hour 24, quarter 4: 23:45–00:00)

    max_capacity:
        Max_Capacity (MW) — operational dispatch ceiling of the battery.

    reference_dispatch:
        Optional (4 × 24) matrix of DAM committed quantities in MW.
        ``reference_dispatch[s][h]`` = d_{s,h}.
        Defaults to all-zero (battery fully available for adjustment market).

    Returns
    -------
    AdjustmentResult
        Optimal q_{s,h} for every (s, h) pair together with KPIs.

    Notes
    -----
    The LP decomposes into 96 independent scalar sub-problems (separable
    box-constrained) each solved in O(1):

        · d_{s,h} ≠ 0  →  q_{s,h} = 0      (complementarity, Constraint 1)
        · a_{s,h} > 0  →  q_{s,h} = q_min  (Constraint 3 active)
        · a_{s,h} < 0  →  q_{s,h} = q_max  (Constraint 2 active)
        · a_{s,h} = 0  →  q_{s,h} = midpoint (objective insensitive)
    """
    if len(prices) != NUM_INTERVALS:
        raise ValueError(
            f"Expected {NUM_INTERVALS} prices (4 quarters × 24 hours), "
            f"got {len(prices)}"
        )
    if max_capacity <= 0:
        raise ValueError("max_capacity must be positive")

    q_min = max_capacity * 0.2
    q_max = max_capacity * 0.8
    q_mid = (q_min + q_max) / 2.0

    # Validate / default reference-dispatch matrix  d[s][h]  (0-indexed)
    if reference_dispatch is None:
        d: list[list[float]] = [[0.0] * NUM_HOURS for _ in range(NUM_SCENARIOS)]
    else:
        if (
            len(reference_dispatch) != NUM_SCENARIOS
            or any(len(row) != NUM_HOURS for row in reference_dispatch)
        ):
            raise ValueError(
                f"reference_dispatch must be a "
                f"{NUM_SCENARIOS}×{NUM_HOURS} matrix"
            )
        d = [list(reference_dispatch[s]) for s in range(NUM_SCENARIOS)]

    # --- Solve the LP analytically (96 independent scalar problems) ---------
    schedule: list[AdjustmentRow] = []
    total_objective = 0.0
    scenario_objectives = [0.0] * NUM_SCENARIOS

    for h in range(NUM_HOURS):
        for s in range(NUM_SCENARIOS):
            # Price coefficient: a_{s,h} = λ at chronological index
            a_sh: float = prices[h * NUM_SCENARIOS + s]
            d_sh: float = d[s][h]

            # Constraint (1) — complementarity
            if abs(d_sh) > 1e-9:
                q_sh = 0.0
            # Constraints (2) & (3) — optimise over box [q_min, q_max]
            elif a_sh > 0:
                q_sh = q_min   # minimise q  →  maximise −a·q  (a > 0)
            elif a_sh < 0:
                q_sh = q_max   # maximise q  →  adjustment earns revenue (a < 0)
            else:
                q_sh = q_mid   # indifferent

            contribution = a_sh * (d_sh - q_sh)
            total_objective += contribution
            scenario_objectives[s] += contribution

            schedule.append(
                AdjustmentRow(
                    scenario=s + 1,
                    hour=h + 1,
                    price_coefficient=round(a_sh, 4),
                    reference_dispatch_mw=round(d_sh, 4),
                    adjusted_quantity_mw=round(q_sh, 4),
                    obj_contribution=round(contribution, 4),
                )
            )

    kpis: dict[str, float] = {
        "total_objective_eur": round(total_objective, 2),
        "max_capacity_mw": round(max_capacity, 4),
        "q_lower_bound_mw": round(q_min, 4),
        "q_upper_bound_mw": round(q_max, 4),
        "num_intervals": NUM_INTERVALS,
        **{
            f"scenario_{s + 1}_objective_eur": round(scenario_objectives[s], 2)
            for s in range(NUM_SCENARIOS)
        },
    }

    return AdjustmentResult(status="optimized", kpis=kpis, schedule=schedule)
