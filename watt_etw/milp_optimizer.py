"""Battery Arbitrage MILP — Python/PuLP implementation.

Faithful translation of model_mock/battery_qa.mod.
Maximises day-ahead arbitrage revenue over a 24-hour, 15-min resolution horizon.

Mathematical model (unchanged from AMPL):
    max   Σ_{h=1}^{24} Σ_{s=1}^{4}  λ_{h,s} · (d_{h,s} − c_{h,s})

    s.t.
        E[1,1]   = E0 + √η · c[1,1] − d[1,1] / √η
        E[h,s]   = E[h,s-1] + √η · c[h,s] − d[h,s] / √η   ∀ s > 1
        E[h,1]   = E[h-1,4] + √η · c[h,1] − d[h,1] / √η   ∀ h > 1
        E_min  ≤  E[h,s]  ≤  E_max                          ∀ h,s
        c[h,s] ≤  Q_max · (1 − z[h,s])                     ∀ h,s
        d[h,s] ≤  Q_max · z[h,s]                            ∀ h,s
        |c[h,s] − c[h,s']| / dt ≤ ramp                     ∀ adjacent (h,s),(h,s')
        |d[h,s] − d[h,s']| / dt ≤ ramp                     ∀ adjacent (h,s),(h,s')

    where:
        Q_max    = availability · E_max · dt
        √η       = sqrt(eta)
        dt       = 0.25 h
        z[h,s]   ∈ {0, 1}   (1 = discharging)
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass

import pulp

# ---------------------------------------------------------------------------
# Constants  (mirror battery_qa.mod)
# ---------------------------------------------------------------------------
NUM_HOURS: int = 24
NUM_QUARTERS: int = 4
NUM_INTERVALS: int = NUM_HOURS * NUM_QUARTERS   # 96
DT: float = 0.25                                 # quarter-hour time step [h]

_HOURS = range(1, NUM_HOURS + 1)        # 1 … 24
_QUARTERS = range(1, NUM_QUARTERS + 1)  # 1 … 4


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScheduleRow:
    """Optimal solution for one (hour, quarter) cell — mirrors AMPL output."""
    hour: int                  # h ∈ 1..24
    quarter: int               # s ∈ 1..4
    lambda_eur_mwh: float      # λ_{h,s}
    charge_mwh: float          # c[h,s]
    discharge_mwh: float       # d[h,s]
    soc_mwh: float             # E[h,s]
    is_discharging: int        # z[h,s]  (1 = DIS, 0 = CHG)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ArbitrageResult:
    status: str
    revenue_eur: float
    kpis: dict
    schedule: list[ScheduleRow]

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "revenue_eur": self.revenue_eur,
            "kpis": self.kpis,
            "schedule": [row.to_dict() for row in self.schedule],
        }


# ---------------------------------------------------------------------------
# Core MILP  (direct translation of battery_qa.mod)
# ---------------------------------------------------------------------------

def optimize_battery(
    prices: list[float],
    E_max: float,
    E_min: float = 0.0,
    eta: float = 0.90,
    availability: float = 1.0,
    ramp: float | None = None,
    E0: float | None = None,
    solver_msg: bool = False,
) -> ArbitrageResult:
    """Solve the battery arbitrage MILP.

    Parameters mirror battery_qa.mod / battery_qa.dat exactly.

    Parameters
    ----------
    prices        96 prices λ[h,s] in EUR/MWh.
                  Ordered chronologically: index = (h-1)*4 + (s-1).
                  h ∈ 1..24, s ∈ 1..4.
    E_max         Maximum capacity [MWh]             (param E_max)
    E_min         Minimum SoC [MWh]                  (param E_min)
    eta           Round-trip efficiency ∈ (0, 1]     (param eta)
    availability  Availability factor ∈ (0, 1]       (param availability)
    ramp          Ramp rate [MW]; None = unconstrained (param ramp)
    E0            Initial SoC [MWh]; None → E_max/2  (param E0)
    solver_msg    Show solver log
    """
    if len(prices) != NUM_INTERVALS:
        raise ValueError(f"Expected {NUM_INTERVALS} prices, got {len(prices)}")
    if E_max <= 0:
        raise ValueError("E_max must be positive")

    # ------------------------------------------------------------------
    # Derived parameters  (mirror battery_qa.mod)
    # ------------------------------------------------------------------
    sqrt_eta: float = math.sqrt(eta)
    Q_max: float = availability * E_max * DT   # per-quarter throughput [MWh]
    if E0 is None:
        E0 = E_max / 2.0
    if ramp is None:
        ramp = Q_max / DT                      # effectively unconstrained

    # λ[h,s] — 1-indexed like AMPL
    lam: dict[tuple[int, int], float] = {
        (h, s): prices[(h - 1) * NUM_QUARTERS + (s - 1)]
        for h in _HOURS for s in _QUARTERS
    }

    # ------------------------------------------------------------------
    # Build LP/MIP problem
    # ------------------------------------------------------------------
    prob = pulp.LpProblem("BatteryArbitrage", pulp.LpMaximize)

    # ---------- VARIABLES ----------
    d = {(h, s): pulp.LpVariable(f"d_{h}_{s}", lowBound=0)
         for h in _HOURS for s in _QUARTERS}
    c = {(h, s): pulp.LpVariable(f"c_{h}_{s}", lowBound=0)
         for h in _HOURS for s in _QUARTERS}
    E = {(h, s): pulp.LpVariable(f"E_{h}_{s}", lowBound=0)
         for h in _HOURS for s in _QUARTERS}
    z = {(h, s): pulp.LpVariable(f"z_{h}_{s}", cat="Binary")
         for h in _HOURS for s in _QUARTERS}

    # ---------- OBJECTIVE: maximize Revenue ----------
    prob += pulp.lpSum(
        lam[h, s] * (d[h, s] - c[h, s])
        for h in _HOURS for s in _QUARTERS
    )

    # ---------- SoC_first ----------
    prob += E[1, 1] == E0 + sqrt_eta * c[1, 1] - d[1, 1] / sqrt_eta

    # ---------- SoC_within_hour  {h ∈ HOURS, s ∈ 2..4} ----------
    for h in _HOURS:
        for s in range(2, NUM_QUARTERS + 1):
            prob += (
                E[h, s] == E[h, s - 1] + sqrt_eta * c[h, s] - d[h, s] / sqrt_eta
            )

    # ---------- SoC_across_hour  {h ∈ 2..24} ----------
    for h in range(2, NUM_HOURS + 1):
        prob += (
            E[h, 1] == E[h - 1, 4] + sqrt_eta * c[h, 1] - d[h, 1] / sqrt_eta
        )

    # ---------- Cap_min / Cap_max ----------
    for h in _HOURS:
        for s in _QUARTERS:
            prob += E[h, s] >= E_min
            prob += E[h, s] <= E_max

    # ---------- Charge_limit  c[h,s] ≤ Q_max·(1−z) ----------
    for h in _HOURS:
        for s in _QUARTERS:
            prob += c[h, s] <= Q_max * (1 - z[h, s])

    # ---------- Discharge_limit  d[h,s] ≤ Q_max·z ----------
    for h in _HOURS:
        for s in _QUARTERS:
            prob += d[h, s] <= Q_max * z[h, s]

    # ---------- Ramp constraints — charge ----------
    # Within hour
    for h in _HOURS:
        for s in range(2, NUM_QUARTERS + 1):
            prob += (c[h, s] - c[h, s - 1]) / DT <= ramp
            prob += (c[h, s] - c[h, s - 1]) / DT >= -ramp
    # Across hours
    for h in range(2, NUM_HOURS + 1):
        prob += (c[h, 1] - c[h - 1, 4]) / DT <= ramp
        prob += (c[h, 1] - c[h - 1, 4]) / DT >= -ramp

    # ---------- Ramp constraints — discharge ----------
    # Within hour
    for h in _HOURS:
        for s in range(2, NUM_QUARTERS + 1):
            prob += (d[h, s] - d[h, s - 1]) / DT <= ramp
            prob += (d[h, s] - d[h, s - 1]) / DT >= -ramp
    # Across hours
    for h in range(2, NUM_HOURS + 1):
        prob += (d[h, 1] - d[h - 1, 4]) / DT <= ramp
        prob += (d[h, 1] - d[h - 1, 4]) / DT >= -ramp

    # ------------------------------------------------------------------
    # Solve
    # ------------------------------------------------------------------
    prob.solve(_pick_solver(solver_msg))

    # ------------------------------------------------------------------
    # Extract results
    # ------------------------------------------------------------------
    solve_status = pulp.LpStatus[prob.status]
    revenue = pulp.value(prob.objective) or 0.0

    total_charged = 0.0
    total_discharged = 0.0
    schedule: list[ScheduleRow] = []

    for h in _HOURS:
        for s in _QUARTERS:
            ch = max(pulp.value(c[h, s]) or 0.0, 0.0)
            dh = max(pulp.value(d[h, s]) or 0.0, 0.0)
            eh = max(pulp.value(E[h, s]) or 0.0, 0.0)
            zh = int(round(pulp.value(z[h, s]) or 0.0))
            total_charged += ch
            total_discharged += dh
            schedule.append(ScheduleRow(
                hour=h,
                quarter=s,
                lambda_eur_mwh=round(lam[h, s], 4),
                charge_mwh=round(ch, 6),
                discharge_mwh=round(dh, 6),
                soc_mwh=round(eh, 6),
                is_discharging=zh,
            ))

    kpis: dict = {
        "revenue_eur": round(revenue, 2),
        "total_charged_mwh": round(total_charged, 3),
        "total_discharged_mwh": round(total_discharged, 3),
        "round_trip_loss_mwh": round(total_charged - total_discharged, 3),
        "E_max_mwh": E_max,
        "E_min_mwh": E_min,
        "eta": eta,
        "sqrt_eta": round(sqrt_eta, 6),
        "availability": availability,
        "Q_max_mwh_per_quarter": round(Q_max, 4),
        "ramp_mw": round(ramp, 4),
        "E0_mwh": round(E0, 4),
        "final_soc_mwh": round(max(pulp.value(E[24, 4]) or 0.0, 0.0), 4),
    }

    return ArbitrageResult(
        status=solve_status,
        revenue_eur=round(revenue, 2),
        kpis=kpis,
        schedule=schedule,
    )


# ---------------------------------------------------------------------------
# BatterySpec / AggregatedFleet → MILP parameters bridge
# ---------------------------------------------------------------------------

def optimize_fleet(
    fleet,                   # AggregatedFleet instance
    prices: list[float],     # 96 quarter-hour prices in EUR/MWh
    solver_msg: bool = False,
) -> ArbitrageResult:
    """Convenience wrapper: converts AggregatedFleet to MILP parameters."""
    return optimize_battery(
        prices=prices,
        E_max=fleet.max_soc_mwh,
        E_min=fleet.min_soc_mwh,
        eta=fleet.round_trip_efficiency,
        availability=fleet.availability_pct / 100.0,
        ramp=fleet.ramp_mw,
        E0=fleet.initial_soc_mwh,
        solver_msg=solver_msg,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pick_solver(msg: bool) -> pulp.LpSolver:
    """HiGHS (Python API) if available, otherwise CBC."""
    try:
        solver = pulp.HiGHS(msg=msg)
        if solver.available():
            return solver
    except Exception:
        pass
    return pulp.PULP_CBC_CMD(msg=msg)


def hourly_to_quarterly(hourly_prices: list[float]) -> list[float]:
    """Repeat each of 24 hourly prices 4 times → 96 quarter-hour prices."""
    if len(hourly_prices) != 24:
        raise ValueError(f"Expected 24 hourly prices, got {len(hourly_prices)}")
    return [p for p in hourly_prices for _ in range(NUM_QUARTERS)]
