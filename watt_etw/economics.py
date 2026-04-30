"""Battery investment economics: CAPEX, NPV, IRR, simple payback.

Inputs come from the optimizer (daily revenue, daily throughput) plus the
investor's CAPEX/OPEX/finance assumptions. Outputs are the year-by-year cash
flow and the headline metrics used to decide whether the project is worth it.

All defaults reflect mid-range Greek 2026 utility-scale Li-ion BESS values and
should be overridable by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EconomicsInputs:
    energy_capacity_mwh: float
    power_capacity_mw: float
    daily_revenue_eur: float
    daily_throughput_mwh: float

    realization_ratio: float = 0.75
    availability: float = 0.97
    operating_days_per_year: int = 365
    annual_degradation: float = 0.02

    capex_per_mwh_energy: float = 300_000.0
    capex_per_mw_power: float = 0.0
    grid_connection_eur: float = 0.0
    grant_eur: float = 0.0

    opex_fixed_pct: float = 0.02
    opex_var_eur_per_mwh: float = 2.0
    augmentation_pct: float = 0.012

    wacc: float = 0.08
    lifetime_years: int = 12
    salvage_pct: float = 0.07
    tax_rate: float = 0.24
    depreciation_years: int = 10


@dataclass(frozen=True)
class YearCashFlow:
    year: int
    revenue: float
    opex: float
    depreciation: float
    taxable_income: float
    tax: float
    net_cash_flow: float
    discounted: float
    cumulative_undiscounted: float


@dataclass(frozen=True)
class EconomicsResult:
    inputs: EconomicsInputs
    capex: float
    energy_capex: float
    power_capex: float
    annual_revenue_year1: float
    annual_opex_year1: float
    fixed_opex: float
    augmentation: float
    var_opex_year1: float
    annual_throughput: float
    annual_depreciation: float
    salvage_value: float
    discounted_cf_sum: float
    payback_years: float | None
    npv: float
    irr: float | None
    verdict: str
    cash_flows: list[YearCashFlow] = field(default_factory=list)

    def to_dict(self) -> dict:
        inp = self.inputs
        return {
            "capex": round(self.capex, 2),
            "energy_capex": round(self.energy_capex, 2),
            "power_capex": round(self.power_capex, 2),
            "annual_revenue_year1": round(self.annual_revenue_year1, 2),
            "annual_opex_year1": round(self.annual_opex_year1, 2),
            "payback_years": (
                round(self.payback_years, 2) if self.payback_years is not None else None
            ),
            "npv": round(self.npv, 2),
            "irr": round(self.irr, 4) if self.irr is not None else None,
            "verdict": self.verdict,
            "cash_flows": [
                {
                    "year": cf.year,
                    "revenue": round(cf.revenue, 2),
                    "opex": round(cf.opex, 2),
                    "depreciation": round(cf.depreciation, 2),
                    "taxable_income": round(cf.taxable_income, 2),
                    "tax": round(cf.tax, 2),
                    "net_cash_flow": round(cf.net_cash_flow, 2),
                    "discounted": round(cf.discounted, 2),
                    "cumulative_undiscounted": round(cf.cumulative_undiscounted, 2),
                }
                for cf in self.cash_flows
            ],
            "inputs": {
                "energy_capacity_mwh": inp.energy_capacity_mwh,
                "power_capacity_mw": inp.power_capacity_mw,
                "daily_revenue_eur": inp.daily_revenue_eur,
                "daily_throughput_mwh": inp.daily_throughput_mwh,
                "realization_ratio": inp.realization_ratio,
                "availability": inp.availability,
                "operating_days_per_year": inp.operating_days_per_year,
                "annual_degradation": inp.annual_degradation,
                "capex_per_mwh_energy": inp.capex_per_mwh_energy,
                "capex_per_mw_power": inp.capex_per_mw_power,
                "grid_connection_eur": inp.grid_connection_eur,
                "grant_eur": inp.grant_eur,
                "opex_fixed_pct": inp.opex_fixed_pct,
                "opex_var_eur_per_mwh": inp.opex_var_eur_per_mwh,
                "augmentation_pct": inp.augmentation_pct,
                "wacc": inp.wacc,
                "lifetime_years": inp.lifetime_years,
                "salvage_pct": inp.salvage_pct,
                "tax_rate": inp.tax_rate,
                "depreciation_years": inp.depreciation_years,
            },
            "breakdown": {
                "energy_capex": round(self.energy_capex, 2),
                "power_capex": round(self.power_capex, 2),
                "grid_connection": round(inp.grid_connection_eur, 2),
                "grant": round(inp.grant_eur, 2),
                "fixed_opex": round(self.fixed_opex, 2),
                "augmentation": round(self.augmentation, 2),
                "var_opex_year1": round(self.var_opex_year1, 2),
                "annual_throughput_mwh": round(self.annual_throughput, 2),
                "annual_depreciation": round(self.annual_depreciation, 2),
                "salvage_value": round(self.salvage_value, 2),
                "discounted_cf_sum": round(self.discounted_cf_sum, 2),
            },
        }


def compute_economics(inp: EconomicsInputs) -> EconomicsResult:
    energy_capex = inp.energy_capacity_mwh * inp.capex_per_mwh_energy
    power_capex = inp.power_capacity_mw * inp.capex_per_mw_power
    capex = energy_capex + power_capex + inp.grid_connection_eur - inp.grant_eur
    if capex <= 0:
        raise ValueError("CAPEX after grant must be positive")

    base_revenue = (
        inp.daily_revenue_eur
        * inp.operating_days_per_year
        * inp.realization_ratio
        * inp.availability
    )
    annual_throughput = (
        inp.daily_throughput_mwh
        * inp.operating_days_per_year
        * inp.availability
    )

    fixed_opex = capex * inp.opex_fixed_pct
    augmentation = energy_capex * inp.augmentation_pct
    var_opex_year1 = annual_throughput * inp.opex_var_eur_per_mwh
    annual_opex_year1 = fixed_opex + augmentation + var_opex_year1

    annual_depreciation = capex / inp.depreciation_years if inp.depreciation_years > 0 else 0.0

    cash_flows: list[YearCashFlow] = []
    cumulative = 0.0
    payback_years: float | None = None

    for y in range(1, inp.lifetime_years + 1):
        capacity_factor = (1.0 - inp.annual_degradation) ** (y - 1)
        revenue = base_revenue * capacity_factor
        var_opex = var_opex_year1 * capacity_factor
        opex = fixed_opex + augmentation + var_opex

        depreciation = annual_depreciation if y <= inp.depreciation_years else 0.0
        taxable = revenue - opex - depreciation
        tax = inp.tax_rate * taxable if taxable > 0 else 0.0

        net_cf = revenue - opex - tax
        if y == inp.lifetime_years:
            net_cf += capex * inp.salvage_pct

        prev_cum = cumulative
        cumulative += net_cf

        if payback_years is None and prev_cum < capex <= cumulative:
            payback_years = (y - 1) + (capex - prev_cum) / net_cf

        discounted = net_cf / ((1.0 + inp.wacc) ** y)
        cash_flows.append(
            YearCashFlow(
                year=y,
                revenue=revenue,
                opex=opex,
                depreciation=depreciation,
                taxable_income=taxable,
                tax=tax,
                net_cash_flow=net_cf,
                discounted=discounted,
                cumulative_undiscounted=cumulative,
            )
        )

    discounted_cf_sum = sum(cf.discounted for cf in cash_flows)
    npv = -capex + discounted_cf_sum
    irr = _solve_irr(capex, [cf.net_cash_flow for cf in cash_flows])
    verdict = _verdict(payback_years, npv, irr, inp.wacc)
    salvage_value = capex * inp.salvage_pct

    return EconomicsResult(
        inputs=inp,
        capex=capex,
        energy_capex=energy_capex,
        power_capex=power_capex,
        annual_revenue_year1=base_revenue,
        annual_opex_year1=annual_opex_year1,
        fixed_opex=fixed_opex,
        augmentation=augmentation,
        var_opex_year1=var_opex_year1,
        annual_throughput=annual_throughput,
        annual_depreciation=annual_depreciation,
        salvage_value=salvage_value,
        discounted_cf_sum=discounted_cf_sum,
        payback_years=payback_years,
        npv=npv,
        irr=irr,
        verdict=verdict,
        cash_flows=cash_flows,
    )


def _npv_at(rate: float, capex: float, flows: list[float]) -> float:
    return -capex + sum(cf / ((1.0 + rate) ** (i + 1)) for i, cf in enumerate(flows))


def _solve_irr(capex: float, flows: list[float]) -> float | None:
    """Bisection IRR. Returns None if no sign change in [-0.99, 10.0]."""
    lo, hi = -0.99, 10.0
    f_lo = _npv_at(lo, capex, flows)
    f_hi = _npv_at(hi, capex, flows)
    if f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        f_mid = _npv_at(mid, capex, flows)
        if abs(f_mid) < 1e-3:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return 0.5 * (lo + hi)


def _verdict(
    payback_years: float | None,
    npv: float,
    irr: float | None,
    wacc: float,
) -> str:
    if npv <= 0:
        return "burning_money"
    if irr is not None and irr < wacc:
        return "burning_money"
    if payback_years is None:
        return "marginal"
    if payback_years < 8:
        return "worth_it"
    if payback_years <= 12:
        return "marginal"
    return "burning_money"
