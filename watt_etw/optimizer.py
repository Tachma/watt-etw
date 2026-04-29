from __future__ import annotations

from dataclasses import asdict, dataclass
from math import sqrt
from statistics import mean

from watt_etw.battery_fleet import AggregatedFleet
from watt_etw.explanations import explain_action
from watt_etw.market_import import MarketRow


@dataclass(frozen=True)
class ScheduleRow:
    timestamp: str
    price_eur_mwh: float
    action: str
    charge_mw: float
    discharge_mw: float
    soc_mwh: float
    interval_hours: float
    explanation: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class OptimizationResult:
    status: str
    kpis: dict[str, float]
    schedule: list[ScheduleRow]

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "kpis": self.kpis,
            "schedule": [row.to_dict() for row in self.schedule],
        }


def optimize_schedule(rows: list[MarketRow], fleet: AggregatedFleet) -> OptimizationResult:
    if len(rows) < 2:
        raise ValueError("At least two market intervals are required")
    interval_hours = _interval_hours(rows)
    if interval_hours <= 0:
        raise ValueError("Could not determine a positive market interval")

    prices = [row.price_eur_mwh for row in rows]
    low_threshold, high_threshold = _price_thresholds(prices, fleet)
    charge_eff = sqrt(fleet.round_trip_efficiency)
    discharge_eff = sqrt(fleet.round_trip_efficiency)

    soc = fleet.initial_soc_mwh
    schedule: list[ScheduleRow] = []
    charge_cost = 0.0
    discharge_revenue = 0.0
    charged_mwh = 0.0
    discharged_mwh = 0.0
    throughput_mwh = 0.0

    max_throughput = (
        None
        if fleet.max_cycles_per_day is None
        else fleet.max_cycles_per_day * 2 * fleet.capacity_mwh
    )

    for index, row in enumerate(rows):
        future_prices = prices[index + 1 :]
        future_max = max(future_prices) if future_prices else row.price_eur_mwh
        future_min = min(future_prices) if future_prices else row.price_eur_mwh

        action = "idle"
        charge_mw = 0.0
        discharge_mw = 0.0
        current_throughput_room = None if max_throughput is None else max(0.0, max_throughput - throughput_mwh)

        profitable_charge = (
            row.price_eur_mwh <= low_threshold
            and future_max * discharge_eff * charge_eff
            > row.price_eur_mwh + fleet.degradation_cost_eur_mwh
        )
        profitable_discharge = row.price_eur_mwh >= high_threshold or (
            row.price_eur_mwh > future_min + fleet.degradation_cost_eur_mwh
            and soc > fleet.initial_soc_mwh
        )

        if profitable_discharge and soc > fleet.min_soc_mwh + 1e-9:
            available_mwh = (soc - fleet.min_soc_mwh) * discharge_eff
            discharge_energy = min(fleet.power_mw * interval_hours, available_mwh)
            if current_throughput_room is not None:
                discharge_energy = min(discharge_energy, current_throughput_room)
            if discharge_energy > 1e-9:
                action = "discharge"
                discharge_mw = discharge_energy / interval_hours
                soc -= discharge_energy / discharge_eff
                discharge_revenue += discharge_energy * row.price_eur_mwh
                discharged_mwh += discharge_energy
                throughput_mwh += discharge_energy
        elif profitable_charge and soc < fleet.max_soc_mwh - 1e-9:
            capacity_room_input = (fleet.max_soc_mwh - soc) / charge_eff
            charge_energy = min(fleet.power_mw * interval_hours, capacity_room_input)
            if current_throughput_room is not None:
                charge_energy = min(charge_energy, current_throughput_room)
            if charge_energy > 1e-9:
                action = "charge"
                charge_mw = charge_energy / interval_hours
                soc += charge_energy * charge_eff
                charge_cost += charge_energy * row.price_eur_mwh
                charged_mwh += charge_energy
                throughput_mwh += charge_energy

        explanation = explain_action(
            action,
            row.price_eur_mwh,
            low_threshold,
            high_threshold,
            soc,
            fleet.min_soc_mwh,
            fleet.max_soc_mwh,
        )
        schedule.append(
            ScheduleRow(
                timestamp=row.timestamp.isoformat(),
                price_eur_mwh=round(row.price_eur_mwh, 4),
                action=action,
                charge_mw=round(charge_mw, 4),
                discharge_mw=round(discharge_mw, 4),
                soc_mwh=round(soc, 4),
                interval_hours=interval_hours,
                explanation=explanation,
            )
        )

    # Preserve the default final SOC by buying back energy if the heuristic finishes short.
    if soc < fleet.initial_soc_mwh - 1e-6:
        schedule, charge_cost, charged_mwh, throughput_mwh, soc = _restore_final_soc(
            schedule,
            rows,
            fleet,
            soc,
            charge_eff,
            charge_cost,
            charged_mwh,
            throughput_mwh,
        )

    degradation_cost = throughput_mwh * fleet.degradation_cost_eur_mwh
    net_profit = discharge_revenue - charge_cost - degradation_cost
    kpis = {
        "expected_profit": round(net_profit, 2),
        "discharge_revenue": round(discharge_revenue, 2),
        "charging_cost": round(charge_cost, 2),
        "degradation_cost": round(degradation_cost, 2),
        "charged_mwh": round(charged_mwh, 4),
        "discharged_mwh": round(discharged_mwh, 4),
        "average_buy_price": round(charge_cost / charged_mwh, 2) if charged_mwh else 0.0,
        "average_sell_price": round(discharge_revenue / discharged_mwh, 2) if discharged_mwh else 0.0,
        "equivalent_cycles": round(throughput_mwh / (2 * fleet.capacity_mwh), 4),
        "final_soc_mwh": round(soc, 4),
        "fleet_capacity_mwh": round(fleet.capacity_mwh, 4),
        "fleet_power_mw": round(fleet.power_mw, 4),
    }
    return OptimizationResult(status="optimized", kpis=kpis, schedule=schedule)


def _price_thresholds(prices: list[float], fleet: AggregatedFleet) -> tuple[float, float]:
    ordered = sorted(prices)
    low_index = max(0, int(len(ordered) * 0.3) - 1)
    high_index = min(len(ordered) - 1, int(len(ordered) * 0.7))
    low = ordered[low_index]
    high = ordered[high_index]
    if high - low < fleet.degradation_cost_eur_mwh / max(fleet.round_trip_efficiency, 1e-9):
        avg = mean(prices)
        return avg - 1e-6, avg + fleet.degradation_cost_eur_mwh + 1e-6
    return low, high


def _interval_hours(rows: list[MarketRow]) -> float:
    deltas = [
        (rows[index + 1].timestamp - rows[index].timestamp).total_seconds() / 3600
        for index in range(len(rows) - 1)
        if rows[index + 1].timestamp > rows[index].timestamp
    ]
    return min(deltas) if deltas else 1.0


def _restore_final_soc(
    schedule: list[ScheduleRow],
    rows: list[MarketRow],
    fleet: AggregatedFleet,
    soc: float,
    charge_eff: float,
    charge_cost: float,
    charged_mwh: float,
    throughput_mwh: float,
) -> tuple[list[ScheduleRow], float, float, float, float]:
    needed_input = (fleet.initial_soc_mwh - soc) / charge_eff
    if needed_input <= 0:
        return schedule, charge_cost, charged_mwh, throughput_mwh, soc
    cheapest_index = min(range(len(rows)), key=lambda index: rows[index].price_eur_mwh)
    row = rows[cheapest_index]
    charge_cost += needed_input * row.price_eur_mwh
    charged_mwh += needed_input
    throughput_mwh += needed_input
    soc = fleet.initial_soc_mwh
    original = schedule[cheapest_index]
    schedule[cheapest_index] = ScheduleRow(
        timestamp=original.timestamp,
        price_eur_mwh=original.price_eur_mwh,
        action="charge" if original.action == "idle" else original.action,
        charge_mw=round(original.charge_mw + needed_input / original.interval_hours, 4),
        discharge_mw=original.discharge_mw,
        soc_mwh=original.soc_mwh,
        interval_hours=original.interval_hours,
        explanation="Charge: restores the required final state of charge.",
    )
    return schedule, charge_cost, charged_mwh, throughput_mwh, soc
