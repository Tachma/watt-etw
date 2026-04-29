from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatterySpec:
    name: str
    capacity_mwh: float
    power_mw: float
    round_trip_efficiency: float = 0.9
    initial_soc_pct: float = 50.0
    min_soc_pct: float = 10.0
    max_soc_pct: float = 95.0
    degradation_cost_eur_mwh: float = 5.0
    max_cycles_per_day: float | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "BatterySpec":
        return cls(
            name=str(data.get("name") or "Battery"),
            capacity_mwh=float(data.get("capacity_mwh", 0)),
            power_mw=float(data.get("power_mw", 0)),
            round_trip_efficiency=float(data.get("round_trip_efficiency", 0.9)),
            initial_soc_pct=float(data.get("initial_soc_pct", 50)),
            min_soc_pct=float(data.get("min_soc_pct", 10)),
            max_soc_pct=float(data.get("max_soc_pct", 95)),
            degradation_cost_eur_mwh=float(data.get("degradation_cost_eur_mwh", 5)),
            max_cycles_per_day=(
                None
                if data.get("max_cycles_per_day") in (None, "")
                else float(data.get("max_cycles_per_day"))
            ),
        )


@dataclass(frozen=True)
class AggregatedFleet:
    name: str
    capacity_mwh: float
    power_mw: float
    round_trip_efficiency: float
    initial_soc_mwh: float
    min_soc_mwh: float
    max_soc_mwh: float
    degradation_cost_eur_mwh: float
    max_cycles_per_day: float | None
    battery_count: int


def validate_battery(spec: BatterySpec) -> None:
    if spec.capacity_mwh <= 0:
        raise ValueError(f"{spec.name}: capacity_mwh must be positive")
    if spec.power_mw <= 0:
        raise ValueError(f"{spec.name}: power_mw must be positive")
    if not 0 < spec.round_trip_efficiency <= 1:
        raise ValueError(f"{spec.name}: round_trip_efficiency must be in (0, 1]")
    if not 0 <= spec.min_soc_pct <= spec.initial_soc_pct <= spec.max_soc_pct <= 100:
        raise ValueError(
            f"{spec.name}: SOC percentages must satisfy min <= initial <= max"
        )
    if spec.degradation_cost_eur_mwh < 0:
        raise ValueError(f"{spec.name}: degradation cost cannot be negative")
    if spec.max_cycles_per_day is not None and spec.max_cycles_per_day <= 0:
        raise ValueError(f"{spec.name}: max cycles must be positive when provided")


def aggregate_fleet(specs: list[BatterySpec]) -> AggregatedFleet:
    if not specs:
        raise ValueError("At least one battery is required")
    for spec in specs:
        validate_battery(spec)

    capacity = sum(spec.capacity_mwh for spec in specs)
    power = sum(spec.power_mw for spec in specs)

    def weighted(attr: str) -> float:
        return sum(getattr(spec, attr) * spec.capacity_mwh for spec in specs) / capacity

    max_cycles_values = [spec.max_cycles_per_day for spec in specs]
    max_cycles = (
        None
        if any(value is None for value in max_cycles_values)
        else min(value for value in max_cycles_values if value is not None)
    )

    return AggregatedFleet(
        name="Aggregated Fleet",
        capacity_mwh=capacity,
        power_mw=power,
        round_trip_efficiency=weighted("round_trip_efficiency"),
        initial_soc_mwh=capacity * weighted("initial_soc_pct") / 100,
        min_soc_mwh=capacity * weighted("min_soc_pct") / 100,
        max_soc_mwh=capacity * weighted("max_soc_pct") / 100,
        degradation_cost_eur_mwh=weighted("degradation_cost_eur_mwh"),
        max_cycles_per_day=max_cycles,
        battery_count=len(specs),
    )
