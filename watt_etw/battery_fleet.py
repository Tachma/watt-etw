from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class BatterySpec:
    name: str
    capacity_mwh: float
    power_mw: float
    round_trip_efficiency: float = 0.9
    availability_pct: float = 100.0
    ramp_mw: float | None = None
    initial_soc_pct: float = 50.0
    min_soc_pct: float = 10.0
    max_soc_pct: float = 95.0
    degradation_cost_eur_mwh: float = 5.0
    max_cycles_per_day: float | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "BatterySpec":
        capacity = _float_from(
            data, ("capacity_mwh", "capacity", "max_capacity_mwh", "max_capacity"), 0
        )
        min_capacity = _optional_float_from(data, ("min_capacity_mwh", "min_capacity"))
        ramp = _optional_float_from(data, ("ramp_mw", "ramp"))
        power = _float_from(data, ("power_mw",), ramp or 0)
        return cls(
            name=str(data.get("name") or "Battery"),
            capacity_mwh=capacity,
            power_mw=power,
            round_trip_efficiency=_float_from(
                data, ("round_trip_efficiency", "efficiency", "effieciency"), 0.9
            ),
            availability_pct=_normalize_percentage(
                _float_from(data, ("availability_pct", "availability"), 100)
            ),
            ramp_mw=ramp if ramp is not None else power,
            initial_soc_pct=float(data.get("initial_soc_pct", 50)),
            min_soc_pct=(
                min_capacity / capacity * 100
                if min_capacity is not None and capacity > 0
                else float(data.get("min_soc_pct", 10))
            ),
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
    availability_pct: float
    ramp_mw: float
    initial_soc_mwh: float
    min_soc_mwh: float
    max_soc_mwh: float
    degradation_cost_eur_mwh: float
    max_cycles_per_day: float | None
    battery_count: int


def validate_battery(spec: BatterySpec) -> None:
    if spec.capacity_mwh <= 0:
        raise ValueError(f"{spec.name}: capacity must be positive")
    if spec.power_mw <= 0:
        raise ValueError(f"{spec.name}: power_mw must be positive")
    if not 0 < spec.round_trip_efficiency <= 1:
        raise ValueError(f"{spec.name}: round_trip_efficiency must be in (0, 1]")
    if not 0 < spec.availability_pct <= 100:
        raise ValueError(f"{spec.name}: availability_pct must be in (0, 100]")
    if spec.ramp_mw is not None and spec.ramp_mw <= 0:
        raise ValueError(f"{spec.name}: ramp_mw must be positive when provided")
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
    power = sum(spec.power_mw * spec.availability_pct / 100 for spec in specs)
    ramp = sum((spec.ramp_mw or spec.power_mw) * spec.availability_pct / 100 for spec in specs)

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
        power_mw=min(power, ramp),
        round_trip_efficiency=weighted("round_trip_efficiency"),
        availability_pct=weighted("availability_pct"),
        ramp_mw=ramp,
        initial_soc_mwh=capacity * weighted("initial_soc_pct") / 100,
        min_soc_mwh=capacity * weighted("min_soc_pct") / 100,
        max_soc_mwh=capacity * weighted("max_soc_pct") / 100,
        degradation_cost_eur_mwh=weighted("degradation_cost_eur_mwh"),
        max_cycles_per_day=max_cycles,
        battery_count=len(specs),
    )


def _float_from(data: dict, keys: tuple[str, ...], default: float) -> float:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return _finite_float(value)
    return float(default)


def _optional_float_from(data: dict, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return _finite_float(value)
    return None


def _finite_float(value: object) -> float:
    number = float(value)
    if not isfinite(number):
        raise ValueError("Battery numeric fields must be finite numbers")
    return number


def _normalize_percentage(value: float) -> float:
    return value * 100 if 0 < value <= 1 else value
