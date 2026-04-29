from __future__ import annotations


def explain_action(
    action: str,
    price: float,
    low_threshold: float,
    high_threshold: float,
    soc_mwh: float,
    min_soc_mwh: float,
    max_soc_mwh: float,
) -> str:
    if action == "charge":
        return "Charge: price is in a low-price interval and the fleet has available capacity."
    if action == "discharge":
        return "Discharge: price is in a high-price interval and stored energy is available."
    if soc_mwh <= min_soc_mwh + 1e-6:
        return "Idle: fleet is near minimum state of charge."
    if soc_mwh >= max_soc_mwh - 1e-6:
        return "Idle: fleet is near maximum state of charge."
    if low_threshold < price < high_threshold:
        return "Idle: price spread is not attractive enough after losses and degradation."
    return "Idle: optimization preserved energy for a more valuable interval."
