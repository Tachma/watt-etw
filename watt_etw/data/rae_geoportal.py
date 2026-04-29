"""Fetch and normalize renewable asset locations from the RAE Geoportal.

The RAE Geoportal is a GeoServer instance. Vector layers can be requested
through WFS as GeoJSON, then reduced to one representative coordinate per
feature so weather can be matched by location.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

import requests

try:
    import pandas as pd
except ImportError:  # pragma: no cover - pandas is used by the feature pipeline.
    pd = None


WFS_URL = "https://geo.rae.gr/geoserver/ows"

DEFAULT_RES_LAYERS = {
    "wind": "rae_status:V_SDI_R_AIOLIKA_ALL",
    "wind_turbine": "rae_status:V_SDI_R_ANEMOGENNHTRIES11",
    "hydro": "rae_status:V_HYDRO_GROUPED_ALL",
    "hybrid": "rae_status:V_SDI_ISLANDS_HYB_OTHERVALUES",
}

CAPACITY_HINTS = (
    "capacity",
    "power",
    "mw",
    "ισχυ",
    "ισχύ",
    "dynamikotita",
)


@dataclass(frozen=True)
class RenewableAsset:
    technology: str
    layer: str
    latitude: float
    longitude: float
    capacity_mw: float | None
    properties: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def fetch_assets(
    layers: dict[str, str] | None = None,
    *,
    limit_per_layer: int | None = None,
    wfs_url: str = WFS_URL,
    timeout: int = 60,
) -> list[RenewableAsset]:
    """Fetch configured RAE layers and return normalized asset coordinates."""
    selected_layers = layers or DEFAULT_RES_LAYERS
    assets: list[RenewableAsset] = []
    for technology, layer in selected_layers.items():
        geojson = fetch_layer_geojson(
            layer,
            wfs_url=wfs_url,
            limit=limit_per_layer,
            timeout=timeout,
        )
        assets.extend(parse_geojson_assets(geojson, technology=technology, layer=layer))
    return assets


def fetch_layer_geojson(
    layer: str,
    *,
    wfs_url: str = WFS_URL,
    limit: int | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    """Fetch one RAE WFS layer as GeoJSON."""
    params: dict[str, Any] = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": layer,
        "outputFormat": "application/json",
    }
    if limit is not None:
        params["count"] = limit
    response = requests.get(wfs_url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def parse_geojson_assets(
    geojson: dict[str, Any],
    *,
    technology: str,
    layer: str,
) -> list[RenewableAsset]:
    """Convert GeoJSON features to assets with representative coordinates."""
    assets: list[RenewableAsset] = []
    for feature in geojson.get("features", []):
        geometry = feature.get("geometry") or {}
        coordinate = representative_coordinate(geometry)
        if coordinate is None:
            continue
        longitude, latitude = coordinate
        properties = dict(feature.get("properties") or {})
        assets.append(
            RenewableAsset(
                technology=technology,
                layer=layer,
                latitude=latitude,
                longitude=longitude,
                capacity_mw=find_capacity_mw(properties),
                properties=properties,
            )
        )
    return assets


def representative_coordinate(geometry: dict[str, Any]) -> tuple[float, float] | None:
    """Return a simple lon/lat representative point for any GeoJSON geometry."""
    pairs = list(_coordinate_pairs(geometry.get("coordinates")))
    if not pairs:
        return None
    lon = sum(pair[0] for pair in pairs) / len(pairs)
    lat = sum(pair[1] for pair in pairs) / len(pairs)
    return lon, lat


def find_capacity_mw(properties: dict[str, Any]) -> float | None:
    """Best-effort capacity extraction from layer-specific attribute names."""
    for key, value in properties.items():
        normalized = _normalize_key(key)
        if any(hint in normalized for hint in CAPACITY_HINTS):
            number = _safe_float(value)
            if number is not None and number >= 0:
                return number
    return None


def assets_to_frame(assets: list[RenewableAsset]):
    """Return assets as a pandas DataFrame for downstream feature joins."""
    if pd is None:
        raise RuntimeError("pandas is required to convert assets to a DataFrame")
    rows = [asset.to_dict() for asset in assets]
    return pd.DataFrame(rows)


def _coordinate_pairs(value: Any):
    if not isinstance(value, list):
        return
    if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        yield float(value[0]), float(value[1])
        return
    for item in value:
        yield from _coordinate_pairs(item)


def _normalize_key(key: str) -> str:
    return str(key).strip().lower().replace(" ", "_")


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None
