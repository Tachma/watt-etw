"""Fetch day-ahead load and RES forecasts from IPTO/ADMIE.

Source: ADMIE File Download API (public, no authentication required).
  https://www.admie.gr/en/market/market-statistics/file-download-api

We use ISP1 (published D-1 at 13:30) day-ahead forecasts — the most
relevant forecast horizon for battery scheduling.

Two file formats exist:
  - Pre-Oct 2025:  48 MTUs (30-min intervals), values start at column 2
  - Oct 2025+:     96 MTUs (15-min intervals), values start at column 4

Two output resolutions are supported:
  - resolution="15min" (default): one row per (date, mtu) with mtu in 0..95.
        Pre-Oct 30-min values are broadcast 2× to fill 96 MTUs.
  - resolution="hourly":           one row per (date, hour) with hour in 0..23.

15-min schema:
    date, mtu, hour, quarter, load_forecast_mw, res_forecast_mw
"""
from __future__ import annotations

import io
import json
import logging
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.admie.gr"
_CACHE_DIR = Path("data/external/admie")

# We take ISP1 (D-1 at 13:30) as the representative forecast
_CATEGORIES = {
    "load": "ISP1DayAheadLoadForecast",
    "res":  "ISP1DayAheadRESForecast",
}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get_file_list(category: str, start: date, end: date) -> list[dict]:
    """Query ADMIE API for file metadata in a date range."""
    r = requests.get(
        f"{_BASE_URL}/getOperationMarketFilewRange",
        params={
            "dateStart": start.isoformat(),
            "dateEnd":   end.isoformat(),
            "FileCategory": category,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _best_file_per_date(files: list[dict]) -> dict[date, str]:
    """Keep the highest-version file per coverage date."""
    by_date: dict[date, tuple[str, str]] = {}  # date → (url, description)
    for f in files:
        raw = f.get("file_fromdate", "")        # "DD.MM.YYYY"
        url = f.get("file_path", "")
        try:
            d = date(int(raw[6:10]), int(raw[3:5]), int(raw[0:2]))
        except (ValueError, IndexError):
            continue
        prev = by_date.get(d)
        # Higher URL (v02 > v01 lexicographically) wins
        if prev is None or url > prev[0]:
            by_date[d] = (url, f.get("file_description", ""))
    return {d: v[0] for d, v in by_date.items()}


# ---------------------------------------------------------------------------
# XLSX parser → 96-MTU series
# ---------------------------------------------------------------------------

def _shared_strings(zf: zipfile.ZipFile) -> dict[int, str]:
    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zf.open("xl/sharedStrings.xml") as fh:
        tree = ET.parse(fh)
    ss: dict[int, str] = {}
    for i, si in enumerate(tree.findall(f".//{NS}si")):
        texts = si.findall(f".//{NS}t")
        ss[i] = "".join(t.text or "" for t in texts)
    return ss


def _cell_value(c: ET.Element, ss: dict[int, str]) -> str:
    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    t = c.get("t", "")
    v = c.find(f"{NS}v")
    if v is None:
        return ""
    raw = v.text or ""
    return ss.get(int(raw), raw) if t == "s" else raw


def _parse_forecast_xlsx(content: bytes) -> list[float | None]:
    """Parse one ISP forecast file and return 96 quarter-hour MW values.

    Pre-Oct 2025 (48 intervals, 30-min) values are duplicated 2× to fill 96.
    Returns 96 values, possibly with Nones for missing data.
    """
    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        ss = _shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as fh:
            tree = ET.parse(fh)

    rows_el = tree.findall(f"{NS}sheetData/{NS}row")
    grid: list[list[str]] = [
        [_cell_value(c, ss) for c in row_el.findall(f"{NS}c")]
        for row_el in rows_el
    ]

    data_row: list[str] | None = None
    n_mtu = 48

    for row in grid:
        if not row:
            continue
        label = row[0].strip().lower()
        if label in ("load forecast", "res forecast"):
            data_row = row
            break

    if data_row is None:
        return [None] * 96

    for row in grid[:6]:
        nums = [v for v in row[1:] if v.strip().lstrip("-").isdigit()]
        if len(nums) >= 90:
            n_mtu = 96
            break
        elif len(nums) >= 44:
            n_mtu = 48
            break

    col_offset = 4 if n_mtu == 96 else 2

    raw_vals: list[float | None] = []
    for i in range(n_mtu):
        idx = col_offset + i
        try:
            raw_vals.append(float(data_row[idx]))
        except (IndexError, ValueError, TypeError):
            raw_vals.append(None)

    if n_mtu == 96:
        return raw_vals
    # 30-min → 15-min: each 30-min value covers 2 quarters
    out: list[float | None] = []
    for v in raw_vals:
        out.extend([v, v])
    # Pad to 96 if short
    while len(out) < 96:
        out.append(None)
    return out[:96]


def _to_hourly(vals_96: list[float | None]) -> list[float | None]:
    """Average 96 quarter-hour MW values into 24 hourly MW values."""
    out: list[float | None] = []
    for h in range(24):
        chunk = vals_96[h * 4: h * 4 + 4]
        nums = [v for v in chunk if v is not None]
        out.append(round(sum(nums) / len(nums), 3) if nums else None)
    return out


# ---------------------------------------------------------------------------
# Cache helpers (all caches store 96-MTU series; hourly is derived on read)
# ---------------------------------------------------------------------------

def _cache_path(d: date, kind: str) -> Path:
    """15-min cache. Stores a JSON list of 96 floats/Nones."""
    return _CACHE_DIR / str(d.year) / kind / f"{d.isoformat()}_15min.json"


def _legacy_hourly_cache_path(d: date, kind: str) -> Path:
    """Older cache layout from the hourly-only era."""
    return _CACHE_DIR / str(d.year) / kind / f"{d.isoformat()}.json"


def _save_cache(d: date, kind: str, vals_96: list[float | None]) -> None:
    p = _cache_path(d, kind)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(vals_96))


def _load_cache(d: date, kind: str) -> list[float | None] | None:
    p = _cache_path(d, kind)
    if p.exists():
        return json.loads(p.read_text())
    # Legacy hourly cache fallback: broadcast 24 values × 4 to fill 96 MTUs.
    legacy = _legacy_hourly_cache_path(d, kind)
    if legacy.exists():
        try:
            hourly = json.loads(legacy.read_text())
        except Exception:
            return None
        if isinstance(hourly, list) and len(hourly) >= 24:
            vals_96: list[float | None] = []
            for h in range(24):
                v = hourly[h]
                vals_96.extend([v, v, v, v])
            return vals_96
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _download_with_retry(url: str, max_retries: int = 3) -> bytes:
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.content
        except Exception as exc:
            if attempt == max_retries - 1:
                raise
            logger.warning("Retry %d for %s: %s", attempt + 1, url, exc)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to download {url}")


def fetch(
    start_date: date,
    end_date: date,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
    batch_days: int = 30,
    resolution: str = "15min",
) -> pd.DataFrame:
    """Download ISP1 day-ahead load and RES forecasts for [start_date, end_date].

    Cached per day at 15-min resolution under data/external/admie/.

    Returns DataFrame:
        - resolution="15min":   date, mtu, hour, quarter, load_forecast_mw, res_forecast_mw
        - resolution="hourly":  date, hour, load_forecast_mw, res_forecast_mw
    """
    if resolution not in ("15min", "hourly"):
        raise ValueError(f"resolution must be '15min' or 'hourly', got {resolution!r}")

    global _CACHE_DIR
    _CACHE_DIR = Path(cache_dir)

    all_days = [
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    # Identify which days still need downloading for each category
    missing: dict[str, list[date]] = {kind: [] for kind in _CATEGORIES}
    for d in all_days:
        for kind in _CATEGORIES:
            if force or _load_cache(d, kind) is None:
                missing[kind].append(d)

    # Download in monthly batches to avoid hammering the API
    for kind, category in _CATEGORIES.items():
        days_needed = missing[kind]
        if not days_needed:
            continue

        logger.info(
            "Fetching ADMIE %s for %d days (%s → %s)",
            kind, len(days_needed), days_needed[0], days_needed[-1],
        )

        i = 0
        while i < len(days_needed):
            batch = days_needed[i: i + batch_days]
            b_start, b_end = batch[0], batch[-1]

            try:
                files = _get_file_list(category, b_start, b_end)
                url_by_date = _best_file_per_date(files)
            except Exception as exc:
                logger.error("API error for %s %s–%s: %s", kind, b_start, b_end, exc)
                i += batch_days
                continue

            for d in batch:
                if _load_cache(d, kind) is not None and not force:
                    continue

                url = url_by_date.get(d)
                if url is None:
                    logger.warning("No %s file for %s", kind, d)
                    _save_cache(d, kind, [None] * 96)
                    continue

                try:
                    content = _download_with_retry(url)
                    vals_96 = _parse_forecast_xlsx(content)
                    _save_cache(d, kind, vals_96)
                except Exception as exc:
                    logger.error("Failed to parse %s for %s: %s", kind, d, exc)
                    _save_cache(d, kind, [None] * 96)

            i += batch_days

    # Assemble DataFrame from cache
    records: list[dict] = []
    for d in all_days:
        load_vals = _load_cache(d, "load") or [None] * 96
        res_vals  = _load_cache(d, "res")  or [None] * 96

        if resolution == "15min":
            for mtu in range(96):
                records.append({
                    "date": d,
                    "mtu": mtu,
                    "hour": mtu // 4,
                    "quarter": mtu % 4,
                    "load_forecast_mw": load_vals[mtu] if mtu < len(load_vals) else None,
                    "res_forecast_mw":  res_vals[mtu]  if mtu < len(res_vals)  else None,
                })
        else:
            load_h = _to_hourly(load_vals)
            res_h = _to_hourly(res_vals)
            for h in range(24):
                records.append({
                    "date": d,
                    "hour": h,
                    "load_forecast_mw": load_h[h],
                    "res_forecast_mw":  res_h[h],
                })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df["load_forecast_mw"] = pd.to_numeric(df["load_forecast_mw"], errors="coerce")
    df["res_forecast_mw"]  = pd.to_numeric(df["res_forecast_mw"],  errors="coerce")

    logger.info(
        "ADMIE: %d rows (%s), load NaN=%d, res NaN=%d",
        len(df), resolution,
        df["load_forecast_mw"].isna().sum(),
        df["res_forecast_mw"].isna().sum(),
    )
    return df
