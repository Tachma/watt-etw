"""Fetch day-ahead load and RES forecasts from IPTO/ADMIE.

Source: ADMIE File Download API (public, no authentication required).
  https://www.admie.gr/en/market/market-statistics/file-download-api

We use ISP1 (published D-1 at 13:30) day-ahead forecasts — the most
relevant forecast horizon for battery scheduling.

Two file formats exist:
  - Pre-Oct 2025:  48 MTUs (30-min intervals), values start at column 2
  - Oct 2025+:     96 MTUs (15-min intervals), values start at column 4

Both are aggregated to hourly by averaging (values are in MW).

Output: one row per date-hour with columns:
    date, hour, load_forecast_mw, res_forecast_mw
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
_15MIN_CUTOVER = date(2025, 10, 1)   # ADMIE switched to 96 intervals here

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
# XLSX parser
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


def _parse_forecast_xlsx(content: bytes, trading_date: date) -> list[float | None]:
    """Parse one ISP forecast file and return 24 hourly MW values.

    Handles both 48-interval (30-min, pre-Oct 2025) and
    96-interval (15-min, Oct 2025+) formats automatically.
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

    # Find the row that starts with "Load Forecast" or "RES forecast"
    data_row: list[str] | None = None
    n_mtu = 48    # default (pre-Oct 2025)

    for row in grid:
        if not row:
            continue
        label = row[0].strip().lower()
        if label in ("load forecast", "res forecast"):
            data_row = row
            break

    if data_row is None:
        return [None] * 24

    # Detect number of MTU intervals from the header row
    for row in grid[:6]:
        nums = [v for v in row[1:] if v.strip().lstrip("-").isdigit()]
        if len(nums) >= 90:
            n_mtu = 96
            break
        elif len(nums) >= 44:
            n_mtu = 48
            break

    # Determine the data column offset
    # Pre-Oct 2025: col 0=label, col 1=source, col 2..2+48 = values
    # Oct 2025+:    col 0=label, col 1=label, col 2=NA, col 3=date, col 4..4+96 = values
    col_offset = 4 if n_mtu == 96 else 2

    # Extract the MTU values
    mtu_vals: list[float | None] = []
    for i in range(n_mtu):
        idx = col_offset + i
        try:
            mtu_vals.append(float(data_row[idx]))
        except (IndexError, ValueError, TypeError):
            mtu_vals.append(None)

    # Aggregate to 24 hourly values (average MW across intervals per hour)
    intervals_per_hour = n_mtu // 24   # 2 for 30-min, 4 for 15-min
    hourly: list[float | None] = []
    for h in range(24):
        chunk = mtu_vals[h * intervals_per_hour: (h + 1) * intervals_per_hour]
        nums = [v for v in chunk if v is not None]
        hourly.append(round(sum(nums) / len(nums), 3) if nums else None)

    return hourly


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_path(d: date, kind: str) -> Path:
    return _CACHE_DIR / str(d.year) / kind / f"{d.isoformat()}.json"


def _save_cache(d: date, kind: str, hourly: list[float | None]) -> None:
    p = _cache_path(d, kind)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(hourly))


def _load_cache(d: date, kind: str) -> list[float | None] | None:
    p = _cache_path(d, kind)
    if not p.exists():
        return None
    return json.loads(p.read_text())


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
) -> pd.DataFrame:
    """Download ISP1 day-ahead load and RES forecasts for [start_date, end_date].

    Results are cached per day under data/external/admie/.

    Returns DataFrame: date, hour, load_forecast_mw, res_forecast_mw
    """
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

    # Download in batches to avoid hammering the API
    for kind, category in _CATEGORIES.items():
        days_needed = missing[kind]
        if not days_needed:
            continue

        logger.info(
            "Fetching ADMIE %s for %d days (%s → %s)",
            kind, len(days_needed), days_needed[0], days_needed[-1],
        )

        # Break into monthly batches
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
                    _save_cache(d, kind, [None] * 24)
                    continue

                try:
                    content = _download_with_retry(url)
                    hourly = _parse_forecast_xlsx(content, d)
                    _save_cache(d, kind, hourly)
                except Exception as exc:
                    logger.error("Failed to parse %s for %s: %s", kind, d, exc)
                    _save_cache(d, kind, [None] * 24)

            i += batch_days

    # Assemble DataFrame from cache
    records: list[dict] = []
    for d in all_days:
        load_vals = _load_cache(d, "load") or [None] * 24
        res_vals  = _load_cache(d, "res")  or [None] * 24
        for h in range(24):
            records.append({
                "date": d,
                "hour": h,
                "load_forecast_mw": load_vals[h] if h < len(load_vals) else None,
                "res_forecast_mw":  res_vals[h]  if h < len(res_vals)  else None,
            })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df["load_forecast_mw"] = pd.to_numeric(df["load_forecast_mw"], errors="coerce")
    df["res_forecast_mw"]  = pd.to_numeric(df["res_forecast_mw"],  errors="coerce")

    logger.info(
        "ADMIE: %d rows, load NaN=%d, res NaN=%d",
        len(df),
        df["load_forecast_mw"].isna().sum(),
        df["res_forecast_mw"].isna().sum(),
    )
    return df
