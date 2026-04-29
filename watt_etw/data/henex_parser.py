"""Parse HENEX DAM Results Summary XLSX files into a tidy hourly DataFrame.

Handles two file formats automatically:
  - Pre-Oct 2025: 24-column hourly (MTU 1-24), MCP row = "Greece Mainland"
  - Oct 2025+:    96-column 15-min (MTU 1-96), MCP from "Greece Mainland (60min Index)"
                  Volumes are summed across 4 quarters per hour.

When multiple revisions exist for a date (v01, v02…), the highest is used.

Output columns per date-hour (hour 0-23):
    date, hour, mcp_eur_mwh, sell_total_mwh,
    gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
"""
from __future__ import annotations

import logging
import zipfile
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

logger = logging.getLogger(__name__)

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


# ---------------------------------------------------------------------------
# Low-level XLSX helpers
# ---------------------------------------------------------------------------

def _shared_strings(zf: zipfile.ZipFile) -> dict[int, str]:
    with zf.open("xl/sharedStrings.xml") as fh:
        tree = ET.parse(fh)
    ss: dict[int, str] = {}
    for i, si in enumerate(tree.findall(f".//{_NS}si")):
        texts = si.findall(f".//{_NS}t")
        ss[i] = "".join(t.text or "" for t in texts)
    return ss


def _cell_value(c: ET.Element, ss: dict[int, str]) -> str:
    t = c.get("t", "")
    v = c.find(f"{_NS}v")
    if v is None:
        return ""
    raw = v.text or ""
    return ss.get(int(raw), raw) if t == "s" else raw


def _to_float(s: str) -> float | None:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_cells(row: list[str], col_start: int, n_cols: int) -> list[float | None]:
    return [_to_float(row[i]) if i < len(row) else None
            for i in range(col_start, col_start + n_cols)]


def _agg_15min_to_hourly(vals: list[float | None]) -> list[float | None]:
    """Collapse 96 quarterly MW values into 24 hourly MWh values.

    Each 15-min column is average MW for that interval.
    MWh per hour = average MW over the hour (4 quarters averaged).
    """
    out: list[float | None] = []
    for h in range(24):
        quarter = vals[h * 4: h * 4 + 4]
        nums = [v for v in quarter if v is not None]
        out.append(sum(nums) / len(nums) if nums else None)
    return out


# ---------------------------------------------------------------------------
# Single-file parser
# ---------------------------------------------------------------------------

def _parse_xlsx_day(path: Path) -> list[dict] | None:
    """Return 24 hourly dicts for one HENEX Results Summary file, or None on error."""
    try:
        date_str = path.stem[:8]
        trading_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except Exception:
        logger.warning("Cannot parse date from filename: %s", path.name)
        return None

    try:
        zf = zipfile.ZipFile(path)
    except Exception as exc:
        logger.warning("Cannot open %s: %s", path.name, exc)
        return None

    with zf:
        ss = _shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as fh:
            tree = ET.parse(fh)

    rows_el = tree.findall(f"{_NS}sheetData/{_NS}row")
    grid: list[list[str]] = [
        [_cell_value(c, ss) for c in row_el.findall(f"{_NS}c")]
        for row_el in rows_el
    ]

    # Detect format: find the maximum number of integer-valued cells in the
    # first 5 rows. New 15-min format has a row with 96 MTU labels; old
    # hourly format peaks at 24.
    max_numerics = max(
        sum(1 for v in row[1:] if v.strip().lstrip("-").isdigit())
        for row in grid[:5]
        if row
    )
    n_mtu = 96 if max_numerics >= 90 else 24

    is_quarterly = (n_mtu == 96)
    data_cols = n_mtu   # number of value columns starting at index 1

    sections: dict[str, list[float | None]] = {}
    next_is_mainland = False
    mainland_ctx = ""
    found_60min_index = False

    for row in grid:
        if not row:
            continue
        raw_label = row[0]
        label = raw_label.strip().lower()

        # ---- MCP section ------------------------------------------------
        if label == "market clearing price":
            next_is_mainland = True
            mainland_ctx = "mcp"
            continue

        if is_quarterly:
            # New format: prefer "60min Index" row (already hourly averages)
            if "60min index" in label:
                vals = _parse_cells(row, 1, data_cols)
                # These repeat the same value 4× per hour; take every 4th
                sections["mcp_eur_mwh"] = [vals[h * 4] for h in range(24)
                                            if h * 4 < len(vals)]
                found_60min_index = True
                next_is_mainland = False
                continue
            # Fallback: average 15-min MCP if 60min index not found yet
            if "15min mcp" in label and not found_60min_index:
                raw = _parse_cells(row, 1, data_cols)
                sections["mcp_eur_mwh"] = _agg_15min_to_hourly(raw)
                next_is_mainland = False
                continue
        else:
            # Old format: "Greece Mainland" row directly after the header
            if next_is_mainland and label == "greece mainland":
                key = "mcp_eur_mwh" if mainland_ctx == "mcp" else "sell_total_mwh"
                sections[key] = _parse_cells(row, 1, data_cols)
                next_is_mainland = False
                continue

        # ---- Sell trades section ----------------------------------------
        if label == "total sell trades":
            next_is_mainland = True
            mainland_ctx = "sell"
            continue

        # "Greece Mainland" sell row (both formats — comes right after the header)
        if next_is_mainland and label == "greece mainland":
            raw = _parse_cells(row, 1, data_cols)
            sections["sell_total_mwh"] = _agg_15min_to_hourly(raw) if is_quarterly else raw
            next_is_mainland = False
            continue

        # Volume rows (same logic for both formats)
        next_is_mainland = False

        if label == "gas":
            raw = _parse_cells(row, 1, data_cols)
            sections["gas_mwh"] = _agg_15min_to_hourly(raw) if is_quarterly else raw
        elif label == "hydro":
            raw = _parse_cells(row, 1, data_cols)
            sections["hydro_mwh"] = _agg_15min_to_hourly(raw) if is_quarterly else raw
        elif label == "renewables":
            raw = _parse_cells(row, 1, data_cols)
            sections["res_mwh"] = _agg_15min_to_hourly(raw) if is_quarterly else raw
        elif label == "lignite":
            raw = _parse_cells(row, 1, data_cols)
            sections["lignite_mwh"] = _agg_15min_to_hourly(raw) if is_quarterly else raw
        elif label == "imports":
            if "(implicit)" not in raw_label.lower():
                raw = _parse_cells(row, 1, data_cols)
                sections["imports_mwh"] = _agg_15min_to_hourly(raw) if is_quarterly else raw

    if "mcp_eur_mwh" not in sections:
        logger.warning("No MCP data in %s — skipping", path.name)
        return None

    return [
        {"date": trading_date, "hour": h,
         **{k: (v[h] if v and h < len(v) else None) for k, v in sections.items()}}
        for h in range(24)
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _best_file_per_date(files: list[Path]) -> list[Path]:
    """Keep only the highest revision (v02 > v01) per date."""
    by_date: dict[str, Path] = {}
    for p in files:
        key = p.stem[:8]
        prev = by_date.get(key)
        if prev is None or p.stem > prev.stem:
            by_date[key] = p
    return sorted(by_date.values())


def parse_dirs(*data_dirs: str | Path) -> pd.DataFrame:
    """Parse all HENEX Results Summary XLSX files across one or more directories.

    Returns a DataFrame sorted by date and hour with columns:
        date, hour, mcp_eur_mwh, sell_total_mwh,
        gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
    """
    all_files: list[Path] = []
    for d in data_dirs:
        d = Path(d)
        found = list(d.glob("*.xlsx"))
        if not found:
            logger.warning("No XLSX files in %s", d)
        all_files.extend(found)

    if not all_files:
        raise FileNotFoundError(f"No XLSX files found in: {data_dirs}")

    files = _best_file_per_date(all_files)
    logger.info("Parsing %d files from %d director(ies)", len(files), len(data_dirs))

    all_records: list[dict] = []
    skipped = 0
    for path in files:
        records = _parse_xlsx_day(path)
        if records is None:
            skipped += 1
        else:
            all_records.extend(records)

    if not all_records:
        raise ValueError("No valid records parsed.")

    df = pd.DataFrame(all_records)
    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = [c for c in df.columns if c not in ("date", "hour")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df = df.sort_values(["date", "hour"]).reset_index(drop=True)

    logger.info(
        "Parsed %d trading days (%d rows), skipped %d files",
        df["date"].nunique(), len(df), skipped,
    )
    return df


def load_or_parse(
    *data_dirs: str | Path,
    cache_path: str | Path = "data/processed/prices.parquet",
    force: bool = False,
) -> pd.DataFrame:
    """Return cached parquet if available, otherwise parse and cache."""
    cache_path = Path(cache_path)
    if cache_path.exists() and not force:
        logger.info("Loading HENEX prices from cache: %s", cache_path)
        return pd.read_parquet(cache_path)

    df = parse_dirs(*data_dirs)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    logger.info("Saved %d rows → %s", len(df), cache_path)
    return df
