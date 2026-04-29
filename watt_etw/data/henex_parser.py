"""Parse HENEX DAM Results Summary XLSX files into a tidy hourly DataFrame.

Each file covers one trading day and contains per-MTU (hour 1-24) values for:
  - Market Clearing Price (€/MWh)
  - Total cleared sell volume (MWh)
  - Supply by technology: Gas, Hydro, Renewables, Lignite
  - Total imports
"""
from __future__ import annotations

import logging
import re
import zipfile
from datetime import date, datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

logger = logging.getLogger(__name__)

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


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


def _parse_floats(cells: list[str], cols: range) -> list[float | None]:
    """Return float values for column indices in `cols`."""
    out: list[float | None] = []
    for i in cols:
        try:
            out.append(float(cells[i]))
        except (IndexError, ValueError):
            out.append(None)
    return out


def _parse_xlsx_day(path: Path) -> list[dict] | None:
    """Return list of 24 dicts (one per hour) for a single trading day file."""
    try:
        date_str = path.stem[:8]  # YYYYMMDD
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
        # sheet1.xml is the Results Summary sheet
        with zf.open("xl/worksheets/sheet1.xml") as fh:
            tree = ET.parse(fh)

        rows_el = tree.findall(f"{_NS}sheetData/{_NS}row")
        # Convert to list of cell-value lists
        grid: list[list[str]] = []
        for row_el in rows_el:
            cells = [_cell_value(c, ss) for c in row_el.findall(f"{_NS}c")]
            grid.append(cells)

    # The MTU columns 1-24 sit in columns B-Y (index 1-24) of row 2 (index 1).
    # We identify sections by scanning col A (index 0) for known labels.
    hour_cols = range(1, 25)  # 24 columns

    sections: dict[str, list[float | None]] = {}
    next_row_is_mainland = False
    mainland_context = ""

    for row in grid:
        if not row:
            continue
        label = (row[0] if row else "").strip().lower()

        if label == "market clearing price":
            next_row_is_mainland = True
            mainland_context = "mcp"
            continue
        if label == "total sell trades":
            next_row_is_mainland = True
            mainland_context = "sell"
            continue
        if next_row_is_mainland and label == "greece mainland":
            key = "mcp_eur_mwh" if mainland_context == "mcp" else "sell_total_mwh"
            sections[key] = _parse_floats(row, hour_cols)
            next_row_is_mainland = False
            continue

        next_row_is_mainland = False

        if label == "gas":
            sections["gas_mwh"] = _parse_floats(row, hour_cols)
        elif label == "hydro":
            sections["hydro_mwh"] = _parse_floats(row, hour_cols)
        elif label == "renewables":
            sections["res_mwh"] = _parse_floats(row, hour_cols)
        elif label == "lignite":
            sections["lignite_mwh"] = _parse_floats(row, hour_cols)
        elif label == "imports":  # " IMPORTS" in file, stripped to "imports"
            # skip "imports (implicit)" — we want the explicit total only
            raw_label = (row[0] if row else "")
            if "(implicit)" not in raw_label.lower():
                sections["imports_mwh"] = _parse_floats(row, hour_cols)

    if "mcp_eur_mwh" not in sections:
        logger.warning("No MCP found in %s", path.name)
        return None

    records = []
    for h in range(24):
        rec: dict = {"date": trading_date, "hour": h}
        for key, vals in sections.items():
            rec[key] = vals[h] if h < len(vals) else None
        records.append(rec)

    return records


def parse_all(data_dir: str | Path, glob: str = "*.xlsx") -> pd.DataFrame:
    """Parse every HENEX DAM Results Summary file in `data_dir`.

    Returns a DataFrame with columns:
        date, hour, mcp_eur_mwh, sell_total_mwh,
        gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
    sorted by date and hour.
    """
    data_dir = Path(data_dir)
    files = sorted(data_dir.glob(glob))
    if not files:
        raise FileNotFoundError(f"No XLSX files found in {data_dir}")

    all_records: list[dict] = []
    skipped = 0
    for path in files:
        records = _parse_xlsx_day(path)
        if records is None:
            skipped += 1
        else:
            all_records.extend(records)

    if not all_records:
        raise ValueError("No valid records parsed from any file.")

    df = pd.DataFrame(all_records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "hour"]).reset_index(drop=True)

    numeric_cols = [c for c in df.columns if c not in ("date", "hour")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    logger.info(
        "Parsed %d days (%d records), skipped %d files",
        df["date"].nunique(), len(df), skipped,
    )
    return df


def load_or_parse(
    data_dir: str | Path,
    cache_path: str | Path = "data/processed/prices.parquet",
    force: bool = False,
) -> pd.DataFrame:
    """Return cached parquet if it exists, otherwise parse and cache."""
    cache_path = Path(cache_path)
    if cache_path.exists() and not force:
        logger.info("Loading prices from cache: %s", cache_path)
        return pd.read_parquet(cache_path)

    df = parse_all(data_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    logger.info("Saved %d rows to %s", len(df), cache_path)
    return df
