"""Parse HENEX DAM Results Summary XLSX files into a tidy DataFrame.

Handles two file formats automatically:
  - Pre-Oct 2025: 24-column hourly (MTU 1-24), MCP row = "Greece Mainland"
  - Oct 2025+:    96-column 15-min (MTU 1-96), MCP from "Greece Mainland (60min Index)"

When multiple revisions exist for a date (v01, v02…), the highest is used.

Two output resolutions are supported via `resolution`:
    - "15min" (default): one row per (date, mtu) with mtu in 0..95.
                          For pre-Oct hourly files, the hourly value is broadcast
                          across the 4 MTUs of that hour.
    - "hourly":           one row per (date, hour) with hour in 0..23. Post-Oct
                          15-min values are averaged into hours.

15-min output schema:
    date, mtu, hour, quarter, mcp_eur_mwh,
    sell_total_mwh, gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh
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
_VOLUME_LABELS = ("gas", "hydro", "renewables", "lignite", "imports")
_VOLUME_KEY = {
    "gas": "gas_mwh",
    "hydro": "hydro_mwh",
    "renewables": "res_mwh",
    "lignite": "lignite_mwh",
    "imports": "imports_mwh",
}


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


def _avg_per_hour(vals_96: list[float | None]) -> list[float | None]:
    """Average 96 quarter values into 24 hourly values."""
    out: list[float | None] = []
    for h in range(24):
        chunk = vals_96[h * 4: h * 4 + 4]
        nums = [v for v in chunk if v is not None]
        out.append(sum(nums) / len(nums) if nums else None)
    return out


def _broadcast_to_96(vals_24: list[float | None]) -> list[float | None]:
    """Replicate each of 24 hourly values 4× to fill 96 MTUs."""
    out: list[float | None] = []
    for v in vals_24:
        out.extend([v] * 4)
    return out


# ---------------------------------------------------------------------------
# Single-file parser
# ---------------------------------------------------------------------------

def _parse_xlsx_day(path: Path) -> dict[str, list[float | None]] | None:
    """Parse one HENEX file and return per-section MTU-96 series.

    Always normalises to 96-MTU (15-min) internally; pre-Oct hourly files are
    broadcast 4×. Returns None on parse error. Returned dict has at minimum:
        {"trading_date": date, "mcp_eur_mwh": [96 floats],
         "sell_total_mwh": [...], "gas_mwh": [...], ...}
    Missing sections are simply absent.
    """
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

    if not grid:
        return None

    max_numerics = max(
        (sum(1 for v in row[1:] if v.strip().lstrip("-").isdigit())
         for row in grid[:5] if row),
        default=0,
    )
    n_mtu = 96 if max_numerics >= 90 else 24
    is_quarterly = (n_mtu == 96)
    data_cols = n_mtu

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
            if "60min index" in label:
                vals = _parse_cells(row, 1, data_cols)
                # 60min index repeats hourly value across 4 MTUs — keep as-is for 96.
                sections["mcp_eur_mwh"] = vals[:96]
                found_60min_index = True
                next_is_mainland = False
                continue
            if "15min mcp" in label and not found_60min_index:
                sections["mcp_eur_mwh"] = _parse_cells(row, 1, data_cols)
                next_is_mainland = False
                continue
        else:
            if next_is_mainland and label == "greece mainland":
                key = "mcp_eur_mwh" if mainland_ctx == "mcp" else "sell_total_mwh"
                vals_24 = _parse_cells(row, 1, data_cols)
                sections[key] = _broadcast_to_96(vals_24)
                next_is_mainland = False
                continue

        # ---- Sell trades section ----------------------------------------
        if label == "total sell trades":
            next_is_mainland = True
            mainland_ctx = "sell"
            continue

        if next_is_mainland and label == "greece mainland":
            raw = _parse_cells(row, 1, data_cols)
            sections["sell_total_mwh"] = raw if is_quarterly else _broadcast_to_96(raw)
            next_is_mainland = False
            continue

        # Volume rows
        next_is_mainland = False

        if label in _VOLUME_LABELS:
            if label == "imports" and "(implicit)" in raw_label.lower():
                continue
            raw = _parse_cells(row, 1, data_cols)
            sections[_VOLUME_KEY[label]] = raw if is_quarterly else _broadcast_to_96(raw)

    if "mcp_eur_mwh" not in sections:
        logger.warning("No MCP data in %s — skipping", path.name)
        return None

    sections["trading_date"] = trading_date
    return sections


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


def _records_15min(parsed: dict) -> list[dict]:
    trading_date: date = parsed["trading_date"]
    records: list[dict] = []
    for mtu in range(96):
        rec: dict = {
            "date": trading_date,
            "mtu": mtu,
            "hour": mtu // 4,
            "quarter": mtu % 4,
        }
        for key, vals in parsed.items():
            if key == "trading_date":
                continue
            rec[key] = vals[mtu] if mtu < len(vals) else None
        records.append(rec)
    return records


def _records_hourly(parsed: dict) -> list[dict]:
    trading_date: date = parsed["trading_date"]
    hourly_sections: dict[str, list[float | None]] = {}
    for key, vals in parsed.items():
        if key == "trading_date":
            continue
        hourly_sections[key] = _avg_per_hour(vals)

    records: list[dict] = []
    for h in range(24):
        rec: dict = {"date": trading_date, "hour": h}
        for key, vals in hourly_sections.items():
            rec[key] = vals[h] if h < len(vals) else None
        records.append(rec)
    return records


def parse_dirs(
    *data_dirs: str | Path,
    resolution: str = "15min",
) -> pd.DataFrame:
    """Parse all HENEX Results Summary XLSX files into a tidy DataFrame.

    `resolution` must be either "15min" (default) or "hourly". Output sort key:
        - 15min:   date, mtu
        - hourly:  date, hour
    """
    if resolution not in ("15min", "hourly"):
        raise ValueError(f"resolution must be '15min' or 'hourly', got {resolution!r}")

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
    logger.info(
        "Parsing %d files (%s) from %d director(ies)",
        len(files), resolution, len(data_dirs),
    )

    all_records: list[dict] = []
    skipped = 0
    for path in files:
        parsed = _parse_xlsx_day(path)
        if parsed is None:
            skipped += 1
            continue
        if resolution == "15min":
            all_records.extend(_records_15min(parsed))
        else:
            all_records.extend(_records_hourly(parsed))

    if not all_records:
        raise ValueError("No valid records parsed.")

    df = pd.DataFrame(all_records)
    df["date"] = pd.to_datetime(df["date"])
    sort_keys = ["date", "mtu"] if resolution == "15min" else ["date", "hour"]
    numeric_cols = [c for c in df.columns if c not in ("date", *sort_keys, "hour", "quarter")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df = df.sort_values(sort_keys).reset_index(drop=True)

    logger.info(
        "Parsed %d trading days (%d rows), skipped %d files",
        df["date"].nunique(), len(df), skipped,
    )
    return df


def load_or_parse(
    *data_dirs: str | Path,
    cache_path: str | Path = "data/processed/prices.parquet",
    force: bool = False,
    resolution: str = "15min",
) -> pd.DataFrame:
    """Return cached parquet if available, otherwise parse and cache.

    The cache file path is suffixed with the resolution so 15-min and hourly
    caches do not clash.
    """
    cache_path = Path(cache_path)
    suffixed = cache_path.with_name(f"{cache_path.stem}_{resolution}{cache_path.suffix}")

    if suffixed.exists() and not force:
        logger.info("Loading HENEX prices from cache: %s", suffixed)
        return pd.read_parquet(suffixed)

    df = parse_dirs(*data_dirs, resolution=resolution)
    suffixed.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(suffixed, index=False)
    logger.info("Saved %d rows → %s", len(df), suffixed)
    return df
