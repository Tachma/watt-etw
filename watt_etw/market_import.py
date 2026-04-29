from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree


@dataclass(frozen=True)
class MarketRow:
    timestamp: datetime
    price_eur_mwh: float
    extra: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat()
        return result


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    errors: list[str]
    warnings: list[str]
    detected_dates: list[str]
    interval_minutes: int | None
    row_count: int
    price_summary: dict[str, float] | None
    rows: list[MarketRow]
    columns: list[str]

    def to_dict(self, include_rows: bool = True) -> dict[str, Any]:
        data = asdict(self)
        if include_rows:
            data["rows"] = [row.to_dict() for row in self.rows]
        else:
            data.pop("rows", None)
        return data


TIME_ALIASES = {
    "timestamp",
    "datetime",
    "date time",
    "delivery time",
    "delivery datetime",
    "mtu",
    "market time unit",
}
DATE_ALIASES = {"date", "delivery date", "dispatch date"}
PERIOD_ALIASES = {"period", "hour", "mtu", "delivery period", "interval"}
PRICE_HINTS = (
    "price_eur_mwh",
    "price",
    "clearing price",
    "market clearing price",
    "mcp",
    "dam price",
    "€/mwh",
    "eur/mwh",
    "euro/mwh",
)


def load_market_file(filename: str, content: bytes) -> ValidationResult:
    suffix = Path(filename).suffix.lower()
    try:
        if suffix == ".csv":
            raw_rows = _read_csv(content)
        elif suffix in {".xlsx", ".xlsm"}:
            raw_rows = _read_xlsx(content)
        else:
            return ValidationResult(
                valid=False,
                errors=["Only CSV and XLSX files are supported."],
                warnings=[],
                detected_dates=[],
                interval_minutes=None,
                row_count=0,
                price_summary=None,
                rows=[],
                columns=[],
            )
    except Exception as exc:
        return ValidationResult(
            valid=False,
            errors=[f"Could not read file: {exc}"],
            warnings=[],
            detected_dates=[],
            interval_minutes=None,
            row_count=0,
            price_summary=None,
            rows=[],
            columns=[],
        )

    return validate_market_rows(raw_rows)


def validate_market_rows(raw_rows: list[dict[str, Any]]) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    if not raw_rows:
        return ValidationResult(False, ["No rows were found."], [], [], None, 0, None, [], [])

    columns = list(raw_rows[0].keys())
    timestamp_col = _find_column(columns, TIME_ALIASES)
    date_col = _find_column(columns, DATE_ALIASES)
    period_col = _find_column(columns, PERIOD_ALIASES)
    price_col = _find_price_column(columns)

    if not price_col:
        errors.append("Could not detect a DAM price column.")
    if not timestamp_col and not (date_col and period_col):
        errors.append("Could not detect timestamp, or date plus period columns.")
    if errors:
        return ValidationResult(False, errors, warnings, [], None, 0, None, [], columns)

    rows: list[MarketRow] = []
    skipped = 0
    for raw in raw_rows:
        try:
            timestamp = (
                _parse_datetime(raw[timestamp_col])
                if timestamp_col
                else _parse_date_period(raw[date_col], raw[period_col])
            )
            price = _parse_float(raw[price_col])
        except Exception:
            skipped += 1
            continue
        extra = {key: value for key, value in raw.items() if key not in {timestamp_col, date_col, period_col, price_col}}
        rows.append(MarketRow(timestamp=timestamp, price_eur_mwh=price, extra=extra))

    rows.sort(key=lambda row: row.timestamp)
    if skipped:
        warnings.append(f"Skipped {skipped} rows with invalid timestamp or price values.")
    if not rows:
        return ValidationResult(False, ["No valid market rows were found."], warnings, [], None, 0, None, [], columns)

    duplicates = len(rows) - len({row.timestamp for row in rows})
    if duplicates:
        warnings.append(f"Detected {duplicates} duplicate timestamps.")

    interval = _detect_interval_minutes(rows)
    missing = _count_missing_intervals(rows, interval)
    if missing:
        warnings.append(f"Detected {missing} missing intervals.")

    prices = [row.price_eur_mwh for row in rows]
    dates = sorted({row.timestamp.date().isoformat() for row in rows})
    summary = {
        "min": min(prices),
        "max": max(prices),
        "average": sum(prices) / len(prices),
    }
    return ValidationResult(True, [], warnings, dates, interval, len(rows), summary, rows, columns)


def filter_rows_for_date(rows: list[MarketRow], selected_date: str | None) -> list[MarketRow]:
    if not selected_date:
        return rows
    target = date.fromisoformat(selected_date)
    return [row for row in rows if row.timestamp.date() == target]


def rows_from_payload(items: list[dict[str, Any]]) -> list[MarketRow]:
    rows = []
    for item in items:
        rows.append(
            MarketRow(
                timestamp=_parse_datetime(item["timestamp"]),
                price_eur_mwh=float(item["price_eur_mwh"]),
                extra=dict(item.get("extra") or {}),
            )
        )
    return sorted(rows, key=lambda row: row.timestamp)


def _read_csv(content: bytes) -> list[dict[str, Any]]:
    text = content.decode("utf-8-sig")
    sample = text[:2048]
    dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    return [_clean_row(row) for row in reader]


def _read_xlsx(content: bytes) -> list[dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared = _read_shared_strings(archive)
        sheet_names = [
            name
            for name in archive.namelist()
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
        ]
        sheet_prefixes = _xlsx_sheet_prefixes(archive)
        henex_rows = _read_henex_summary_workbook(archive, sheet_names, shared, sheet_prefixes)
        if henex_rows:
            return henex_rows
        for sheet_name in sheet_names:
            rows = _sheet_rows(archive.read(sheet_name), shared)
            table = _rows_to_dicts(rows)
            if table:
                return table
    return []


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        xml = archive.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ElementTree.fromstring(xml)
    strings = []
    for si in root.findall(".//{*}si"):
        strings.append("".join(t.text or "" for t in si.findall(".//{*}t")))
    return strings


def _sheet_rows(xml: bytes, shared: list[str]) -> list[list[Any]]:
    root = ElementTree.fromstring(xml)
    rows: list[list[Any]] = []
    for row in root.findall(".//{*}row"):
        values: list[Any] = []
        for cell in row.findall("{*}c"):
            cell_type = cell.attrib.get("t")
            value_node = cell.find("{*}v")
            inline_node = cell.find("{*}is/{*}t")
            value = inline_node.text if inline_node is not None else (value_node.text if value_node is not None else "")
            if cell_type == "s" and value != "":
                value = shared[int(value)]
            values.append(value)
        rows.append(values)
    return rows


def _read_henex_summary_workbook(
    archive: zipfile.ZipFile,
    sheet_names: list[str],
    shared: list[str],
    sheet_prefixes: dict[str, str],
) -> list[dict[str, Any]]:
    combined: dict[int, dict[str, Any]] = {}
    for sheet_name in sheet_names:
        rows = _sheet_rows(archive.read(sheet_name), shared)
        prefix = sheet_prefixes.get(sheet_name, "market")
        sheet_data = _extract_henex_summary_sheet(rows, prefix)
        for mtu, values in sheet_data.items():
            combined.setdefault(mtu, {}).update(values)
    return [combined[index] for index in sorted(combined)]


def _xlsx_sheet_prefixes(archive: zipfile.ZipFile) -> dict[str, str]:
    try:
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        rels = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    except KeyError:
        return {}

    rel_targets = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    prefixes: dict[str, str] = {}
    for sheet in workbook.findall(".//{*}sheet"):
        sheet_name = sheet.attrib.get("name", "").lower()
        rel_id = sheet.attrib.get(
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        )
        target = rel_targets.get(rel_id or "")
        if not target or target.startswith("../"):
            continue
        path = f"xl/{target}" if not target.startswith("xl/") else target
        if not path.startswith("xl/worksheets/"):
            continue
        if "sell" in sheet_name:
            prefixes[path] = "sell"
        elif "buy" in sheet_name:
            prefixes[path] = "buy"
        elif "coupling" in sheet_name:
            prefixes[path] = "coupling"
        else:
            prefixes[path] = "market"
    return prefixes


def _extract_henex_summary_sheet(rows: list[list[Any]], prefix: str) -> dict[int, dict[str, Any]]:
    time_row_index = _find_henex_time_row(rows)
    if time_row_index is None:
        return {}

    time_row = rows[time_row_index]
    delivery_date = _parse_excel_or_text_date(time_row[0])
    mtu_columns: list[tuple[int, int]] = []
    for column_index, value in enumerate(time_row[1:], start=1):
        mtu = _parse_int_like(value)
        if mtu is not None and 1 <= mtu <= 96:
            mtu_columns.append((column_index, mtu))
    if not mtu_columns:
        return {}

    price_row_index = _find_henex_price_row(rows)
    result: dict[int, dict[str, Any]] = {}
    for column_index, mtu in mtu_columns:
        timestamp = datetime.combine(delivery_date, time()) + timedelta(minutes=15 * (mtu - 1))
        result[mtu] = {"timestamp": timestamp.isoformat()}
        if price_row_index is not None:
            price = _safe_float_cell(_cell(rows[price_row_index], column_index))
            if price is not None:
                result[mtu]["price_eur_mwh"] = price

    for row in rows:
        label = str(_cell(row, 0) or "").strip()
        if not label or label.lower() in {"market clearing price", "production technology / mtu"}:
            continue
        if _safe_float_cell(label) is not None:
            continue
        key = _henex_extra_key(prefix, label)
        if key in {"sell_greece_mainland_15min_mcp", "buy_greece_mainland_15min_mcp", "coupling_greece_mainland_15min_mcp"}:
            continue
        for column_index, mtu in mtu_columns:
            value = _safe_float_cell(_cell(row, column_index))
            if value is not None:
                result[mtu][key] = value

    return {
        mtu: values
        for mtu, values in result.items()
        if "price_eur_mwh" in values and "timestamp" in values
    }


def _find_henex_time_row(rows: list[list[Any]]) -> int | None:
    for index, row in enumerate(rows[:12]):
        if len(row) < 5:
            continue
        mtu_values = [_parse_int_like(value) for value in row[1:9]]
        if mtu_values[:4] == [1, 2, 3, 4]:
            return index
    return None


def _find_henex_price_row(rows: list[list[Any]]) -> int | None:
    for index, row in enumerate(rows):
        label = str(_cell(row, 0) or "").lower()
        if "15min" in label and "mcp" in label:
            return index
    for index, row in enumerate(rows):
        label = str(_cell(row, 0) or "").lower()
        if "market clearing price" in label:
            next_index = index + 1
            if next_index < len(rows):
                return next_index
    return None


def _parse_excel_or_text_date(value: Any) -> date:
    number = _safe_float_cell(value)
    if number is not None:
        return (datetime(1899, 12, 30) + timedelta(days=number)).date()
    return _parse_date(value)


def _parse_int_like(value: Any) -> int | None:
    number = _safe_float_cell(value)
    if number is None:
        return None
    rounded = round(number)
    if abs(number - rounded) < 1e-9:
        return int(rounded)
    return None


def _safe_float_cell(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return _parse_float(value)
    except Exception:
        return None


def _cell(row: list[Any], index: int) -> Any:
    return row[index] if index < len(row) else ""


def _henex_extra_key(prefix: str, label: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return f"{prefix}_{cleaned}"


def _rows_to_dicts(rows: list[list[Any]]) -> list[dict[str, Any]]:
    for index, row in enumerate(rows[:20]):
        headers = [_clean_header(value) for value in row]
        if _find_price_column(headers) and (_find_column(headers, TIME_ALIASES) or _find_column(headers, DATE_ALIASES)):
            data = []
            for body in rows[index + 1 :]:
                if not any(str(value).strip() for value in body):
                    continue
                padded = body + [""] * (len(headers) - len(body))
                data.append(_clean_row(dict(zip(headers, padded))))
            return data
    return []


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {_clean_header(key): value for key, value in row.items() if key is not None}


def _clean_header(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _find_column(columns: list[str], aliases: set[str]) -> str | None:
    cleaned = {_clean_header(column): column for column in columns}
    for alias in aliases:
        if alias in cleaned:
            return cleaned[alias]
    for column in columns:
        if _clean_header(column) in aliases:
            return column
    return None


def _find_price_column(columns: list[str]) -> str | None:
    for column in columns:
        cleaned = _clean_header(column)
        if any(hint in cleaned for hint in PRICE_HINTS):
            return column
    return None


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime(1899, 12, 30) + timedelta(days=float(value))
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%d.%m.%Y %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return datetime.fromisoformat(text)


def _parse_date_period(date_value: Any, period_value: Any) -> datetime:
    day = _parse_date(date_value)
    period = str(period_value).strip()
    if re.fullmatch(r"\d+", period):
        index = int(period)
        if 1 <= index <= 96:
            return datetime.combine(day, time()) + timedelta(minutes=15 * (index - 1))
        if 1 <= index <= 24:
            return datetime.combine(day, time(index - 1))
    if "-" in period:
        period = period.split("-", 1)[0].strip()
    hour_minute = datetime.strptime(period[:5], "%H:%M").time()
    return datetime.combine(day, hour_minute)


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return datetime.fromisoformat(text).date()


def _parse_float(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("€", "").replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    return float(text)


def _detect_interval_minutes(rows: list[MarketRow]) -> int | None:
    deltas = [
        int((rows[index + 1].timestamp - rows[index].timestamp).total_seconds() / 60)
        for index in range(len(rows) - 1)
        if rows[index + 1].timestamp > rows[index].timestamp
    ]
    return min(deltas) if deltas else None


def _count_missing_intervals(rows: list[MarketRow], interval: int | None) -> int:
    if not interval:
        return 0
    missing = 0
    for index in range(len(rows) - 1):
        delta = int((rows[index + 1].timestamp - rows[index].timestamp).total_seconds() / 60)
        if delta > interval:
            missing += max(0, delta // interval - 1)
    return missing
