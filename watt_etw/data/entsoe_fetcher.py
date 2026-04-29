"""ENTSO-E Transparency Platform fetcher for the Greek bidding zone.

Provides day-ahead prices, generation per fuel type, and net cross-border
imports — i.e. the same numbers the HEnEx ResultsSummary XLSX contains —
in machine-readable form. Used to extend the price/supply-mix dataset
beyond what HEnEx publishes on its public archive page.

Requires a free ENTSO-E REST token, set via env var WATT_ENTSOE_TOKEN
or passed to fetch().

Output schema (one row per date × mtu, matching henex_parser.parse_dirs
in 15-min mode):
    date, mtu, hour, quarter,
    mcp_eur_mwh, gas_mwh, hydro_mwh, res_mwh, lignite_mwh, imports_mwh,
    sell_total_mwh   (computed as the sum of generation + net imports)
"""
from __future__ import annotations

import io
import logging
import os
import re
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_API = "https://web-api.tp.entsoe.eu/api"
_GR = "10YGR-HTSO-----Y"
_TOKEN_ENV = "WATT_ENTSOE_TOKEN"
_CACHE_DIR = Path("data/external/entsoe")

# Greek interconnections (each line is a list of EIC codes whose flows into
# and out of Greece sum to net imports).
_NEIGHBOURS = {
    "italy": "10Y1001A1001A893",
    "bulgaria": "10YCA-BULGARIA-R",
    "north_macedonia": "10YMK-MEPSO----8",
    "albania": "10YAL-KESH-----5",
    "turkey": "10YTR-TEIAS----W",
}

# ENTSO-E PSR (production type) → our schema bucket
_PSR_BUCKET = {
    "B02": "lignite_mwh",   # Fossil Brown coal/Lignite
    "B04": "gas_mwh",        # Fossil Gas
    "B10": "hydro_mwh",      # Hydro Pumped Storage
    "B11": "hydro_mwh",      # Hydro Run-of-river
    "B12": "hydro_mwh",      # Hydro Water Reservoir
    "B16": "res_mwh",        # Solar
    "B18": "res_mwh",        # Wind Offshore
    "B19": "res_mwh",        # Wind Onshore
    "B01": "res_mwh",        # Biomass
    "B15": "res_mwh",        # Other renewable
    "B09": "res_mwh",        # Geothermal
}

_NS = {"d": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
# A75 (generation per type) uses a different namespace
_NS_GL = {"d": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}


def _resolve_token(token: str | None) -> str:
    if token:
        return token
    t = os.environ.get(_TOKEN_ENV)
    if not t:
        raise RuntimeError(
            f"ENTSO-E token missing. Pass token= or set {_TOKEN_ENV} env var."
        )
    return t


def _to_utc_period(start_d: date, end_d: date) -> tuple[str, str]:
    """ENTSO-E periods are UTC. We convert Greek delivery-day boundaries
    (Europe/Athens 00:00 → next day 00:00) to UTC YYYYMMDDhhmm strings.

    Pad ±4 hours to safely cover both UTC+2 (winter) and UTC+3 (summer DST)
    plus an hour on each side for inclusive boundaries. We crop back to the
    requested Athens-local window after parsing.
    """
    s = datetime(start_d.year, start_d.month, start_d.day, tzinfo=timezone.utc) - timedelta(hours=4)
    e = datetime(end_d.year, end_d.month, end_d.day, tzinfo=timezone.utc) + timedelta(hours=28)
    return s.strftime("%Y%m%d%H%M"), e.strftime("%Y%m%d%H%M")


def _cache_path(kind: str, start_d: date, end_d: date, suffix: str = "xml") -> Path:
    name = f"{kind}_{start_d.isoformat()}_{end_d.isoformat()}.{suffix}"
    return _CACHE_DIR / kind / name


def _get_xml(params: dict, *, retries: int = 3) -> bytes:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(_API, params=params, timeout=60)
            if r.status_code == 200:
                return r.content
            last_exc = RuntimeError(f"ENTSO-E HTTP {r.status_code}: {r.text[:200]}")
        except requests.RequestException as exc:
            last_exc = exc
        wait = 2 ** attempt
        logger.warning("ENTSO-E retry %d in %ds: %s", attempt + 1, wait, last_exc)
        time.sleep(wait)
    raise last_exc  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# XML parsing                                                                   #
# --------------------------------------------------------------------------- #

def _root_ns(root: ET.Element) -> str:
    """Extract the XML namespace URI from a root element's tag."""
    m = re.match(r"\{([^}]+)\}", root.tag)
    return m.group(1) if m else ""


def _parse_publication(xml_bytes: bytes, value_tag: str = "price.amount") -> list[tuple[datetime, float]]:
    """Parse an A44/A11 Publication_MarketDocument response into UTC-tagged
    (timestamp, value) tuples. Resolution is honoured (PT15M / PT60M).
    Tolerant to schema version (7:0 vs 7:3 etc.).
    """
    root = ET.fromstring(xml_bytes)
    ns = _root_ns(root)
    if not ns or "Acknowledgement" in root.tag:
        return []

    out: list[tuple[datetime, float]] = []
    for ts in root.findall(f"{{{ns}}}TimeSeries"):
        for period in ts.findall(f"{{{ns}}}Period"):
            ti = period.find(f"{{{ns}}}timeInterval")
            start = ti.find(f"{{{ns}}}start").text
            res = period.find(f"{{{ns}}}resolution").text
            t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
            step = pd.Timedelta(res).to_pytimedelta()
            for pt in period.findall(f"{{{ns}}}Point"):
                pos = int(pt.find(f"{{{ns}}}position").text)
                v_el = pt.find(f"{{{ns}}}{value_tag}")
                if v_el is None or v_el.text is None:
                    continue
                t = t0 + step * (pos - 1)
                out.append((t, float(v_el.text)))
    return out


def _parse_generation(xml_bytes: bytes) -> list[tuple[datetime, str, float]]:
    """Parse A75 generation-per-type into [(t_utc, psr_code, mw)] tuples."""
    root = ET.fromstring(xml_bytes)
    ns = _root_ns(root)
    if not ns or "Acknowledgement" in root.tag:
        return []

    out: list[tuple[datetime, str, float]] = []
    for ts in root.findall(f"{{{ns}}}TimeSeries"):
        psr_el = ts.find(f".//{{{ns}}}MktPSRType/{{{ns}}}psrType")
        if psr_el is None or psr_el.text is None:
            continue
        psr = psr_el.text
        for period in ts.findall(f"{{{ns}}}Period"):
            ti = period.find(f"{{{ns}}}timeInterval")
            start = ti.find(f"{{{ns}}}start").text
            res = period.find(f"{{{ns}}}resolution").text
            t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
            step = pd.Timedelta(res).to_pytimedelta()
            for pt in period.findall(f"{{{ns}}}Point"):
                pos = int(pt.find(f"{{{ns}}}position").text)
                v_el = pt.find(f"{{{ns}}}quantity")
                if v_el is None or v_el.text is None:
                    continue
                t = t0 + step * (pos - 1)
                out.append((t, psr, float(v_el.text)))
    return out


# --------------------------------------------------------------------------- #
# Per-feed fetchers (per month)                                                 #
# --------------------------------------------------------------------------- #

def _fetch_or_cache(
    kind: str,
    start_d: date,
    end_d: date,
    params: dict,
    token: str,
    force: bool,
) -> bytes:
    cp = _cache_path(kind, start_d, end_d)
    if cp.exists() and not force:
        return cp.read_bytes()
    p = {**params, "securityToken": token}
    xml = _get_xml(p)
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_bytes(xml)
    return xml


def _to_athens_15min(rows: list[tuple[datetime, float]],
                       resolution_hint: str | None = None) -> pd.DataFrame:
    """Convert UTC-tagged (timestamp, value) rows into a 15-min Athens grid.

    Accepts mixed PT15M / PT60M resolutions. Hourly rows are forward-filled
    across the four 15-min slots of that hour. Returns DataFrame with
    columns date (datetime64, naive), mtu (0..95), value.
    """
    if not rows:
        return pd.DataFrame(columns=["date", "mtu", "value"])

    df = pd.DataFrame(rows, columns=["t_utc", "value"])
    df["t_utc"] = pd.to_datetime(df["t_utc"], utc=True)
    df["t_local"] = df["t_utc"].dt.tz_convert("Europe/Athens")
    df = df.dropna(subset=["value"])

    # Detect hourly vs 15-min by minute alignment within each hour.
    # If every record sits at minute=0 and we only see one record per hour,
    # treat as hourly and broadcast across 4 quarters.
    df["minute"] = df["t_local"].dt.minute
    hourly = bool((df["minute"] == 0).all() and
                  df.groupby([df["t_local"].dt.date,
                              df["t_local"].dt.hour]).size().max() == 1)

    if hourly:
        # Build 4 child rows per hourly record at quarters 0/15/30/45.
        rows_15: list[tuple[pd.Timestamp, float]] = []
        for _, r in df.iterrows():
            base = r["t_local"].replace(minute=0, second=0, microsecond=0)
            for q in range(4):
                rows_15.append((base + pd.Timedelta(minutes=15 * q), r["value"]))
        df = pd.DataFrame(rows_15, columns=["t_local", "value"])
        # t_local already in Athens TZ
    df["date"] = df["t_local"].dt.normalize().dt.tz_localize(None)
    df["mtu"] = df["t_local"].dt.hour * 4 + df["t_local"].dt.minute // 15
    return df[["date", "mtu", "value"]]


# --------------------------------------------------------------------------- #
# Public API                                                                    #
# --------------------------------------------------------------------------- #

def fetch_prices(start_d: date, end_d: date, *, token: str, force: bool = False) -> pd.DataFrame:
    """A44 day-ahead prices for the Greek bidding zone."""
    s, e = _to_utc_period(start_d, end_d)
    params = {
        "documentType": "A44",
        "in_Domain": _GR,
        "out_Domain": _GR,
        "periodStart": s,
        "periodEnd": e,
    }
    xml = _fetch_or_cache("prices", start_d, end_d, params, token, force)
    rows = _parse_publication(xml, value_tag="price.amount")
    df = _to_athens_15min(rows).rename(columns={"value": "mcp_eur_mwh"})
    return df


def fetch_generation(start_d: date, end_d: date, *, token: str, force: bool = False) -> pd.DataFrame:
    """A75 actual generation per production type, aggregated to our four buckets."""
    s, e = _to_utc_period(start_d, end_d)
    params = {
        "documentType": "A75",
        "processType": "A16",   # Realised
        "in_Domain": _GR,
        "periodStart": s,
        "periodEnd": e,
    }
    xml = _fetch_or_cache("generation", start_d, end_d, params, token, force)
    triples = _parse_generation(xml)
    if not triples:
        return pd.DataFrame(columns=["date", "mtu", "gas_mwh", "hydro_mwh",
                                      "res_mwh", "lignite_mwh"])

    # Process each PSR bucket independently so we run the hourly→15-min
    # expansion uniformly per fuel.
    df_in = pd.DataFrame(triples, columns=["t_utc", "psr", "mw"])
    df_in["bucket"] = df_in["psr"].map(_PSR_BUCKET)
    df_in = df_in.dropna(subset=["bucket"])

    pieces: list[pd.DataFrame] = []
    for bucket, sub in df_in.groupby("bucket"):
        # Sum across PSRs that map to the same bucket (e.g. hydro = B10+B11+B12)
        agg = sub.groupby("t_utc", as_index=False)["mw"].sum()
        rows = list(zip(agg["t_utc"], agg["mw"]))
        grid = _to_athens_15min(rows).rename(columns={"value": bucket})
        pieces.append(grid)

    if not pieces:
        return pd.DataFrame(columns=["date", "mtu", "gas_mwh", "hydro_mwh",
                                      "res_mwh", "lignite_mwh"])

    out = pieces[0]
    for p in pieces[1:]:
        out = out.merge(p, on=["date", "mtu"], how="outer")
    for col in ("gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh"):
        if col not in out.columns:
            out[col] = 0.0
    return out[["date", "mtu", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh"]]


def fetch_net_imports(start_d: date, end_d: date, *, token: str, force: bool = False) -> pd.DataFrame:
    """A11 cross-border physical flows summed across all 5 Greek borders.

    Net imports = Σ flow(neighbour → GR) − Σ flow(GR → neighbour).
    """
    s, e = _to_utc_period(start_d, end_d)

    def _direction(in_d: str, out_d: str, kind: str) -> pd.DataFrame:
        params = {
            "documentType": "A11",
            "in_Domain": in_d,
            "out_Domain": out_d,
            "periodStart": s,
            "periodEnd": e,
        }
        try:
            xml = _fetch_or_cache(kind, start_d, end_d, params, token, force)
        except Exception as exc:
            logger.warning("Imports fetch failed for %s: %s", kind, exc)
            return pd.DataFrame(columns=["date", "mtu", "value"])
        rows = _parse_publication(xml, value_tag="quantity")
        return _to_athens_15min(rows)

    parts: list[pd.DataFrame] = []
    for name, eic in _NEIGHBOURS.items():
        # Inbound (neighbour → GR)
        inb = _direction(eic, _GR, f"flow_in_{name}")
        if not inb.empty:
            inb["sign"] = +1
            parts.append(inb)
        # Outbound (GR → neighbour)
        out = _direction(_GR, eic, f"flow_out_{name}")
        if not out.empty:
            out["sign"] = -1
            parts.append(out)

    if not parts:
        return pd.DataFrame(columns=["date", "mtu", "imports_mwh"])
    cat = pd.concat(parts, ignore_index=True)
    cat["signed"] = cat["value"] * cat["sign"]
    net = cat.groupby(["date", "mtu"], as_index=False)["signed"].sum()
    return net.rename(columns={"signed": "imports_mwh"})


def _maybe_unzip(content: bytes) -> list[bytes]:
    """ENTSO-E unavailability responses come as ZIP archives. Returns the list
    of inner XML bytes, or [content] if not a zip.
    """
    if content[:2] == b"PK":
        out: list[bytes] = []
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".xml"):
                    out.append(zf.read(name))
        return out
    return [content]


def _parse_outages(xml_bytes: bytes) -> list[tuple[datetime, datetime, float, float]]:
    """Parse A77/A80 unavailability XML.

    Each TimeSeries = one outage notice for one production unit, with
    1+ Available_Period blocks describing how much capacity (MW) is
    AVAILABLE during sub-intervals. The unit's nominal power is in
    `nominal_P`. We return tuples of (period_start_utc, period_end_utc,
    available_mw, nominal_mw) so the caller can compute unavailable =
    nominal − available.

    Tolerant to namespace version (outagedocument:6:1, 7:0, etc.).
    """
    root = ET.fromstring(xml_bytes)
    ns = _root_ns(root)
    if not ns or "Acknowledgement" in root.tag:
        return []

    out: list[tuple[datetime, datetime, float, float]] = []
    for ts in root.findall(f"{{{ns}}}TimeSeries"):
        # Find the unit's nominal power. Field name varies across schemas.
        nom_mw = None
        for tag_suffix in (
            "nominal_Power_PowerSystemResources.nominalP",
            "resource_Power.nominalP",
            "production_RegisteredResource.pSRType.powerSystemResources.nominalP",
        ):
            for el in ts.iter():
                if el.tag.endswith("}" + tag_suffix):
                    try:
                        nom_mw = float(el.text)
                    except (TypeError, ValueError):
                        pass
                    if nom_mw is not None:
                        break
            if nom_mw is not None:
                break
        if nom_mw is None:
            nom_mw = 0.0

        for period in ts.findall(f"{{{ns}}}Available_Period"):
            ti = period.find(f"{{{ns}}}timeInterval")
            if ti is None:
                continue
            t_start = datetime.fromisoformat(
                ti.find(f"{{{ns}}}start").text.replace("Z", "+00:00"))
            t_end = datetime.fromisoformat(
                ti.find(f"{{{ns}}}end").text.replace("Z", "+00:00"))
            res_el = period.find(f"{{{ns}}}resolution")
            step = pd.Timedelta(res_el.text).to_pytimedelta() if res_el is not None else (t_end - t_start)

            for pt in period.findall(f"{{{ns}}}Point"):
                pos = int(pt.find(f"{{{ns}}}position").text)
                q_el = pt.find(f"{{{ns}}}quantity")
                if q_el is None or q_el.text is None:
                    continue
                seg_start = t_start + step * (pos - 1)
                seg_end = min(seg_start + step, t_end)
                out.append((seg_start, seg_end, float(q_el.text), nom_mw))

    return out


def _outage_grid(
    rows: list[tuple[datetime, datetime, float, float]],
) -> pd.DataFrame:
    """Map (start, end, available_mw, nominal_mw) outage segments onto a
    15-min Athens grid. Returns DataFrame with columns:
        date, mtu, mw_unavailable
    where mw_unavailable = sum_over_active_notices(nominal − available).
    A larger value means more MW are derated/offline at that 15-min slot.
    """
    if not rows:
        return pd.DataFrame(columns=["date", "mtu", "mw_unavailable"])

    rec: dict[pd.Timestamp, float] = {}
    for s, e, avail, nom in rows:
        unavail = max(0.0, (nom or 0.0) - avail)
        if unavail <= 0:
            continue
        s_local = pd.Timestamp(s).tz_convert("Europe/Athens")
        e_local = pd.Timestamp(e).tz_convert("Europe/Athens")
        cur = s_local.floor("15min")
        end = e_local.ceil("15min")
        while cur < end:
            rec[cur] = rec.get(cur, 0.0) + unavail
            cur += pd.Timedelta(minutes=15)

    if not rec:
        return pd.DataFrame(columns=["date", "mtu", "mw_unavailable"])

    df = pd.DataFrame({"t_local": list(rec.keys()),
                        "mw_unavailable": list(rec.values())})
    df["date"] = df["t_local"].dt.normalize().dt.tz_localize(None)
    df["mtu"] = df["t_local"].dt.hour * 4 + df["t_local"].dt.minute // 15
    return df.groupby(["date", "mtu"], as_index=False)["mw_unavailable"].sum()


def fetch_outages(
    start_d: date,
    end_d: date,
    *,
    token: str | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Fetch ENTSO-E unavailability of production units (A80 planned + A77 forced).

    Returns DataFrame with columns:
        date, mtu, mw_outage_avail_planned, mw_outage_avail_forced

    `mw_outage_avail_*` is the SUM of "available capacity" reported across
    all active outage notices for that 15-min slot. Lower values mean more
    units are derated. (We use available rather than unavailable because
    some notices don't report nominal power, but all notices include
    available capacity.)
    """
    tok = _resolve_token(token)
    s, e = _to_utc_period(start_d, end_d)

    out_frames: dict[str, pd.DataFrame] = {}
    for kind, doc_type, suffix in [("planned", "A80", "planned"),
                                    ("forced", "A77", "forced")]:
        params = {
            "documentType": doc_type,
            "biddingZone_Domain": _GR,
            "periodStart": s,
            "periodEnd": e,
        }
        try:
            content = _fetch_or_cache(f"outages_{kind}", start_d, end_d,
                                       params, tok, force)
        except Exception as exc:
            logger.warning("Outage %s fetch failed: %s", kind, exc)
            out_frames[suffix] = pd.DataFrame(columns=["date", "mtu",
                                                        f"mw_unavailable_{suffix}"])
            continue
        all_rows: list[tuple[datetime, datetime, float, float]] = []
        for xml in _maybe_unzip(content):
            try:
                all_rows.extend(_parse_outages(xml))
            except ET.ParseError as exc:
                logger.warning("Outage %s XML parse error: %s", kind, exc)
        df = _outage_grid(all_rows).rename(
            columns={"mw_unavailable": f"mw_unavailable_{suffix}"})
        logger.info("Outages %s: %d notices → %d 15-min slots",
                     kind, len(all_rows), len(df))
        out_frames[suffix] = df

    if not out_frames:
        return pd.DataFrame(columns=["date", "mtu",
                                       "mw_outage_avail_planned",
                                       "mw_outage_avail_forced"])

    merged = out_frames["planned"]
    for k, v in out_frames.items():
        if k == "planned":
            continue
        merged = merged.merge(v, on=["date", "mtu"], how="outer")

    merged = merged.sort_values(["date", "mtu"]).reset_index(drop=True)
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged[
        (merged["date"].dt.date >= start_d) & (merged["date"].dt.date <= end_d)
    ]
    return merged


def fetch(
    start_date: date,
    end_date: date,
    *,
    token: str | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Combined price + generation + imports DataFrame in HEnEx-parser schema.

    The query is chunked monthly to keep XML responses manageable and to
    allow per-month caching. Output columns:
        date, mtu, hour, quarter,
        mcp_eur_mwh, gas_mwh, hydro_mwh, res_mwh, lignite_mwh,
        imports_mwh, sell_total_mwh
    """
    tok = _resolve_token(token)

    # Build inclusive list of monthly chunks
    chunks: list[tuple[date, date]] = []
    cur = date(start_date.year, start_date.month, 1)
    while cur <= end_date:
        # last day of this month
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        chunk_start = max(cur, start_date)
        chunk_end = min(nxt - timedelta(days=1), end_date)
        chunks.append((chunk_start, chunk_end))
        cur = nxt

    monthly: list[pd.DataFrame] = []
    for cs, ce in chunks:
        logger.info("ENTSO-E chunk %s → %s", cs, ce)
        prices = fetch_prices(cs, ce, token=tok, force=force)
        gen = fetch_generation(cs, ce, token=tok, force=force)
        imp = fetch_net_imports(cs, ce, token=tok, force=force)

        # Outer-merge on (date, mtu); keep one row per (date, mtu)
        merged = prices.merge(gen, on=["date", "mtu"], how="outer")
        merged = merged.merge(imp, on=["date", "mtu"], how="outer")
        monthly.append(merged)

    df = pd.concat(monthly, ignore_index=True) if monthly else pd.DataFrame()
    if df.empty:
        return df

    df = df.sort_values(["date", "mtu"]).drop_duplicates(["date", "mtu"]).reset_index(drop=True)
    df["hour"] = df["mtu"] // 4
    df["quarter"] = df["mtu"] % 4
    # sell_total_mwh ≈ generation + net imports (best proxy we can compute)
    gen_cols = ["gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh"]
    df["sell_total_mwh"] = df[gen_cols].sum(axis=1, min_count=1) + df["imports_mwh"].fillna(0)

    cols = ["date", "mtu", "hour", "quarter", "mcp_eur_mwh",
            "sell_total_mwh", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh",
            "imports_mwh"]
    df = df[[c for c in cols if c in df.columns]]
    df["date"] = pd.to_datetime(df["date"])

    # Crop to requested window after Athens-local snapping (UTC padding can
    # leak rows into adjacent days).
    df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)]

    logger.info("ENTSO-E final: %d rows × %d cols, %s → %s",
                len(df), len(df.columns),
                df["date"].min().date(), df["date"].max().date())
    return df
