"""Load Dutch TTF Natural Gas futures prices.

Primary source: the pre-downloaded Investing.com CSV at
  data/external/ttf_gas/Dutch TTF Natural Gas Futures Historical Data.csv
  Format: "Date","Price","Open","High","Low","Vol.","Change %"
  Date format: MM/DD/YYYY  (most-recent-first)

Fallback: yfinance (TTF=F ticker) for any missing dates.

Weekends and public holidays are backfilled with the last known trading price.
Output: daily DataFrame with columns (date: date, ttf_eur_mwh: float).
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path("data/external/ttf_gas")
_EXISTING_CSV = _CACHE_DIR / "Dutch TTF Natural Gas Futures Historical Data.csv"


def _read_investing_csv(path: Path) -> pd.DataFrame:
    """Parse Investing.com-style CSV (MM/DD/YYYY, newest first)."""
    text = path.read_text(encoding="utf-8-sig")  # strip BOM if present
    reader = csv.DictReader(io.StringIO(text))

    rows = []
    for row in reader:
        raw_date = row.get("Date", "").strip().strip('"')
        raw_price = row.get("Price", "").strip().strip('"').replace(",", "")
        try:
            d = date(int(raw_date[6:10]), int(raw_date[0:2]), int(raw_date[3:5]))
            price = float(raw_price)
            rows.append({"date": d, "ttf_eur_mwh": price})
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values("date").reset_index(drop=True)


def _fill_calendar(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Expand to every calendar day, backfilling weekends/holidays with last price."""
    all_days = pd.DataFrame(
        {"date": [start + timedelta(days=i) for i in range((end - start).days + 1)]}
    )
    all_days["date"] = pd.to_datetime(all_days["date"]).dt.date
    df["date"] = pd.to_datetime(df["date"]).dt.date
    merged = all_days.merge(df, on="date", how="left")
    merged["ttf_eur_mwh"] = merged["ttf_eur_mwh"].ffill().bfill()
    return merged


def _fetch_yfinance(start: date, end: date) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — cannot fetch missing TTF dates")
        return pd.DataFrame(columns=["date", "ttf_eur_mwh"])

    logger.info("Fetching TTF from Yahoo Finance for %s → %s", start, end)
    hist = yf.Ticker("TTF=F").history(
        start=start.isoformat(), end=end.isoformat(), interval="1d"
    )
    if hist.empty:
        return pd.DataFrame(columns=["date", "ttf_eur_mwh"])

    hist = hist.reset_index()
    date_col = "Date" if "Date" in hist.columns else hist.columns[0]
    return pd.DataFrame({
        "date": pd.to_datetime(hist[date_col]).dt.date,
        "ttf_eur_mwh": hist["Close"].values,
    }).dropna()


def load(
    start_date: date,
    end_date: date,
    cache_dir: str | Path = _CACHE_DIR,
) -> pd.DataFrame:
    """Return daily TTF prices for [start_date, end_date] inclusive.

    Reads from the pre-downloaded CSV first; falls back to yfinance for
    any dates not covered.

    Returns DataFrame: date (date), ttf_eur_mwh (float).
    """
    cache_dir = Path(cache_dir)
    existing_csv = cache_dir / "Dutch TTF Natural Gas Futures Historical Data.csv"

    known: pd.DataFrame = pd.DataFrame(columns=["date", "ttf_eur_mwh"])

    if existing_csv.exists():
        known = _read_investing_csv(existing_csv)
        logger.info(
            "Loaded %d TTF prices from CSV (%s → %s)",
            len(known),
            known["date"].min() if len(known) else "n/a",
            known["date"].max() if len(known) else "n/a",
        )

    # Check coverage
    covered_dates = set(known["date"].tolist())
    needed = {start_date + timedelta(days=i)
              for i in range((end_date - start_date).days + 1)}
    # Only fetch trading days (Mon-Fri) that are missing
    missing_trading = sorted(
        d for d in needed if d not in covered_dates and d.weekday() < 5
    )

    if missing_trading:
        logger.info(
            "%d trading dates not in CSV — fetching from yfinance",
            len(missing_trading),
        )
        fetch_start = missing_trading[0]
        fetch_end = missing_trading[-1]
        extra = _fetch_yfinance(fetch_start, fetch_end)
        if not extra.empty:
            known = pd.concat([known, extra], ignore_index=True)
            known = known.drop_duplicates("date").sort_values("date")

    # Fill calendar (weekends/holidays → forward-fill)
    filled = _fill_calendar(known, start_date, end_date)

    mask = (filled["date"] >= start_date) & (filled["date"] <= end_date)
    result = filled[mask].reset_index(drop=True)

    logger.info(
        "TTF: %d days, range %s → %s, price range %.2f–%.2f EUR/MWh",
        len(result),
        result["date"].min() if len(result) else "n/a",
        result["date"].max() if len(result) else "n/a",
        result["ttf_eur_mwh"].min() if len(result) else 0,
        result["ttf_eur_mwh"].max() if len(result) else 0,
    )
    return result
