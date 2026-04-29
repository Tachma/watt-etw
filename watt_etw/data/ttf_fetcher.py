"""Fetch Dutch TTF Natural Gas front-month futures prices.

Source: Yahoo Finance via yfinance (TTF=F ticker).
TTF is quoted in EUR/MWh on ICE, so no conversion needed.
Prices are cached by year to `data/external/ttf_gas/ttf_YYYY.csv`.
Weekends and holidays are backfilled with the last known settlement.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_TICKER = "TTF=F"
_CACHE_DIR = Path("data/external/ttf_gas")


def _cache_path(year: int) -> Path:
    return _CACHE_DIR / f"ttf_{year}.csv"


def _load_cache(year: int) -> pd.DataFrame | None:
    p = _cache_path(year)
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def _fetch_year(year: int) -> pd.DataFrame:
    """Download a full calendar year from Yahoo Finance and return a daily Series."""
    try:
        import yfinance as yf
    except ImportError as e:
        raise ImportError("yfinance is required: pip install yfinance") from e

    start = f"{year}-01-01"
    end = f"{year}-12-31"
    logger.info("Fetching TTF data for %d from Yahoo Finance", year)
    ticker = yf.Ticker(_TICKER)
    hist = ticker.history(start=start, end=end, interval="1d")

    if hist.empty:
        logger.warning("No TTF data returned for %d", year)
        return pd.DataFrame(columns=["date", "ttf_eur_mwh"])

    hist = hist.reset_index()
    # Column name varies slightly across yfinance versions
    date_col = "Date" if "Date" in hist.columns else hist.columns[0]
    price_col = "Close"

    df = pd.DataFrame({
        "date": pd.to_datetime(hist[date_col]).dt.date,
        "ttf_eur_mwh": hist[price_col].values,
    })
    return df.dropna(subset=["ttf_eur_mwh"])


def _fill_calendar(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Expand to every calendar day of the year, backfilling weekends/holidays."""
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    all_days = pd.DataFrame(
        {"date": [start + timedelta(days=i) for i in range((end - start).days + 1)]}
    )
    merged = all_days.merge(df, on="date", how="left")
    merged["ttf_eur_mwh"] = merged["ttf_eur_mwh"].ffill().bfill()
    return merged


def fetch(
    start_date: date,
    end_date: date,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
) -> pd.DataFrame:
    """Return daily TTF prices for [start_date, end_date] inclusive.

    Fetches from Yahoo Finance only for years not already cached.
    Returns DataFrame with columns: date (date), ttf_eur_mwh (float).
    """
    global _CACHE_DIR
    _CACHE_DIR = Path(cache_dir)
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    years = range(start_date.year, end_date.year + 1)
    frames: list[pd.DataFrame] = []

    for year in years:
        cached = None if force else _load_cache(year)
        if cached is not None:
            logger.info("TTF %d loaded from cache (%d rows)", year, len(cached))
            frames.append(cached)
            continue

        raw = _fetch_year(year)
        filled = _fill_calendar(raw, year)
        filled.to_csv(_cache_path(year), index=False)
        logger.info("TTF %d saved to cache (%d rows)", year, len(filled))
        frames.append(filled)

    if not frames:
        return pd.DataFrame(columns=["date", "ttf_eur_mwh"])

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    mask = (combined["date"] >= start_date) & (combined["date"] <= end_date)
    return combined[mask].reset_index(drop=True)
