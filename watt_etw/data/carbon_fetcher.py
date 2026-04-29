"""Fetch EU ETS carbon allowance (EUA) prices.

Source: Yahoo Finance via yfinance. Yahoo does not expose a fully reliable
continuous front-month EEX EUA future, so the default ticker is `KRBN`
(KraneShares Global Carbon Strategy ETF), which tracks EUA + CCA + RGGI
futures and provides a daily proxy when no licensed EEX feed is available.

Override the ticker for a real EUA series:
    - constructor arg `ticker=`
    - env var WATT_EUA_TICKER

The output column is `eua_eur_t` for downstream feature consistency. When
using a non-EUR ticker the values are in the ticker's quote currency; the
column name is preserved so the forecaster sees a stable feature name.

Prices are cached by year to `data/external/eua/eua_YYYY.csv`. Weekends and
holidays are forward/backfilled from the nearest settlement, matching the
TTF fetcher pattern so a daily series can be broadcast to hourly features.
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_TICKER = "KRBN"
_TICKER_ENV = "WATT_EUA_TICKER"
_CACHE_DIR = Path("data/external/eua")


def _resolve_ticker(ticker: str | None) -> str:
    if ticker:
        return ticker
    return os.environ.get(_TICKER_ENV, _DEFAULT_TICKER)


def _cache_path(year: int, ticker: str) -> Path:
    safe = ticker.replace("=", "_").replace(".", "_").replace("^", "")
    return _CACHE_DIR / f"eua_{safe}_{year}.csv"


def _load_cache(year: int, ticker: str) -> pd.DataFrame | None:
    p = _cache_path(year, ticker)
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def _fetch_year(year: int, ticker: str) -> pd.DataFrame:
    """Download a full calendar year from Yahoo Finance."""
    try:
        import yfinance as yf
    except ImportError as e:
        raise ImportError("yfinance is required: pip install yfinance") from e

    start = f"{year}-01-01"
    end = f"{year}-12-31"
    logger.info("Fetching EUA proxy %s for %d from Yahoo Finance", ticker, year)
    hist = yf.Ticker(ticker).history(start=start, end=end, interval="1d")

    if hist.empty:
        logger.warning("No EUA data returned for %s in %d", ticker, year)
        return pd.DataFrame(columns=["date", "eua_eur_t"])

    hist = hist.reset_index()
    date_col = "Date" if "Date" in hist.columns else hist.columns[0]
    df = pd.DataFrame({
        "date": pd.to_datetime(hist[date_col]).dt.date,
        "eua_eur_t": hist["Close"].values,
    })
    return df.dropna(subset=["eua_eur_t"])


def _fill_calendar(df: pd.DataFrame, year: int) -> pd.DataFrame:
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    all_days = pd.DataFrame(
        {"date": [start + timedelta(days=i) for i in range((end - start).days + 1)]}
    )
    merged = all_days.merge(df, on="date", how="left")
    merged["eua_eur_t"] = merged["eua_eur_t"].ffill().bfill()
    return merged


def fetch(
    start_date: date,
    end_date: date,
    ticker: str | None = None,
    cache_dir: str | Path = _CACHE_DIR,
    force: bool = False,
) -> pd.DataFrame:
    """Return daily EUA prices for [start_date, end_date] inclusive.

    Returns DataFrame with columns: date (date), eua_eur_t (float).
    Empty DataFrame if the chosen ticker returned no data for any year.
    """
    global _CACHE_DIR
    _CACHE_DIR = Path(cache_dir)
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    resolved = _resolve_ticker(ticker)
    years = range(start_date.year, end_date.year + 1)
    frames: list[pd.DataFrame] = []

    for year in years:
        cached = None if force else _load_cache(year, resolved)
        if cached is not None:
            logger.info("EUA %s %d loaded from cache (%d rows)", resolved, year, len(cached))
            frames.append(cached)
            continue

        raw = _fetch_year(year, resolved)
        filled = _fill_calendar(raw, year) if not raw.empty else raw
        if not filled.empty:
            filled.to_csv(_cache_path(year, resolved), index=False)
            logger.info("EUA %s %d saved to cache (%d rows)", resolved, year, len(filled))
        frames.append(filled)

    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=["date", "eua_eur_t"])

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    mask = (combined["date"] >= start_date) & (combined["date"] <= end_date)
    return combined[mask].reset_index(drop=True)
