from __future__ import annotations

import sys
import types
from datetime import date

import pytest

pd = pytest.importorskip("pandas")

from watt_etw.data import carbon_fetcher


class _FakeTicker:
    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def history(self, **_kwargs):
        return self._frame


def _install_fake_yfinance(monkeypatch, frame: pd.DataFrame) -> dict[str, int]:
    calls = {"n": 0}

    def _ticker_factory(symbol: str):
        calls["n"] += 1
        return _FakeTicker(frame)

    fake_module = types.ModuleType("yfinance")
    fake_module.Ticker = _ticker_factory
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)
    return calls


def _yahoo_frame() -> pd.DataFrame:
    idx = pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"])
    return pd.DataFrame({"Close": [70.0, 71.5, 72.25]}, index=idx).rename_axis("Date")


def test_fetch_returns_calendar_filled_daily_series(tmp_path, monkeypatch):
    calls = _install_fake_yfinance(monkeypatch, _yahoo_frame())

    df = carbon_fetcher.fetch(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
        ticker="FAKE",
        cache_dir=tmp_path,
    )

    assert list(df.columns) == ["date", "eua_eur_t"]
    assert len(df) == 7  # one row per calendar day
    # Weekend (Jan 4–5) should backfill from Friday's settlement (71.5)
    sat = df.loc[df["date"] == date(2025, 1, 4), "eua_eur_t"].iloc[0]
    sun = df.loc[df["date"] == date(2025, 1, 5), "eua_eur_t"].iloc[0]
    assert sat == 71.5
    assert sun == 71.5
    # Jan 1 has no Yahoo row before it; backfill from Jan 2 (70.0)
    assert df.loc[df["date"] == date(2025, 1, 1), "eua_eur_t"].iloc[0] == 70.0
    assert calls["n"] == 1


def test_fetch_uses_cache_on_second_call(tmp_path, monkeypatch):
    calls = _install_fake_yfinance(monkeypatch, _yahoo_frame())

    carbon_fetcher.fetch(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
        ticker="FAKE",
        cache_dir=tmp_path,
    )
    carbon_fetcher.fetch(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
        ticker="FAKE",
        cache_dir=tmp_path,
    )

    assert calls["n"] == 1, "second fetch should hit the on-disk cache"


def test_fetch_returns_empty_when_yahoo_has_no_data(tmp_path, monkeypatch):
    _install_fake_yfinance(monkeypatch, pd.DataFrame())

    df = carbon_fetcher.fetch(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
        ticker="EMPTY",
        cache_dir=tmp_path,
    )
    assert df.empty
    assert list(df.columns) == ["date", "eua_eur_t"]


def test_ticker_resolves_from_env_var(monkeypatch):
    monkeypatch.setenv("WATT_EUA_TICKER", "FROM_ENV")
    assert carbon_fetcher._resolve_ticker(None) == "FROM_ENV"
    assert carbon_fetcher._resolve_ticker("explicit") == "explicit"
