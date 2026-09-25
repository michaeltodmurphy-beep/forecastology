"""Tests for the per-city TIGHTER sunrise spread override.

SUNRISE_MAX_SPREAD_TIGHT, when > 0, applies to tickers whose series prefix is
listed in SUNRISE_MAX_SPREAD_TIGHT_CITIES, and ONLY during the sunrise band.
All other cities keep SUNRISE_MAX_SPREAD; midam/pm bands are unaffected.
"""
import datetime
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config import AppConfig
from core.state_machine import (
    _ticker_series_prefix,
    get_max_spread_for_entry,
)


def _make_config(**overrides) -> AppConfig:
    cfg = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="mysql://localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=1,
        monitor_start_price=80,
        buy_trigger_price_low=82,
        buy_trigger_price_high=82,
        spread_monitor_price=90,
        sunrise_max_spread=50,
        midam_max_spread=40,
        pm_max_spread=30,
        stop_loss_price=35,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _utc(year, month, day, hour, minute=0):
    return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.timezone.utc)


# KXLOWTLV = Las Vegas -> America/Los_Angeles (UTC-7 in summer).
# 14:00 UTC on 2026-09-25 == 07:00 local (sunrise band).
def _lv_sunrise_utc():
    return _utc(2026, 9, 25, 14, 0)


# 18:00 UTC on 2026-09-25 == 11:00 local (midam band).
def _lv_midam_utc():
    return _utc(2026, 9, 25, 18, 0)


# 21:00 UTC on 2026-09-25 == 14:00 local (pm band).
def _lv_pm_utc():
    return _utc(2026, 9, 25, 21, 0)


TICKER_LV = "KXLOWTLV-26SEP25-B72.5"
TICKER_NYC = "KXLOWTNYC-26SEP25-B72.5"  # New York -> Eastern


def test_ticker_series_prefix_lowercased():
    assert _ticker_series_prefix("KXLOWTLV-26SEP25-B72.5") == "kxlowtlv"
    assert _ticker_series_prefix("kxlowtlv-26sep25-b72.5") == "kxlowtlv"


def test_tight_applies_in_sunrise_band_for_listed_city():
    cfg = _make_config(
        sunrise_max_spread_tight=20,
        sunrise_max_spread_tight_cities={"kxlowtlv"},
    )
    spread, band = get_max_spread_for_entry(cfg, TICKER_LV, _lv_sunrise_utc())
    assert (spread, band) == (20, "sunrise")


def test_non_listed_city_uses_global_sunrise_in_sunrise_band():
    cfg = _make_config(
        sunrise_max_spread_tight=20,
        sunrise_max_spread_tight_cities={"kxlowtlv"},
    )
    spread, band = get_max_spread_for_entry(cfg, TICKER_NYC, _lv_sunrise_utc())
    # NYC local time at 14:00 UTC is 10:00 -> midam band, so to test the
    # SUNRISE band for a non-listed city we use an NYC-sunrise instant.
    # At 11:00 UTC NYC is 07:00 local (sunrise).
    spread2, band2 = get_max_spread_for_entry(cfg, TICKER_NYC, _utc(2026, 9, 25, 11, 0))
    assert (spread2, band2) == (50, "sunrise")


def test_tight_does_not_leak_into_midam_band():
    cfg = _make_config(
        sunrise_max_spread_tight=20,
        sunrise_max_spread_tight_cities={"kxlowtlv"},
    )
    spread, band = get_max_spread_for_entry(cfg, TICKER_LV, _lv_midam_utc())
    assert (spread, band) == (40, "midam")


def test_tight_does_not_leak_into_pm_band():
    cfg = _make_config(
        sunrise_max_spread_tight=20,
        sunrise_max_spread_tight_cities={"kxlowtlv"},
    )
    spread, band = get_max_spread_for_entry(cfg, TICKER_LV, _lv_pm_utc())
    assert (spread, band) == (30, "pm")


def test_tight_zero_disabled_uses_global():
    cfg = _make_config(
        sunrise_max_spread_tight=0,
        sunrise_max_spread_tight_cities={"kxlowtlv"},
    )
    spread, band = get_max_spread_for_entry(cfg, TICKER_LV, _lv_sunrise_utc())
    assert (spread, band) == (50, "sunrise")


def test_tight_set_but_no_cities_uses_global():
    cfg = _make_config(
        sunrise_max_spread_tight=20,
        sunrise_max_spread_tight_cities=set(),
    )
    spread, band = get_max_spread_for_entry(cfg, TICKER_LV, _lv_sunrise_utc())
    assert (spread, band) == (50, "sunrise")


def test_multiple_cities():
    cfg = _make_config(
        sunrise_max_spread_tight=15,
        sunrise_max_spread_tight_cities={"kxlowtlv", "kxlowtchi"},
    )
    chi_ticker = "KXLOWTCHI-26SEP25-B58.5"  # Chicago -> Central
    # 12:00 UTC Chicago == 07:00 local (sunrise).
    spread, band = get_max_spread_for_entry(cfg, chi_ticker, _utc(2026, 9, 25, 12, 0))
    assert (spread, band) == (15, "sunrise")


def test_config_parses_tight_values(monkeypatch):
    os.environ["SUNRISE_MAX_SPREAD_TIGHT"] = "0.20"
    os.environ["SUNRISE_MAX_SPREAD_TIGHT_CITIES"] = " kxlowtlv , kxlowtchi "
    os.environ.setdefault("KALSHI_API_KEY", "k")
    os.environ.setdefault("KALSHI_PRIVATE_KEY_PATH", "p.pem")
    os.environ.setdefault("MYSQL_DATABASE_URL", "mysql://localhost/db")
    os.environ.setdefault("STOP_LOSS_PRICE_ASK", "0.35")
    os.environ.setdefault("INITIAL_CONTRACT_COUNT", "1")
    os.environ.setdefault("MONITOR_START_PRICE", "0.80")
    os.environ.setdefault("BUY_TRIGGER_PRICE_LOW", "0.82")
    os.environ.setdefault("BUY_TRIGGER_PRICE_HIGH", "0.82")
    os.environ.setdefault("SPREAD_MONITOR_PRICE", "0.90")
    os.environ.setdefault("SUNRISE_MAX_SPREAD", "0.50")
    os.environ.setdefault("MIDAM_MAX_SPREAD", "0.05")
    os.environ.setdefault("PM_MAX_SPREAD", "0.06")
    cfg = AppConfig.from_env()
    assert cfg.sunrise_max_spread_tight == 20
    assert cfg.sunrise_max_spread_tight_cities == {"kxlowtlv", "kxlowtchi"}
