"""Unit tests for core/local_time_gate.py.

Tests cover:
- Gate disabled → always allowed
- City before threshold → blocked
- City at/after threshold → allowed
- Phoenix midnight rule (00:00 MST threshold)
- DST-sensitive city behavior (ET/PT/CT transitions)
- Unknown ticker → fail-open (allowed)
- All 40 series are mapped
"""
import datetime
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config import AppConfig
from core.local_time_gate import (
    SERIES_CITY,
    SERIES_TIMEZONE,
    get_series_prefix,
    get_series_timezone,
    is_entry_allowed,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**overrides) -> AppConfig:
    cfg = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=1,
        monitor_start_price=80,
        buy_trigger_price=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=35,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime.datetime:
    return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Mapping coverage
# ---------------------------------------------------------------------------

class TestSeriesMapping:
    def test_all_40_series_are_mapped(self):
        from core.constants import SERIES_LIST
        for series in SERIES_LIST:
            assert series in SERIES_TIMEZONE, f"{series} missing from SERIES_TIMEZONE"
            assert series in SERIES_CITY, f"{series} missing from SERIES_CITY"

    def test_phoenix_uses_america_phoenix(self):
        assert SERIES_TIMEZONE["KXHIGHTPHX"] == "America/Phoenix"
        assert SERIES_TIMEZONE["KXLOWTPHX"] == "America/Phoenix"

    def test_denver_uses_america_denver(self):
        assert SERIES_TIMEZONE["KXHIGHDEN"] == "America/Denver"
        assert SERIES_TIMEZONE["KXLOWTDEN"] == "America/Denver"

    def test_nyc_uses_america_new_york(self):
        assert SERIES_TIMEZONE["KXHIGHNY"] == "America/New_York"
        assert SERIES_TIMEZONE["KXLOWTNYC"] == "America/New_York"

    def test_chicago_uses_america_chicago(self):
        assert SERIES_TIMEZONE["KXHIGHCHI"] == "America/Chicago"
        assert SERIES_TIMEZONE["KXLOWTCHI"] == "America/Chicago"

    def test_seattle_uses_america_los_angeles(self):
        assert SERIES_TIMEZONE["KXHIGHTSEA"] == "America/Los_Angeles"
        assert SERIES_TIMEZONE["KXLOWTSEA"] == "America/Los_Angeles"


class TestGetSeriesPrefix:
    def test_extracts_prefix_from_full_ticker(self):
        assert get_series_prefix("KXLOWTBOS-26JUN25-B52.5") == "KXLOWTBOS"
        assert get_series_prefix("KXHIGHLAX-26JUN25-T87") == "KXHIGHLAX"
        assert get_series_prefix("KXHIGHTPHX-26JUN25-T110") == "KXHIGHTPHX"


class TestGetSeriesTimezone:
    def test_known_series_returns_tz(self):
        tz = get_series_timezone("KXLOWTBOS-26JUN25-B52.5")
        assert tz == "America/New_York"

    def test_unknown_series_returns_none(self):
        tz = get_series_timezone("KXUNKNOWN-26JUN25-T99")
        assert tz is None


# ---------------------------------------------------------------------------
# Gate disabled
# ---------------------------------------------------------------------------

class TestGateDisabled:
    def test_gate_disabled_always_allows(self):
        cfg = _make_config(enable_local_settle_gate=False)
        # Well before any threshold
        now = _utc(2025, 6, 25, 4, 0)  # 00:00 ET = blocked normally
        allowed, ctx = is_entry_allowed("KXLOWTNYC-25JUN25-B72", cfg, now_utc=now)
        assert allowed is True
        assert ctx == {}

    def test_gate_disabled_phoenix_always_allows(self):
        cfg = _make_config(enable_local_settle_gate=False)
        now = _utc(2025, 6, 25, 5, 59)  # 22:59 Phoenix = blocked normally
        allowed, ctx = is_entry_allowed("KXHIGHTPHX-25JUN25-T110", cfg, now_utc=now)
        assert allowed is True
        assert ctx == {}


# ---------------------------------------------------------------------------
# Default threshold (01:00 local) — Eastern Time examples
# ---------------------------------------------------------------------------

class TestDefaultThresholdET:
    """New York City uses America/New_York (ET, with DST)."""

    NYC_TICKER = "KXLOWTNYC-25JUN25-B72"

    def test_before_threshold_blocked(self):
        """00:59 ET → blocked."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        # In summer (EDT = UTC-4): 04:59 UTC = 00:59 EDT
        now = _utc(2025, 6, 25, 4, 59)
        allowed, ctx = is_entry_allowed(self.NYC_TICKER, cfg, now_utc=now)
        assert allowed is False
        assert ctx["timezone"] == "America/New_York"
        assert ctx["threshold"] == "01:00"
        assert ctx["city"] == "New York City"
        assert ctx["ticker"] == self.NYC_TICKER

    def test_at_threshold_allowed(self):
        """01:00 ET → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        # 05:00 UTC = 01:00 EDT (summer, UTC-4)
        now = _utc(2025, 6, 25, 5, 0)
        allowed, ctx = is_entry_allowed(self.NYC_TICKER, cfg, now_utc=now)
        assert allowed is True

    def test_after_threshold_allowed(self):
        """13:30 ET → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        # 17:30 UTC = 13:30 EDT
        now = _utc(2025, 6, 25, 17, 30)
        allowed, ctx = is_entry_allowed(self.NYC_TICKER, cfg, now_utc=now)
        assert allowed is True

    def test_midnight_exact_blocked(self):
        """00:00 ET → blocked (below 01:00 threshold)."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        # 04:00 UTC = 00:00 EDT
        now = _utc(2025, 6, 25, 4, 0)
        allowed, _ = is_entry_allowed(self.NYC_TICKER, cfg, now_utc=now)
        assert allowed is False


# ---------------------------------------------------------------------------
# Phoenix-specific threshold (00:00 local = midnight)
# ---------------------------------------------------------------------------

class TestPhoenixThreshold:
    """Phoenix uses America/Phoenix (MST, UTC-7, no DST year-round).

    With phoenix_entry_start_local="00:00", the threshold is midnight.
    Any time >= 00:00 in Phoenix is allowed — there is no blocked window.
    The meaningful test is that Phoenix uses its own (lower) threshold
    compared to other MT cities like Denver.
    """

    PHX_TICKER = "KXHIGHTPHX-25JUN25-T110"

    def test_phoenix_midnight_is_allowed(self):
        """00:00 Phoenix (= UTC 07:00) → allowed (at threshold)."""
        cfg = _make_config(
            enable_local_settle_gate=True,
            default_entry_start_local="01:00",
            phoenix_entry_start_local="00:00",
        )
        # Phoenix is UTC-7 always: 07:00 UTC = 00:00 MST
        now = _utc(2025, 6, 25, 7, 0)
        allowed, ctx = is_entry_allowed(self.PHX_TICKER, cfg, now_utc=now)
        assert allowed is True
        assert ctx["timezone"] == "America/Phoenix"
        assert ctx["threshold"] == "00:00"
        assert ctx["city"] == "Phoenix"

    def test_phoenix_after_midnight_is_allowed(self):
        """00:30 Phoenix → allowed."""
        cfg = _make_config(
            enable_local_settle_gate=True,
            default_entry_start_local="01:00",
            phoenix_entry_start_local="00:00",
        )
        now = _utc(2025, 6, 25, 7, 30)  # 00:30 MST
        allowed, ctx = is_entry_allowed(self.PHX_TICKER, cfg, now_utc=now)
        assert allowed is True

    def test_phoenix_allowed_where_denver_is_blocked(self):
        """In winter (both MST=UTC-7): 00:30 local → Denver blocked (01:00 threshold),
        Phoenix allowed (00:00 threshold)."""
        cfg = _make_config(
            enable_local_settle_gate=True,
            default_entry_start_local="01:00",
            phoenix_entry_start_local="00:00",
        )
        # Winter: Denver=MST=UTC-7, Phoenix=MST=UTC-7 — same offset.
        # 07:30 UTC = 00:30 for BOTH Denver and Phoenix.
        now = _utc(2025, 1, 15, 7, 30)

        phx_allowed, phx_ctx = is_entry_allowed(self.PHX_TICKER, cfg, now_utc=now)
        den_allowed, den_ctx = is_entry_allowed("KXHIGHDEN-25JAN15-T35", cfg, now_utc=now)

        assert phx_allowed is True, "Phoenix (00:00 threshold) should allow at 00:30"
        assert den_allowed is False, "Denver (01:00 threshold) should block at 00:30 MT"
        assert phx_ctx["timezone"] == "America/Phoenix"
        assert den_ctx["timezone"] == "America/Denver"

    def test_phoenix_threshold_uses_mst_not_mdt(self):
        """Phoenix is always UTC-7 (MST), even in summer when Denver shifts to MDT (UTC-6)."""
        cfg = _make_config(
            enable_local_settle_gate=True,
            default_entry_start_local="01:00",
            phoenix_entry_start_local="00:00",
        )
        # In summer, Denver = MDT = UTC-6.  Phoenix = MST = UTC-7.
        # 06:30 UTC:
        #   Denver:  06:30 - 6h = 00:30 MDT  → blocked (< 01:00)
        #   Phoenix: 06:30 - 7h = 23:30 MST  → allowed (23:30 >= 00:00)
        now_summer = _utc(2025, 7, 4, 6, 30)
        phx_allowed, _ = is_entry_allowed(self.PHX_TICKER, cfg, now_utc=now_summer)
        den_allowed, _ = is_entry_allowed("KXHIGHDEN-25JUL04-T85", cfg, now_utc=now_summer)
        assert phx_allowed is True
        assert den_allowed is False


# ---------------------------------------------------------------------------
# DST transitions — ET (America/New_York) spring forward / fall back
# ---------------------------------------------------------------------------

class TestDSTTransitions:
    """Validate that DST shifts are handled correctly via zoneinfo."""

    BOS_TICKER = "KXHIGHTBOS-25MAR09-T55"

    def test_et_standard_time_winter(self):
        """In winter (EST = UTC-5): 06:00 UTC = 01:00 EST → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        now = _utc(2025, 1, 15, 6, 0)  # 01:00 EST
        allowed, ctx = is_entry_allowed(self.BOS_TICKER, cfg, now_utc=now)
        assert allowed is True
        assert "America/New_York" in ctx["timezone"]

    def test_et_standard_time_winter_before_threshold(self):
        """In winter (EST = UTC-5): 05:59 UTC = 00:59 EST → blocked."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        now = _utc(2025, 1, 15, 5, 59)  # 00:59 EST
        allowed, _ = is_entry_allowed(self.BOS_TICKER, cfg, now_utc=now)
        assert allowed is False

    def test_et_daylight_time_summer(self):
        """In summer (EDT = UTC-4): 05:00 UTC = 01:00 EDT → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        now = _utc(2025, 7, 4, 5, 0)  # 01:00 EDT
        allowed, _ = is_entry_allowed(self.BOS_TICKER, cfg, now_utc=now)
        assert allowed is True

    def test_et_daylight_time_summer_before_threshold(self):
        """In summer (EDT = UTC-4): 04:59 UTC = 00:59 EDT → blocked."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        now = _utc(2025, 7, 4, 4, 59)  # 00:59 EDT
        allowed, _ = is_entry_allowed(self.BOS_TICKER, cfg, now_utc=now)
        assert allowed is False

    def test_pt_daylight_time_summer(self):
        """In summer (PDT = UTC-7): 08:00 UTC = 01:00 PDT → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        sea_ticker = "KXHIGHTSEA-25JUL04-T80"
        now = _utc(2025, 7, 4, 8, 0)  # 01:00 PDT
        allowed, ctx = is_entry_allowed(sea_ticker, cfg, now_utc=now)
        assert allowed is True
        assert ctx["timezone"] == "America/Los_Angeles"

    def test_pt_standard_time_winter(self):
        """In winter (PST = UTC-8): 09:00 UTC = 01:00 PST → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        sea_ticker = "KXHIGHTSEA-25DEC01-T50"
        now = _utc(2025, 12, 1, 9, 0)  # 01:00 PST
        allowed, _ = is_entry_allowed(sea_ticker, cfg, now_utc=now)
        assert allowed is True

    def test_ct_daylight_time_summer(self):
        """In summer (CDT = UTC-5): 06:00 UTC = 01:00 CDT → allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="01:00")
        chi_ticker = "KXHIGHCHI-25JUL04-T80"
        now = _utc(2025, 7, 4, 6, 0)
        allowed, ctx = is_entry_allowed(chi_ticker, cfg, now_utc=now)
        assert allowed is True
        assert ctx["timezone"] == "America/Chicago"


# ---------------------------------------------------------------------------
# Unknown ticker — fail-open behaviour
# ---------------------------------------------------------------------------

class TestUnknownTicker:
    def test_unknown_ticker_is_allowed(self):
        """An unrecognised series prefix must not block entry (fail-open)."""
        cfg = _make_config(enable_local_settle_gate=True)
        allowed, ctx = is_entry_allowed("KXOTHER-25JUN25-T99", cfg)
        assert allowed is True
        assert ctx == {}


# ---------------------------------------------------------------------------
# Configurable threshold override
# ---------------------------------------------------------------------------

class TestCustomThreshold:
    NYC_TICKER = "KXLOWTNYC-25JUN25-B72"

    def test_custom_threshold_02_00_blocks_at_01_30(self):
        """If threshold is 02:00, then 01:30 ET must be blocked."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="02:00")
        # 05:30 UTC = 01:30 EDT (summer)
        now = _utc(2025, 6, 25, 5, 30)
        allowed, ctx = is_entry_allowed(self.NYC_TICKER, cfg, now_utc=now)
        assert allowed is False
        assert ctx["threshold"] == "02:00"

    def test_custom_threshold_02_00_allows_at_02_00(self):
        """If threshold is 02:00, then 02:00 ET must be allowed."""
        cfg = _make_config(enable_local_settle_gate=True, default_entry_start_local="02:00")
        # 06:00 UTC = 02:00 EDT
        now = _utc(2025, 6, 25, 6, 0)
        allowed, _ = is_entry_allowed(self.NYC_TICKER, cfg, now_utc=now)
        assert allowed is True
