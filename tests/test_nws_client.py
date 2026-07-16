"""Tests for nws/client.py — NWSClient.derive_daily_high_low_times() and
NWSClient.derive_daily_high_low_times_local().

These tests exercise the pure-Python parsing logic without making any network
calls, so they run in any environment without mocking.
"""
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nws.client import NWSClient


def _utc(year: int, month: int, day: int, hour: int) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def _period(start: datetime, temp: float) -> dict:
    return {"startTime": start.isoformat(), "temperature": temp}


class TestDeriveDailyHighLowTimes:
    """Unit tests for NWSClient.derive_daily_high_low_times."""

    def _client(self) -> NWSClient:
        # user_agent not needed for pure parsing tests
        return NWSClient(user_agent="test/1.0")

    def test_returns_none_none_for_empty_periods(self):
        client = self._client()
        target = _utc(2025, 7, 4, 12)
        high, low = client.derive_daily_high_low_times([], target)
        assert high is None
        assert low is None

    def test_returns_none_none_when_no_periods_match_target_day(self):
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        # All periods are on a different day
        periods = [
            _period(_utc(2025, 7, 5, 10), 90.0),
            _period(_utc(2025, 7, 5, 14), 95.0),
        ]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high is None
        assert low is None

    def test_basic_high_and_low(self):
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        periods = [
            _period(_utc(2025, 7, 4, 6), 65.0),   # morning low
            _period(_utc(2025, 7, 4, 14), 95.0),  # afternoon high
            _period(_utc(2025, 7, 4, 20), 80.0),  # evening
        ]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high == _utc(2025, 7, 4, 14)
        assert low == _utc(2025, 7, 4, 6)

    def test_only_same_day_periods_used(self):
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        periods = [
            _period(_utc(2025, 7, 3, 23), 50.0),  # day before — excluded
            _period(_utc(2025, 7, 4, 6), 70.0),
            _period(_utc(2025, 7, 4, 15), 90.0),
            _period(_utc(2025, 7, 5, 0), 68.0),   # next day midnight — excluded
        ]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high == _utc(2025, 7, 4, 15)
        assert low == _utc(2025, 7, 4, 6)

    def test_single_period_is_both_high_and_low(self):
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        periods = [_period(_utc(2025, 7, 4, 12), 85.0)]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high == _utc(2025, 7, 4, 12)
        assert low == _utc(2025, 7, 4, 12)

    def test_tie_selects_earliest_occurrence_for_both(self):
        """On a temperature tie, the first occurrence (by time) should be used."""
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        periods = [
            _period(_utc(2025, 7, 4, 8), 80.0),
            _period(_utc(2025, 7, 4, 10), 80.0),  # same high temp, later
            _period(_utc(2025, 7, 4, 14), 60.0),
            _period(_utc(2025, 7, 4, 20), 60.0),  # same low temp, later
        ]
        high, low = client.derive_daily_high_low_times(periods, target)
        # Earliest occurrence on tie
        assert high == _utc(2025, 7, 4, 8)
        assert low == _utc(2025, 7, 4, 14)

    def test_skips_periods_missing_temperature(self):
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        periods = [
            {"startTime": _utc(2025, 7, 4, 6).isoformat(), "temperature": None},
            _period(_utc(2025, 7, 4, 14), 95.0),
        ]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high == _utc(2025, 7, 4, 14)
        assert low == _utc(2025, 7, 4, 14)

    def test_skips_periods_missing_start_time(self):
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        periods = [
            {"startTime": None, "temperature": 50.0},
            _period(_utc(2025, 7, 4, 14), 95.0),
        ]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high == _utc(2025, 7, 4, 14)
        assert low == _utc(2025, 7, 4, 14)

    def test_non_utc_offset_period_still_matches_utc_day(self):
        """Periods with a UTC offset are converted to UTC before day comparison."""
        client = self._client()
        target = _utc(2025, 7, 4, 0)
        # 2025-07-04T00:00-05:00  == 2025-07-04T05:00Z → on target day
        from datetime import timezone as tz, timedelta
        eastern = tz(timedelta(hours=-5))
        period_dt = datetime(2025, 7, 4, 0, 0, tzinfo=eastern)
        periods = [{"startTime": period_dt.isoformat(), "temperature": 72.0}]
        high, low = client.derive_daily_high_low_times(periods, target)
        assert high is not None
        assert low is not None


class TestParseIsoDt:
    def test_utc_offset_string(self):
        client = NWSClient(user_agent="test/1.0")
        dt = client._parse_iso_dt("2025-07-04T14:00:00+00:00")
        assert dt.tzinfo is timezone.utc
        assert dt.hour == 14

    def test_naive_string_assumed_utc(self):
        client = NWSClient(user_agent="test/1.0")
        dt = client._parse_iso_dt("2025-07-04T14:00:00")
        assert dt.tzinfo is timezone.utc
        assert dt.hour == 14

    def test_negative_offset_converts_to_utc(self):
        client = NWSClient(user_agent="test/1.0")
        # 2025-07-04T09:00:00-05:00 → 2025-07-04T14:00:00Z
        dt = client._parse_iso_dt("2025-07-04T09:00:00-05:00")
        assert dt.tzinfo == timezone.utc
        assert dt.hour == 14


# ---------------------------------------------------------------------------
# Tests for derive_daily_high_low_times_local (local-timezone-aware filtering)
# ---------------------------------------------------------------------------

class TestDeriveDailyHighLowTimesLocal:
    """Unit tests for NWSClient.derive_daily_high_low_times_local.

    Key scenarios:
    - Basic high/low selection within a local day.
    - UTC boundary crossing: a station near UTC midnight should use its local
      date, not the UTC date, so western-US stations at 01:00 UTC are still
      "yesterday" locally.
    - Tie-breaking (earliest occurrence wins).
    - Missing/null fields skipped.
    """

    def _client(self) -> NWSClient:
        return NWSClient(user_agent="test/1.0")

    def test_basic_local_high_and_low(self):
        """Standard case: periods span a single local day."""
        client = self._client()
        tz = ZoneInfo("America/New_York")  # UTC-4 in summer
        # now_utc = 2025-07-04T20:00Z → local = 2025-07-04T16:00 EDT (same day)
        now_utc = datetime(2025, 7, 4, 20, 0, tzinfo=timezone.utc)
        periods = [
            _period(_utc(2025, 7, 4, 10), 65.0),   # 06:00 EDT — low
            _period(_utc(2025, 7, 4, 18), 95.0),   # 14:00 EDT — high
            _period(_utc(2025, 7, 4, 22), 80.0),   # 18:00 EDT — mid
        ]
        high, low = client.derive_daily_high_low_times_local(periods, tz, now_utc)
        assert high == _utc(2025, 7, 4, 18)
        assert low == _utc(2025, 7, 4, 10)

    def test_western_station_utc_boundary_exclusion(self):
        """Periods from UTC 00:00-04:59 belong to the *previous* local day for
        a US/Pacific station (UTC-7 in summer) and must NOT appear in the
        local-day result when now_utc is early on 2025-07-05 UTC."""
        client = self._client()
        tz = ZoneInfo("America/Los_Angeles")  # UTC-7 in summer (PDT)
        # 2025-07-05T02:00Z → 2025-07-04T19:00 PDT → local date = 2025-07-04
        now_utc = datetime(2025, 7, 5, 2, 0, tzinfo=timezone.utc)
        periods = [
            # 2025-07-04 local (should be included)
            _period(datetime(2025, 7, 4, 15, 0, tzinfo=timezone.utc), 60.0),  # 08:00 PDT
            _period(datetime(2025, 7, 4, 22, 0, tzinfo=timezone.utc), 95.0),  # 15:00 PDT — high
            # 2025-07-05T00:00Z = 2025-07-04T17:00 PDT → still local July 4
            _period(datetime(2025, 7, 5, 0, 0, tzinfo=timezone.utc), 85.0),
            # 2025-07-05T07:00Z = 2025-07-05T00:00 PDT → local July 5, excluded
            _period(datetime(2025, 7, 5, 7, 0, tzinfo=timezone.utc), 55.0),
        ]
        high, low = client.derive_daily_high_low_times_local(periods, tz, now_utc)
        # Only the three periods on local 2025-07-04 should be used
        assert high == datetime(2025, 7, 4, 22, 0, tzinfo=timezone.utc)
        assert low == datetime(2025, 7, 4, 15, 0, tzinfo=timezone.utc)

    def test_eastern_station_utc_boundary_inclusion(self):
        """For a US/Eastern station, UTC 00:00-03:59 is still the prior local
        day, so a period at 00:30 UTC on July 5 falls on local July 4."""
        client = self._client()
        tz = ZoneInfo("America/New_York")  # UTC-4 in summer (EDT)
        # 2025-07-04T22:00Z → 2025-07-04T18:00 EDT → local date = 2025-07-04
        now_utc = datetime(2025, 7, 4, 22, 0, tzinfo=timezone.utc)
        periods = [
            _period(_utc(2025, 7, 4, 12), 65.0),   # 08:00 EDT — low
            _period(_utc(2025, 7, 4, 18), 90.0),   # 14:00 EDT — high
            # 2025-07-05T00:30Z = 2025-07-04T20:30 EDT → still local July 4
            _period(datetime(2025, 7, 5, 0, 30, tzinfo=timezone.utc), 78.0),
            # 2025-07-05T04:00Z = 2025-07-05T00:00 EDT → local July 5, excluded
            _period(datetime(2025, 7, 5, 4, 0, tzinfo=timezone.utc), 55.0),
        ]
        high, low = client.derive_daily_high_low_times_local(periods, tz, now_utc)
        assert high == _utc(2025, 7, 4, 18)
        assert low == _utc(2025, 7, 4, 12)

    def test_returns_none_none_for_empty_periods(self):
        client = self._client()
        tz = ZoneInfo("America/Chicago")
        now_utc = _utc(2025, 7, 4, 12)
        high, low = client.derive_daily_high_low_times_local([], tz, now_utc)
        assert high is None
        assert low is None

    def test_returns_none_none_when_no_local_day_match(self):
        """If all periods fall on a different local date, return (None, None)."""
        client = self._client()
        tz = ZoneInfo("America/Denver")  # UTC-6 in summer (MDT)
        # now_utc = 2025-07-04T18:00Z → local = 2025-07-04T12:00 MDT
        now_utc = datetime(2025, 7, 4, 18, 0, tzinfo=timezone.utc)
        # Periods are all on local July 5
        periods = [
            _period(datetime(2025, 7, 5, 12, 0, tzinfo=timezone.utc), 90.0),
        ]
        high, low = client.derive_daily_high_low_times_local(periods, tz, now_utc)
        assert high is None
        assert low is None

    def test_tie_selects_earliest_local(self):
        """On temperature tie, the earliest UTC time wins (same as UTC method)."""
        client = self._client()
        tz = ZoneInfo("America/Chicago")  # UTC-5 in summer (CDT)
        now_utc = datetime(2025, 7, 4, 18, 0, tzinfo=timezone.utc)
        periods = [
            _period(_utc(2025, 7, 4, 14), 80.0),   # first high
            _period(_utc(2025, 7, 4, 16), 80.0),   # same temp, later
            _period(_utc(2025, 7, 4, 18), 55.0),   # first low
            _period(_utc(2025, 7, 4, 20), 55.0),   # same temp, later
        ]
        high, low = client.derive_daily_high_low_times_local(periods, tz, now_utc)
        assert high == _utc(2025, 7, 4, 14)
        assert low == _utc(2025, 7, 4, 18)

    def test_skips_missing_fields(self):
        client = self._client()
        tz = ZoneInfo("America/New_York")
        now_utc = _utc(2025, 7, 4, 14)
        periods = [
            {"startTime": None, "temperature": 70.0},
            {"startTime": _utc(2025, 7, 4, 10).isoformat(), "temperature": None},
            _period(_utc(2025, 7, 4, 15), 88.0),
        ]
        high, low = client.derive_daily_high_low_times_local(periods, tz, now_utc)
        assert high == _utc(2025, 7, 4, 15)
        assert low == _utc(2025, 7, 4, 15)
