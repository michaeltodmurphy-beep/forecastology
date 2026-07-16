"""Tests for nws/client.py — NWSClient.derive_daily_high_low_times().

These tests exercise the pure-Python parsing logic without making any network
calls, so they run in any environment without mocking.
"""
import os
import sys
from datetime import datetime, timezone

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
