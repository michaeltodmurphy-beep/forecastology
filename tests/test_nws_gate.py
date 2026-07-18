"""Tests for nws/gate.py — is_trading_gate_open().

Uses an in-memory SQLite database (via SQLAlchemy) to avoid requiring a real
MySQL server; this mirrors the approach used elsewhere in the test suite.
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.models import Base, StationForecast


# ---------------------------------------------------------------------------
# SQLite-backed in-memory session factory
# ---------------------------------------------------------------------------

def _make_sqlite_engine():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)
    return engine


def _make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def _insert_forecast(
    session,
    station_code: str,
    forecast_date: datetime,
    high_time: datetime | None,
    low_time: datetime | None,
    _counter: list = [0],
) -> StationForecast:
    _counter[0] += 1
    row = StationForecast(
        id=_counter[0],
        station_code=station_code,
        forecast_date_utc=forecast_date,
        high_time_utc=high_time,
        low_time_utc=low_time,
        updated_at=datetime.now(timezone.utc),
    )
    session.add(row)
    session.commit()
    return row


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTradingGateOpen:
    """Tests for is_trading_gate_open with a mock SQLite DB session."""

    # Patch the gate module's get_session so it uses our in-memory SQLite DB
    def _gate_open(self, station_code: str, current_time: datetime, session):
        """Call is_trading_gate_open with a mocked get_session."""
        from contextlib import contextmanager

        @contextmanager
        def mock_get_session():
            yield session

        with patch("nws.gate.get_session", mock_get_session):
            from nws.gate import is_trading_gate_open
            return is_trading_gate_open(station_code, current_time)

    def setup_method(self):
        from nws.client import _station_cache

        _station_cache.clear()
        self.engine = _make_sqlite_engine()
        self.Session = _make_session_factory(self.engine)
        self.session = self.Session()

    def teardown_method(self):
        self.session.close()

    # -----------------------------------------------------------------------
    # No data → gate closed
    # -----------------------------------------------------------------------

    def test_no_forecast_returns_false(self):
        result = self._gate_open("KATL", _utc(2025, 7, 4, 14), self.session)
        assert result is False

    # -----------------------------------------------------------------------
    # Low window tests
    # -----------------------------------------------------------------------

    def test_inside_low_window_returns_true(self):
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0), None, low_time
        )
        # 30 minutes before low (default GATE_LOW_BEFORE=120)
        now = low_time - timedelta(minutes=30)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KATL", now, self.session)
        assert result is True

    def test_before_low_window_returns_false(self):
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0), None, low_time
        )
        # 121 minutes before low — outside the 120-minute window
        now = low_time - timedelta(minutes=121)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KATL", now, self.session)
        assert result is False

    def test_after_low_window_returns_false(self):
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0), None, low_time
        )
        # 46 minutes after low — outside the 45-minute window
        now = low_time + timedelta(minutes=46)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KATL", now, self.session)
        assert result is False

    def test_at_low_open_boundary_returns_true(self):
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0), None, low_time
        )
        now = low_time - timedelta(minutes=120)  # exactly at boundary
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KATL", now, self.session)
        assert result is True

    def test_at_low_close_boundary_returns_true(self):
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0), None, low_time
        )
        now = low_time + timedelta(minutes=45)  # exactly at close boundary
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KATL", now, self.session)
        assert result is True

    # -----------------------------------------------------------------------
    # High window tests
    # -----------------------------------------------------------------------

    def test_inside_high_window_returns_true(self):
        high_time = _utc(2025, 7, 4, 14, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 4, 0), high_time, None
        )
        now = high_time - timedelta(minutes=30)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KBOS", now, self.session)
        assert result is True

    def test_before_high_window_returns_false(self):
        high_time = _utc(2025, 7, 4, 14, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 4, 0), high_time, None
        )
        now = high_time - timedelta(minutes=61)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KBOS", now, self.session)
        assert result is False

    def test_after_high_window_returns_false(self):
        high_time = _utc(2025, 7, 4, 14, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 4, 0), high_time, None
        )
        now = high_time + timedelta(minutes=31)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KBOS", now, self.session)
        assert result is False

    # -----------------------------------------------------------------------
    # Both windows present
    # -----------------------------------------------------------------------

    def test_inside_low_window_when_both_present(self):
        high_time = _utc(2025, 7, 4, 14, 0)
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KDFW",
            _utc(2025, 7, 4, 0), high_time, low_time
        )
        now = low_time - timedelta(minutes=30)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KDFW", now, self.session)
        assert result is True

    def test_outside_both_windows_returns_false(self):
        high_time = _utc(2025, 7, 4, 14, 0)
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KDFW",
            _utc(2025, 7, 4, 0), high_time, low_time
        )
        # Midday between the two windows
        now = _utc(2025, 7, 4, 11, 0)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KDFW", now, self.session)
        assert result is False

    # -----------------------------------------------------------------------
    # Naive datetime input
    # -----------------------------------------------------------------------

    def test_naive_utc_input_treated_as_utc(self):
        low_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2025, 7, 4, 0), None, low_time
        )
        # Pass naive datetime (no tzinfo)
        now_naive = datetime(2025, 7, 4, 6, 0)  # naive = no tzinfo
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KLAX", now_naive, self.session)
        assert result is True

    # -----------------------------------------------------------------------
    # Latest row is used (not an old date)
    # -----------------------------------------------------------------------

    def test_uses_most_recent_forecast(self):
        # Insert an old forecast with different times
        _insert_forecast(
            self.session, "KMIA",
            _utc(2025, 7, 3, 0),
            high_time=_utc(2025, 7, 3, 20, 0),
            low_time=_utc(2025, 7, 3, 4, 0),
        )
        # Insert today's forecast
        today_low = _utc(2025, 7, 4, 5, 0)
        _insert_forecast(
            self.session, "KMIA",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 15, 0),
            low_time=today_low,
        )
        # Time is inside today's low window
        now = today_low - timedelta(minutes=10)
        with patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KMIA", now, self.session)
        assert result is True

    def test_stale_previous_trading_day_returns_false(self):
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 3, 0),
            high_time=_utc(2025, 7, 4, 6, 0),
            low_time=None,
        )
        now = _utc(2025, 7, 4, 6, 20)
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ), patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            result = self._gate_open("KORD", now, self.session)
        assert result is False

    def test_current_trading_day_forecast_still_opens_inside_window_and_closes_outside(self):
        high_time = _utc(2025, 7, 4, 6, 0)
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 4, 0),
            high_time=high_time,
            low_time=None,
        )
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ), patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate_open("KORD", _utc(2025, 7, 4, 6, 20), self.session) is True
            assert self._gate_open("KORD", _utc(2025, 7, 4, 6, 31), self.session) is False

    def test_current_day_out_of_window_high_time_is_ignored(self):
        # For KORD at this "now", trading day is [2025-07-04T06:00Z, 2025-07-05T06:00Z).
        # Stored high time is corrupt/out-of-window, but forecast_date_utc still matches.
        high_time = _utc(2025, 7, 4, 5, 50)
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 4, 0),
            high_time=high_time,
            low_time=None,
        )
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ), patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            # Would be inside high window if corrupt high_time_utc were used.
            assert self._gate_open("KORD", _utc(2025, 7, 4, 6, 10), self.session) is False

    def test_current_day_both_out_of_window_times_returns_no_valid_data_false(self):
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 5, 50),
            low_time=_utc(2025, 7, 4, 5, 10),
        )
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ), patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate_open("KORD", _utc(2025, 7, 4, 6, 10), self.session) is False

    def test_current_day_in_window_timestamps_preserve_open_and_close_behavior(self):
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 6, 0),
            low_time=_utc(2025, 7, 4, 7, 0),
        )
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ), patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate_open("KORD", _utc(2025, 7, 4, 6, 20), self.session) is True
            assert self._gate_open("KORD", _utc(2025, 7, 4, 8, 0), self.session) is False


# ---------------------------------------------------------------------------
# Tests for get_series_station_code mapping helper
# ---------------------------------------------------------------------------

class TestGetSeriesStationCode:
    """Tests for core.local_time_gate.get_series_station_code."""

    def _station(self, ticker: str):
        from core.local_time_gate import get_series_station_code
        return get_series_station_code(ticker)

    def test_atl_high(self):
        assert self._station("KXHIGHTATL-26JUL16-B95") == "KATL"

    def test_atl_low(self):
        assert self._station("KXLOWTATL-26JUL16-B55") == "KATL"

    def test_satx_low(self):
        assert self._station("KXLOWTSATX-26JUL16-B55.5") == "KSAT"

    def test_sfo_high(self):
        assert self._station("KXHIGHTSFO-26JUL16-B70") == "KSFO"

    def test_phx_high(self):
        assert self._station("KXHIGHTPHX-26JUL16-B100") == "KPHX"

    def test_phx_low(self):
        assert self._station("KXLOWTPHX-26JUL16-B60") == "KPHX"

    def test_nyc_low(self):
        assert self._station("KXLOWTNYC-26JUL16-B72") == "KNYC"

    def test_dc_high(self):
        assert self._station("KXHIGHTDC-26JUL16-B88") == "KDCA"

    def test_unknown_prefix_returns_none(self):
        assert self._station("KXUNKNOWN-26JUL16-B50") is None

    def test_all_40_series_prefixes_resolve(self):
        """Guard: every entry in SERIES_CITY must map to a non-None ICAO code."""
        from core.local_time_gate import SERIES_CITY, get_series_station_code
        unresolved = []
        for prefix in SERIES_CITY:
            # Build a fake ticker for each prefix
            fake_ticker = f"{prefix}-26JUL16-B50"
            code = get_series_station_code(fake_ticker)
            if code is None:
                unresolved.append(prefix)
        assert unresolved == [], f"Prefixes with no ICAO mapping: {unresolved}"


# ---------------------------------------------------------------------------
# Tests for has_forecast helper
# ---------------------------------------------------------------------------

class TestHasForecast:
    """Tests for nws.gate.has_forecast."""

    def _has_forecast(self, station_code: str, session, current_time: datetime | None = None):
        from contextlib import contextmanager

        @contextmanager
        def mock_get_session():
            yield session

        with patch("nws.gate.get_session", mock_get_session):
            from nws.gate import has_forecast
            return has_forecast(station_code, current_time)

    def setup_method(self):
        from nws.client import _station_cache

        _station_cache.clear()
        self.engine = _make_sqlite_engine()
        self.Session = _make_session_factory(self.engine)
        self.session = self.Session()

    def teardown_method(self):
        self.session.close()

    def test_returns_false_when_table_empty(self):
        assert self._has_forecast("KATL", self.session) is False

    def test_returns_true_when_row_exists(self):
        now = _utc(2025, 7, 4, 12, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 15, 0),
            low_time=_utc(2025, 7, 4, 6, 0),
        )
        assert self._has_forecast("KATL", self.session, now) is True

    def test_returns_false_for_different_station(self):
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 15, 0),
            low_time=None,
        )
        assert self._has_forecast("KATL", self.session) is False

    def test_returns_false_for_stale_previous_trading_day_row(self):
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 3, 0),
            high_time=_utc(2025, 7, 4, 6, 0),
            low_time=None,
        )
        now = _utc(2025, 7, 4, 6, 20)
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ):
            assert self._has_forecast("KORD", self.session, now) is False


class TestDetachedForecastSession:
    """Regression coverage for detached/expired ORM forecast rows."""

    def setup_method(self):
        from nws.client import _station_cache

        _station_cache.clear()
        self.engine = _make_sqlite_engine()
        self.Session = _make_session_factory(self.engine)
        self.session = self.Session()

    def teardown_method(self):
        self.session.close()

    def test_current_day_forecast_does_not_raise_when_session_expires_on_exit(self):
        from contextlib import contextmanager

        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 6, 30),
            low_time=_utc(2025, 7, 4, 7, 0),
        )
        now = _utc(2025, 7, 4, 6, 20)

        @contextmanager
        def expiring_get_session():
            session = self.Session()
            try:
                yield session
            finally:
                session.expire_all()
                session.close()

        with patch("nws.gate.get_session", expiring_get_session), patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ), patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            from nws.gate import has_forecast, is_trading_gate_open

            assert has_forecast("KORD", now) is True
            assert is_trading_gate_open("KORD", now) is True


# ---------------------------------------------------------------------------
# Tests for market_date-aware gate behaviour (date-blind bug regression)
# ---------------------------------------------------------------------------

class TestMarketDateAwareGate:
    """Regression tests ensuring both gate functions respect the ticker's market date.

    Scenario: a Los Angeles ticker dated Jul 18 (KXHIGHLAX-26JUL18-*) is
    evaluated at 17:09 PDT on Jul 17 — the prior day's evening session.
    Without date-awareness the gate would open; with date-awareness it must
    stay closed.

    The Jul-18 market's trading window for LA (PDT = UTC-7) is:
        01:00 PDT Jul-18 = 08:00 UTC Jul-18  →  01:00 PDT Jul-19 = 08:00 UTC Jul-19
    """

    # Station cache entry for KLAX (LA): lat, lon, hourly_url, tz
    KLAX_CACHE = {"KLAX": (33.9, -118.4, "https://example.test/hourly", "America/Los_Angeles")}

    def _gate_open_with_date(self, station_code, current_time, session, market_date=None):
        from contextlib import contextmanager

        @contextmanager
        def mock_get_session():
            yield session

        with patch("nws.gate.get_session", mock_get_session):
            from nws.gate import is_trading_gate_open
            return is_trading_gate_open(station_code, current_time, market_date)

    def _has_forecast_with_date(self, station_code, session, current_time, market_date=None):
        from contextlib import contextmanager

        @contextmanager
        def mock_get_session():
            yield session

        with patch("nws.gate.get_session", mock_get_session):
            from nws.gate import has_forecast
            return has_forecast(station_code, current_time, market_date)

    def setup_method(self):
        from nws.client import _station_cache

        _station_cache.clear()
        self.engine = _make_sqlite_engine()
        self.Session = _make_session_factory(self.engine)
        self.session = self.Session()

    def teardown_method(self):
        self.session.close()

    def test_next_day_market_gate_closed_during_prior_evening(self):
        """Jul-18 LA market evaluated at 17:09 PDT Jul-17 → gate CLOSED."""
        from datetime import date

        # Jul-18 forecast stored with forecast_date_utc = 2026-07-18T00:00Z
        high_time = _utc(2026, 7, 18, 19, 0)   # ~noon LA local Jul-18
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 18, 0),  # forecast_date_utc = Jul-18 UTC midnight
            high_time, None,
        )
        # now = 2026-07-18T00:09Z = 2026-07-17T17:09 PDT
        now = _utc(2026, 7, 18, 0, 9)
        market_date = date(2026, 7, 18)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            # Gate must be CLOSED — current time is outside the Jul-18 window
            assert self._gate_open_with_date("KLAX", now, self.session, market_date) is False

    def test_next_day_has_forecast_sees_data_for_market_date(self):
        """has_forecast returns True when market_date matches stored forecast."""
        from datetime import date

        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 18, 0),
            _utc(2026, 7, 18, 19, 0), None,
        )
        now = _utc(2026, 7, 18, 0, 9)
        market_date = date(2026, 7, 18)

        # has_forecast keyed to market_date=Jul-18 should return True
        # (the row exists for Jul-18, even though "now" is in the prior trading day)
        assert self._has_forecast_with_date("KLAX", self.session, now, market_date) is True

    def test_has_forecast_false_for_wrong_market_date(self):
        """has_forecast returns False when stored forecast date != market_date."""
        from datetime import date

        # Store a Jul-17 forecast but ask for Jul-18
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            _utc(2026, 7, 17, 19, 0), None,
        )
        now = _utc(2026, 7, 18, 0, 9)
        market_date = date(2026, 7, 18)

        assert self._has_forecast_with_date("KLAX", self.session, now, market_date) is False

    def test_same_day_market_gate_opens_inside_window(self):
        """Jul-17 LA market 30 min before high at ~11:30 local → gate OPEN."""
        from datetime import date

        # high_time = 18:00 UTC = 11:00 PDT Jul-17 (GATE_HIGH_BEFORE=60 → opens at 17:00Z)
        high_time = _utc(2026, 7, 17, 18, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            high_time, None,
        )
        now = high_time - timedelta(minutes=30)  # 30 min inside window
        market_date = date(2026, 7, 17)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate_open_with_date("KLAX", now, self.session, market_date) is True

    def test_same_day_market_gate_closes_outside_window(self):
        """Jul-17 LA market well after the high window → gate CLOSED."""
        from datetime import date

        high_time = _utc(2026, 7, 17, 18, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            high_time, None,
        )
        now = high_time + timedelta(minutes=31)  # just past GATE_HIGH_AFTER=30
        market_date = date(2026, 7, 17)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate_open_with_date("KLAX", now, self.session, market_date) is False

    def test_gate_open_without_market_date_unchanged(self):
        """Calling without market_date preserves existing in-window behaviour."""
        high_time = _utc(2026, 7, 17, 18, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            high_time, None,
        )
        now = high_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            # No market_date → old behaviour
            assert self._gate_open_with_date("KLAX", now, self.session) is True

    def test_next_day_gate_closed_at_prior_day_window_boundary(self):
        """Jul-18 market: even at the outer GATE_HIGH_BEFORE boundary of Jul-18,
        gate is closed if 'now' precedes the Jul-18 trading window start."""
        from datetime import date

        # high_time on Jul-18 at 18:00Z (11:00 PDT)
        high_time = _utc(2026, 7, 18, 18, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 18, 0),
            high_time, None,
        )
        # now = Jul-17 evening in PDT — far before the Jul-18 window (08:00Z Jul-18)
        now = _utc(2026, 7, 17, 23, 0)
        market_date = date(2026, 7, 18)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate_open_with_date("KLAX", now, self.session, market_date) is False
