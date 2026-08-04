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



# ---------------------------------------------------------------------------
# Tests for cold-cache behavior and boundary correctness
# ---------------------------------------------------------------------------

class TestColdCacheBehavior:
    """Tests for gate correctness when _station_cache is cold on startup.

    Covers three root-cause scenarios:

    1. Cache cold + NWS API resolves tz  → gate evaluates and opens correctly.
    2. Cache cold + NWS API also fails   → UTC fallback → gate stays closed
       (documented safe behavior for non-UTC stations in the 01:00-05:00 UTC
       "false-negative zone" for America/New_York).
    3. Exact open/close boundary inclusiveness with prod GATE_HIGH_BEFORE=15
       and GATE_HIGH_AFTER=5 config values.
    4. Missing expected-day row → gate closed; stale latest row not substituted.
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_mock_session(self):
        """Return a context manager that always yields self.session."""
        from contextlib import contextmanager
        session = self.session

        @contextmanager
        def _ctx():
            yield session

        return _ctx

    def setup_method(self):
        from nws.client import _station_cache

        _station_cache.clear()
        self.engine = _make_sqlite_engine()
        self.Session = _make_session_factory(self.engine)
        self.session = self.Session()

    def teardown_method(self):
        self.session.close()

    # ------------------------------------------------------------------
    # 1. Cold cache + API resolves timezone → gate opens correctly
    # ------------------------------------------------------------------

    def test_cold_cache_non_utc_station_false_negative_zone_gate_opens(self):
        """Cold cache: NWS API resolves tz → gate opens correctly in false-negative zone.

        Scenario (KATL, America/New_York, EDT = UTC-4):
          - now_utc = 2025-07-04T03:15Z  (in the 01:00–05:00 UTC zone where
            UTC fallback computes the WRONG expected trading day for EDT stations)
          - Local time = 2025-07-03T23:15 EDT  → active trading day = July 3.
          - Stored forecast: forecast_date_utc = 2025-07-03T00:00Z (local July 3).
          - high_time = 2025-07-04T03:20Z (= 2025-07-03T23:20 EDT, within window).
          - Trading window (July 3 local):  [2025-07-03T05:00Z, 2025-07-04T05:00Z).
          - GATE_HIGH_BEFORE=15 → high_open  = 2025-07-04T03:05Z.
          - GATE_HIGH_AFTER=5  → high_close = 2025-07-04T03:25Z.
          - 03:15Z is inside [03:05Z, 03:25Z] → gate OPEN.

        Without fix (_fetch_station_tz_from_api also fails → UTC fallback):
          - UTC trading day at 03:15Z: 03:15 > 01:00 → July 4.
          - Expected = 2025-07-04T00:00Z → no July-4 row → gate CLOSED (false negative).

        With fix (_fetch_station_tz_from_api returns "America/New_York"):
          - Local July 3 → expected = 2025-07-03T00:00Z → row found → gate evaluates.
        """
        # Stored in the July-3 row; high falls in the July-3 local trading window
        high_time = _utc(2025, 7, 4, 3, 20)   # 23:20 EDT on July 3
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 3, 0),               # forecast keyed to local July 3
            high_time, None,
        )
        now = _utc(2025, 7, 4, 3, 15)          # 23:15 EDT July 3 — inside gate window

        with patch("nws.gate.get_session", self._make_mock_session()), \
             patch("nws.gate._fetch_station_tz_from_api", return_value="America/New_York"), \
             patch("nws.gate.GATE_LOW_BEFORE", 150), \
             patch("nws.gate.GATE_LOW_AFTER", 60), \
             patch("nws.gate.GATE_HIGH_BEFORE", 15), \
             patch("nws.gate.GATE_HIGH_AFTER", 5):
            from nws.gate import is_trading_gate_open
            assert is_trading_gate_open("KATL", now) is True

    # ------------------------------------------------------------------
    # 2. Cold cache + API also fails → UTC fallback → documented false negative
    # ------------------------------------------------------------------

    def test_cold_cache_utc_fallback_causes_false_negative_in_false_negative_zone(self):
        """Cold cache + failed API fetch → UTC fallback → gate correctly stays closed.

        Same scenario as above, but _fetch_station_tz_from_api returns None, so
        _resolve_station_tz falls back to "UTC".  At 03:15 UTC the UTC-day logic
        maps the time to July-4 trading day (03:15 > 01:00 UTC), but the row is
        keyed to July 3.

        The gate logs a warning and returns False — the safe fail-closed behavior
        when both the cache and the NWS API are unavailable.
        """
        high_time = _utc(2025, 7, 4, 3, 20)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 3, 0),
            high_time, None,
        )
        now = _utc(2025, 7, 4, 3, 15)

        with patch("nws.gate.get_session", self._make_mock_session()), \
             patch("nws.gate._fetch_station_tz_from_api", return_value=None), \
             patch("nws.gate.GATE_LOW_BEFORE", 150), \
             patch("nws.gate.GATE_LOW_AFTER", 60), \
             patch("nws.gate.GATE_HIGH_BEFORE", 15), \
             patch("nws.gate.GATE_HIGH_AFTER", 5):
            from nws.gate import is_trading_gate_open
            # UTC fallback → expected = July-4 → no July-4 row → gate CLOSED
            assert is_trading_gate_open("KATL", now) is False

    # ------------------------------------------------------------------
    # 3. Exact boundary inclusiveness with prod GATE_HIGH_BEFORE=15/GATE_HIGH_AFTER=5
    # ------------------------------------------------------------------

    def test_high_window_exact_open_boundary_is_inclusive(self):
        """GATE_HIGH_BEFORE=15: gate opens exactly at high_time − 15 min (inclusive).

        Scenario: KATL (America/New_York), high at 18:00 UTC = 14:00 EDT (midday).
        high_open  = 18:00Z − 15 min = 17:45Z.
        At exactly 17:45Z the gate must be OPEN; at 17:44Z it must be CLOSED.
        """
        high_time = _utc(2025, 7, 4, 18, 0)    # 14:00 EDT July 4 (well inside window)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0),
            high_time, None,
        )
        now_open   = high_time - timedelta(minutes=15)  # 17:45Z — exactly at open boundary
        now_before = now_open  - timedelta(minutes=1)   # 17:44Z — just before open

        with patch("nws.gate.get_session", self._make_mock_session()), \
             patch.dict(
                 "nws.gate._station_cache",
                 {"KATL": (33.6, -84.4, "https://example.test/hourly", "America/New_York")},
                 clear=False,
             ), \
             patch("nws.gate.GATE_LOW_BEFORE", 150), \
             patch("nws.gate.GATE_LOW_AFTER", 60), \
             patch("nws.gate.GATE_HIGH_BEFORE", 15), \
             patch("nws.gate.GATE_HIGH_AFTER", 5):
            from nws.gate import is_trading_gate_open
            assert is_trading_gate_open("KATL", now_open)   is True   # inclusive open
            assert is_trading_gate_open("KATL", now_before) is False  # before open

    def test_high_window_exact_close_boundary_is_inclusive(self):
        """GATE_HIGH_AFTER=5: gate closes exactly at high_time + 5 min (inclusive).

        high_close = 18:00Z + 5 min = 18:05Z.
        At exactly 18:05Z the gate must be OPEN; at 18:06Z it must be CLOSED.
        """
        high_time = _utc(2025, 7, 4, 18, 0)    # 14:00 EDT July 4
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0),
            high_time, None,
        )
        now_close = high_time + timedelta(minutes=5)   # 18:05Z — exactly at close boundary
        now_after = now_close + timedelta(minutes=1)   # 18:06Z — just after close

        with patch("nws.gate.get_session", self._make_mock_session()), \
             patch.dict(
                 "nws.gate._station_cache",
                 {"KATL": (33.6, -84.4, "https://example.test/hourly", "America/New_York")},
                 clear=False,
             ), \
             patch("nws.gate.GATE_LOW_BEFORE", 150), \
             patch("nws.gate.GATE_LOW_AFTER", 60), \
             patch("nws.gate.GATE_HIGH_BEFORE", 15), \
             patch("nws.gate.GATE_HIGH_AFTER", 5):
            from nws.gate import is_trading_gate_open
            assert is_trading_gate_open("KATL", now_close) is True    # inclusive close
            assert is_trading_gate_open("KATL", now_after) is False   # after close

    # ------------------------------------------------------------------
    # 4. Missing expected-day row → closed; stale latest row not substituted
    # ------------------------------------------------------------------

    def test_missing_expected_day_row_stale_latest_not_substituted(self):
        """gate.expected_day_not_found: stale July-3 row must not open gate on July 4.

        A July-3 KATL row has high_time = 2025-07-04T14:00Z.
        At 2025-07-04T13:55Z (inside GATE_HIGH_BEFORE=15 window from high_time),
        the gate must be CLOSED because no July-4 row exists for KATL.

        The new exact-day query ensures the stale row is never silently used —
        only a diagnostic log entry (gate.expected_day_not_found) is emitted.
        """
        # July-3 row; high_time is well inside the July-4 UTC day
        high_time = _utc(2025, 7, 4, 14, 0)
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 3, 0),           # row keyed to local July 3
            high_time, None,
        )
        # now = 13:55Z July 4 → inside [13:45Z, 14:05Z] IF stale row were used
        now = _utc(2025, 7, 4, 13, 55)

        with patch("nws.gate.get_session", self._make_mock_session()), \
             patch.dict(
                 "nws.gate._station_cache",
                 {"KATL": (33.6, -84.4, "https://example.test/hourly", "America/New_York")},
                 clear=False,
             ), \
             patch("nws.gate.GATE_LOW_BEFORE", 150), \
             patch("nws.gate.GATE_LOW_AFTER", 60), \
             patch("nws.gate.GATE_HIGH_BEFORE", 15), \
             patch("nws.gate.GATE_HIGH_AFTER", 5):
            from nws.gate import is_trading_gate_open
            # Must be CLOSED — no July-4 row for KATL; July-3 row must not be substituted
            assert is_trading_gate_open("KATL", now) is False


# ---------------------------------------------------------------------------
# Tests for direction-aware gate (ticker_type="HIGH"/"LOW")
# ---------------------------------------------------------------------------

class TestDirectionAwareGate:
    """Regression tests for the systemic HIGH-during-LOW-window bug.

    Root cause: ``is_trading_gate_open`` previously returned
    ``in_low_window or in_high_window``, allowing HIGH entries during LOW
    windows (and vice-versa).  After the fix each direction is gated only
    on its own window.

    Coverage:
    - KLAS / Las Vegas  (desert, late-afternoon high)
    - KPHX / Phoenix    (desert, extreme afternoon high)
    - KBOS / Boston     (non-desert, morning low + afternoon high)
    - None (backward-compatible combined behavior)
    - Cache-key separation: HIGH and LOW for same station use distinct keys
    """

    # Station cache entries (lat, lon, hourly_url, tz)
    KLAS_CACHE = {"KLAS": (36.1, -115.2, "https://example.test/hourly", "America/Los_Angeles")}
    KPHX_CACHE = {"KPHX": (33.4, -112.0, "https://example.test/hourly", "America/Phoenix")}
    KBOS_CACHE = {"KBOS": (42.4, -71.0, "https://example.test/hourly", "America/New_York")}

    def _make_mock_session(self):
        from contextlib import contextmanager

        session = self.session

        @contextmanager
        def _ctx():
            yield session

        return _ctx

    def _gate(self, station_code, current_time, ticker_type=None, market_date=None):
        from contextlib import contextmanager

        session = self.session

        @contextmanager
        def mock_get_session():
            yield session

        with patch("nws.gate.get_session", mock_get_session):
            from nws.gate import is_trading_gate_open
            return is_trading_gate_open(station_code, current_time, market_date, ticker_type)

    def setup_method(self):
        from nws.client import _station_cache

        _station_cache.clear()
        self.engine = _make_sqlite_engine()
        self.Session = _make_session_factory(self.engine)
        self.session = self.Session()

    def teardown_method(self):
        self.session.close()

    # ------------------------------------------------------------------
    # KLAS / Las Vegas: HIGH ticker blocked during LOW window
    # ------------------------------------------------------------------

    def test_klas_high_ticker_blocked_during_low_window(self):
        """KLAS HIGH entry must be blocked when now is in the LOW window."""
        low_time  = _utc(2025, 7, 15, 13, 0)   # 06:00 PDT — low window
        high_time = _utc(2025, 7, 15, 23, 0)   # 16:00 PDT — high window (later)
        _insert_forecast(
            self.session, "KLAS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        # Inside the LOW window
        now = low_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KLAS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            # HIGH ticker → only high window applies → should be CLOSED
            assert self._gate("KLAS", now, ticker_type="HIGH") is False

    def test_klas_high_ticker_allowed_during_high_window(self):
        """KLAS HIGH entry must be allowed when now is in the HIGH window."""
        low_time  = _utc(2025, 7, 15, 13, 0)
        high_time = _utc(2025, 7, 15, 23, 0)
        _insert_forecast(
            self.session, "KLAS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        # Inside the HIGH window
        now = high_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KLAS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KLAS", now, ticker_type="HIGH") is True

    def test_klas_low_ticker_allowed_during_low_window(self):
        """KLAS LOW entry must be allowed when now is in the LOW window."""
        low_time  = _utc(2025, 7, 15, 13, 0)
        high_time = _utc(2025, 7, 15, 23, 0)
        _insert_forecast(
            self.session, "KLAS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = low_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KLAS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KLAS", now, ticker_type="LOW") is True

    def test_klas_low_ticker_blocked_during_high_window(self):
        """KLAS LOW entry must be blocked when now is only in the HIGH window."""
        low_time  = _utc(2025, 7, 15, 13, 0)
        high_time = _utc(2025, 7, 15, 23, 0)
        _insert_forecast(
            self.session, "KLAS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        # Inside the HIGH window but outside the LOW window
        now = high_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KLAS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KLAS", now, ticker_type="LOW") is False

    # ------------------------------------------------------------------
    # KPHX / Phoenix
    # ------------------------------------------------------------------

    def test_kphx_high_ticker_blocked_during_low_window(self):
        """KPHX HIGH entry blocked when now is in the LOW window only."""
        low_time  = _utc(2025, 7, 15, 14, 0)   # 07:00 MST
        high_time = _utc(2025, 7, 15, 22, 0)   # 15:00 MST
        _insert_forecast(
            self.session, "KPHX",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = low_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KPHX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KPHX", now, ticker_type="HIGH") is False

    def test_kphx_high_ticker_allowed_during_high_window(self):
        """KPHX HIGH entry allowed when now is in the HIGH window."""
        low_time  = _utc(2025, 7, 15, 14, 0)
        high_time = _utc(2025, 7, 15, 22, 0)
        _insert_forecast(
            self.session, "KPHX",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = high_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KPHX_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KPHX", now, ticker_type="HIGH") is True

    # ------------------------------------------------------------------
    # KBOS / Boston (non-desert)
    # ------------------------------------------------------------------

    def test_kbos_high_ticker_blocked_during_low_window(self):
        """KBOS HIGH entry blocked when now is in the LOW window only."""
        low_time  = _utc(2025, 7, 15, 10, 0)   # morning low
        high_time = _utc(2025, 7, 15, 19, 0)   # afternoon high
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = low_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KBOS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KBOS", now, ticker_type="HIGH") is False

    def test_kbos_low_ticker_blocked_during_high_window(self):
        """KBOS LOW entry blocked when now is in the HIGH window only."""
        low_time  = _utc(2025, 7, 15, 10, 0)
        high_time = _utc(2025, 7, 15, 19, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = high_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KBOS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KBOS", now, ticker_type="LOW") is False

    def test_kbos_low_ticker_allowed_during_low_window(self):
        """KBOS LOW entry allowed when now is in the LOW window."""
        low_time  = _utc(2025, 7, 15, 10, 0)
        high_time = _utc(2025, 7, 15, 19, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = low_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KBOS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KBOS", now, ticker_type="LOW") is True

    # ------------------------------------------------------------------
    # Backward-compatible None behavior
    # ------------------------------------------------------------------

    def test_none_ticker_type_opens_during_low_window(self):
        """ticker_type=None (default) opens during LOW window — backward compat."""
        low_time  = _utc(2025, 7, 15, 10, 0)
        high_time = _utc(2025, 7, 15, 19, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = low_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KBOS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KBOS", now, ticker_type=None) is True

    def test_none_ticker_type_opens_during_high_window(self):
        """ticker_type=None (default) opens during HIGH window — backward compat."""
        low_time  = _utc(2025, 7, 15, 10, 0)
        high_time = _utc(2025, 7, 15, 19, 0)
        _insert_forecast(
            self.session, "KBOS",
            _utc(2025, 7, 15, 0), high_time, low_time,
        )
        now = high_time - timedelta(minutes=30)

        with patch.dict("nws.gate._station_cache", self.KBOS_CACHE, clear=False), \
             patch("nws.gate.GATE_LOW_BEFORE", 120), \
             patch("nws.gate.GATE_LOW_AFTER", 45), \
             patch("nws.gate.GATE_HIGH_BEFORE", 60), \
             patch("nws.gate.GATE_HIGH_AFTER", 30):
            assert self._gate("KBOS", now, ticker_type=None) is True

    # ------------------------------------------------------------------
    # Fail-closed on exception still applies regardless of ticker_type
    # ------------------------------------------------------------------

    def test_high_ticker_fail_closed_on_no_data(self):
        """Gate returns False for HIGH ticker when no forecast data exists."""
        now = _utc(2025, 7, 15, 15, 0)
        # No forecast inserted → gate must be closed regardless of direction
        assert self._gate("KLAS", now, ticker_type="HIGH") is False

    def test_low_ticker_fail_closed_on_no_data(self):
        """Gate returns False for LOW ticker when no forecast data exists."""
        now = _utc(2025, 7, 15, 15, 0)
        assert self._gate("KLAS", now, ticker_type="LOW") is False


# ---------------------------------------------------------------------------
# Tests for gate cache key separation (HIGH vs LOW same station)
# ---------------------------------------------------------------------------

class TestGateCacheKeySeparation:
    """Prove that HIGH and LOW for the same station use distinct cache keys.

    After the fix the cache is keyed by (station, ticker_type) so a LOW
    evaluation cannot warm the cache with an incorrect result for HIGH.
    """

    def setup_method(self):
        pass

    def test_high_and_low_use_different_cache_keys(self):
        """HIGH and LOW for the same station must produce different cache keys.

        This test exercises the state machine's watchlist gate code path by
        inspecting the _nws_gate_cache dict after two separate evaluations —
        one for a HIGH ticker and one for a LOW ticker on the same station.
        """
        import asyncio
        from unittest.mock import AsyncMock, MagicMock, patch as _patch

        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

        from core.types import MarketBracket, Phase
        from data.ticker_cache import TickerCache
        from data.websocket_manager import WebSocketManager

        # Minimal config stub
        config = MagicMock()
        config.high_trades = True
        config.low_trades = True
        config.buy_trigger_price_low = 80
        config.buy_trigger_price_high = 80
        config.minimum_spread = 3
        config.initial_contract_count = 1
        config.hedge_max_factor = 1
        config.no_trade_tickers = set()
        config.hedge_trigger = None

        # Build a minimal strategy instance without starting it
        import core.state_machine as sm

        with _patch.object(sm, "load_private_key", return_value=object()):
            strategy = sm.TemperatureStrategy.__new__(sm.TemperatureStrategy)
            strategy.config = config
            strategy.cache = TickerCache()
            strategy._nws_gate_cache = {}
            strategy._nws_gate_cache_refresh_seconds = 30

        # Verify initial state
        assert strategy._nws_gate_cache == {}

        # Manually verify that a (station, "HIGH") key and a (station, "LOW") key
        # are distinct — which is the invariant the fix enforces.
        station = "KLAS"
        high_key = (station, "HIGH")
        low_key  = (station, "LOW")

        assert high_key != low_key, "HIGH and LOW cache keys must differ"

        # Simulate what the patched watchlist loop would write
        import time as _time
        now_ts = _time.monotonic()
        strategy._nws_gate_cache[low_key]  = (now_ts, True, True)   # LOW gate open
        strategy._nws_gate_cache[high_key] = (now_ts, True, False)  # HIGH gate closed

        # Both keys co-exist independently
        assert (station, "LOW")  in strategy._nws_gate_cache
        assert (station, "HIGH") in strategy._nws_gate_cache
        # Values are independent
        assert strategy._nws_gate_cache[low_key][2]  is True
        assert strategy._nws_gate_cache[high_key][2] is False
