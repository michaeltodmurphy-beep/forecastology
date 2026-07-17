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
