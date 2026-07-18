"""Tests for nws/gate.py — is_trading_gate_open() and has_forecast().

Uses an in-memory SQLite database (via SQLAlchemy) to avoid requiring a real
MySQL server; this mirrors the approach used elsewhere in the test suite.

NWS gate semantics (settlement-day only):
- Gate OPEN when the stored forecast's settlement day matches the target
  trading day (market_date when provided, else station's current trading day).
- Gate CLOSED when no forecast exists, the forecast is stale (wrong day),
  or a DB error occurs.
- The old ±minute high/low window logic has been removed.
"""
import os
import sys
from datetime import date, datetime, timedelta, timezone
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
    """Tests for is_trading_gate_open with a mock SQLite DB session.

    The gate now uses settlement-day matching only — no ±minute windows.
    """

    def _gate_open(self, station_code: str, current_time: datetime, session,
                   market_date=None):
        """Call is_trading_gate_open with a mocked get_session."""
        from contextlib import contextmanager

        @contextmanager
        def mock_get_session():
            yield session

        with patch("nws.gate.get_session", mock_get_session):
            from nws.gate import is_trading_gate_open
            return is_trading_gate_open(station_code, current_time, market_date)

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
    # Settlement-day match → gate open (regardless of time-of-day)
    # -----------------------------------------------------------------------

    def test_settlement_day_match_opens_gate(self):
        """Gate is OPEN any time on the correct settlement day."""
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 14, 0),
            low_time=_utc(2025, 7, 4, 6, 0),
        )
        # Early morning — well before the old low window would have opened
        assert self._gate_open("KATL", _utc(2025, 7, 4, 2, 0), self.session) is True

    def test_settlement_day_match_opens_gate_midday(self):
        """Gate is OPEN midday between old low/high windows."""
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 14, 0),
            low_time=_utc(2025, 7, 4, 6, 0),
        )
        # 11:00 UTC — previously outside both windows
        assert self._gate_open("KATL", _utc(2025, 7, 4, 11, 0), self.session) is True

    def test_settlement_day_match_opens_gate_late(self):
        """Gate is OPEN late in the day (after old high window would have closed)."""
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 14, 0),
            low_time=None,
        )
        # 2 hours after old GATE_HIGH_AFTER=30 would have closed
        assert self._gate_open("KATL", _utc(2025, 7, 4, 16, 30), self.session) is True

    # -----------------------------------------------------------------------
    # Stale / wrong-day forecast → gate closed
    # -----------------------------------------------------------------------

    def test_stale_previous_day_forecast_returns_false(self):
        """Forecast from yesterday → gate CLOSED today."""
        _insert_forecast(
            self.session, "KATL",
            _utc(2025, 7, 3, 0),  # yesterday
            high_time=_utc(2025, 7, 3, 14, 0),
            low_time=None,
        )
        now = _utc(2025, 7, 4, 6, 0)  # today
        result = self._gate_open("KATL", now, self.session)
        assert result is False

    def test_stale_previous_trading_day_returns_false(self):
        """Forecast date_utc is Jul-3 but evaluated on Jul-4 (KORD, America/Chicago)."""
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
            result = self._gate_open("KORD", now, self.session)
        assert result is False

    # -----------------------------------------------------------------------
    # Current trading day → gate open (KORD with station cache)
    # -----------------------------------------------------------------------

    def test_current_trading_day_forecast_opens_gate(self):
        """Forecast for today's settlement day → gate OPEN at any time during the day."""
        _insert_forecast(
            self.session,
            "KORD",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 6, 0),
            low_time=None,
        )
        with patch.dict(
            "nws.gate._station_cache",
            {"KORD": (41.0, -87.0, "https://example.test/hourly", "America/Chicago")},
            clear=False,
        ):
            # Various times on the correct settlement day — all OPEN
            assert self._gate_open("KORD", _utc(2025, 7, 4, 6, 20), self.session) is True
            assert self._gate_open("KORD", _utc(2025, 7, 4, 10, 0), self.session) is True
            assert self._gate_open("KORD", _utc(2025, 7, 4, 20, 0), self.session) is True

    # -----------------------------------------------------------------------
    # Uses most-recent forecast row
    # -----------------------------------------------------------------------

    def test_uses_most_recent_forecast(self):
        """When two rows exist, uses the newer one (latest forecast_date_utc)."""
        # Old row (Jul-3)
        _insert_forecast(
            self.session, "KMIA",
            _utc(2025, 7, 3, 0),
            high_time=_utc(2025, 7, 3, 20, 0),
            low_time=_utc(2025, 7, 3, 4, 0),
        )
        # Today's row (Jul-4)
        _insert_forecast(
            self.session, "KMIA",
            _utc(2025, 7, 4, 0),
            high_time=_utc(2025, 7, 4, 15, 0),
            low_time=_utc(2025, 7, 4, 5, 0),
        )
        now = _utc(2025, 7, 4, 8, 0)
        result = self._gate_open("KMIA", now, self.session)
        assert result is True

    # -----------------------------------------------------------------------
    # Naive datetime input
    # -----------------------------------------------------------------------

    def test_naive_utc_input_treated_as_utc(self):
        _insert_forecast(
            self.session, "KLAX",
            _utc(2025, 7, 4, 0), None, _utc(2025, 7, 4, 6, 0)
        )
        now_naive = datetime(2025, 7, 4, 6, 0)  # naive = no tzinfo
        result = self._gate_open("KLAX", now_naive, self.session)
        assert result is True

    # -----------------------------------------------------------------------
    # market_date parameter — settlement-day keyed to ticker's own date
    # -----------------------------------------------------------------------

    def test_market_date_match_opens_gate(self):
        """With market_date provided, gate opens when forecast_date_utc matches."""
        md = date(2026, 7, 18)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 18, 0),  # forecast for Jul-18
            high_time=_utc(2026, 7, 18, 19, 0),
            low_time=None,
        )
        now = _utc(2026, 7, 18, 12, 0)
        result = self._gate_open("KLAX", now, self.session, market_date=md)
        assert result is True

    def test_market_date_mismatch_closes_gate(self):
        """Forecast for Jul-17 but market_date is Jul-18 → gate CLOSED."""
        md = date(2026, 7, 18)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),  # prior day forecast
            high_time=_utc(2026, 7, 17, 19, 0),
            low_time=None,
        )
        # now is during Jul-17 evening — prior day's session
        now = _utc(2026, 7, 18, 0, 9)
        result = self._gate_open("KLAX", now, self.session, market_date=md)
        assert result is False


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
        ):
            from nws.gate import has_forecast, is_trading_gate_open

            assert has_forecast("KORD", now) is True
            assert is_trading_gate_open("KORD", now) is True


# ---------------------------------------------------------------------------
# Tests for market_date-aware gate behaviour (settlement-day semantics)
# ---------------------------------------------------------------------------

class TestMarketDateAwareGate:
    """Regression tests ensuring both gate functions respect the ticker's market date.

    The NWS gate now gates on settlement day only: OPEN when the stored
    forecast's settlement day matches the target trading day, CLOSED otherwise.
    The old ±minute window logic has been removed.

    Scenario for mismatch: the Jun-17 forecast is in the DB, but the ticker is
    a Jul-18 market → forecast_date_utc (Jul-17) != market_date (Jul-18) → CLOSED.

    Scenario for match: a Jul-18 forecast is in the DB for a Jul-18 market
    → OPEN regardless of what time-of-day 'now' is.
    """

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

    def test_next_day_market_gate_closed_when_forecast_is_prior_day(self):
        """Jul-18 LA market: prior-day (Jul-17) forecast in DB → gate CLOSED.

        The forecast_date_utc is Jul-17 but the ticker's market_date is Jul-18,
        so the settlement days don't match → gate CLOSED regardless of 'now'.
        """
        # Store a Jul-17 forecast (the current trading day's data)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),  # prior day
            _utc(2026, 7, 17, 19, 0), None,
        )
        # Evaluate with market_date=Jul-18 (next-day market, prior-day session)
        now = _utc(2026, 7, 18, 0, 9)
        market_date = date(2026, 7, 18)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False):
            assert self._gate_open_with_date("KLAX", now, self.session, market_date) is False

    def test_next_day_market_gate_open_when_forecast_matches_market_date(self):
        """Jul-18 LA market: Jul-18 forecast in DB → gate OPEN (settlement day matches).

        The ±window logic is gone: gate opens as soon as the forecast is present
        for the correct settlement day, regardless of time-of-day.
        """
        high_time = _utc(2026, 7, 18, 19, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 18, 0),  # correct settlement day
            high_time, None,
        )
        now = _utc(2026, 7, 18, 12, 0)
        market_date = date(2026, 7, 18)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False):
            assert self._gate_open_with_date("KLAX", now, self.session, market_date) is True

    def test_next_day_has_forecast_sees_data_for_market_date(self):
        """has_forecast returns True when market_date matches stored forecast."""
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 18, 0),
            _utc(2026, 7, 18, 19, 0), None,
        )
        now = _utc(2026, 7, 18, 0, 9)
        market_date = date(2026, 7, 18)

        assert self._has_forecast_with_date("KLAX", self.session, now, market_date) is True

    def test_has_forecast_false_for_wrong_market_date(self):
        """has_forecast returns False when stored forecast date != market_date."""
        # Store a Jul-17 forecast but ask for Jul-18
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            _utc(2026, 7, 17, 19, 0), None,
        )
        now = _utc(2026, 7, 18, 0, 9)
        market_date = date(2026, 7, 18)

        assert self._has_forecast_with_date("KLAX", self.session, now, market_date) is False

    def test_same_day_market_gate_open_at_any_time(self):
        """Jul-17 LA market with Jul-17 forecast → gate OPEN at any time on Jul-17.

        Previously the gate would close after GATE_HIGH_AFTER minutes; now it stays
        OPEN for the entire settlement day as long as the forecast exists.
        """
        high_time = _utc(2026, 7, 17, 18, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            high_time, None,
        )
        market_date = date(2026, 7, 17)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False):
            # Before the old high window
            assert self._gate_open_with_date(
                "KLAX", high_time - timedelta(hours=3), self.session, market_date
            ) is True
            # Well after the old GATE_HIGH_AFTER=30 would have closed
            assert self._gate_open_with_date(
                "KLAX", high_time + timedelta(minutes=31), self.session, market_date
            ) is True
            # Late evening
            assert self._gate_open_with_date(
                "KLAX", _utc(2026, 7, 17, 23, 0), self.session, market_date
            ) is True

    def test_gate_open_without_market_date_uses_current_trading_day(self):
        """Calling without market_date derives trading day from 'now'."""
        high_time = _utc(2026, 7, 17, 18, 0)
        _insert_forecast(
            self.session, "KLAX",
            _utc(2026, 7, 17, 0),
            high_time, None,
        )
        # now is mid-day on Jul-17 UTC (within the Jul-17 PDT trading window)
        now = _utc(2026, 7, 17, 15, 0)

        with patch.dict("nws.gate._station_cache", self.KLAX_CACHE, clear=False):
            assert self._gate_open_with_date("KLAX", now, self.session) is True

