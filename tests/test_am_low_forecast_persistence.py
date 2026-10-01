"""Restart-safety tests for the AM-low *hourly forecast* gate.

The NWS ``forecastHourly`` endpoint only returns FUTURE hours.  A process
restarted after the morning low already happened therefore sees only the rest
of the day, finds its minimum in the evening and would — before this fix —
block the series for the remainder of the local day (production incident,
2026-10-01 KXLOWTSATX / KXLOWTMIA).

Two mechanisms are covered here:

1. a decision derived from a partial (post-morning) forecast is never locked,
   never persisted, and fails **open**;
2. a lockable decision is persisted to ``daily_am_low_forecast`` and read back
   by a fresh gate instance without any NWS fetch.
"""
import datetime
import os
import sys
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from structlog.testing import capture_logs

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.models import Base, DailyAmLowForecast  # noqa: E402
from core.sunrise_gate import SunriseEntryGate  # noqa: E402
from tests.test_sunrise_gate import _FakeNWSClient, _make_config  # noqa: E402

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]


TZ = ZoneInfo("America/New_York")
LOCAL_DATE = datetime.date(2026, 10, 1)
TICKER = "KXLOWTNYC-26OCT01-B73.5"
SERIES = "KXLOWTNYC"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)
    sess = sessionmaker(bind=engine, autoflush=False, autocommit=False)()
    yield sess
    sess.close()


@contextmanager
def _patched_session(session):
    @contextmanager
    def _mock_get_session():
        yield session
        session.commit()

    with patch("core.sunrise_gate.get_session", _mock_get_session):
        yield


def _periods(first_hour: int, last_hour: int, min_hour: int) -> list:
    """Hourly periods from *first_hour* to *last_hour* with the min at *min_hour*."""
    out = []
    for hour in range(first_hour, last_hour + 1):
        temp = 55.0 if hour == min_hour else 70.0
        start = datetime.datetime(
            LOCAL_DATE.year, LOCAL_DATE.month, LOCAL_DATE.day, hour, 0, tzinfo=TZ
        )
        out.append(
            {"startTime": start.isoformat(), "temperature": temp, "temperatureUnit": "F"}
        )
    return out


def _make_gate(monkeypatch, periods, *, snapshot_hour="04:00") -> SunriseEntryGate:
    client = _FakeNWSClient(
        forecast_periods=periods,
        station_meta=(40.0, -74.0, "https://api.weather.gov/hourly", "America/New_York"),
    )
    cfg = _make_config(
        sunrise_require_am_low=True,
        nws_low_deadline_hour=12,
        am_low_snapshot_local_hour=snapshot_hour,
        sunrise_temp_rise_required=0.0,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=0,
        sunrise_entry_window_minutes=1200,
    )
    gate = SunriseEntryGate(cfg, nws_client=client)
    fixed_sunrise = datetime.datetime(2026, 10, 1, 6, 0, tzinfo=TZ)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))
    return gate


def _local_utc(hour: int, minute: int = 0) -> datetime.datetime:
    return datetime.datetime(
        LOCAL_DATE.year, LOCAL_DATE.month, LOCAL_DATE.day, hour, minute, tzinfo=TZ
    ).astimezone(datetime.timezone.utc)


def _check(gate, hour: int, minute: int = 0) -> bool:
    """Invoke the AM-low forecast check directly for a given local time.

    The 03:00-05:00 snapshot window is before the sunrise gate opens, so the
    full ``evaluate()`` path cannot be used to exercise it.
    """
    now_utc = _local_utc(hour, minute)
    return gate._check_am_low_forecast(
        series=SERIES,
        station_id="KNYC",
        now_utc=now_utc,
        now_local=now_utc.astimezone(TZ),
        local_date=LOCAL_DATE,
        tz=TZ,
    )


def _rows(session) -> list:
    return session.query(DailyAmLowForecast).all()


# ---------------------------------------------------------------------------
# Regression: partial (post-morning) forecast must fail OPEN and not persist
# ---------------------------------------------------------------------------

def test_partial_forecast_after_morning_fails_open(monkeypatch, session):
    """Restart at 10:56 local: forecast starts at 11:00, min at 20:00 → OPEN."""
    gate = _make_gate(monkeypatch, _periods(11, 23, min_hour=20))

    with _patched_session(session), capture_logs() as logs:
        result = gate.evaluate(TICKER, now_utc=_local_utc(10, 56))

    assert result.allowed is True
    assert [e for e in logs if e.get("event") == "sunrise.am_low_partial_forecast_fail_open"]
    assert not [e for e in logs if e.get("event") == "sunrise.am_low_blocked"]
    assert _rows(session) == []
    locked, *_ = gate._am_low_cache[(SERIES, LOCAL_DATE)]
    assert locked is False


def test_full_day_forecast_morning_low_is_persisted_and_locked(monkeypatch, session):
    """04:30 local, periods from 00:00, min at 07:00 → passes, persisted, locked."""
    gate = _make_gate(monkeypatch, _periods(0, 23, min_hour=7))

    with _patched_session(session):
        passed = _check(gate, 4, 30)

    assert passed is True
    rows = _rows(session)
    assert len(rows) == 1
    assert rows[0].passed is True
    assert rows[0].series_prefix == SERIES
    assert rows[0].local_date == LOCAL_DATE
    assert gate._am_low_cache[(SERIES, LOCAL_DATE)][0] is True


def test_full_day_forecast_evening_low_is_persisted_and_blocks(monkeypatch, session):
    """04:30 local, periods from 00:00, min at 21:00 → blocks, persisted, locked."""
    gate = _make_gate(monkeypatch, _periods(0, 23, min_hour=21))

    with _patched_session(session):
        passed = _check(gate, 4, 30)

    assert passed is False
    rows = _rows(session)
    assert len(rows) == 1
    assert rows[0].passed is False
    assert gate._am_low_cache[(SERIES, LOCAL_DATE)][0] is True


# ---------------------------------------------------------------------------
# Stored rows are honoured on a fresh gate instance (simulated restart)
# ---------------------------------------------------------------------------

def _store(session, passed: bool) -> None:
    session.add(
        DailyAmLowForecast(
            series_prefix=SERIES,
            local_date=LOCAL_DATE,
            passed=passed,
            forecast_min_temp_f=55.0,
            min_time_local="2026-10-01T07:00:00-04:00",
            evaluated_at_local_hour=4,
        )
    )
    session.commit()


class _ExplodingClient(_FakeNWSClient):
    """Any NWS access is a test failure — the stored row must be used."""

    def _get_station_metadata(self, station_id):  # noqa: ANN001
        raise AssertionError("NWS must not be fetched when a stored row exists")

    def _get_hourly_periods(self, hourly_url):  # noqa: ANN001
        raise AssertionError("NWS must not be fetched when a stored row exists")


def _gate_with_exploding_client(monkeypatch) -> SunriseEntryGate:
    gate = _make_gate(monkeypatch, _periods(0, 23, min_hour=7))
    gate.nws_client = _ExplodingClient()
    return gate


def test_stored_blocked_row_is_honoured_after_restart(monkeypatch, session):
    _store(session, passed=False)
    gate = _gate_with_exploding_client(monkeypatch)

    with _patched_session(session), capture_logs() as logs:
        result = gate.evaluate(TICKER, now_utc=_local_utc(14, 54))

    assert result.allowed is False
    db_logs = [
        e for e in logs
        if e.get("event") == "sunrise.am_low_check" and e.get("source") == "db"
    ]
    assert db_logs and db_logs[0]["cached"] is True and db_logs[0]["locked"] is True


def test_stored_passed_row_is_honoured_after_restart(monkeypatch, session):
    _store(session, passed=True)
    gate = _gate_with_exploding_client(monkeypatch)

    with _patched_session(session):
        result = gate.evaluate(TICKER, now_utc=_local_utc(14, 54))

    assert result.allowed is True


# ---------------------------------------------------------------------------
# Storage failures must never raise into the gate
# ---------------------------------------------------------------------------

def test_storage_failure_does_not_raise(monkeypatch):
    gate = _make_gate(monkeypatch, _periods(0, 23, min_hour=7))

    @contextmanager
    def _boom():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    with patch("core.sunrise_gate.get_session", _boom):
        passed = _check(gate, 4, 30)

    assert passed is True


# ---------------------------------------------------------------------------
# Scheduler wiring
# ---------------------------------------------------------------------------

def test_scheduler_am_low_job_id_is_whitelisted_and_starts(monkeypatch):
    import nws.scheduler as scheduler

    monkeypatch.setenv("SUNRISE_REQUIRE_AM_LOW", "yes")
    monkeypatch.setattr(scheduler, "_scheduler", None)
    monkeypatch.setattr(scheduler, "run_forecast_update_job", lambda: None)
    try:
        scheduler.start_scheduler()
        scheduler.schedule_am_low_forecast_jobs()
        job_ids = {j.id for j in scheduler._scheduler.get_jobs()}
        assert any(jid.startswith("am_low_forecast_") for jid in job_ids)
    finally:
        scheduler.shutdown()


def test_am_low_snapshot_job_reschedules_itself(monkeypatch):
    """The one-shot job must re-register itself so it keeps firing daily."""
    import nws.scheduler as scheduler

    monkeypatch.setenv("SUNRISE_REQUIRE_AM_LOW", "yes")
    monkeypatch.setattr(scheduler, "_scheduler", None)
    monkeypatch.setattr(scheduler, "run_forecast_update_job", lambda: None)
    monkeypatch.setattr(
        "core.sunrise_gate.snapshot_am_low_forecast", lambda series: True
    )
    try:
        scheduler.start_scheduler()
        scheduler._run_am_low_forecast_snapshot(SERIES)
        job = scheduler._scheduler.get_job(f"am_low_forecast_{SERIES}")
        assert job is not None
        assert job.next_run_time > datetime.datetime.now(datetime.timezone.utc)
    finally:
        scheduler.shutdown()
