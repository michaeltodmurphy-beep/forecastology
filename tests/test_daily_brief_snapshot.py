"""AM_LOW_FORECAST daily-brief gate: snapshot-only, write-once, today-only.

Covers the 2026-10-01 production defects:

1. ``DailyBriefGate.get_block`` lazily fetched + persisted at local midnight;
   it must now only read the stored snapshot and never call NWS.
2. The "Tonight" period (tomorrow morning's weather) leaked into the scanned
   text — e.g. KXLOWTSDF was blocked on "thunderstorms after 5am" for Oct 2.
3. The one-shot ``daily_brief_*`` job never re-registered itself.
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

import nws.daily_brief as daily_brief  # noqa: E402
from app.models import Base, DailyForecastBlock  # noqa: E402
from nws.daily_brief import (  # noqa: E402
    DailyBriefGate,
    _fetch_daily_brief_text,
    matches_any_keyword,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]


KEYWORDS = {"rain", "thunderstorm", "thunderstorms"}
SDF_TZ = ZoneInfo("America/Kentucky/Louisville")
LOCAL_DATE = datetime.date(2026, 10, 1)


def _local(tz, hour: int, minute: int = 0, date: datetime.date = LOCAL_DATE):
    return datetime.datetime(date.year, date.month, date.day, hour, minute, tzinfo=tz)


class _FakeClient:
    """Minimal NWS client stub serving a fixed daily-forecast period array."""

    def __init__(self, periods):
        self.periods = periods
        self.calls = []

    def _get_json(self, url):  # noqa: ANN001
        self.calls.append(url)
        if "/points/" in url:
            return {"properties": {"forecast": "https://api.weather.gov/forecast"}}
        return {"properties": {"periods": self.periods}}


class _ExplodingClient:
    def _get_json(self, url):  # noqa: ANN001
        raise AssertionError(f"NWS must not be called (url={url})")


def _period(tz, hour: int, text: str, date: datetime.date = LOCAL_DATE) -> dict:
    return {"startTime": _local(tz, hour, date=date).isoformat(), "detailedForecast": text}


# Mirrors the stored KXLOWTSDF text from 2026-10-01: Overnight, Today, Tonight.
SDF_PERIODS = [
    _period(SDF_TZ, 0, "Partly cloudy, with a low around 66. South wind around 6 mph."),
    _period(SDF_TZ, 6, "Mostly sunny, with a high near 92. South wind 6 to 15 mph."),
    _period(
        SDF_TZ, 18,
        "A chance of showers and thunderstorms after 5am. Mostly cloudy, with a "
        "low around 70. Chance of precipitation is 30%.",
    ),
]


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

    with patch("nws.daily_brief.get_session", _mock_get_session):
        yield


def _store(session, series: str, local_date: datetime.date, *, blocked: bool,
           matched: str = None, text: str = "stored") -> None:
    # Explicit id: the BIGINT primary key does not autoincrement on SQLite.
    next_id = (session.query(DailyForecastBlock).count() or 0) + 1
    session.add(DailyForecastBlock(
        id=next_id, series_prefix=series, local_date=local_date,
        blocked=blocked, matched_keywords=matched, forecast_text=text,
    ))
    session.commit()


class _Cfg:
    am_low_forecast_keywords = KEYWORDS
    am_low_snapshot_local_hour = "04:00"


# ---------------------------------------------------------------------------
# Period filter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("text", "keywords", "expected"),
    [
        ("THUNDERSTORMS likely.", {"thunderstorm"}, {"thunderstorm"}),
        ("A thunderstorm is possible.", {"thunderstorm"}, {"thunderstorm"}),
        ("Thunderstorms likely.", KEYWORDS, {"thunderstorm", "thunderstorms"}),
        ("Terrain remains dry; rainbows possible.", {"rain"}, set()),
        ("Thunderstormish conditions.", {"thunderstorm"}, set()),
        ("Heavy RAIN likely.", {"rain"}, {"rain"}),
        ("No thunderstorms expected.", KEYWORDS, set()),
        ("WITHOUT THUNDERSTORMS.", KEYWORDS, set()),
        ("Thunderstorms not expected.", KEYWORDS, set()),
        ("Thunderstorms are not expected.", KEYWORDS, set()),
        ("Thunderstorms ending.", KEYWORDS, set()),
        ("Thunderstorms are ending this morning.", KEYWORDS, set()),
        ("No rain, but thunderstorms likely.", KEYWORDS, {"thunderstorm", "thunderstorms"}),
        ("No thunderstorms early; thunderstorms later.", KEYWORDS, {"thunderstorm", "thunderstorms"}),
        ("Thunderstorms ending; rain likely.", KEYWORDS, {"rain"}),
        ("Thunderstorms not expected. Thunderstorms possible later.", KEYWORDS, {"thunderstorm", "thunderstorms"}),
        ("No thunderstorms early, but rain likely.", KEYWORDS, {"rain"}),
        ("Thunderstorms not ruled out.", {"thunderstorm"}, {"thunderstorm"}),
        ("", KEYWORDS, set()),
        ("Thunderstorms likely.", set(), set()),
    ],
)
def test_matches_any_keyword_word_boundaries_and_local_negations(text, keywords, expected):
    assert matches_any_keyword(text, keywords) == expected


def test_catchup_at_13_local_includes_today_before_deadline_not_tonight():
    periods = [
        _period(SDF_TZ, 6, "Thunderstorms likely today."),
        _period(SDF_TZ, 12, "Rain this afternoon."),
        _period(SDF_TZ, 18, "Rain tonight."),
        _period(SDF_TZ, 6, "Rain tomorrow.", date=LOCAL_DATE + datetime.timedelta(days=1)),
    ]
    text = _fetch_daily_brief_text(
        _FakeClient(periods), 38.25, -85.76, tz_name="America/Kentucky/Louisville",
        now_utc=_local(SDF_TZ, 13).astimezone(datetime.timezone.utc),
        deadline_hour=12,
    )
    assert text == "Thunderstorms likely today."
    assert matches_any_keyword(text, {"thunderstorm", "rain"}) == {"thunderstorm"}


def test_tonight_period_is_excluded_sdf():
    client = _FakeClient(SDF_PERIODS)
    text = _fetch_daily_brief_text(
        client, 38.25, -85.76, tz_name="America/Kentucky/Louisville",
        now_utc=_local(SDF_TZ, 4).astimezone(datetime.timezone.utc),
        deadline_hour=12,
    )
    assert "Partly cloudy" in text
    assert "Mostly sunny" in text
    assert "thunderstorms" not in text
    assert matches_any_keyword(text, KEYWORDS) == set()


def test_periods_at_or_after_deadline_are_excluded():
    periods = SDF_PERIODS + [_period(SDF_TZ, 12, "Rain this afternoon.")]
    text = _fetch_daily_brief_text(
        _FakeClient(periods), 38.25, -85.76, tz_name="America/Kentucky/Louisville",
        now_utc=_local(SDF_TZ, 4).astimezone(datetime.timezone.utc),
        deadline_hour=12,
    )
    assert "Rain" not in text


def test_overnight_period_still_matches_austin():
    tz = ZoneInfo("America/Chicago")
    periods = [
        _period(tz, 0, "A chance of showers and thunderstorms before 4am. Low around 70."),
        _period(tz, 6, "Mostly sunny, with a high near 90."),
        _period(tz, 18, "Clear, with a low around 68."),
    ]
    text = _fetch_daily_brief_text(
        _FakeClient(periods), 30.19, -97.67, tz_name="America/Chicago",
        now_utc=_local(tz, 4).astimezone(datetime.timezone.utc),
        deadline_hour=12,
    )
    assert "thunderstorms" in text
    assert "Clear" not in text
    assert matches_any_keyword(text, KEYWORDS) == {"thunderstorm", "thunderstorms"}


def test_deadline_hour_from_env(monkeypatch):
    monkeypatch.delenv("NWS_LOW_DEADLINE_HOUR", raising=False)
    assert daily_brief._deadline_hour_from_env() == 12
    monkeypatch.setenv("NWS_LOW_DEADLINE_HOUR", "10")
    assert daily_brief._deadline_hour_from_env() == 10
    monkeypatch.setenv("NWS_LOW_DEADLINE_HOUR", "bogus")
    assert daily_brief._deadline_hour_from_env() == 12


# ---------------------------------------------------------------------------
# get_block: read-only, never fetches
# ---------------------------------------------------------------------------

def test_get_block_without_series_city_fails_open(monkeypatch):
    monkeypatch.setattr(daily_brief, "SERIES_CITY", {})
    gate = DailyBriefGate(_Cfg(), nws_client=_ExplodingClient())

    def unexpected_read(*args):
        raise AssertionError("Unmapped series must not query the snapshot DB")

    monkeypatch.setattr(gate, "_stored_row", unexpected_read)
    assert gate.get_block("KXLOWTSDF") == (False, set())
    assert gate._cache == {}


def test_get_block_without_row_fails_open_and_never_fetches(session):
    gate = DailyBriefGate(_Cfg(), nws_client=_ExplodingClient())
    now_utc = _local(SDF_TZ, 0, 1).astimezone(datetime.timezone.utc)

    with _patched_session(session), capture_logs() as logs:
        first = gate.get_block("KXLOWTSDF", now_utc=now_utc)
        # Cache expired → re-poll DB; still no row; log is deduped.
        gate._cache.clear()
        second = gate.get_block("KXLOWTSDF", now_utc=now_utc)

    assert first == (False, set())
    assert second == (False, set())
    no_snap = [e for e in logs if e.get("event") == "am_low_brief.no_snapshot_yet"]
    assert len(no_snap) == 1
    assert no_snap[0]["series"] == "KXLOWTSDF"
    assert no_snap[0]["local_date"] == "2026-10-01"
    assert no_snap[0]["snapshot_hour"] == 4
    assert session.query(DailyForecastBlock).count() == 0
    locked, *_ = gate._cache[("KXLOWTSDF", LOCAL_DATE)]
    assert locked is False


def test_get_block_with_stored_blocked_row_is_locked(session):
    _store(session, "KXLOWTSDF", LOCAL_DATE, blocked=True, matched="thunderstorms")
    gate = DailyBriefGate(_Cfg(), nws_client=_ExplodingClient())
    now_utc = _local(SDF_TZ, 0, 1).astimezone(datetime.timezone.utc)

    with _patched_session(session):
        result = gate.get_block("KXLOWTSDF", now_utc=now_utc)

    assert result == (True, {"thunderstorms"})
    locked, blocked, matched, _ = gate._cache[("KXLOWTSDF", LOCAL_DATE)]
    assert (locked, blocked, matched) == (True, True, {"thunderstorms"})


# ---------------------------------------------------------------------------
# snapshot_city: write-once
# ---------------------------------------------------------------------------

def _today_sdf() -> datetime.date:
    return datetime.datetime.now(SDF_TZ).date()


def test_snapshot_city_skips_when_row_exists(monkeypatch, session):
    monkeypatch.setenv("AM_LOW_FORECAST", "thunderstorms")
    _store(session, "KXLOWTSDF", _today_sdf(), blocked=False, text="original")
    monkeypatch.setattr(daily_brief, "NWSClient", lambda: _ExplodingClient())

    with _patched_session(session), capture_logs() as logs:
        ok = daily_brief.snapshot_city("KXLOWTSDF", "Louisville", 38.25, -85.76)

    assert ok is True
    assert [e for e in logs if e.get("event") == "nws.daily_brief.snapshot_exists"]
    row = session.query(DailyForecastBlock).one()
    assert row.forecast_text == "original"
    assert row.blocked is False


def test_snapshot_city_force_refetches_and_overwrites(monkeypatch, session):
    monkeypatch.setenv("AM_LOW_FORECAST", "thunderstorms")
    monkeypatch.setenv("NWS_LOW_DEADLINE_HOUR", "12")
    today = _today_sdf()
    _store(session, "KXLOWTSDF", today, blocked=False, text="original")
    periods = [_period(SDF_TZ, 0, "Thunderstorms likely.", date=today)]
    client = _FakeClient(periods)
    monkeypatch.setattr(daily_brief, "NWSClient", lambda: client)

    with _patched_session(session):
        ok = daily_brief.snapshot_city(
            "KXLOWTSDF", "Louisville", 38.25, -85.76, force=True
        )

    assert ok is True
    assert client.calls
    row = session.query(DailyForecastBlock).one()
    assert row.blocked is True
    assert row.matched_keywords == "thunderstorms"
    assert row.forecast_text == "Thunderstorms likely."
    assert row.fetched_at is not None


def test_snapshot_city_writes_when_no_row(monkeypatch, session):
    monkeypatch.setenv("AM_LOW_FORECAST", "thunderstorms")
    today = _today_sdf()
    periods = [_period(SDF_TZ, 18, "Thunderstorms tonight.", date=today)]
    client = _FakeClient(periods)
    monkeypatch.setattr(daily_brief, "NWSClient", lambda: client)
    written = []
    monkeypatch.setattr(
        DailyBriefGate, "_upsert",
        lambda self, *args: written.append(args),
    )

    with _patched_session(session):
        ok = daily_brief.snapshot_city("KXLOWTSDF", "Louisville", 38.25, -85.76)

    assert ok is True
    assert client.calls
    # Tonight is excluded → not blocked.
    assert written == [("KXLOWTSDF", today, False, set(), "")]


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class _FakeScheduler:
    running = True

    def __init__(self):
        self.jobs = {}

    def add_job(self, func, **kwargs):  # noqa: ANN001
        self.jobs[kwargs["id"]] = (func, kwargs)


def test_run_daily_brief_snapshot_reschedules_itself(monkeypatch):
    import nws.scheduler as scheduler

    fake = _FakeScheduler()
    monkeypatch.setattr(scheduler, "_scheduler", fake)
    calls = []
    monkeypatch.setattr(
        daily_brief, "snapshot_city", lambda **kw: calls.append(kw) or True
    )

    scheduler._run_daily_brief_snapshot("KXLOWTSDF", "Louisville", 38.25, -85.76)

    assert calls and calls[0]["series"] == "KXLOWTSDF"
    func, kwargs = fake.jobs["daily_brief_KXLOWTSDF"]
    assert func is scheduler._run_daily_brief_snapshot
    assert kwargs["trigger"] == "date"
    assert kwargs["run_date"] > datetime.datetime.now(datetime.timezone.utc)


def test_run_daily_brief_snapshot_reschedules_even_on_error(monkeypatch):
    import nws.scheduler as scheduler

    fake = _FakeScheduler()
    monkeypatch.setattr(scheduler, "_scheduler", fake)

    def _boom(**kw):  # noqa: ANN001
        raise RuntimeError("boom")

    monkeypatch.setattr(daily_brief, "snapshot_city", _boom)

    scheduler._run_daily_brief_snapshot("KXLOWTSDF", "Louisville", 38.25, -85.76)

    assert "daily_brief_KXLOWTSDF" in fake.jobs


def _fixed_now(monkeypatch, scheduler, now_utc):
    class _DT(datetime.datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            return now_utc if tz is None else now_utc.astimezone(tz)

    monkeypatch.setattr(scheduler, "datetime", _DT)


@pytest.mark.parametrize("row_exists", [False, True])
def test_schedule_daily_brief_jobs_catchup(monkeypatch, row_exists):
    import nws.scheduler as scheduler

    monkeypatch.setenv("AM_LOW_SNAPSHOT_LOCAL_HOUR", "04:00")
    fake = _FakeScheduler()
    monkeypatch.setattr(scheduler, "_scheduler", fake)
    monkeypatch.setattr(scheduler, "SERIES_CITY", {"KXLOWTSDF": "Louisville"})
    now_utc = _local(SDF_TZ, 10).astimezone(datetime.timezone.utc)
    _fixed_now(monkeypatch, scheduler, now_utc)
    checked = []
    monkeypatch.setattr(
        daily_brief, "snapshot_exists",
        lambda series, d: checked.append((series, d)) or row_exists,
    )

    scheduler.schedule_daily_brief_jobs()

    assert checked == [("KXLOWTSDF", LOCAL_DATE)]
    assert "daily_brief_KXLOWTSDF" in fake.jobs
    if row_exists:
        assert "daily_brief_catchup_KXLOWTSDF" not in fake.jobs
    else:
        func, kwargs = fake.jobs["daily_brief_catchup_KXLOWTSDF"]
        assert func is daily_brief.snapshot_city
        assert kwargs["run_date"] == now_utc
        assert kwargs["kwargs"]["series"] == "KXLOWTSDF"


def test_schedule_daily_brief_jobs_no_catchup_before_snapshot_hour(monkeypatch):
    import nws.scheduler as scheduler

    monkeypatch.setenv("AM_LOW_SNAPSHOT_LOCAL_HOUR", "04:00")
    fake = _FakeScheduler()
    monkeypatch.setattr(scheduler, "_scheduler", fake)
    monkeypatch.setattr(scheduler, "SERIES_CITY", {"KXLOWTSDF": "Louisville"})
    _fixed_now(monkeypatch, scheduler, _local(SDF_TZ, 2).astimezone(datetime.timezone.utc))
    monkeypatch.setattr(daily_brief, "snapshot_exists", lambda *a: False)

    scheduler.schedule_daily_brief_jobs()

    assert set(fake.jobs) == {"daily_brief_KXLOWTSDF"}
