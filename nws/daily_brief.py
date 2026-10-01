# nws/daily_brief.py
"""NWS daily-brief forecast keyword gate for AM-low entries.

Pulls the NWS **daily** brief forecast (``/gridpoints/{office}/{gridX},{gridY}/forecast``,
the human-readable forecast, distinct from the hourly ``forecastHourly`` endpoint used
by the high/low temperature gate) once per city-local day and blocks ``KXLOW*`` entry
when the forecast text contains any configured ``AM_LOW_FORECAST`` keyword.

Key behaviours
--------------
* Cities are resolved by **lat/lon** (city-centre coordinates), not ICAO codes.
* Matching is **case-insensitive** substring match; **ANY** match gates the series.
* The decision is taken **once per city-local date** by the scheduler snapshot
  (:func:`snapshot_city`) at ``AM_LOW_SNAPSHOT_LOCAL_HOUR`` local and stored in the
  ``daily_forecast_block`` table.  It is **write-once**: nothing re-fetches or
  rewrites it for the rest of that local day (except ``force=True`` manual re-runs).
* Only periods that start on today's local date **before** ``NWS_LOW_DEADLINE_HOUR``
  are scanned, so the "Tonight" period (tomorrow morning's weather) is excluded.
* :meth:`DailyBriefGate.get_block` never calls NWS — it only reads the stored
  decision.  Before the snapshot exists (or on fetch/API failure) the gate
  **fails open** (does not block trading) and logs loudly.
"""
from __future__ import annotations

import datetime
import logging
import time
from typing import Optional, Tuple

import structlog

from app.config import AppConfig
from app.models import DailyForecastBlock
from nws.client import NWSClient
from nws.db import get_session

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)

# Cache TTL for the *provisional* (pre-snapshot) decision, in seconds.
_AM_LOW_BRIEF_CACHE_TTL_SECONDS = 1800  # 30 minutes

# ---------------------------------------------------------------------------
# City → lat/lon (city-centre coordinates; NOT airport codes)
# ---------------------------------------------------------------------------
CITY_COORDS: dict[str, tuple[float, float]] = {
    "Atlanta": (33.7485, -84.3915),
    "Austin": (30.1945, -97.6699),
    "Boston": (42.3635, -71.0181),
    "Chicago": (41.7885, -87.7417),
    "Dallas": (32.8975, -97.0444),
    "Denver": (39.8482, -104.6738),
    "Houston": (29.6524, -95.2772),
    "Los Angeles": (33.9435, -118.4086),   # "LA"
    "Las Vegas": (36.0852, -115.1507),
    "Miami": (25.7934, -80.2798),
    "Minneapolis": (44.8833, -93.2115),
    "New Orleans": (29.9872, -90.2565),
    "New York City": (40.7823, -73.9654),  # "NYC"
    "Oklahoma City": (35.4685, -97.5213),
    "Philadelphia": (39.8764, -75.2422),
    "Phoenix": (33.4355, -112.0079),
    "San Antonio": (29.4252, -98.4946),
    "San Francisco": (37.6188, -122.3758),
    "Seattle": (47.4436, -122.3029),
    "Washington DC": (38.8921, -77.0199),
    "Newark": (40.7357, -74.1724),
    "Louisville": (38.2527, -85.7585),
    "Trenton": (40.2206, -74.7597),
    "San Diego": (32.7157, -117.1611),
}

# Series prefix → city name.  Keys mirror core/local_time_gate.py SERIES_CITY.
SERIES_CITY: dict[str, str] = {
    "KXLOWTATL": "Atlanta",
    "KXLOWTAUS": "Austin",
    "KXLOWTBOS": "Boston",
    "KXLOWTCHI": "Chicago",
    "KXLOWTDAL": "Dallas",
    "KXLOWTDC": "Washington DC",
    "KXLOWTDEN": "Denver",
    "KXLOWTHOU": "Houston",
    "KXLOWTLAX": "Los Angeles",
    "KXLOWTLV": "Las Vegas",
    "KXLOWTMIA": "Miami",
    "KXLOWTMIN": "Minneapolis",
    "KXLOWTNOLA": "New Orleans",
    "KXLOWTNYC": "New York City",
    "KXLOWTOKC": "Oklahoma City",
    "KXLOWTPHIL": "Philadelphia",
    "KXLOWTPHX": "Phoenix",
    "KXLOWTSATX": "San Antonio",
    "KXLOWTSEA": "Seattle",
    "KXLOWTSFO": "San Francisco",
    "KXLOWTEWR": "Newark",
    "KXLOWTSDF": "Louisville",
    "KXLOWTTTN": "Trenton",
    "KXLOWTSAN": "San Diego",
}


def _snapshot_hour(config: AppConfig) -> int:
    """Return the configured AM-low snapshot hour as an int (0–23)."""
    _raw = getattr(config, "am_low_snapshot_local_hour", "03:00") or "03:00"
    try:
        return int(str(_raw).strip().split(":")[0])
    except (ValueError, TypeError, IndexError):
        return 3


def matches_any_keyword(forecast_text: str, keywords: set[str]) -> set[str]:
    """Return the subset of *keywords* found in *forecast_text*.

    Case-insensitive substring match.  **ANY** single match is sufficient to gate
    (the caller blocks when the returned set is non-empty).

    Args:
        forecast_text: The NWS daily brief text (may be empty).
        keywords: Normalised (lowercased) keyword set from config.

    Returns:
        The matched (lowercased) keywords.  Empty set means no match / fail-open.
    """
    if not keywords or not forecast_text:
        return set()
    lower = forecast_text.lower()
    return {k for k in keywords if k in lower}


def _fetch_daily_brief_text(
    nws_client: NWSClient,
    lat: float,
    lon: float,
    tz_name: str,
    now_utc: datetime.datetime,
    deadline_hour: int = 12,
) -> str:
    """Fetch the NWS daily brief forecast text for *lat*/*lon* for *today*.

    Uses the **daily** (non-hourly) ``/forecast`` grid endpoint.  Only periods
    whose local (city-timezone) start falls on *now_utc*'s local date **and**
    before *deadline_hour* local are included.  This keeps the low-relevant
    *Overnight* (~00:00) and *Today* (~06:00) periods and drops *Tonight*
    (~18:00), which describes **tomorrow** morning's weather.  Raises on any
    HTTP/model failure so the caller can fail open.
    """
    points = nws_client._get_json(  # noqa: SLF001
        f"https://api.weather.gov/points/{round(float(lat), 4):.4f},{round(float(lon), 4):.4f}"
    )
    forecast_url = points["properties"]["forecast"]
    data = nws_client._get_json(forecast_url)  # noqa: SLF001
    periods = data.get("properties", {}).get("periods") or []

    tz = ZoneInfo(tz_name)
    today_date = now_utc.astimezone(tz).date()

    parts: list[str] = []
    for p in periods:
        if not isinstance(p, dict):
            continue
        start_raw = p.get("startTime")
        if not start_raw:
            continue
        try:
            start_dt = datetime.datetime.fromisoformat(str(start_raw))
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=datetime.timezone.utc)
            period_local = start_dt.astimezone(tz)
        except Exception:  # noqa: BLE001
            continue
        # Only include today's periods that start before the low deadline hour.
        if period_local.date() != today_date or period_local.hour >= deadline_hour:
            continue
        text = p.get("detailedForecast") or p.get("shortForecast") or ""
        if text:
            parts.append(str(text))
    return " ".join(parts).strip()


class DailyBriefGate:
    """Per-city daily-brief keyword gate with once-per-day snapshot + lock.

    Thread-safety note: the scheduler runs in a background thread and the state
    machine runs in the asyncio event loop.  Each is driven by the same process
    but never on the same thread simultaneously, so a plain dict cache is
    adequate (matching ``sunrise_gate``).
    """

    def __init__(self, config: AppConfig, nws_client: Optional[NWSClient] = None) -> None:
        self.config = config
        self.nws_client = nws_client or NWSClient()
        # (series, local_date) -> (locked, blocked, matched_keywords, cached_at)
        self._cache: dict[Tuple[str, datetime.date], Tuple[bool, bool, set[str], float]] = {}
        # (series, local_date) keys for which ``no_snapshot_yet`` was already logged.
        self._no_snapshot_logged: set[Tuple[str, datetime.date]] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_block(
        self, series: str, now_utc: Optional[datetime.datetime] = None
    ) -> Tuple[bool, set[str]]:
        """Return ``(blocked, matched_keywords)`` for *series* for its local day.

        Reads ONLY the decision persisted by the scheduler snapshot
        (:func:`snapshot_city`); never calls NWS.  *blocked* is True when that
        snapshot matched any configured keyword.  When no snapshot exists yet
        for the local day the gate fails open → ``(False, set())`` and the DB
        is re-polled after the cache TTL.
        """
        keywords = self.config.am_low_forecast_keywords or None
        if not keywords:
            return False, set()

        city = SERIES_CITY.get(series)
        if city is None:
            return False, set()

        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Resolve city-local date.
        tz_name = self._series_tz(series)
        if tz_name is None:
            return False, set()
        tz = ZoneInfo(tz_name)
        local_date = now_utc.astimezone(tz).date()

        cache_key = (series, local_date)
        now_mono = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None:
            _locked, _blocked, _matched, _cached_at = cached
            if _locked or (now_mono - _cached_at < _AM_LOW_BRIEF_CACHE_TTL_SECONDS):
                logger.debug(
                    "am_low_brief.cached", series=series, local_date=local_date.isoformat(),
                    blocked=_blocked, matched=sorted(_matched), locked=_locked,
                )
                return _blocked, set(_matched)

        # The snapshot row is the single source of truth for the local day.
        stored = self._stored_row(series, local_date)
        if stored is not None:
            blocked, matched_keywords = stored
            matched = self._parse_matched(matched_keywords)
            self._cache[cache_key] = (True, blocked, matched, now_mono)
            logger.info(
                "am_low_brief.evaluated", series=series, city=city,
                local_date=local_date.isoformat(), blocked=blocked,
                matched=sorted(matched), locked=True, source="db",
            )
            return blocked, matched

        # No snapshot yet → fail open; re-poll the DB after the TTL.
        self._cache[cache_key] = (False, False, set(), now_mono)
        if cache_key not in self._no_snapshot_logged:
            self._no_snapshot_logged.add(cache_key)
            logger.info(
                "am_low_brief.no_snapshot_yet", series=series,
                local_date=local_date.isoformat(),
                snapshot_hour=_snapshot_hour(self.config),
                message="No daily-brief snapshot for today yet — failing open",
            )
        return False, set()

    # ------------------------------------------------------------------
    # Timezone helper
    # ------------------------------------------------------------------

    def _series_tz(self, series: str) -> Optional[str]:
        """Return the IANA timezone for a series prefix, or None."""
        try:
            from core.local_time_gate import SERIES_TIMEZONE
            return SERIES_TIMEZONE.get(series)
        except Exception:  # noqa: BLE001
            return None

    # ------------------------------------------------------------------
    # Storage helpers (best-effort; never raise into the gate)
    # ------------------------------------------------------------------

    def _stored_row(
        self, series: str, local_date: datetime.date
    ) -> Optional[Tuple[bool, Optional[str]]]:
        """Return ``(blocked, matched_keywords)`` for a series/day, or None.

        The scalar attributes are read out *inside* the session.  Returning the
        ORM object outside the ``with`` block would detach it, and accessing an
        (expired) attribute on a detached instance raises ``DetachedInstanceError``.
        """
        try:
            with get_session() as session:
                row = (
                    session.query(DailyForecastBlock)
                    .filter(
                        DailyForecastBlock.series_prefix == series,
                        DailyForecastBlock.local_date == local_date,
                    )
                    .one_or_none()
                )
                if row is None:
                    return None
                return (
                    bool(row.blocked),
                    str(row.matched_keywords) if row.matched_keywords else None,
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("am_low_brief.db_read_failed", series=series, error=str(exc))
            return None

    def _upsert(
        self,
        series: str,
        local_date: datetime.date,
        blocked: bool,
        matched: set[str],
        text: str,
    ) -> None:
        try:
            with get_session() as session:
                row = (
                    session.query(DailyForecastBlock)
                    .filter(
                        DailyForecastBlock.series_prefix == series,
                        DailyForecastBlock.local_date == local_date,
                    )
                    .one_or_none()
                )
                matched_str = ",".join(sorted(matched)) if matched else None
                if row is None:
                    row = DailyForecastBlock(
                        series_prefix=series,
                        local_date=local_date,
                        blocked=blocked,
                        matched_keywords=matched_str,
                        forecast_text=text,
                    )
                    session.add(row)
                else:
                    row.blocked = blocked
                    row.matched_keywords = matched_str
                    row.forecast_text = text
                    row.fetched_at = datetime.datetime.now(datetime.timezone.utc)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "am_low_brief.persist_failed", series=series,
                local_date=local_date.isoformat(), error=str(exc),
            )

    @staticmethod
    def _parse_matched(raw: Optional[str]) -> set[str]:
        if not raw:
            return set()
        return {k.strip().lower() for k in raw.split(",") if k.strip()}


# ---------------------------------------------------------------------------
# Standalone snapshot helper (used by the background scheduler)
# ---------------------------------------------------------------------------

def _keywords_from_env() -> set[str]:
    """Return the ``AM_LOW_FORECAST`` keyword set directly from the environment.

    Used by the scheduler snapshot path, which has no full ``AppConfig``.
    """
    import os
    raw = os.getenv("AM_LOW_FORECAST", "") or ""
    return {k.strip().lower() for k in raw.split(",") if k.strip()}


def _snapshot_hour_from_env() -> int:
    """Return the configured AM-low snapshot hour (int 0–23) from the env."""
    import os
    raw = os.getenv("AM_LOW_SNAPSHOT_LOCAL_HOUR", "") or ""
    try:
        hh = int(str(raw).strip().split(":")[0])
        return hh if 0 <= hh <= 23 else 3
    except (ValueError, IndexError, TypeError, AttributeError):
        return 3


def _deadline_hour_from_env() -> int:
    """Return ``NWS_LOW_DEADLINE_HOUR`` (int 0–23) from the env (default 12)."""
    import os
    raw = os.getenv("NWS_LOW_DEADLINE_HOUR", "") or ""
    try:
        hh = int(str(raw).strip().split(":")[0])
        return hh if 0 <= hh <= 23 else 12
    except (ValueError, IndexError, TypeError, AttributeError):
        return 12


def _new_snapshot_gate() -> DailyBriefGate:
    """Build a config-less :class:`DailyBriefGate` for storage helpers."""
    gate = DailyBriefGate.__new__(DailyBriefGate)  # noqa: SLF001
    gate.config = None
    gate.nws_client = None
    gate._cache = {}  # noqa: SLF001
    gate._no_snapshot_logged = set()  # noqa: SLF001
    return gate


def snapshot_exists(series: str, local_date: datetime.date) -> bool:
    """Return True when a daily-brief decision is already stored for the day."""
    return _new_snapshot_gate()._stored_row(series, local_date) is not None  # noqa: SLF001


def snapshot_city(
    series: str, city: str, lat: float, lon: float, force: bool = False
) -> bool:
    """Fetch + persist the daily-brief keyword decision for *city* for today.

    Used by the APScheduler background jobs.  Reads ``AM_LOW_FORECAST`` and
    ``NWS_LOW_DEADLINE_HOUR`` from the environment (so no full ``AppConfig`` is
    required) and writes to the ``daily_forecast_block`` table via
    :meth:`DailyBriefGate._upsert`.

    The decision is **write-once**: when a row already exists for today the
    snapshot is skipped (no NWS call, no overwrite) unless *force* is True.

    Returns True on success, False on any failure (the gate fails open).
    """
    keywords = _keywords_from_env()
    if not keywords:
        return True  # feature disabled — nothing to do

    try:
        gate = _new_snapshot_gate()
        tz_name = _series_tz_name(series) or "UTC"
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        local_date = now_utc.astimezone(ZoneInfo(tz_name)).date()
        if not force and gate._stored_row(series, local_date) is not None:  # noqa: SLF001
            logger.info(
                "nws.daily_brief.snapshot_exists", series=series, city=city,
                local_date=local_date.isoformat(),
            )
            return True
        gate.nws_client = NWSClient()
        deadline_hour = _deadline_hour_from_env()
        text = _fetch_daily_brief_text(
            gate.nws_client, lat, lon, tz_name=tz_name, now_utc=now_utc,
            deadline_hour=deadline_hour,
        )
        matched = matches_any_keyword(text, keywords)
        blocked = bool(matched)
        gate._upsert(series, local_date, blocked, matched, text)  # noqa: SLF001
        logger.info(
            "nws.daily_brief.snapshotted", series=series, city=city,
            local_date=local_date.isoformat(), deadline_hour=deadline_hour,
            blocked=blocked, matched=sorted(matched), forced=force,
        )
        return True
    except Exception:  # noqa: BLE001
        logger.exception(
            "nws.daily_brief.snapshot_error", series=series, city=city,
            lat=lat, lon=lon,
        )
        return False


def _series_tz_name(series: str) -> Optional[str]:
    """Return the IANA timezone name for a series, or None (best-effort)."""
    try:
        from core.local_time_gate import SERIES_TIMEZONE
        return SERIES_TIMEZONE.get(series)
    except Exception:  # noqa: BLE001
        return None

