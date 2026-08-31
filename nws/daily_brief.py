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
* The decision is snapshotted **once per city-local date** and locked in for the rest
  of the day at/after ``AM_LOW_SNAPSHOT_LOCAL_HOUR`` (mirrors ``sunrise_gate``), stored
  in the ``daily_forecast_block`` table so it survives process restarts.
* On fetch/API failure the gate **fails open** (does not block trading) and logs loudly.
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
) -> str:
    """Fetch the NWS daily brief forecast text for *lat*/*lon* for *today*.

    Uses the **daily** (non-hourly) ``/forecast`` grid endpoint.  Only periods
    whose local (city-timezone) date matches *now_utc*'s local date are included,
    so the gate checks the **current calendar day only** — not the rest of the
    multi-day forecast array.  Raises on any HTTP/model failure so the caller can
    fail open.
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
        # Only include periods that fall within the current local calendar day.
        if period_local.date() != today_date:
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_block(
        self, series: str, now_utc: Optional[datetime.datetime] = None
    ) -> Tuple[bool, set[str]]:
        """Return ``(blocked, matched_keywords)`` for *series* for its local day.

        *blocked* is True when the day's brief forecast contains any configured
        keyword.  On fetch failure it fails open → ``(False, set())``.
        """
        keywords = self.config.am_low_forecast_keywords or None
        if not keywords:
            return False, set()

        city = SERIES_CITY.get(series)
        if city is None:
            return False, set()
        coords = CITY_COORDS.get(city)
        if coords is None:
            logger.warning(
                "am_low_brief.no_coords", series=series, city=city,
                message="No lat/lon for city — failing open",
            )
            return False, set()

        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Resolve city-local date.
        tz_name = self._series_tz(series)
        if tz_name is None:
            return False, set()
        tz = ZoneInfo(tz_name)
        now_local = now_utc.astimezone(tz)
        local_date = now_local.date()

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

        # Try a stored DB row first (restart-safe, avoids double pull).
        stored = self._stored_row(series, local_date)
        if stored is not None:
            blocked, matched_keywords = stored
            matched = self._parse_matched(matched_keywords)
            lock_for_day = now_local.hour >= _snapshot_hour(self.config)
            self._cache[cache_key] = (lock_for_day, blocked, matched, now_mono)
            return blocked, matched

        # Otherwise fetch (lazy / scheduled path) and persist.
        lat, lon = coords
        try:
            text = _fetch_daily_brief_text(
                self.nws_client, lat, lon, tz_name=tz_name, now_utc=now_utc
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "am_low_brief.fetch_failed", series=series, city=city,
                local_date=local_date.isoformat(),
                error_class=type(exc).__name__, error_message=str(exc),
                message="Failing open on AM-low daily brief fetch failure",
            )
            self._cache[cache_key] = (False, False, set(), now_mono)
            return False, set()

        matched = matches_any_keyword(text, keywords)
        blocked = bool(matched)
        lock_for_day = now_local.hour >= _snapshot_hour(self.config)
        self._upsert(series, local_date, blocked, matched, text)
        self._cache[cache_key] = (lock_for_day, blocked, matched, now_mono)
        logger.info(
            "am_low_brief.evaluated", series=series, city=city,
            local_date=local_date.isoformat(), blocked=blocked,
            matched=sorted(matched), locked=lock_for_day,
        )
        return blocked, matched

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


def snapshot_city(series: str, city: str, lat: float, lon: float) -> bool:
    """Fetch + persist the daily-brief keyword decision for *city* for today.

    Prepared for use by APScheduler background jobs.  Reads ``AM_LOW_FORECAST``
    from the environment (so no full ``AppConfig`` is required) and writes to
    the ``daily_forecast_block`` table via :meth:`DailyBriefGate._upsert`.

    Returns True on success, False on any failure (the gate fails open).
    """
    keywords = _keywords_from_env()
    if not keywords:
        return True  # feature disabled — nothing to do

    try:
        gate = DailyBriefGate.__new__(DailyBriefGate)  # noqa: SLF001
        gate.config = None
        gate.nws_client = NWSClient()
        gate._cache = {}  # noqa: SLF001
        tz_name = _series_tz_name(series) or "UTC"
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        text = _fetch_daily_brief_text(
            gate.nws_client, lat, lon, tz_name=tz_name, now_utc=now_utc
        )
        matched = matches_any_keyword(text, keywords)
        blocked = bool(matched)
        gate._upsert(series, _today_local(series), blocked, matched, text)  # noqa: SLF001
        logger.info(
            "nws.daily_brief.snapshotted", series=series, city=city,
            blocked=blocked, matched=sorted(matched),
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


def _today_local(series: str) -> datetime.date:
    """Return the current local calendar date for a series (best-effort)."""
    tz_name = _series_tz_name(series)
    if tz_name:
        try:
            return datetime.datetime.now(ZoneInfo(tz_name)).date()
        except Exception:  # noqa: BLE001
            pass
    return datetime.datetime.now(datetime.timezone.utc).date()
