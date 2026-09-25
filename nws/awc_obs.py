# nws/awc_obs.py
"""aviationweather.gov METAR observation client.

Primary observation source for the sunrise gate.  Falls back to the
api.weather.gov ``observations`` endpoint when AWC is unavailable or
returns fewer than 2 usable records.

Only observation fetches live here; forecast fetches remain in nws/client.py.
"""
from __future__ import annotations

import datetime
import time
from typing import TYPE_CHECKING, Callable, List, Optional, Tuple

import requests
import structlog

from nws.config import NWS_USER_AGENT

if TYPE_CHECKING:
    from nws.client import NWSClient

logger = structlog.get_logger(__name__)

AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"

# Type alias: list of (utc_datetime, temp_celsius) tuples
ObsList = List[Tuple[datetime.datetime, float]]


# ---------------------------------------------------------------------------
# Feed-stall tracking (Phase C)
# ---------------------------------------------------------------------------
# Number of DISTINCT stations that must report a stale newest observation
# within one cycle window before a single CRITICAL feed-level alert is emitted.
_STALL_ALERT_STATION_THRESHOLD = 3
# A cycle is reset once this many seconds elapse without a staleness report.
_STALL_CYCLE_WINDOW_SECONDS = 120.0

_stall_stations: dict[str, float] = {}
_stall_cycle_started: Optional[float] = None
_stall_cycle_alerted: bool = False


def _newest_age_minutes(obs: ObsList, now_utc: datetime.datetime) -> Optional[float]:
    """Return the age (minutes) of the newest observation in *obs*, or None."""
    if not obs:
        return None
    newest_ts = max(ts for ts, _temp in obs)
    return (now_utc - newest_ts).total_seconds() / 60.0


def note_obs_staleness(
    station_id: str,
    age_minutes: float,
    *,
    max_age_minutes: float,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> bool:
    """Record a stale observation and alert when many stations are stale at once.

    Returns ``True`` when this call triggered a feed-level CRITICAL alert.

    The tracker groups staleness reports into cycles: consecutive reports
    within :data:`_STALL_CYCLE_WINDOW_SECONDS` belong to the same cycle.  When
    the number of distinct stale stations in a cycle reaches
    :data:`_STALL_ALERT_STATION_THRESHOLD`, a single CRITICAL
    ``feed.observations_stalled`` line is emitted (once per cycle).
    """
    global _stall_cycle_started, _stall_cycle_alerted
    now_mono = monotonic_fn()

    if (
        _stall_cycle_started is None
        or now_mono - _stall_cycle_started > _STALL_CYCLE_WINDOW_SECONDS
    ):
        _stall_stations.clear()
        _stall_cycle_started = now_mono
        _stall_cycle_alerted = False

    _stall_stations[station_id] = age_minutes

    distinct = len(_stall_stations)
    if distinct >= _STALL_ALERT_STATION_THRESHOLD and not _stall_cycle_alerted:
        _stall_cycle_alerted = True
        logger.critical(
            "feed.observations_stalled",
            stations=sorted(_stall_stations),
            station_count=distinct,
            max_age_minutes=max_age_minutes,
            oldest_age_minutes=max(_stall_stations.values()),
        )
        return True
    return False


def reset_obs_stall_tracker() -> None:
    """Clear feed-stall tracking state (used by tests)."""
    global _stall_cycle_started, _stall_cycle_alerted
    _stall_stations.clear()
    _stall_cycle_started = None
    _stall_cycle_alerted = False


# ---------------------------------------------------------------------------
# AWC parsing helpers
# ---------------------------------------------------------------------------


def _parse_awc_response(data: list) -> ObsList:
    """Parse AWC METAR JSON array into (utc_datetime, temp_celsius) list, newest first.

    Handles both Unix-epoch integers and ISO-8601 strings for ``obsTime`` /
    ``reportTime``.  Records with missing or un-parseable temp/time are skipped.
    """
    result: ObsList = []
    for record in data:
        if not isinstance(record, dict):
            continue
        temp = record.get("temp")
        if temp is None:
            continue
        try:
            temp_c = float(temp)
        except (TypeError, ValueError):
            continue

        obs_time = record.get("obsTime") if record.get("obsTime") is not None else record.get("reportTime")
        if obs_time is None:
            continue
        try:
            if isinstance(obs_time, (int, float)):
                obs_ts = datetime.datetime.fromtimestamp(float(obs_time), tz=datetime.timezone.utc)
            else:
                obs_ts = datetime.datetime.fromisoformat(str(obs_time).replace("Z", "+00:00"))
                if obs_ts.tzinfo is None:
                    obs_ts = obs_ts.replace(tzinfo=datetime.timezone.utc)
                obs_ts = obs_ts.astimezone(datetime.timezone.utc)
        except (ValueError, OSError, OverflowError):
            continue

        result.append((obs_ts, temp_c))

    result.sort(key=lambda x: x[0], reverse=True)
    return result


def fetch_awc_obs(
    station_id: str,
    *,
    hours: float = 2.0,
    user_agent: str = "",
    timeout: int = 15,
) -> ObsList:
    """Fetch METAR observations from aviationweather.gov.

    Returns a list of (utc_datetime, temp_celsius) tuples, newest first.
    Raises :class:`RuntimeError` or :mod:`requests` exceptions on failure.
    """
    ua = user_agent or NWS_USER_AGENT or "forecastology/1.0"
    resp = requests.get(
        AWC_METAR_URL,
        params={"ids": station_id, "format": "json", "hours": hours},
        headers={"User-Agent": ua},
        timeout=timeout,
    )
    if resp.status_code >= 400:
        raise RuntimeError(
            f"AWC METAR request failed {resp.status_code} for {station_id}: "
            f"{resp.text[:200]}"
        )
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(
            f"AWC METAR unexpected response type {type(data).__name__} for {station_id}"
        )
    return _parse_awc_response(data)


# ---------------------------------------------------------------------------
# NWS observation parsing helper (shared between primary and fallback paths)
# ---------------------------------------------------------------------------


def parse_nws_obs_payload(payload: dict) -> ObsList:
    """Parse api.weather.gov GeoJSON observations payload.

    Returns a list of (utc_datetime, temp_celsius) tuples, newest first.
    """
    features = payload.get("features") or []
    result: ObsList = []
    for item in features:
        props = item.get("properties") or {}
        temp_val = (props.get("temperature") or {}).get("value")
        timestamp = props.get("timestamp")
        if temp_val is None or not timestamp:
            continue
        try:
            obs_ts = datetime.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        except ValueError:
            continue
        if obs_ts.tzinfo is None:
            obs_ts = obs_ts.replace(tzinfo=datetime.timezone.utc)
        result.append((obs_ts.astimezone(datetime.timezone.utc), float(temp_val)))
    result.sort(key=lambda x: x[0], reverse=True)
    return result


# ---------------------------------------------------------------------------
# Combined fetch with primary/fallback logic
# ---------------------------------------------------------------------------

# Per-station source-switch tracking for INFO-level log suppression
_source_state: dict[str, str] = {}  # station_id → last logged source


def fetch_obs_with_fallback(
    station_id: str,
    *,
    nws_client: "NWSClient",
    nws_url: str,
    obs_source: str = "awc",
    user_agent: str = "",
    timeout: int = 15,
    hours: Optional[float] = None,
    max_age_minutes: Optional[float] = None,
    now_utc: Optional[datetime.datetime] = None,
) -> tuple[ObsList, str]:
    """Fetch station observations using the configured source with fallback.

    ``obs_source="awc"`` (default): try aviationweather.gov first; fall back
    to ``nws_url`` on network error, non-200 response, or fewer than 2 usable
    observations.

    ``obs_source="nws"``: legacy behaviour — use ``nws_url`` directly.

    *hours* bounds how far back the AWC primary path looks.  When ``None`` the
    AWC client default (2.0h) is used, which is fine for callers that only need
    the most recent reports (e.g. the temperature-rise latch).  Callers that
    need the WHOLE trading day (e.g. the day-min "already dipped below"
    tracker, anchored at station-local midnight) MUST pass an explicit *hours*
    large enough to cover it -- otherwise the AWC primary path silently
    truncates the window and early-day dips are missed.  The NWS fallback path
    ignores *hours* (it uses the ``start=`` query embedded in *nws_url*).

    Freshness handling (Phase A):

        When *max_age_minutes* is supplied and the AWC result's newest
        observation is older than that ceiling, the NWS endpoint is ALSO
        queried and whichever source has the *newer* newest-observation wins.
        This defends against AWC "succeeding" (HTTP 200, >=2 records) while
        silently serving a stale newest report -- e.g. a nationwide upstream
        reporting gap.  When ``None`` (the default) AWC is served as-is and
        no extra request is made, preserving existing callers' behaviour.

    Returns ``(obs_list, source)`` where *source* is ``"awc"`` or ``"nws"``.
    Logs source switches at INFO and per-fetch source at DEBUG.
    Raises on NWS fetch failure (let caller handle).
    """
    if obs_source == "nws":
        logger.debug("sunrise.obs_fetch", source="nws", station=station_id)
        obs = parse_nws_obs_payload(nws_client._get_json(nws_url))  # noqa: SLF001
        _maybe_log_source_change(station_id, "nws")
        return obs, "nws"

    _now = now_utc or datetime.datetime.now(datetime.timezone.utc)

    # ---- AWC primary path ------------------------------------------------
    reason: Optional[str] = None
    try:
        obs = fetch_awc_obs(
            station_id,
            hours=hours if hours is not None else 2.0,
            user_agent=user_agent,
            timeout=timeout,
        )
        if len(obs) >= 2:
            awc_age = _newest_age_minutes(obs, _now)
            if (
                max_age_minutes is not None
                and awc_age is not None
                and awc_age > max_age_minutes
            ):
                # AWC looks stale -- consult NWS and keep whichever source is
                # fresher.  A failure of the cross-check must never lose the
                # AWC result, so it is wrapped defensively.
                logger.info(
                    "sunrise.obs_freshness_crosscheck",
                    station=station_id,
                    awc_age_minutes=round(awc_age, 1),
                    max_age_minutes=max_age_minutes,
                )
                try:
                    nws_obs = parse_nws_obs_payload(
                        nws_client._get_json(nws_url)  # noqa: SLF001
                    )
                    nws_age = _newest_age_minutes(nws_obs, _now)
                    if nws_age is not None and (
                        awc_age is None or nws_age < awc_age
                    ):
                        logger.info(
                            "sunrise.obs_freshness_crosscheck",
                            station=station_id,
                            winner="nws",
                            awc_age_minutes=round(awc_age, 1),
                            nws_age_minutes=round(nws_age, 1),
                        )
                        _maybe_log_source_change(station_id, "nws")
                        return nws_obs, "nws"
                    logger.info(
                        "sunrise.obs_freshness_crosscheck",
                        station=station_id,
                        winner="awc",
                        awc_age_minutes=round(awc_age, 1),
                        nws_age_minutes=(
                            round(nws_age, 1) if nws_age is not None else None
                        ),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.info(
                        "sunrise.obs_freshness_crosscheck",
                        station=station_id,
                        winner="awc",
                        reason="nws_crosscheck_failed",
                        error_class=type(exc).__name__,
                        error_message=str(exc)[:200],
                    )
            logger.debug(
                "sunrise.obs_fetch source=awc station=%s count=%d", station_id, len(obs)
            )
            _maybe_log_source_change(station_id, "awc")
            return obs, "awc"
        reason = f"insufficient_obs count={len(obs)}"
    except Exception as exc:  # noqa: BLE001
        reason = type(exc).__name__
        logger.info(
            "sunrise.obs_source_fallback",
            station=station_id,
            reason=reason,
            error_class=type(exc).__name__,
            error_message=str(exc)[:200],
        )

    if reason and "insufficient" in reason:
        logger.info(
            "sunrise.obs_source_fallback",
            station=station_id,
            reason=reason,
        )

    # ---- NWS fallback path -----------------------------------------------
    logger.debug(
        "sunrise.obs_fetch source=nws (fallback) station=%s reason=%s", station_id, reason
    )
    obs = parse_nws_obs_payload(nws_client._get_json(nws_url))  # noqa: SLF001
    _maybe_log_source_change(station_id, "nws")
    return obs, "nws"


def _maybe_log_source_change(station_id: str, new_source: str) -> None:
    """Emit an INFO log only when the source for a station changes."""
    prev = _source_state.get(station_id)
    if prev != new_source:
        if prev is not None:
            # Switched sources
            if new_source == "nws":
                logger.info(
                    "sunrise.obs_source_fallback",
                    station=station_id,
                    reason="source_changed_to_nws",
                )
            else:
                logger.info(
                    "sunrise.obs_source_recovered",
                    station=station_id,
                )
        _source_state[station_id] = new_source
