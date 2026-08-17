# nws/awc_obs.py
"""aviationweather.gov METAR observation client.

Primary observation source for the sunrise gate.  Falls back to the
api.weather.gov ``observations`` endpoint when AWC is unavailable or
returns fewer than 2 usable records.

Only observation fetches live here; forecast fetches remain in nws/client.py.
"""
from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING, List, Optional, Tuple

import requests

from nws.config import NWS_USER_AGENT

if TYPE_CHECKING:
    from nws.client import NWSClient

logger = logging.getLogger("forecastology.nws.awc_obs")

AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"

# Type alias: list of (utc_datetime, temp_celsius) tuples
ObsList = List[Tuple[datetime.datetime, float]]


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
) -> tuple[ObsList, str]:
    """Fetch station observations using the configured source with fallback.

    ``obs_source="awc"`` (default): try aviationweather.gov first; fall back
    to ``nws_url`` on network error, non-200 response, or fewer than 2 usable
    observations.

    ``obs_source="nws"``: legacy behaviour — use ``nws_url`` directly.

    Returns ``(obs_list, source)`` where *source* is ``"awc"`` or ``"nws"``.
    Logs source switches at INFO and per-fetch source at DEBUG.
    Raises on NWS fetch failure (let caller handle).
    """
    if obs_source == "nws":
        logger.debug("sunrise.obs_fetch source=nws station=%s", station_id)
        obs = parse_nws_obs_payload(nws_client._get_json(nws_url))  # noqa: SLF001
        _maybe_log_source_change(station_id, "nws")
        return obs, "nws"

    # ---- AWC primary path ------------------------------------------------
    reason: Optional[str] = None
    try:
        obs = fetch_awc_obs(station_id, user_agent=user_agent, timeout=timeout)
        if len(obs) >= 2:
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
