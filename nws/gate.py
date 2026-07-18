# nws/gate.py
"""Trading gate logic based on NWS forecasted settlement day.

The gate is open when the forecast row's settlement/trading day matches the
target trading day for the station:

- When ``market_date`` is provided: the target is that specific date.
- When ``market_date`` is omitted: the target is the station's current
  trading day derived from the current wall-clock time.

The gate is closed (fail-safe) when:
- No forecast row exists for the station.
- The stored forecast's settlement day does not match the target trading day
  (stale forecast).
- A DB error occurs while loading the forecast.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.exc import SQLAlchemyError

from app.models import StationForecast
from nws.client import _station_cache, get_trading_day_window
from nws.db import get_session

logger = logging.getLogger("forecastology.nws.gate")


def _ensure_utc(dt: datetime) -> datetime:
    """Return *dt* as a timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_forecast(session, station_code: str) -> Optional[StationForecast]:
    """Return the most-recently stored forecast row for *station_code*."""
    return (
        session.query(StationForecast)
        .filter(StationForecast.station_code == station_code)
        .order_by(StationForecast.forecast_date_utc.desc())
        .first()
    )


def _forecast_date_utc_for_local_date(trading_date_local: date) -> datetime:
    """Return UTC midnight used to key a station-local trading date in storage."""
    return datetime(
        trading_date_local.year,
        trading_date_local.month,
        trading_date_local.day,
        tzinfo=timezone.utc,
    )


def _expected_forecast_date_utc(
    station_code: str, now_utc: datetime
) -> tuple[datetime, str]:
    """Return the expected stored forecast_date_utc for the active trading day."""
    cached_station = _station_cache.get(station_code)
    if cached_station is not None:
        _lat, _lon, _hourly_url, tz_name = cached_station
        try:
            station_tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            logger.warning(
                "gate.invalid_station_timezone station=%s tz=%s — falling back to UTC day",
                station_code,
                tz_name,
            )
        else:
            window = get_trading_day_window(station_code, station_tz, now_utc)
            return _forecast_date_utc_for_local_date(window.trading_date_local), tz_name
    return datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc), "UTC"


def _forecast_day_matches(
    forecast_date_utc: datetime, station_code: str, now_utc: datetime
) -> tuple[bool, datetime, datetime, str]:
    """Return whether *forecast_date_utc* matches the station's current trading day."""
    forecast_date_utc = _ensure_utc(forecast_date_utc)
    expected_forecast_date_utc, expected_tz = _expected_forecast_date_utc(
        station_code, now_utc
    )
    return (
        forecast_date_utc == expected_forecast_date_utc,
        forecast_date_utc,
        expected_forecast_date_utc,
        expected_tz,
    )


def has_forecast(
    station_code: str,
    current_utc_time: datetime | None = None,
    market_date: date | None = None,
) -> bool:
    """Return ``True`` if a current trading-day forecast row exists for *station_code*.

    Used by the entry gate to distinguish "gate closed because outside window"
    from "gate closed because no valid data".

    When *market_date* is provided, the check is keyed to that specific date
    instead of the trading day derived from *current_utc_time*.  This ensures
    the gate only considers a forecast valid for the ticker's own market date.

    Returns ``False`` on any DB error (treating it the same as no data).
    """
    now = _ensure_utc(current_utc_time or datetime.now(timezone.utc))
    try:
        with get_session() as session:
            forecast = _latest_forecast(session, station_code)
            if forecast is None:
                return False
            forecast_date_utc = forecast.forecast_date_utc
            high_time_utc = forecast.high_time_utc
            low_time_utc = forecast.low_time_utc
    except SQLAlchemyError:
        logger.exception(
            "gate.db_error station=%s — has_forecast returning False", station_code
        )
        return False

    if market_date is not None:
        expected_forecast_date_utc = _forecast_date_utc_for_local_date(market_date)
        forecast_date_utc_aware = _ensure_utc(forecast_date_utc)
        matches = forecast_date_utc_aware == expected_forecast_date_utc
        expected_tz = "market_date"
        forecast_date_utc_log = forecast_date_utc_aware
    else:
        matches, forecast_date_utc_log, expected_forecast_date_utc, expected_tz = (
            _forecast_day_matches(forecast_date_utc, station_code, now)
        )
    logger.debug(
        "gate.has_forecast_check station=%s tz=%s forecast_date_utc=%s "
        "expected_forecast_date_utc=%s high_time_utc=%s low_time_utc=%s now_utc=%s "
        "has_current_forecast=%s",
        station_code,
        expected_tz,
        forecast_date_utc_log.isoformat(),
        expected_forecast_date_utc.isoformat(),
        high_time_utc.isoformat() if high_time_utc else None,
        low_time_utc.isoformat() if low_time_utc else None,
        now.isoformat(),
        matches,
    )
    return matches


def is_trading_gate_open(
    station_code: str,
    current_utc_time: datetime,
    market_date: date | None = None,
) -> bool:
    """Return ``True`` if the NWS gate is open for *station_code*.

    The gate is open when the stored forecast's settlement/trading day matches
    the target trading day:

    - If *market_date* is provided, the target is that specific date.
    - If *market_date* is omitted, the target is the station's current trading
      day derived from *current_utc_time*.

    The gate is closed (fail-safe) when no forecast row exists, the forecast is
    stale (wrong day), or a DB error occurs.

    Args:
        station_code: NWS ICAO code, e.g. ``"KATL"``.
        current_utc_time: The time to evaluate; naive datetimes are assumed UTC.
        market_date: Optional ticker market date.  When supplied the gate
            checks this date's settlement day instead of deriving it from
            *current_utc_time*.

    Returns:
        ``True`` if the gate is open, ``False`` otherwise.
    """
    now = _ensure_utc(current_utc_time)

    try:
        with get_session() as session:
            forecast = _latest_forecast(session, station_code)
            if forecast is None:
                logger.warning(
                    "gate.no_forecast station=%s — gate closed (no data)", station_code
                )
                return False
            forecast_date_utc = forecast.forecast_date_utc
            high_time_utc = forecast.high_time_utc
            low_time_utc = forecast.low_time_utc
    except SQLAlchemyError:
        logger.exception(
            "gate.db_error station=%s — gate closed (fail-safe)", station_code
        )
        return False

    if market_date is not None:
        expected_forecast_date_utc = _forecast_date_utc_for_local_date(market_date)
        forecast_date_utc_aware = _ensure_utc(forecast_date_utc)
        matches = forecast_date_utc_aware == expected_forecast_date_utc
        expected_tz = "market_date"
        forecast_date_utc = forecast_date_utc_aware
    else:
        matches, forecast_date_utc, expected_forecast_date_utc, expected_tz = (
            _forecast_day_matches(forecast_date_utc, station_code, now)
        )

    if not matches:
        logger.info(
            "gate.stale_forecast station=%s tz=%s forecast_date_utc=%s "
            "expected_forecast_date_utc=%s high_time_utc=%s low_time_utc=%s now_utc=%s "
            "— gate closed (stale forecast)",
            station_code,
            expected_tz,
            forecast_date_utc.isoformat(),
            expected_forecast_date_utc.isoformat(),
            high_time_utc.isoformat() if high_time_utc else None,
            low_time_utc.isoformat() if low_time_utc else None,
            now.isoformat(),
        )
        return False

    logger.debug(
        "gate.settlement_day_match station=%s tz=%s forecast_date_utc=%s "
        "expected_forecast_date_utc=%s high_time_utc=%s low_time_utc=%s now_utc=%s — gate open",
        station_code,
        expected_tz,
        forecast_date_utc.isoformat(),
        expected_forecast_date_utc.isoformat(),
        high_time_utc.isoformat() if high_time_utc else None,
        low_time_utc.isoformat() if low_time_utc else None,
        now.isoformat(),
    )
    return True

