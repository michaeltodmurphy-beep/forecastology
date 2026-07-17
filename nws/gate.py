# nws/gate.py
"""Trading gate logic based on NWS forecasted high/low temperature times.

The gate is open when the current UTC time falls within one of two windows:

    - **Low window**: [low_time − GATE_LOW_BEFORE min, low_time + GATE_LOW_AFTER min]
    - **High window**: [high_time − GATE_HIGH_BEFORE min, high_time + GATE_HIGH_AFTER min]

Window durations are controlled by environment variables (see ``nws/config.py``).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.exc import SQLAlchemyError

from app.models import StationForecast
from nws.config import (
    GATE_HIGH_AFTER,
    GATE_HIGH_BEFORE,
    GATE_LOW_AFTER,
    GATE_LOW_BEFORE,
)
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
    forecast: StationForecast, station_code: str, now_utc: datetime
) -> tuple[bool, datetime, datetime, str]:
    """Return whether *forecast* matches the station's current trading day."""
    forecast_date_utc = _ensure_utc(forecast.forecast_date_utc)
    expected_forecast_date_utc, expected_tz = _expected_forecast_date_utc(
        station_code, now_utc
    )
    return (
        forecast_date_utc == expected_forecast_date_utc,
        forecast_date_utc,
        expected_forecast_date_utc,
        expected_tz,
    )


def _trading_day_window_bounds(station_code: str, now_utc: datetime) -> tuple[datetime, datetime]:
    """Return UTC [start, end) bounds for the station's active trading day."""
    cached_station = _station_cache.get(station_code)
    if cached_station is not None:
        _lat, _lon, _hourly_url, tz_name = cached_station
        try:
            station_tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            logger.warning(
                "gate.invalid_station_timezone station=%s tz=%s — using UTC-day window",
                station_code,
                tz_name,
            )
        else:
            window = get_trading_day_window(station_code, station_tz, now_utc)
            return window.utc_start, window.utc_end_exclusive

    utc_start = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
    return utc_start, utc_start + timedelta(days=1)


def has_forecast(
    station_code: str, current_utc_time: datetime | None = None
) -> bool:
    """Return ``True`` if a current trading-day forecast row exists for *station_code*.

    Used by the entry gate to distinguish "gate closed because outside window"
    from "gate closed because no valid data".

    Returns ``False`` on any DB error (treating it the same as no data).
    """
    now = _ensure_utc(current_utc_time or datetime.now(timezone.utc))
    try:
        with get_session() as session:
            forecast = _latest_forecast(session, station_code)
    except SQLAlchemyError:
        logger.exception(
            "gate.db_error station=%s — has_forecast returning False", station_code
        )
        return False

    if forecast is None:
        return False

    matches, forecast_date_utc, expected_forecast_date_utc, expected_tz = (
        _forecast_day_matches(forecast, station_code, now)
    )
    logger.debug(
        "gate.has_forecast_check station=%s tz=%s forecast_date_utc=%s "
        "expected_forecast_date_utc=%s high_time_utc=%s low_time_utc=%s now_utc=%s "
        "has_current_forecast=%s",
        station_code,
        expected_tz,
        forecast_date_utc.isoformat(),
        expected_forecast_date_utc.isoformat(),
        forecast.high_time_utc.isoformat() if forecast.high_time_utc else None,
        forecast.low_time_utc.isoformat() if forecast.low_time_utc else None,
        now.isoformat(),
        matches,
    )
    return matches


def is_trading_gate_open(station_code: str, current_utc_time: datetime) -> bool:
    """Return ``True`` if trading is currently allowed for *station_code*.

    The gate is open when *current_utc_time* falls within either:

    - ``[low_time − GATE_LOW_BEFORE, low_time + GATE_LOW_AFTER]``
    - ``[high_time − GATE_HIGH_BEFORE, high_time + GATE_HIGH_AFTER]``

    If no forecast data exists for the station, the gate is **closed**
    (fail-safe: do not trade on missing data).

    Args:
        station_code: NWS ICAO code, e.g. ``"KATL"``.
        current_utc_time: The time to evaluate; naive datetimes are assumed UTC.

    Returns:
        ``True`` if the gate is open, ``False`` otherwise.
    """
    now = _ensure_utc(current_utc_time)

    try:
        with get_session() as session:
            forecast = _latest_forecast(session, station_code)
    except SQLAlchemyError:
        logger.exception(
            "gate.db_error station=%s — gate closed (fail-safe)", station_code
        )
        return False

    if forecast is None:
        logger.warning(
            "gate.no_forecast station=%s — gate closed (no data)", station_code
        )
        return False

    matches, forecast_date_utc, expected_forecast_date_utc, expected_tz = (
        _forecast_day_matches(forecast, station_code, now)
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
            forecast.high_time_utc.isoformat() if forecast.high_time_utc else None,
            forecast.low_time_utc.isoformat() if forecast.low_time_utc else None,
            now.isoformat(),
        )
        return False

    window_start_utc, window_end_utc = _trading_day_window_bounds(station_code, now)

    in_low_window = False
    in_high_window = False
    low_utc = low_open = low_close = None
    high_utc = high_open = high_close = None

    if forecast.low_time_utc is not None:
        low_utc = _ensure_utc(forecast.low_time_utc)
        if window_start_utc <= low_utc < window_end_utc:
            low_open = low_utc - timedelta(minutes=GATE_LOW_BEFORE)
            low_close = low_utc + timedelta(minutes=GATE_LOW_AFTER)
            in_low_window = low_open <= now <= low_close
        else:
            logger.info(
                "gate.reject_out_of_window_timestamp station=%s field=low_time_utc "
                "timestamp_utc=%s window_start_utc=%s window_end_exclusive_utc=%s now_utc=%s",
                station_code,
                low_utc.isoformat(),
                window_start_utc.isoformat(),
                window_end_utc.isoformat(),
                now.isoformat(),
            )
            low_utc = None

    if forecast.high_time_utc is not None:
        high_utc = _ensure_utc(forecast.high_time_utc)
        if window_start_utc <= high_utc < window_end_utc:
            high_open = high_utc - timedelta(minutes=GATE_HIGH_BEFORE)
            high_close = high_utc + timedelta(minutes=GATE_HIGH_AFTER)
            in_high_window = high_open <= now <= high_close
        else:
            logger.info(
                "gate.reject_out_of_window_timestamp station=%s field=high_time_utc "
                "timestamp_utc=%s window_start_utc=%s window_end_exclusive_utc=%s now_utc=%s",
                station_code,
                high_utc.isoformat(),
                window_start_utc.isoformat(),
                window_end_utc.isoformat(),
                now.isoformat(),
            )
            high_utc = None

    if low_utc is None and high_utc is None:
        logger.warning(
            "gate.no_valid_forecast_times station=%s tz=%s forecast_date_utc=%s "
            "expected_forecast_date_utc=%s window_start_utc=%s window_end_exclusive_utc=%s "
            "now_utc=%s — gate closed (no valid forecast times)",
            station_code,
            expected_tz,
            forecast_date_utc.isoformat(),
            expected_forecast_date_utc.isoformat(),
            window_start_utc.isoformat(),
            window_end_utc.isoformat(),
            now.isoformat(),
        )
        return False

    gate_open = in_low_window or in_high_window
    logger.debug(
        "gate.window_eval station=%s tz=%s forecast_date_utc=%s "
        "expected_forecast_date_utc=%s high_time_utc=%s low_time_utc=%s "
        "high_open=%s high_close=%s low_open=%s low_close=%s now_utc=%s "
        "in_low=%s in_high=%s open=%s",
        station_code,
        expected_tz,
        forecast_date_utc.isoformat(),
        expected_forecast_date_utc.isoformat(),
        high_utc.isoformat() if high_utc else None,
        low_utc.isoformat() if low_utc else None,
        high_open.isoformat() if high_open else None,
        high_close.isoformat() if high_close else None,
        low_open.isoformat() if low_open else None,
        low_close.isoformat() if low_close else None,
        now.isoformat(),
        in_low_window,
        in_high_window,
        gate_open,
    )
    return gate_open
