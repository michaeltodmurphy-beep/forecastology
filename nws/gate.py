# nws/gate.py
"""Trading gate logic based on NWS forecasted high/low temperature times.

The gate is open when the current UTC time falls within one of two windows:

    - **Low window**: [low_time − GATE_LOW_BEFORE min, low_time + GATE_LOW_AFTER min]
    - **High window**: [high_time − GATE_HIGH_BEFORE min, high_time + GATE_HIGH_AFTER min]

Window durations are controlled by environment variables (see ``nws/config.py``).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.exc import SQLAlchemyError

from app.models import StationForecast
from nws.config import (
    GATE_HIGH_AFTER,
    GATE_HIGH_BEFORE,
    GATE_LOW_AFTER,
    GATE_LOW_BEFORE,
)
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


def has_forecast(station_code: str) -> bool:
    """Return ``True`` if at least one forecast row exists for *station_code*.

    Used by the entry gate to distinguish "gate closed because outside window"
    from "gate closed because no data" — allowing fail-open behavior in the
    latter case without changing :func:`is_trading_gate_open`'s semantics.

    Returns ``False`` on any DB error (treating it the same as no data).
    """
    try:
        with get_session() as session:
            return _latest_forecast(session, station_code) is not None
    except SQLAlchemyError:
        logger.exception(
            "gate.db_error station=%s — has_forecast returning False", station_code
        )
        return False


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

    in_low_window = False
    in_high_window = False

    if forecast.low_time_utc is not None:
        low_utc = _ensure_utc(forecast.low_time_utc)
        low_open = low_utc - timedelta(minutes=GATE_LOW_BEFORE)
        low_close = low_utc + timedelta(minutes=GATE_LOW_AFTER)
        in_low_window = low_open <= now <= low_close

    if forecast.high_time_utc is not None:
        high_utc = _ensure_utc(forecast.high_time_utc)
        high_open = high_utc - timedelta(minutes=GATE_HIGH_BEFORE)
        high_close = high_utc + timedelta(minutes=GATE_HIGH_AFTER)
        in_high_window = high_open <= now <= high_close

    gate_open = in_low_window or in_high_window
    logger.debug(
        "gate station=%s in_low=%s in_high=%s open=%s",
        station_code,
        in_low_window,
        in_high_window,
        gate_open,
    )
    return gate_open
