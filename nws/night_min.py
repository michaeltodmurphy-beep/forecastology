# nws/night_min.py
"""Overnight (21:00-01:00 local) forecast-min helper for the KXLOW gate.

This is a small, standalone helper that wraps an existing
:class:`nws.client.NWSClient` by composition (no changes to the client file)
and reports the minimum forecast temperature (F) across the overnight window
of a station's current LOCAL calendar day.

The window runs from 21:00 on the station's current local date through 01:00
(exclusive) the following local day - only hourly periods whose station-local
clock hour is in {21, 22, 23, 00} are kept.  This is the overnight stretch
that typically produces the daily low.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("forecastology.nws.night_min")


def fetch_night_window_min(
    nws_client,
    station_code: str,
    *,
    window_start_hour: int = 21,
    window_end_hour: int = 1,
    now_utc: Optional[datetime] = None,
) -> Tuple[Optional[float], Optional[date], List[dict], str]:
    """Return the minimum forecast temp (F) in the overnight local window.

    Returns ``(min_f, base_local_date, window_periods, tz_name)`` where
    *min_f* is None when nothing matched or on fetch error (fail-open: callers
    must treat None as "do not block").
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_utc = now_utc if now_utc.tzinfo else now_utc.replace(tzinfo=timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)

    try:
        _lat, _lon, hourly_url, tz_name = nws_client._get_station_metadata(
            station_code
        )
    except Exception:
        logger.warning("nws.night_min.metadata_error station=%s", station_code)
        return None, None, [], ""

    try:
        station_tz = ZoneInfo(tz_name)
    except Exception:
        logger.warning(
            "nws.night_min.tz_error station=%s tz=%s", station_code, tz_name
        )
        return None, None, [], tz_name

    try:
        periods = nws_client._get_hourly_periods(hourly_url)
    except Exception:
        logger.warning("nws.night_min.fetch_error station=%s", station_code)
        return None, None, [], tz_name

    now_local = now_utc.astimezone(station_tz)
    base_local_date = now_local.date()
    day_start = datetime.combine(
        base_local_date, time(window_start_hour, 0), tzinfo=station_tz
    )
    day_end = datetime.combine(
        base_local_date + timedelta(days=1),
        time(window_end_hour, 0),
        tzinfo=station_tz,
    )

    window_periods_list: List[dict] = []
    min_f: Optional[float] = None
    for p in periods:
        start_str = p.get("startTime")
        temp = p.get("temperature")
        if start_str is None or temp is None:
            continue
        try:
            t_local = nws_client._parse_iso_dt(start_str).astimezone(station_tz)
        except (TypeError, ValueError):
            continue
        if not (day_start <= t_local < day_end):
            continue
        try:
            val = float(temp)
        except (TypeError, ValueError):
            continue
        if str(p.get("temperatureUnit", "F")).upper() == "C":
            val = val * 9.0 / 5.0 + 32.0
        window_periods_list.append(p)
        if min_f is None or val < min_f:
            min_f = val
    return min_f, base_local_date, window_periods_list, tz_name