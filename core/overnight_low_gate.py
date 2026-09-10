# core/overnight_low_gate.py
"""Overnight FORECAST-min gate for KXLOW 'daily temp stays >= X F' brackets.

This is the forecast analog of ``core/sunrise_gate.py::day_has_dipped_below``
(which blocks only after the OBSERVED 5-min feed has already breached the
line).  This gate instead asks: given the NWS hourly FORECAST for the LOCAL
hours from now until this market's trading window closes (~01:00 local for
every city, ~00:00 for Phoenix because Kalshi closes Phoenix at local
midnight and Phoenix has no DST), will the forecast minimum dip too close to
the bracket line?

Market-date / window boundary rules reused verbatim from the live code
(``nws/client.get_trading_day_window`` and ``nws/gate._trading_day_window_for_date``):
    - all cities except KPHX: trading day runs LOCAL 01:00 -> next 01:00
    - KPHX only:              trading day runs LOCAL 00:00 -> next 00:00

Design / safety:
    - Never touches the DB and makes no state changes.
    - FAILS OPEN: any missing/unparseable input, unknown series/station, or
      fetch/parse error yields ``(blocked=False, ctx)`` so the gate can never
      stall a legitimate entry.
    - It is NOT wired into any live path yet (see config / caller note).  It
      is exposed so it can be unit-tested and reviewed before enabling.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

# Reuse existing parse/lookup helpers to stay consistent with the codebase.
from core.trade_outcome_utils import detect_family, parse_bracket_temp
from core.local_time_gate import get_series_station_code, get_series_timezone

logger = logging.getLogger("forecastology.core.overnight_low_gate")

# Date segment format in a ticker, e.g. "26JUL16" -> date(2026, 7, 16).
_DATE_RE = re.compile(r"^(\d{2})([A-Z]{3})(\d{2})$")
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_ticker_date(date_segment: str) -> Optional[date]:
    """Parse a ``YYMMMDD`` segment (e.g. ``26JUL16``) into a ``date``."""
    m = _DATE_RE.match(date_segment.strip().upper())
    if m is None:
        return None
    two_year = int(m.group(1))
    month = _MONTHS.get(m.group(2))
    day = int(m.group(3))
    if month is None:
        return None
    year = 2000 + two_year
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _f_from_c(temp_c: float) -> float:
    return temp_c * 9.0 / 5.0 + 32.0


def overnight_low_gate(
    ticker: str,
    nws_client,
    *,
    bracket_temp_f: Optional[float] = None,
    margin_f: float = 1.0,
    now_utc: Optional[datetime] = None,
    periods: Optional[List[dict]] = None,
    station_tz_override: Optional[ZoneInfo] = None,
) -> Tuple[bool, dict]:
    """Return ``(blocked, ctx)`` for a KXLOW overnight-forecast entry gate.

    The rule for a LOW 'daily temp stays >= X' bracket: block the entry if the
    NWS FORECAST minimum for the remaining LOCAL hours of the market's trading
    day (from ``now`` until the next-day close boundary = 01:00 local; 00:00
    for Phoenix) is forecast to drop below ``bracket_temp_f + margin_f``.

    Args:
        ticker: Market ticker, e.g. ``"KXLOWTNYC-26SEP09-B64"``.
        nws_client: An :class:`~nws.client.NWSClient`` used only when *periods*
            is not supplied, to fetch a live NWS hourly forecast.
        bracket_temp_f: The bracket line (deg F) parsed from the ticker when
            not supplied.  You may override it for tests.
        margin_f: Extra cushion (deg F) required above the bracket line before
            an entry is considered safe (default 1.0).
        now_utc: Control the clock (defaults to ``datetime.now(UTC)``).
        periods: Optional prebuilt NWS hourly period list (tests / callers that
            already have forecast data).  When omitted a live fetch is made
            through ``nws_client``.
        station_tz_override: Only used to inject a timezone in tests/offline.

    Returns:
        ``(blocked, ctx)`` where ``blocked`` is True only when this gate wants
        the entry refused.  Fails open (blocked=False) on any error.
    """
    ctx: dict = {"blocked": False, "reason": "ok", "ticker": ticker}

    if detect_family(ticker) != "LOW":
        ctx["reason"] = "not_low"
        return False, ctx

    station_code = get_series_station_code(ticker)
    if station_code is None:
        ctx["reason"] = "unknown_station"
        return False, ctx
    ctx["station_code"] = station_code

    if bracket_temp_f is None:
        bracket_temp_f = parse_bracket_temp(ticker)
    if bracket_temp_f is None:
        ctx["reason"] = "no_bracket_line"
        return False, ctx
    ctx["bracket_temp_f"] = bracket_temp_f

    tz_name = get_series_timezone(ticker)
    if tz_name is None:
        ctx["reason"] = "unknown_timezone"
        return False, ctx
    ctx["tz_name"] = tz_name

    # Parse the ticker's own market date segment -> "which trading day".
    parts = ticker.split("-")
    if len(parts) < 3:
        ctx["reason"] = "no_date_segment"
        return False, ctx
    market_date = _parse_ticker_date(parts[1])
    if market_date is None:
        ctx["reason"] = "bad_market_date"
        return False, ctx
    ctx["market_date"] = market_date.isoformat()

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_utc = now_utc if now_utc.tzinfo else now_utc.replace(tzinfo=timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)

    try:
        station_tz = station_tz_override or ZoneInfo(tz_name)
    except Exception:  # noqa: BLE001
        ctx["reason"] = "bad_timezone"
        return False, ctx

    # Trading-day window (station-local) that this market_date spans.
    is_phoenix = station_code == "KPHX"
    local_start_clock = time(0, 0) if is_phoenix else time(1, 0)
    day_start_local = datetime.combine(market_date, local_start_clock, tzinfo=station_tz)
    day_end_local = day_start_local + timedelta(days=1)
    now_local_dt = now_utc.astimezone(station_tz)

    # Which hourly forecast periods fall inside (now, day_end_local)?
    raw_periods = periods
    if raw_periods is None:
        try:
            _lat, _lon, hourly_url, _tz = nws_client._get_station_metadata(station_code)
            raw_periods = nws_client._get_hourly_periods(hourly_url)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "overnight_low_gate.fetch_error station=%s err=%s",
                station_code,
                exc,
            )
            ctx["reason"] = "fetch_error"
            return False, ctx

    min_f: Optional[float] = None
    min_time_local: Optional[str] = None
    for p in raw_periods:
        start_str = p.get("startTime")
        temp = p.get("temperature")
        if start_str is None or temp is None:
            continue
        try:
            period_local = datetime.fromisoformat(start_str).astimezone(station_tz)
        except (TypeError, ValueError):
            continue
        # Only consider periods still in the future within this trading day.
        if period_local <= now_local_dt:
            continue
        if not (day_start_local <= period_local < day_end_local):
            continue
        try:
            val = float(temp)
        except (TypeError, ValueError):
            continue
        unit = str(p.get("temperatureUnit", "F")).upper()
        if unit == "C":
            val = _f_from_c(val)
        if min_f is None or val < min_f:
            min_f = val
            min_time_local = period_local.isoformat()

    ctx["min_forecast_f"] = None if min_f is None else round(min_f, 2)
    ctx["min_forecast_local"] = min_time_local
    ctx["required_min_f"] = round(bracket_temp_f + margin_f, 2)
    if min_f is None:
        ctx["reason"] = "no_future_periods"
        return False, ctx

    if min_f < (bracket_temp_f + margin_f):
        ctx["reason"] = "forecast_min_too_low"
        ctx["blocked"] = True
        return True, ctx
    ctx["reason"] = "safe"
    return False, ctx
