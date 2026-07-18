# core/local_time_gate.py
"""City-local-time entry gate for Kalshi temperature markets.

Kalshi settles its temperature markets overnight.  To avoid opening new
positions before the market "day" has rolled over in the city's own
timezone, this module enforces a configurable local-time threshold:

    - All listed cities except Phoenix: allow new entries only at/after
      DEFAULT_ENTRY_START_LOCAL (default 01:00 local).
    - Phoenix only: allow new entries only at/after
      PHOENIX_ENTRY_START_LOCAL (default 00:00 local).
      Phoenix observes Mountain Standard Time year-round (no DST).

The gate applies to *new entry orders only*.  Stop-loss, panic-exit,
sell paths, and position management are completely unaffected.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

if TYPE_CHECKING:
    from app.config import AppConfig

# Imported lazily to keep this module free of NWS/DB import-time side-effects.
# nws.stations is a pure data module so this import is safe.
from nws.stations import STATIONS

# ---------------------------------------------------------------------------
# Series-prefix → IANA timezone
# ---------------------------------------------------------------------------
# Each key is the series ticker prefix (uppercase) as it appears in
# core/constants.SERIES_LIST.  The market ticker format is:
#   {SERIES_PREFIX}-{YYMMMDD}-{BRACKET}
# so split("-")[0] gives the series prefix exactly.

SERIES_TIMEZONE: dict[str, str] = {
    # ── Eastern Time ────────────────────────────────────────────────────────
    "KXHIGHTATL":  "America/New_York",   # Atlanta
    "KXLOWTATL":   "America/New_York",
    "KXHIGHTBOS":  "America/New_York",   # Boston
    "KXLOWTBOS":   "America/New_York",
    "KXHIGHMIA":   "America/New_York",   # Miami
    "KXLOWTMIA":   "America/New_York",
    "KXHIGHNY":    "America/New_York",   # New York City
    "KXLOWTNYC":   "America/New_York",
    "KXHIGHPHIL":  "America/New_York",   # Philadelphia
    "KXLOWTPHIL":  "America/New_York",
    "KXHIGHTDC":   "America/New_York",   # Washington DC
    "KXLOWTDC":    "America/New_York",
    # ── Central Time ────────────────────────────────────────────────────────
    "KXHIGHAUS":   "America/Chicago",    # Austin
    "KXLOWTAUS":   "America/Chicago",
    "KXHIGHCHI":   "America/Chicago",    # Chicago
    "KXLOWTCHI":   "America/Chicago",
    "KXHIGHTDAL":  "America/Chicago",    # Dallas
    "KXLOWTDAL":   "America/Chicago",
    "KXHIGHTHOU":  "America/Chicago",    # Houston
    "KXLOWTHOU":   "America/Chicago",
    "KXHIGHTMIN":  "America/Chicago",    # Minneapolis
    "KXLOWTMIN":   "America/Chicago",
    "KXHIGHTNOLA": "America/Chicago",    # New Orleans
    "KXLOWTNOLA":  "America/Chicago",
    "KXHIGHTOKC":  "America/Chicago",    # Oklahoma City
    "KXLOWTOKC":   "America/Chicago",
    "KXHIGHTSATX": "America/Chicago",    # San Antonio
    "KXLOWTSATX":  "America/Chicago",
    # ── Mountain Time (DST) ─────────────────────────────────────────────────
    "KXHIGHDEN":   "America/Denver",     # Denver
    "KXLOWTDEN":   "America/Denver",
    # ── Mountain Standard Time (no DST) ─────────────────────────────────────
    "KXHIGHTPHX":  "America/Phoenix",    # Phoenix
    "KXLOWTPHX":   "America/Phoenix",
    # ── Pacific Time ────────────────────────────────────────────────────────
    "KXHIGHTLV":   "America/Los_Angeles",  # Las Vegas
    "KXLOWTLV":    "America/Los_Angeles",
    "KXHIGHLAX":   "America/Los_Angeles",  # Los Angeles
    "KXLOWTLAX":   "America/Los_Angeles",
    "KXHIGHTSFO":  "America/Los_Angeles",  # San Francisco
    "KXLOWTSFO":   "America/Los_Angeles",
    "KXHIGHTSEA":  "America/Los_Angeles",  # Seattle
    "KXLOWTSEA":   "America/Los_Angeles",
}

# Human-readable city name keyed by series prefix (used in log messages).
SERIES_CITY: dict[str, str] = {
    "KXHIGHTATL":  "Atlanta",       "KXLOWTATL":   "Atlanta",
    "KXHIGHTBOS":  "Boston",        "KXLOWTBOS":   "Boston",
    "KXHIGHMIA":   "Miami",         "KXLOWTMIA":   "Miami",
    "KXHIGHNY":    "New York City", "KXLOWTNYC":   "New York City",
    "KXHIGHPHIL":  "Philadelphia",  "KXLOWTPHIL":  "Philadelphia",
    "KXHIGHTDC":   "Washington DC", "KXLOWTDC":    "Washington DC",
    "KXHIGHAUS":   "Austin",        "KXLOWTAUS":   "Austin",
    "KXHIGHCHI":   "Chicago",       "KXLOWTCHI":   "Chicago",
    "KXHIGHTDAL":  "Dallas",        "KXLOWTDAL":   "Dallas",
    "KXHIGHTHOU":  "Houston",       "KXLOWTHOU":   "Houston",
    "KXHIGHTMIN":  "Minneapolis",   "KXLOWTMIN":   "Minneapolis",
    "KXHIGHTNOLA": "New Orleans",   "KXLOWTNOLA":  "New Orleans",
    "KXHIGHTOKC":  "Oklahoma City", "KXLOWTOKC":   "Oklahoma City",
    "KXHIGHTSATX": "San Antonio",   "KXLOWTSATX":  "San Antonio",
    "KXHIGHDEN":   "Denver",        "KXLOWTDEN":   "Denver",
    "KXHIGHTPHX":  "Phoenix",       "KXLOWTPHX":   "Phoenix",
    "KXHIGHTLV":   "Las Vegas",     "KXLOWTLV":    "Las Vegas",
    "KXHIGHLAX":   "Los Angeles",   "KXLOWTLAX":   "Los Angeles",
    "KXHIGHTSFO":  "San Francisco", "KXLOWTSFO":   "San Francisco",
    "KXHIGHTSEA":  "Seattle",       "KXLOWTSEA":   "Seattle",
}

_PHOENIX_TZ = "America/Phoenix"


def get_series_prefix(ticker: str) -> str:
    """Return the series prefix from a market ticker (everything before the first '-')."""
    return ticker.split("-")[0].upper()


def get_series_timezone(ticker: str) -> Optional[str]:
    """Return the IANA timezone name for a market ticker, or None if unknown."""
    return SERIES_TIMEZONE.get(get_series_prefix(ticker))


def get_series_station_code(ticker: str) -> Optional[str]:
    """Return the NWS ICAO station code for a market ticker, or None if unknown.

    Composes two existing mappings:
        series_prefix → SERIES_CITY[prefix] → STATIONS[city] → ICAO code

    Examples::

        get_series_station_code("KXHIGHTATL-26JUL16-B95")  # → "KATL"
        get_series_station_code("KXLOWTSATX-26JUL16-B55.5")  # → "KSAT"
        get_series_station_code("KXHIGHTSFO-26JUL16-B70")   # → "KSFO"
        get_series_station_code("KXLOWTPHX-26JUL16-B90")   # → "KPHX"

    Returns None for any ticker whose series prefix is not in SERIES_CITY.
    """
    prefix = get_series_prefix(ticker)
    city = SERIES_CITY.get(prefix)
    if city is None:
        return None
    return STATIONS.get(city)


def _parse_hhmm(value: str) -> datetime.time:
    """Parse 'HH:MM' into a :class:`datetime.time` object."""
    h, m = value.strip().split(":")
    return datetime.time(int(h), int(m))


def _current_local_trading_date(
    now_local: datetime.datetime, is_phoenix: bool
) -> datetime.date:
    """Return the station's current local trading date for *now_local*.

    For Phoenix (midnight threshold): the trading date is the calendar date.
    For all other cities (01:00 threshold): the trading date is the calendar
    date, but rolled back by one day if the local time is before 01:00
    (i.e. the very early hours still belong to the *previous* market day).
    """
    if is_phoenix:
        return now_local.date()
    if now_local.time() < datetime.time(1, 0):
        return now_local.date() - datetime.timedelta(days=1)
    return now_local.date()


def is_entry_allowed(
    ticker: str,
    config: "AppConfig",
    now_utc: Optional[datetime.datetime] = None,
    market_date: Optional[datetime.date] = None,
) -> tuple[bool, dict]:
    """Determine whether a new entry order is allowed right now for *ticker*.

    Parameters
    ----------
    ticker:
        Market ticker, e.g. ``"KXLOWTBOS-26JUN25-B52.5"``.
    config:
        Live :class:`~app.config.AppConfig` instance.
    now_utc:
        Current UTC time.  Defaults to ``datetime.datetime.now(UTC)`` if
        omitted; pass an explicit value in tests to control the clock.
    market_date:
        The parsed calendar date embedded in the ticker (e.g. July 18 for a
        ``26JUL18`` ticker).  When provided and the ticker's timezone is
        known, entry is blocked unless *market_date* equals the station's
        current local trading date.  Existing call-sites that omit this
        parameter retain the previous (time-only) behaviour.

    Returns
    -------
    (allowed, log_context)
        *allowed* is ``True`` when entry should proceed, ``False`` when it
        must be blocked.  *log_context* is a :class:`dict` with structured
        fields for logging (empty when gate is disabled or ticker unknown).
    """
    if not config.enable_local_settle_gate:
        return True, {}

    tz_name = get_series_timezone(ticker)
    if tz_name is None:
        # Unknown series — fail open so unknown tickers are never silently blocked.
        return True, {}

    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    tz = ZoneInfo(tz_name)
    now_local = now_utc.astimezone(tz)
    local_time = now_local.time()

    is_phoenix = tz_name == _PHOENIX_TZ
    threshold_str = (
        config.phoenix_entry_start_local if is_phoenix
        else config.default_entry_start_local
    )
    threshold = _parse_hhmm(threshold_str)
    allowed = local_time >= threshold

    series = get_series_prefix(ticker)
    ctx: dict = {
        "ticker": ticker,
        "city": SERIES_CITY.get(series, series),
        "timezone": tz_name,
        "local_time": now_local.strftime("%H:%M:%S"),
        "threshold": threshold_str,
    }

    if allowed and market_date is not None:
        current_trading_date = _current_local_trading_date(now_local, is_phoenix)
        if market_date != current_trading_date:
            ctx["reason"] = "market_date_not_current_trading_day"
            ctx["market_date"] = market_date.isoformat()
            ctx["current_trading_date"] = current_trading_date.isoformat()
            ctx["station"] = get_series_station_code(ticker)
            ctx["now_local"] = now_local.isoformat()
            return False, ctx

    return allowed, ctx
