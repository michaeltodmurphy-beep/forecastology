# nws/client.py
"""NWS (National Weather Service) API client.

Flow per station:
  1. GET /stations/{station_code}       → lat/lon coordinates
  2. GET /points/{lat},{lon}            → forecastHourly URL + station timezone
  3. GET {forecastHourly_url}           → hourly temperature periods
  4. Derive daily high/low times from the hourly periods, filtered by the
     station's LOCAL calendar day (not UTC day)

Grid-point metadata (lat/lon, forecastHourly URL, and IANA timezone name) is
cached in memory per station to avoid redundant round-trips on every scheduled
update.

All datetimes returned are timezone-aware UTC ``datetime`` objects.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nws.config import NWS_USER_AGENT

logger = logging.getLogger("forecastology.nws.client")

NWS_BASE = "https://api.weather.gov"

# In-memory cache: station_code → (lat, lon, forecastHourly_url, tz_name)
_station_cache: Dict[str, Tuple[float, float, str, str]] = {}


class NWSClient:
    """Thread-safe NWS API client with retry handling.

    A single instance should be created at scheduler startup and reused for
    all subsequent update cycles.
    """

    def __init__(self, user_agent: str = NWS_USER_AGENT, timeout: int = 15) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/geo+json",
            }
        )

        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _get_json(self, url: str) -> dict:
        """Perform a GET request and return parsed JSON.

        Raises ``RuntimeError`` for non-2xx responses after all retries.
        """
        resp = self.session.get(url, timeout=self.timeout)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"NWS request failed {resp.status_code} for {url}: "
                f"{resp.text[:300]}"
            )
        return resp.json()

    # ------------------------------------------------------------------
    # Station metadata (cached per process lifetime)
    # ------------------------------------------------------------------

    def _get_station_metadata(
        self, station_code: str
    ) -> Tuple[float, float, str, str]:
        """Return (lat, lon, forecastHourly_url, tz_name) for a station, cached.

        The NWS grid assignment and timezone for an airport code changes
        extremely rarely, so caching for the process lifetime is safe and
        reduces API load.
        """
        if station_code in _station_cache:
            return _station_cache[station_code]

        # Step 1: station → coordinates
        data = self._get_json(f"{NWS_BASE}/stations/{station_code}")
        coords = data.get("geometry", {}).get("coordinates")
        if not coords or len(coords) < 2:
            raise ValueError(
                f"Missing coordinates for station {station_code}"
            )
        lon, lat = coords[0], coords[1]

        # Step 2: coordinates → forecastHourly URL + station timezone
        points_data = self._get_json(f"{NWS_BASE}/points/{lat},{lon}")
        props = points_data.get("properties", {})
        hourly_url: Optional[str] = props.get("forecastHourly")
        if not hourly_url:
            raise ValueError(
                f"No forecastHourly URL from /points/{lat},{lon}"
            )
        tz_name: str = props.get("timeZone") or "UTC"

        _station_cache[station_code] = (lat, lon, hourly_url, tz_name)
        logger.debug(
            "Cached grid metadata for %s: lat=%.4f lon=%.4f tz=%s url=%s",
            station_code,
            lat,
            lon,
            tz_name,
            hourly_url,
        )
        return lat, lon, hourly_url, tz_name

    # ------------------------------------------------------------------
    # Hourly forecast parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_iso_dt(dt_str: str) -> datetime:
        """Parse an ISO-8601 string (possibly with offset) into UTC datetime."""
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _get_hourly_periods(self, hourly_url: str) -> List[dict]:
        """Fetch and return the hourly forecast periods list."""
        data = self._get_json(hourly_url)
        periods = data.get("properties", {}).get("periods", [])
        if not periods:
            raise ValueError(
                f"No hourly periods returned from {hourly_url}"
            )
        return periods

    def derive_daily_high_low_times(
        self, periods: List[dict], target_date_utc: datetime
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Find the UTC times of the daily high and low temperatures.

        Filters hourly periods to those within the UTC calendar day of
        *target_date_utc*, then returns the (high_time_utc, low_time_utc)
        pair.  On temperature tie, the earliest occurrence is chosen.

        Args:
            periods: List of NWS hourly forecast period dicts.
            target_date_utc: Any datetime within the target UTC day.

        Returns:
            Tuple of (high_time_utc, low_time_utc); either may be None if
            no hourly data exists for that UTC day.
        """
        day_start = datetime(
            target_date_utc.year,
            target_date_utc.month,
            target_date_utc.day,
            tzinfo=timezone.utc,
        )
        day_end = day_start + timedelta(days=1)

        day_periods: List[Tuple[datetime, float]] = []
        for p in periods:
            start_str = p.get("startTime")
            temp = p.get("temperature")
            if start_str is None or temp is None:
                continue
            t_utc = self._parse_iso_dt(start_str)
            if day_start <= t_utc < day_end:
                day_periods.append((t_utc, float(temp)))

        if not day_periods:
            return None, None

        # Earliest occurrence on tie (sort key uses timestamp for natural order)
        high_time = max(day_periods, key=lambda x: (x[1], -x[0].timestamp()))[0]
        low_time = min(day_periods, key=lambda x: (x[1], x[0].timestamp()))[0]
        return high_time, low_time

    def derive_daily_high_low_times_local(
        self,
        periods: List[dict],
        station_tz: ZoneInfo,
        now_utc: datetime,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Find the UTC times of the daily high and low using the station's LOCAL date.

        Converts each period's start time to the station's local timezone and
        filters by the local calendar day that corresponds to *now_utc*.  This
        prevents UTC day boundaries from splitting a station's effective trading
        day (e.g. a US/Pacific station at 01:00 UTC is still on the previous
        local calendar day).

        On temperature tie, the earliest occurrence is chosen.

        Args:
            periods: List of NWS hourly forecast period dicts.
            station_tz: IANA timezone for the station (from /points response).
            now_utc: The UTC instant that defines "today" in station-local time.

        Returns:
            Tuple of (high_time_utc, low_time_utc); either may be None if
            no hourly data exists for the station's local day.
        """
        target_local_date = now_utc.astimezone(station_tz).date()

        day_periods: List[Tuple[datetime, float]] = []
        for p in periods:
            start_str = p.get("startTime")
            temp = p.get("temperature")
            if start_str is None or temp is None:
                continue
            t_utc = self._parse_iso_dt(start_str)
            if t_utc.astimezone(station_tz).date() == target_local_date:
                day_periods.append((t_utc, float(temp)))

        if not day_periods:
            return None, None

        # Earliest occurrence on tie (sort key uses timestamp for natural order)
        high_time = max(day_periods, key=lambda x: (x[1], -x[0].timestamp()))[0]
        low_time = min(day_periods, key=lambda x: (x[1], x[0].timestamp()))[0]
        return high_time, low_time

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_high_low_for_date(
        self, station_code: str, target_date_utc: datetime
    ) -> Tuple[Optional[datetime], Optional[datetime], datetime]:
        """Fetch the daily high/low temperature times for a station.

        Periods are filtered by the station's LOCAL calendar day that
        corresponds to *target_date_utc*, so that UTC day boundaries never
        split the station's effective trading day.

        Args:
            station_code: NWS ICAO code (e.g. ``"KATL"``).
            target_date_utc: UTC instant that defines which local calendar
                day to use (typically ``datetime.now(timezone.utc)``).

        Returns:
            ``(high_time_utc, low_time_utc, local_forecast_date_utc)`` where
            the first two are timezone-aware UTC datetimes of the local-day
            high/low (or ``None`` when no data exists), and the third is UTC
            midnight of the station's local today (used as the DB row key).
        """
        _lat, _lon, hourly_url, tz_name = self._get_station_metadata(station_code)
        station_tz = ZoneInfo(tz_name)
        local_date = target_date_utc.astimezone(station_tz).date()
        local_forecast_date_utc = datetime(
            local_date.year, local_date.month, local_date.day, tzinfo=timezone.utc
        )
        logger.debug(
            "nws.local_tz station=%s tz=%s local_date=%s",
            station_code,
            tz_name,
            local_date,
        )
        periods = self._get_hourly_periods(hourly_url)
        high_time, low_time = self.derive_daily_high_low_times_local(
            periods, station_tz, target_date_utc
        )
        return high_time, low_time, local_forecast_date_utc
