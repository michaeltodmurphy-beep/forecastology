from __future__ import annotations

import datetime
import time
from dataclasses import dataclass
from typing import Optional

import requests
import structlog
from astral import Observer
from astral.sun import sun

from app.config import AppConfig
from core.local_time_gate import get_series_prefix, get_series_timezone
from core.station_coords import SERIES_STATION_COORDS
from nws.client import NWSClient

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SunriseGateDecision:
    allowed: bool
    use_nws_window_fallback: bool = False


class SunriseEntryGate:
    def __init__(self, config: AppConfig, nws_client: Optional[NWSClient] = None) -> None:
        self.config = config
        self.nws_client = nws_client or NWSClient()
        self._sunrise_cache: dict[tuple[str, datetime.date], tuple[datetime.datetime, str]] = {}
        self._temp_cache: dict[tuple[str, str], tuple[float, bool, str, dict]] = {}
        self._temp_cache_ttl_seconds = 180
        self._logged_gate_open: set[tuple[str, datetime.date]] = set()
        self._missing_coords_warned: set[str] = set()

    def evaluate(
        self,
        ticker: str,
        now_utc: Optional[datetime.datetime] = None,
    ) -> SunriseGateDecision:
        series = get_series_prefix(ticker)
        if not series.startswith("KXLOW"):
            return SunriseGateDecision(allowed=True)

        coords = SERIES_STATION_COORDS.get(series)
        if coords is None:
            if series not in self._missing_coords_warned:
                self._missing_coords_warned.add(series)
                logger.warning("sunrise.no_coords_fallback", series=series, ticker=ticker)
            return SunriseGateDecision(allowed=True, use_nws_window_fallback=True)

        tz_name = get_series_timezone(ticker)
        if tz_name is None:
            return SunriseGateDecision(allowed=True, use_nws_window_fallback=True)
        tz = ZoneInfo(tz_name)

        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_local = now_utc.astimezone(tz)
        local_date = now_local.date()

        station_id, lat, lon = coords
        sunrise_local, _source = self._get_sunrise_local(series, tz, local_date, lat, lon)
        gate_open = sunrise_local + datetime.timedelta(minutes=int(self.config.sunrise_strategy_time))
        gate_close = gate_open + datetime.timedelta(
            minutes=int(self.config.sunrise_entry_window_minutes)
        )

        if now_local < gate_open:
            logger.info(
                "sunrise.gate_blocked",
                ticker=ticker,
                sunrise_local=sunrise_local.isoformat(),
                gate_open_local=gate_open.isoformat(),
                now_local=now_local.isoformat(),
            )
            return SunriseGateDecision(allowed=False)
        if now_local > gate_close:
            logger.info(
                "sunrise.gate_window_closed",
                ticker=ticker,
                sunrise_local=sunrise_local.isoformat(),
                gate_open_local=gate_open.isoformat(),
                gate_close_local=gate_close.isoformat(),
                now_local=now_local.isoformat(),
            )
            return SunriseGateDecision(allowed=False)

        if self.config.sunrise_require_temp_rising:
            passed, event_name, event_ctx = self._check_temp_rising(station_id, now_utc)
            if not passed:
                logger.info(event_name, ticker=ticker, station=station_id, **event_ctx)
                return SunriseGateDecision(allowed=False)

        open_key = (ticker, local_date)
        if open_key not in self._logged_gate_open:
            self._logged_gate_open.add(open_key)
            logger.info(
                "sunrise.gate_open",
                ticker=ticker,
                station=station_id,
                sunrise_local=sunrise_local.isoformat(),
                gate_open_local=gate_open.isoformat(),
                gate_close_local=gate_close.isoformat(),
                now_local=now_local.isoformat(),
            )
        return SunriseGateDecision(allowed=True)

    def _get_sunrise_local(
        self,
        series: str,
        tz: ZoneInfo,
        local_date: datetime.date,
        lat: float,
        lon: float,
    ) -> tuple[datetime.datetime, str]:
        cache_key = (series, local_date)
        cached = self._sunrise_cache.get(cache_key)
        if cached is not None:
            return cached

        source = "astral"
        sunrise_local: datetime.datetime
        if self.config.sunrise_source == "api":
            try:
                sunrise_utc = self._fetch_api_sunrise_utc(lat, lon, local_date)
                sunrise_local = sunrise_utc.astimezone(tz)
                source = "api"
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "sunrise.api_fallback",
                    series=series,
                    date=local_date.isoformat(),
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                )
                sunrise_local = self._compute_astral_sunrise_local(lat, lon, tz, local_date)
        else:
            sunrise_local = self._compute_astral_sunrise_local(lat, lon, tz, local_date)

        logger.info(
            "sunrise.computed",
            series=series,
            date=local_date.isoformat(),
            sunrise_local=sunrise_local.isoformat(),
            source=source,
        )
        self._sunrise_cache[cache_key] = (sunrise_local, source)
        return sunrise_local, source

    def _compute_astral_sunrise_local(
        self,
        lat: float,
        lon: float,
        tz: ZoneInfo,
        local_date: datetime.date,
    ) -> datetime.datetime:
        observer = Observer(latitude=lat, longitude=lon)
        result = sun(observer=observer, date=local_date, tzinfo=tz)
        sunrise_local = result["sunrise"]
        if sunrise_local.tzinfo is None:
            sunrise_local = sunrise_local.replace(tzinfo=tz)
        return sunrise_local

    def _fetch_api_sunrise_utc(
        self,
        lat: float,
        lon: float,
        local_date: datetime.date,
    ) -> datetime.datetime:
        resp = requests.get(
            "https://api.sunrise-sunset.org/json",
            params={
                "lat": f"{lat:.6f}",
                "lng": f"{lon:.6f}",
                "date": local_date.isoformat(),
                "formatted": 0,
            },
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("status") != "OK":
            raise RuntimeError(f"sunrise-sunset status={payload.get('status')}")
        sunrise_raw = ((payload.get("results") or {}).get("sunrise") or "").strip()
        if not sunrise_raw:
            raise RuntimeError("sunrise-sunset missing sunrise")
        sunrise_utc = datetime.datetime.fromisoformat(sunrise_raw)
        if sunrise_utc.tzinfo is None:
            sunrise_utc = sunrise_utc.replace(tzinfo=datetime.timezone.utc)
        return sunrise_utc.astimezone(datetime.timezone.utc)

    def _check_temp_rising(
        self,
        station_id: str,
        now_utc: datetime.datetime,
    ) -> tuple[bool, str, dict]:
        cache_key = (station_id, "temp_rising")
        cache_now = time.monotonic()
        cached = self._temp_cache.get(cache_key)
        if cached is not None and cache_now - cached[0] < self._temp_cache_ttl_seconds:
            return cached[1], cached[2], dict(cached[3])

        try:
            payload = self.nws_client._get_json(  # noqa: SLF001
                f"https://api.weather.gov/stations/{station_id}/observations?limit=2"
            )
        except Exception as exc:  # noqa: BLE001
            ctx = {
                "reason": "fetch_error",
                "error_class": type(exc).__name__,
                "error_message": str(exc),
            }
            self._temp_cache[cache_key] = (cache_now, False, "sunrise.obs_unavailable", ctx)
            return False, "sunrise.obs_unavailable", ctx

        features = payload.get("features") or []
        parsed_obs: list[tuple[datetime.datetime, float]] = []
        for item in features:
            props = item.get("properties") or {}
            temp_val = ((props.get("temperature") or {}).get("value"))
            timestamp = props.get("timestamp")
            if temp_val is None or not timestamp:
                continue
            try:
                obs_ts = datetime.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            except ValueError:
                continue
            if obs_ts.tzinfo is None:
                obs_ts = obs_ts.replace(tzinfo=datetime.timezone.utc)
            parsed_obs.append((obs_ts.astimezone(datetime.timezone.utc), float(temp_val)))

        parsed_obs.sort(key=lambda x: x[0], reverse=True)
        if len(parsed_obs) < 2:
            ctx = {"reason": "insufficient_observations", "observation_count": len(parsed_obs)}
            self._temp_cache[cache_key] = (cache_now, False, "sunrise.obs_unavailable", ctx)
            return False, "sunrise.obs_unavailable", ctx

        latest_ts, latest_val = parsed_obs[0]
        prev_ts, prev_val = parsed_obs[1]
        age_minutes = (now_utc - latest_ts).total_seconds() / 60.0
        check_ctx = {
            "latest_temp_c": round(latest_val, 2),
            "latest_obs_utc": latest_ts.isoformat(),
            "prev_temp_c": round(prev_val, 2),
            "prev_obs_utc": prev_ts.isoformat(),
        }
        logger.info("sunrise.temp_rising_check", station=station_id, **check_ctx)
        if age_minutes > 90:
            ctx = {
                **check_ctx,
                "reason": "stale_observation",
                "latest_age_minutes": round(age_minutes, 2),
            }
            self._temp_cache[cache_key] = (cache_now, False, "sunrise.obs_unavailable", ctx)
            return False, "sunrise.obs_unavailable", ctx

        if latest_val < prev_val:
            self._temp_cache[cache_key] = (
                cache_now,
                False,
                "sunrise.temp_rising_blocked",
                check_ctx,
            )
            return False, "sunrise.temp_rising_blocked", check_ctx

        self._temp_cache[cache_key] = (cache_now, True, "sunrise.gate_open", check_ctx)
        return True, "sunrise.gate_open", check_ctx

