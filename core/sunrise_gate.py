from __future__ import annotations

import datetime
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
import structlog
from astral import Observer
from astral.sun import sun

from app.config import AppConfig
from core.local_time_gate import get_series_prefix, get_series_timezone
from core.log_dedupe import DedupeLogger
from core.station_coords import SERIES_STATION_COORDS
from nws.awc_obs import ObsList, fetch_obs_with_fallback
from nws.client import NWSClient

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)

_CELSIUS_TO_F_FACTOR = 9.0 / 5.0
_AM_LOW_CACHE_TTL_SECONDS = 1800  # 30 minutes


def _c_to_f(celsius: float) -> float:
    return celsius * _CELSIUS_TO_F_FACTOR + 32.0


@dataclass(frozen=True)
class SunriseGateDecision:
    allowed: bool
    use_nws_window_fallback: bool = False


@dataclass
class _TempRiseState:
    """Per-series mutable state for the temperature-rise latch."""

    state_date: Optional[datetime.date] = None
    running_min_f: float = float("inf")
    latched: bool = False
    latch_baseline_f: Optional[float] = None
    latch_current_f: Optional[float] = None
    latch_time_utc: Optional[datetime.datetime] = None
    last_obs_time_utc: Optional[datetime.datetime] = None
    obs_cadence_logged_date: Optional[datetime.date] = None


class SunriseEntryGate:
    def __init__(self, config: AppConfig, nws_client: Optional[NWSClient] = None) -> None:
        self.config = config
        self.nws_client = nws_client or NWSClient()
        self._sunrise_cache: dict[tuple[str, datetime.date], tuple[datetime.datetime, str]] = {}
        # AM-low forecast cache: series -> (fetch_monotonic, passed: bool, min_temp_f, min_time_local_iso)
        self._am_low_cache: dict[tuple[str, datetime.date], tuple[float, bool, Optional[float], Optional[str]]] = {}
        # Temp-rise latch state: series -> _TempRiseState
        self._rise_state: dict[str, _TempRiseState] = {}
        self._logged_gate_open: set[tuple[str, datetime.date]] = set()
        self._missing_coords_warned: set[str] = set()
        # Legacy simple-check cache (only used when sunrise_temp_rise_required == 0 and
        # sunrise_require_temp_rising is explicitly True for backward compat).
        self._temp_cache: dict[tuple[str, str], tuple[float, bool, str, dict]] = {}
        self._temp_cache_ttl_seconds = 180
        self._log_dedupe = DedupeLogger(summary_interval_seconds=300)

    def _deduped_info(
        self,
        event: str,
        key: str,
        local_date: datetime.date,
        **fields,
    ) -> None:
        self._log_dedupe.log(logger, "info", event, key, day=local_date, **fields)

    def _station_obs_max_age_minutes(self, station_id: str) -> int:
        overrides = self.config.sunrise_obs_max_age_overrides or {}
        station_upper = station_id.upper()
        if station_upper in overrides:
            return int(overrides[station_upper])
        return int(self.config.sunrise_obs_max_age_minutes)

    def _fetch_station_obs(
        self,
        station_id: str,
        nws_url: str,
    ) -> tuple[ObsList, str]:
        """Fetch station observations using the configured source with fallback.

        Delegates to :func:`nws.awc_obs.fetch_obs_with_fallback`.
        Returns ``(obs_list, source)`` where *obs_list* is sorted newest first
        and *source* is ``"awc"`` or ``"nws"``.
        Raises on irrecoverable fetch failure.
        """
        return fetch_obs_with_fallback(
            station_id,
            nws_client=self.nws_client,
            nws_url=nws_url,
            obs_source=getattr(self.config, "sunrise_obs_source", "awc"),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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

        # 1. Time-window gate
        if now_local < gate_open:
            self._deduped_info(
                "sunrise.gate_blocked",
                ticker,
                local_date,
                sunrise_local=sunrise_local.isoformat(),
                gate_open_local=gate_open.isoformat(),
                now_local=now_local.isoformat(),
                ticker=ticker,
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

        # 2. AM-low forecast check (SUNRISE_REQUIRE_AM_LOW)
        if self.config.sunrise_require_am_low:
            am_passed = self._check_am_low_forecast(
                series, station_id, now_utc, now_local, local_date, tz
            )
            if not am_passed:
                return SunriseGateDecision(allowed=False)

        # 3. Temperature rise-from-baseline latch
        #    (replaces old binary sunrise_require_temp_rising check)
        #    sunrise_temp_rise_required == 0.0 disables it entirely.
        if self.config.sunrise_temp_rise_required > 0.0:
            baseline_start = sunrise_local - datetime.timedelta(
                minutes=int(self.config.sunrise_temp_baseline_minutes)
            )
            rise_ok = self._check_temp_rise_latch(
                series, station_id, now_utc, baseline_start, gate_close, tz, local_date
            )
            if not rise_ok:
                return SunriseGateDecision(allowed=False)
        elif self.config.sunrise_require_temp_rising:
            # Legacy path: SUNRISE_TEMP_RISE_REQUIRED=0 but old flag still set
            passed, event_name, event_ctx = self._check_temp_rising(station_id, now_utc)
            if not passed:
                if event_name == "sunrise.obs_unavailable":
                    self._deduped_info(
                        event_name,
                        series,
                        local_date,
                        ticker=ticker,
                        station=station_id,
                        series=series,
                        **event_ctx,
                    )
                else:
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

    def evaluate_am_low_only(
        self,
        ticker: str,
        now_utc: Optional[datetime.datetime] = None,
    ) -> SunriseGateDecision:
        """Evaluate only the AM-low deadline for **warm** tickers.

        Warm-trade series should be able to enter *before sunrise* (so morning
        moves are not missed) while still respecting the AM-low deadline that
        says "the day's forecast low must occur before NWS_LOW_DEADLINE_HOUR
        local".  This method applies only that check and skips the sunrise
        time-window and temperature-rise latch.

        Callers gate on :attr:`warm_trade_tickers` before calling this; the
        local settle gate (``is_entry_allowed``) is still applied separately by
        the state machine, as required.

        Non-KXLOW tickers are always allowed (mirrors :meth:`evaluate`).
        """
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

        station_id, _lat, _lon = coords

        # Only the AM-low deadline check applies to warm tickers. If the
        # AM-low requirement is disabled, warm tickers are un-gated.
        if not self.config.sunrise_require_am_low:
            return SunriseGateDecision(allowed=True)

        am_passed = self._check_am_low_forecast(
            series, station_id, now_utc, now_local, local_date, tz
        )
        return SunriseGateDecision(allowed=am_passed)

    # ------------------------------------------------------------------
    # AM-low forecast check
    # ------------------------------------------------------------------

    def _check_am_low_forecast(
        self,
        series: str,
        station_id: str,
        now_utc: datetime.datetime,
        now_local: datetime.datetime,
        local_date: datetime.date,
        tz: ZoneInfo,
    ) -> bool:
        """Return True iff the NWS hourly forecast day-low is before the deadline hour."""
        cache_key = (series, local_date)
        now_mono = time.monotonic()
        cached = self._am_low_cache.get(cache_key)
        if cached is not None and now_mono - cached[0] < _AM_LOW_CACHE_TTL_SECONDS:
            passed, min_temp_f, min_time_iso = cached[1], cached[2], cached[3]
            logger.debug(
                "sunrise.am_low_check",
                series=series,
                forecast_min_temp_f=min_temp_f,
                min_time_local=min_time_iso,
                deadline_hour=self.config.nws_low_deadline_hour,
                passed=passed,
                cached=True,
            )
            return passed

        try:
            _lat, _lon, hourly_url, _tz_name = self.nws_client._get_station_metadata(station_id)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "sunrise.forecast_unavailable",
                series=series,
                station=station_id,
                reason="metadata_error",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            self._am_low_cache[cache_key] = (now_mono, False, None, None)
            return False

        try:
            periods = self.nws_client._get_hourly_periods(hourly_url)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "sunrise.forecast_unavailable",
                series=series,
                station=station_id,
                reason="forecast_fetch_error",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            self._am_low_cache[cache_key] = (now_mono, False, None, None)
            return False

        # Find the minimum temperature period within the LOCAL calendar day
        day_start_local = datetime.datetime.combine(local_date, datetime.time.min, tzinfo=tz)
        day_end_local = day_start_local + datetime.timedelta(days=1)

        min_temp_f: Optional[float] = None
        min_time_local: Optional[datetime.datetime] = None

        for period in periods:
            start_str = period.get("startTime")
            temp_raw = period.get("temperature")
            temp_unit = period.get("temperatureUnit", "F")
            if start_str is None or temp_raw is None:
                continue
            try:
                t_dt = datetime.datetime.fromisoformat(str(start_str))
                if t_dt.tzinfo is None:
                    t_dt = t_dt.replace(tzinfo=datetime.timezone.utc)
                t_local = t_dt.astimezone(tz)
            except ValueError:
                continue
            if not (day_start_local <= t_local < day_end_local):
                continue
            try:
                temp_f = float(temp_raw)
                if temp_unit == "C":
                    temp_f = _c_to_f(temp_f)
            except (TypeError, ValueError):
                continue
            if min_temp_f is None or temp_f < min_temp_f:
                min_temp_f = temp_f
                min_time_local = t_local

        if min_temp_f is None or min_time_local is None:
            logger.warning(
                "sunrise.forecast_unavailable",
                series=series,
                station=station_id,
                reason="no_periods_for_local_date",
                local_date=local_date.isoformat(),
            )
            self._am_low_cache[cache_key] = (now_mono, False, None, None)
            return False

        deadline_hour = int(self.config.nws_low_deadline_hour)
        min_time_iso = min_time_local.isoformat()
        passed = min_time_local.hour < deadline_hour

        logger.info(
            "sunrise.am_low_check",
            series=series,
            forecast_min_temp_f=round(min_temp_f, 1),
            min_time_local=min_time_iso,
            deadline_hour=deadline_hour,
            passed=passed,
            cached=False,
        )
        if not passed:
            logger.info(
                "sunrise.am_low_blocked",
                series=series,
                forecast_min_temp_f=round(min_temp_f, 1),
                min_time_local=min_time_iso,
                deadline_hour=deadline_hour,
            )

        self._am_low_cache[cache_key] = (now_mono, passed, round(min_temp_f, 1), min_time_iso)
        return passed

    # ------------------------------------------------------------------
    # Temperature rise-from-baseline latch
    # ------------------------------------------------------------------

    def _check_temp_rise_latch(
        self,
        series: str,
        station_id: str,
        now_utc: datetime.datetime,
        baseline_start_utc: datetime.datetime,
        gate_close_utc: datetime.datetime,
        tz: ZoneInfo,
        local_date: datetime.date,
    ) -> bool:
        """Update the running-min / latch for *series* and return latch state."""
        state = self._rise_state.setdefault(series, _TempRiseState())
        previous_date = state.state_date
        if previous_date != local_date:
            state = _TempRiseState(state_date=local_date)
            self._rise_state[series] = state
            if previous_date is not None:
                logger.info(
                    "sunrise.temp_rise_state_reset_new_day",
                    series=series,
                    previous_date=previous_date.isoformat(),
                    new_date=local_date.isoformat(),
                )

        # Fetch recent observations since baseline start
        start_iso = baseline_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            raw_obs, _obs_source = self._fetch_station_obs(
                station_id,
                nws_url=(
                    f"https://api.weather.gov/stations/{station_id}/observations"
                    f"?start={start_iso}"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self._deduped_info(
                "sunrise.obs_unavailable",
                series,
                local_date,
                series=series,
                station=station_id,
                reason="fetch_error",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            return False

        # Filter by baseline window and convert °C → °F; sort ascending for latch logic
        parsed_obs: list[tuple[datetime.datetime, float]] = sorted(
            [
                (obs_ts_utc, _c_to_f(temp_c))
                for obs_ts_utc, temp_c in raw_obs
                if obs_ts_utc >= baseline_start_utc
            ],
            key=lambda x: x[0],
        )

        if not parsed_obs:
            self._deduped_info(
                "sunrise.obs_unavailable",
                series,
                local_date,
                series=series,
                station=station_id,
                reason="no_observations_in_window",
                baseline_start=start_iso,
            )
            return False

        latest_ts, latest_f = parsed_obs[-1]

        # Log observation cadence once per day per station
        if state.obs_cadence_logged_date != local_date and len(parsed_obs) >= 2:
            obs_gaps = [
                (parsed_obs[i][0] - parsed_obs[i - 1][0]).total_seconds() / 60.0
                for i in range(1, len(parsed_obs))
            ]
            avg_gap = sum(obs_gaps) / len(obs_gaps) if obs_gaps else 0.0
            logger.info(
                "sunrise.obs_cadence",
                series=series,
                station=station_id,
                obs_count=len(parsed_obs),
                avg_gap_minutes=round(avg_gap, 1),
                date=local_date.isoformat(),
            )
            state.obs_cadence_logged_date = local_date

        # Stale-obs check
        age_minutes = (now_utc - latest_ts).total_seconds() / 60.0
        max_age_minutes = self._station_obs_max_age_minutes(station_id)
        if age_minutes > max_age_minutes:
            self._deduped_info(
                "sunrise.obs_unavailable",
                series,
                local_date,
                series=series,
                station=station_id,
                reason="stale_observation",
                latest_obs_utc=latest_ts.isoformat(),
                age_minutes=round(age_minutes, 1),
                max_age_minutes=max_age_minutes,
            )
            return False

        # Update running minimum from all observations in window
        for obs_ts, obs_f in parsed_obs:
            if obs_f < state.running_min_f:
                if state.latched and obs_f < (state.latch_baseline_f or float("inf")):
                    # New low below the baseline used when latched → reset latch
                    logger.info(
                        "sunrise.temp_rise_reset",
                        series=series,
                        station=station_id,
                        new_min_f=round(obs_f, 2),
                        previous_baseline_f=round(state.latch_baseline_f, 2) if state.latch_baseline_f is not None else None,
                        obs_utc=obs_ts.isoformat(),
                    )
                    state.latched = False
                    state.latch_baseline_f = None
                    state.latch_current_f = None
                    state.latch_time_utc = None
                state.running_min_f = obs_f

        # Check rise condition
        if not state.latched:
            rise = latest_f - state.running_min_f
            if rise >= self.config.sunrise_temp_rise_required:
                state.latched = True
                state.latch_baseline_f = state.running_min_f
                state.latch_current_f = latest_f
                state.latch_time_utc = latest_ts
                logger.info(
                    "sunrise.temp_rise_latched",
                    series=series,
                    station=station_id,
                    baseline_f=round(state.running_min_f, 2),
                    current_f=round(latest_f, 2),
                    rise_f=round(rise, 2),
                    required_f=self.config.sunrise_temp_rise_required,
                    baseline_utc=latest_ts.isoformat(),
                )

        state.last_obs_time_utc = latest_ts
        return state.latched

    # ------------------------------------------------------------------
    # Sunrise computation helpers
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Legacy 2-observation check (kept for sunrise_temp_rise_required=0 path)
    # ------------------------------------------------------------------

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
            parsed_obs, _obs_source = self._fetch_station_obs(
                station_id,
                nws_url=f"https://api.weather.gov/stations/{station_id}/observations?limit=2",
            )
        except Exception as exc:  # noqa: BLE001
            ctx = {
                "reason": "fetch_error",
                "error_class": type(exc).__name__,
                "error_message": str(exc),
            }
            self._temp_cache[cache_key] = (cache_now, False, "sunrise.obs_unavailable", ctx)
            return False, "sunrise.obs_unavailable", ctx
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
