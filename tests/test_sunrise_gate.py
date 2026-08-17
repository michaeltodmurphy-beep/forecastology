import datetime
import os
import warnings

from app.config import AppConfig
from core.sunrise_gate import SunriseEntryGate, _c_to_f
from structlog.testing import capture_logs

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Fake NWS client helpers
# ---------------------------------------------------------------------------

class _FakeNWSClient:
    """Minimal fake NWS client.

    Supports:
    - ``obs_payload``: returned by ``_get_json`` for observation URLs.
    - ``forecast_periods``: returned by ``_get_hourly_periods`` for AM-low check.
    - ``station_meta``: (lat, lon, hourly_url, tz_name) returned by ``_get_station_metadata``.
    """

    def __init__(
        self,
        obs_payload=None,
        forecast_periods=None,
        station_meta=None,
        raise_obs=False,
        raise_meta=False,
    ):
        self.obs_payload = obs_payload or {"features": []}
        self.forecast_periods = forecast_periods
        self.station_meta = station_meta or (0.0, 0.0, "https://api.weather.gov/hourly", "UTC")
        self.raise_obs = raise_obs
        self.raise_meta = raise_meta

    def _get_json(self, url: str):
        if self.raise_obs:
            raise RuntimeError("obs fetch error")
        return self.obs_payload

    def _get_station_metadata(self, station_id: str):
        if self.raise_meta:
            raise RuntimeError("metadata error")
        return self.station_meta

    def _get_hourly_periods(self, hourly_url: str):
        if self.forecast_periods is None:
            raise ValueError("no forecast periods")
        return self.forecast_periods


def _make_config(**overrides) -> AppConfig:
    cfg = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="mysql+mysqlconnector://localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=1,
        monitor_start_price=80,
        buy_trigger_price_low=82,
        buy_trigger_price_high=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=35,
        entry_gate_mode="SUNRISE",
        sunrise_obs_source="nws",  # use legacy NWS path so _FakeNWSClient._get_json works
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_forecast_periods_local_low_before_noon(tz: ZoneInfo, local_date: datetime.date) -> list:
    """Return hourly periods where the day-min is at 06:00 (well before noon)."""
    periods = []
    temps = {5: 65.0, 6: 63.0, 7: 64.0, 8: 66.0, 9: 68.0, 10: 70.0, 11: 72.0}
    for hour, temp in temps.items():
        t = datetime.datetime(local_date.year, local_date.month, local_date.day, hour, 0, tzinfo=tz)
        periods.append({"startTime": t.isoformat(), "temperature": temp, "temperatureUnit": "F"})
    return periods


def _make_forecast_periods_local_low_after_noon(tz: ZoneInfo, local_date: datetime.date) -> list:
    """Return hourly periods where the day-min is at 14:00 (past noon deadline)."""
    periods = []
    temps = {6: 70.0, 7: 68.0, 8: 66.0, 9: 65.0, 10: 64.0, 11: 63.0, 12: 62.0, 13: 61.0, 14: 60.0, 15: 62.0}
    for hour, temp in temps.items():
        t = datetime.datetime(local_date.year, local_date.month, local_date.day, hour, 0, tzinfo=tz)
        periods.append({"startTime": t.isoformat(), "temperature": temp, "temperatureUnit": "F"})
    return periods


def _obs_features(entries: list[tuple[str, float]]) -> dict:
    """Build an observations payload from (iso_timestamp, celsius_value) pairs."""
    return {
        "features": [
            {"properties": {"timestamp": ts, "temperature": {"value": c}}}
            for ts, c in entries
        ]
    }


# ---------------------------------------------------------------------------
# Existing tests (updated for new gate logic)
# ---------------------------------------------------------------------------

def test_astral_sunrise_computation_known_city_date():
    gate = SunriseEntryGate(_make_config())
    tz = ZoneInfo("America/New_York")
    sunrise = gate._compute_astral_sunrise_local(39.8729, -75.2437, tz, datetime.date(2026, 8, 9))
    assert sunrise.tzinfo is not None
    assert sunrise.hour in {5, 6}


def test_sunrise_gate_blocks_before_open_and_after_window(monkeypatch):
    """Time-window gate works; AM-low and temp-rise checks disabled for isolation."""
    cfg = _make_config(
        sunrise_strategy_time=30,
        sunrise_entry_window_minutes=120,
        sunrise_require_am_low=False,
        sunrise_temp_rise_required=0.0,
        sunrise_require_temp_rising=False,
    )
    gate = SunriseEntryGate(cfg, nws_client=_FakeNWSClient())
    tz = ZoneInfo("America/Chicago")
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *_args, **_kwargs: (fixed_sunrise, "astral"))

    before_open = datetime.datetime(2026, 8, 9, 11, 20, tzinfo=datetime.timezone.utc)  # 06:20 local
    within_window = datetime.datetime(2026, 8, 9, 11, 40, tzinfo=datetime.timezone.utc)  # 06:40 local
    after_close = datetime.datetime(2026, 8, 9, 13, 40, tzinfo=datetime.timezone.utc)  # 08:40 local

    assert gate.evaluate("KXLOWTCHI-26AUG09-B67.5", now_utc=before_open).allowed is False
    assert gate.evaluate("KXLOWTCHI-26AUG09-B67.5", now_utc=within_window).allowed is True
    assert gate.evaluate("KXLOWTCHI-26AUG09-B67.5", now_utc=after_close).allowed is False


def test_temp_rising_legacy_path_still_works_when_rise_required_zero(monkeypatch):
    """Legacy sunrise_require_temp_rising check still works when temp_rise_required=0."""
    tz = ZoneInfo("America/Phoenix")
    fixed_sunrise = datetime.datetime(2026, 8, 9, 5, 45, tzinfo=tz)
    now_utc = datetime.datetime(2026, 8, 9, 13, 0, tzinfo=datetime.timezone.utc)

    def _gate_for_payload(payload, require_rising: bool):
        gate = SunriseEntryGate(
            _make_config(
                sunrise_strategy_time=0,
                sunrise_entry_window_minutes=300,
                sunrise_require_temp_rising=require_rising,
                sunrise_temp_rise_required=0.0,
                sunrise_require_am_low=False,
            ),
            nws_client=_FakeNWSClient(obs_payload=payload),
        )
        monkeypatch.setattr(gate, "_get_sunrise_local", lambda *_args, **_kwargs: (fixed_sunrise, "astral"))
        return gate

    rising_payload = {
        "features": [
            {"properties": {"timestamp": "2026-08-09T11:50:00+00:00", "temperature": {"value": 28.0}}},
            {"properties": {"timestamp": "2026-08-09T11:40:00+00:00", "temperature": {"value": 27.5}}},
        ]
    }
    falling_payload = {
        "features": [
            {"properties": {"timestamp": "2026-08-09T11:50:00+00:00", "temperature": {"value": 27.0}}},
            {"properties": {"timestamp": "2026-08-09T11:40:00+00:00", "temperature": {"value": 27.5}}},
        ]
    }
    stale_payload = {
        "features": [
            {"properties": {"timestamp": "2026-08-09T09:00:00+00:00", "temperature": {"value": 27.0}}},
            {"properties": {"timestamp": "2026-08-09T08:50:00+00:00", "temperature": {"value": 26.8}}},
        ]
    }

    assert _gate_for_payload(rising_payload, True).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is True
    assert _gate_for_payload(falling_payload, True).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is False
    assert _gate_for_payload(stale_payload, True).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is False
    # require_rising=False and rise_required=0 → no temp check at all
    assert _gate_for_payload(rising_payload, False).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is True


def test_api_source_falls_back_to_astral(monkeypatch):
    gate = SunriseEntryGate(_make_config(sunrise_source="api"))
    tz = ZoneInfo("America/New_York")
    astral_sunrise = datetime.datetime(2026, 8, 9, 6, 1, tzinfo=tz)
    monkeypatch.setattr(gate, "_fetch_api_sunrise_utc", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(gate, "_compute_astral_sunrise_local", lambda *_a, **_k: astral_sunrise)

    computed, source = gate._get_sunrise_local("KXLOWTPHIL", tz, datetime.date(2026, 8, 9), 39.8729, -75.2437)
    assert computed == astral_sunrise
    assert source == "astral"


def test_missing_coords_uses_nws_window_fallback():
    gate = SunriseEntryGate(_make_config())
    now_utc = datetime.datetime(2026, 8, 9, 12, 0, tzinfo=datetime.timezone.utc)
    decision = gate.evaluate("KXLOWTXYZ-26AUG09-B70", now_utc=now_utc)
    assert decision.allowed is True
    assert decision.use_nws_window_fallback is True


_REQUIRED_FROM_ENV = {
    "KALSHI_API_KEY": "test-key",
    "KALSHI_PRIVATE_KEY_PATH": "unused.pem",
    "MYSQL_DATABASE_URL": "mysql+mysqlconnector://localhost:3306/test",
    "TRADING_MODE": "PAPER",
    "BUY_TRIGGER_PRICE_LOW": "0.82",
    "BUY_TRIGGER_PRICE_HIGH": "0.83",
    "STOP_LOSS_PRICE_ASK": "0.35",
    "INITIAL_CONTRACT_COUNT": "1",
    "MINIMUM_SPREAD": "0.04",
    "MONITOR_START_PRICE": "0.80",
    "SPREAD_MONITOR_PRICE": "0.90",
    "HEDGE_TRIGGER_PRICE": "0.48",
}


def _set_required_env(monkeypatch) -> None:
    """Set the minimal env vars required for AppConfig.from_env()."""
    for k, v in _REQUIRED_FROM_ENV.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# Config: new fields / defaults / deprecation warning
# ---------------------------------------------------------------------------

def test_config_new_field_defaults():
    cfg = _make_config()
    assert cfg.sunrise_require_am_low is True
    assert cfg.nws_low_deadline_hour == 12
    assert cfg.sunrise_temp_rise_required == 1.0
    assert cfg.sunrise_temp_baseline_minutes == 15


def test_config_new_fields_from_env(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SUNRISE_REQUIRE_AM_LOW", "no")
    monkeypatch.setenv("NWS_LOW_DEADLINE_HOUR", "10")
    monkeypatch.setenv("SUNRISE_TEMP_RISE_REQUIRED", "2.5")
    monkeypatch.setenv("SUNRISE_TEMP_BASELINE_MINUTES", "20")
    cfg = AppConfig.from_env()
    assert cfg.sunrise_require_am_low is False
    assert cfg.nws_low_deadline_hour == 10
    assert cfg.sunrise_temp_rise_required == 2.5
    assert cfg.sunrise_temp_baseline_minutes == 20


def test_config_sunrise_require_temp_rising_deprecation_warning(monkeypatch):
    from structlog.testing import capture_logs
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SUNRISE_REQUIRE_TEMP_RISING", "no")
    with capture_logs() as logs:
        AppConfig.from_env()
    assert any(
        "deprecated" in str(e.get("message", "")).lower()
        for e in logs
        if e.get("log_level") == "warning"
    )


def test_config_nws_low_deadline_hour_out_of_range(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("NWS_LOW_DEADLINE_HOUR", "30")
    cfg = AppConfig.from_env()
    assert cfg.nws_low_deadline_hour == 12  # clamped to default


def test_config_sunrise_temp_rise_required_invalid(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SUNRISE_TEMP_RISE_REQUIRED", "bad")
    cfg = AppConfig.from_env()
    assert cfg.sunrise_temp_rise_required == 1.0  # default


def test_config_sunrise_temp_rise_required_negative(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SUNRISE_TEMP_RISE_REQUIRED", "-1.0")
    cfg = AppConfig.from_env()
    assert cfg.sunrise_temp_rise_required == 1.0  # default


# ---------------------------------------------------------------------------
# AM-low forecast check: pass / block / unavailable / deadline boundary
# ---------------------------------------------------------------------------

def _gate_with_am_low(forecast_periods, tz, deadline_hour=12, raise_meta=False, raise_forecast=False, monkeypatch=None):
    """Build a gate with only AM-low check active; temp-rise and time checks disabled."""
    tz_str = str(getattr(tz, "key", tz))
    client = _FakeNWSClient(
        forecast_periods=forecast_periods,
        station_meta=(40.0, -74.0, "https://api.weather.gov/hourly", tz_str),
        raise_meta=raise_meta,
    )
    if raise_forecast:
        client.forecast_periods = None  # triggers ValueError in _get_hourly_periods

    cfg = _make_config(
        sunrise_require_am_low=True,
        nws_low_deadline_hour=deadline_hour,
        sunrise_temp_rise_required=0.0,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=0,
        sunrise_entry_window_minutes=600,
    )
    gate = SunriseEntryGate(cfg, nws_client=client)
    if monkeypatch is not None:
        tz_zone = ZoneInfo(tz_str) if isinstance(tz, str) else tz
        fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz_zone)
        monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))
    return gate


def test_am_low_passes_when_min_before_deadline(monkeypatch):
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    periods = _make_forecast_periods_local_low_before_noon(tz, local_date)
    gate = _gate_with_am_low(periods, tz, deadline_hour=12, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)  # 07:00 EDT
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_am_low_blocks_when_min_after_deadline(monkeypatch):
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    periods = _make_forecast_periods_local_low_after_noon(tz, local_date)
    gate = _gate_with_am_low(periods, tz, deadline_hour=12, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)  # 07:00 EDT
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_am_low_blocks_on_metadata_unavailable(monkeypatch):
    tz = ZoneInfo("America/New_York")
    gate = _gate_with_am_low(None, tz, raise_meta=True, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_am_low_blocks_on_forecast_unavailable(monkeypatch):
    tz = ZoneInfo("America/New_York")
    gate = _gate_with_am_low(None, tz, raise_forecast=True, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_am_low_deadline_boundary_exact_hour_blocks(monkeypatch):
    """Deadline is exclusive: min at exactly deadline_hour should block."""
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    deadline = 10
    # Build periods where min is at exactly hour=10
    t_min = datetime.datetime(local_date.year, local_date.month, local_date.day, deadline, 0, tzinfo=tz)
    t_other = datetime.datetime(local_date.year, local_date.month, local_date.day, 8, 0, tzinfo=tz)
    periods = [
        {"startTime": t_other.isoformat(), "temperature": 70.0, "temperatureUnit": "F"},
        {"startTime": t_min.isoformat(), "temperature": 60.0, "temperatureUnit": "F"},  # min at deadline
    ]
    gate = _gate_with_am_low(periods, tz, deadline_hour=deadline, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_am_low_deadline_boundary_one_hour_before_passes(monkeypatch):
    """Min at (deadline_hour - 1) should pass."""
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    deadline = 10
    t_min = datetime.datetime(local_date.year, local_date.month, local_date.day, deadline - 1, 0, tzinfo=tz)
    t_other = datetime.datetime(local_date.year, local_date.month, local_date.day, 14, 0, tzinfo=tz)
    periods = [
        {"startTime": t_other.isoformat(), "temperature": 70.0, "temperatureUnit": "F"},
        {"startTime": t_min.isoformat(), "temperature": 60.0, "temperatureUnit": "F"},
    ]
    gate = _gate_with_am_low(periods, tz, deadline_hour=deadline, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_am_low_forecast_cache_is_used(monkeypatch):
    """Second call within cache TTL reuses cached result."""
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    periods = _make_forecast_periods_local_low_before_noon(tz, local_date)
    call_count = [0]

    class _CountingClient(_FakeNWSClient):
        def _get_hourly_periods(self, url):
            call_count[0] += 1
            return periods

    cfg = _make_config(
        sunrise_require_am_low=True,
        nws_low_deadline_hour=12,
        sunrise_temp_rise_required=0.0,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=0,
        sunrise_entry_window_minutes=600,
    )
    tz_str = "America/New_York"
    client = _CountingClient(
        station_meta=(40.0, -74.0, "https://api.weather.gov/hourly", tz_str),
    )
    gate = SunriseEntryGate(cfg, nws_client=client)
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)

    gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert call_count[0] == 1  # second call used cache


def test_am_low_cached_result_logs_at_debug(monkeypatch):
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    periods = _make_forecast_periods_local_low_before_noon(tz, local_date)
    gate = _gate_with_am_low(periods, tz, deadline_hour=12, monkeypatch=monkeypatch)
    now_utc = datetime.datetime(2026, 8, 9, 11, 0, tzinfo=datetime.timezone.utc)

    with capture_logs() as logs:
        gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
        gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)

    cached_logs = [e for e in logs if e.get("event") == "sunrise.am_low_check" and e.get("cached") is True]
    assert cached_logs
    assert all(e.get("log_level") == "debug" for e in cached_logs)


# ---------------------------------------------------------------------------
# Celsius conversion helper
# ---------------------------------------------------------------------------

def test_celsius_to_fahrenheit_conversion():
    assert abs(_c_to_f(0.0) - 32.0) < 0.01
    assert abs(_c_to_f(100.0) - 212.0) < 0.01
    assert abs(_c_to_f(20.0) - 68.0) < 0.01
    assert abs(_c_to_f(-40.0) - (-40.0)) < 0.01


# ---------------------------------------------------------------------------
# Temperature rise-from-baseline: latch / reset / threshold / stale-obs
# ---------------------------------------------------------------------------

def _gate_with_rise(obs_payload, rise_required=1.0, baseline_minutes=15, monkeypatch=None, raise_obs=False):
    """Build a gate with only temp-rise check active (AM-low disabled)."""
    client = _FakeNWSClient(
        obs_payload=obs_payload,
        raise_obs=raise_obs,
        station_meta=(40.0, -74.0, "https://api.weather.gov/hourly", "America/New_York"),
        forecast_periods=[],
    )
    cfg = _make_config(
        sunrise_require_am_low=False,
        sunrise_temp_rise_required=rise_required,
        sunrise_temp_baseline_minutes=baseline_minutes,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=30,
        sunrise_entry_window_minutes=120,
    )
    gate = SunriseEntryGate(cfg, nws_client=client)
    if monkeypatch is not None:
        tz = ZoneInfo("America/New_York")
        fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
        monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))
    return gate


def test_temp_rise_latch_set_on_sufficient_rise(monkeypatch):
    """Temp rises ≥ SUNRISE_TEMP_RISE_REQUIRED → latch set → entry allowed."""
    tz = ZoneInfo("America/New_York")
    # baseline_start = 06:00 - 15 = 05:45 UTC-4 = 09:45 UTC
    # within gate: now = 06:40 EDT = 10:40 UTC
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    # Two observations: baseline 20°C at 09:50, then 20.56°C at 10:30 (≥1°F rise)
    # 20°C = 68°F, 20.56°C = 69.01°F → rise = 1.01°F ≥ 1.0
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),   # baseline
        ("2026-08-09T10:30:00+00:00", 20.56),  # rise ~1°F
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_temp_rise_latch_not_set_below_threshold(monkeypatch):
    """Temp rise < SUNRISE_TEMP_RISE_REQUIRED → latch not set → entry blocked."""
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    # 20°C = 68°F, 20.3°C = 68.54°F → rise = 0.54°F < 1.0
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),
        ("2026-08-09T10:30:00+00:00", 20.3),
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_temp_rise_threshold_boundary_exact(monkeypatch):
    """Rise exactly equal to threshold → latch set."""
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    # Need exactly 1°F rise: if baseline is 20°C = 68°F, then 20 + (1/1.8) = 20.556°C = 69°F
    rise_f = 1.0
    baseline_c = 20.0
    current_c = baseline_c + rise_f / (9.0 / 5.0)
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", baseline_c),
        ("2026-08-09T10:30:00+00:00", current_c),
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_temp_rise_celsius_conversion(monkeypatch):
    """Temp values (Celsius from NWS) are correctly converted to °F for comparison."""
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    # 10°C = 50°F, 10.556°C ≈ 51°F → rise = 1°F ≥ 1.0
    baseline_c = 10.0
    current_c = 10.0 + 1.0 / (9.0 / 5.0)
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", baseline_c),
        ("2026-08-09T10:30:00+00:00", current_c),
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_temp_rise_stale_observation_blocks(monkeypatch):
    """Latest observation older than 15 min → treat as obs_unavailable → block."""
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    # Latest obs is 20 minutes old
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),
        ("2026-08-09T10:20:00+00:00", 20.6),  # 20 min old at 10:40 UTC
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_temp_rise_station_staleness_override_applies(monkeypatch):
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),
        ("2026-08-09T10:20:00+00:00", 20.6),  # 20 minutes old
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    gate.config.sunrise_obs_max_age_overrides = {"KNYC": 25}
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_temp_rise_obs_fetch_error_blocks(monkeypatch):
    """Observation fetch error → obs_unavailable → block entry."""
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    gate = _gate_with_rise(None, raise_obs=True, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_temp_rise_no_observations_blocks(monkeypatch):
    """No observations in window → obs_unavailable → block entry."""
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    gate = _gate_with_rise({"features": []}, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


def test_temp_rise_latch_resets_on_new_running_minimum(monkeypatch):
    """After latching, a new obs below the latch baseline resets the latch."""
    tz = ZoneInfo("America/New_York")
    now_utc_1 = datetime.datetime(2026, 8, 9, 10, 35, tzinfo=datetime.timezone.utc)
    now_utc_2 = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)

    # First call: obs that satisfy rise requirement → latch
    obs1 = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),   # running min = 20°C
        ("2026-08-09T10:30:00+00:00", 20.56),  # rise ≥ 1°F → latch
    ])
    cfg = _make_config(
        sunrise_require_am_low=False,
        sunrise_temp_rise_required=1.0,
        sunrise_temp_baseline_minutes=15,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=30,
        sunrise_entry_window_minutes=120,
    )
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)

    call_count = [0]
    payloads = [obs1, None]  # second call will have reset obs

    class _SequentialClient(_FakeNWSClient):
        def _get_json(self, url):
            idx = min(call_count[0], len(payloads) - 1)
            call_count[0] += 1
            return payloads[idx]

    gate = SunriseEntryGate(
        cfg,
        nws_client=_SequentialClient(
            station_meta=(40.0, -74.0, "https://api.weather.gov/hourly", "America/New_York"),
        ),
    )
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))

    r1 = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc_1)
    assert r1.allowed is True  # latched

    # Second call with obs that includes a new low below the latch baseline
    obs2 = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),
        ("2026-08-09T10:30:00+00:00", 20.56),  # this latched it
        ("2026-08-09T10:35:00+00:00", 19.5),   # NEW lower reading → reset latch
    ])
    payloads[1] = obs2

    r2 = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc_2)
    assert r2.allowed is False  # latch was reset


# ---------------------------------------------------------------------------
# Monitoring window start/stop times
# ---------------------------------------------------------------------------

def test_temp_rise_monitoring_window_start_time(monkeypatch):
    """Observations before (sunrise - baseline_minutes) are excluded."""
    tz = ZoneInfo("America/Denver")  # UTC-6
    # sunrise = 06:00 MDT = 12:00 UTC
    # baseline_start = 05:45 MDT = 11:45 UTC
    # gate_open = 06:30 MDT = 12:30 UTC
    now_utc = datetime.datetime(2026, 8, 9, 12, 40, tzinfo=datetime.timezone.utc)  # 06:40 MDT

    # Observation at 11:30 UTC is BEFORE baseline_start (11:45 UTC) → excluded
    # Observation at 11:50 UTC is within window
    # Rise from 11:50 obs to latest
    obs = _obs_features([
        ("2026-08-09T11:30:00+00:00", 10.0),   # before window – should be excluded
        ("2026-08-09T11:50:00+00:00", 20.0),   # within window, becomes running min
        ("2026-08-09T12:35:00+00:00", 20.56),  # rise from 20°C ≥ 1°F
    ])
    cfg = _make_config(
        sunrise_require_am_low=False,
        sunrise_temp_rise_required=1.0,
        sunrise_temp_baseline_minutes=15,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=30,
        sunrise_entry_window_minutes=120,
    )
    client = _FakeNWSClient(obs_payload=obs, station_meta=(40.0, -74.0, "https://api.weather.gov/hourly", "America/Denver"))
    gate = SunriseEntryGate(cfg, nws_client=client)
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))

    result = gate.evaluate("KXLOWTDEN-26AUG09-B60", now_utc=now_utc)
    assert result.allowed is True


def test_temp_rise_coarse_cadence_knyc(monkeypatch):
    """KNYC reports ~hourly; gate must tolerate coarse cadence (no false stale block)."""
    tz = ZoneInfo("America/New_York")
    # sunrise = 06:00 EDT = 10:00 UTC; baseline_start = 05:45 EDT = 09:45 UTC
    # Latest obs at 10:30 UTC is 10 minutes before now → not stale (< 15 min threshold)
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),   # within window (after 09:45 UTC)
        ("2026-08-09T10:30:00+00:00", 20.56),  # 10 min old → not stale, ≥1°F rise
    ])
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


# ---------------------------------------------------------------------------
# Restart-mid-window: baseline rebuild from obs start param
# ---------------------------------------------------------------------------

def test_restart_mid_window_baseline_rebuild(monkeypatch):
    """On restart within the monitoring window, running min is rebuilt from obs history."""
    tz = ZoneInfo("America/New_York")
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)
    # Fresh gate (simulating restart): no prior state, but obs history back to baseline_start
    # The NWS client returns ALL obs since baseline start, including earlier cooler ones
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 19.0),   # earliest – lowest
        ("2026-08-09T10:00:00+00:00", 19.2),
        ("2026-08-09T10:15:00+00:00", 19.5),
        ("2026-08-09T10:30:00+00:00", 19.56),  # 19.0°C → 66.2°F, 19.56°C → 67.2°F, rise ~1°F
    ])
    # 19.0°C = 66.2°F, 19.56°C = 67.21°F → rise = 1.01°F ≥ 1.0
    gate = _gate_with_rise(obs, rise_required=1.0, monkeypatch=monkeypatch)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


# ---------------------------------------------------------------------------
# Combined gate ordering
# ---------------------------------------------------------------------------

def test_combined_gate_ordering_all_pass(monkeypatch):
    """All conditions pass → allowed."""
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    forecast = _make_forecast_periods_local_low_before_noon(tz, local_date)
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),
        ("2026-08-09T10:30:00+00:00", 20.56),
    ])
    client = _FakeNWSClient(
        obs_payload=obs,
        forecast_periods=forecast,
        station_meta=(40.778, -73.969, "https://api.weather.gov/hourly", "America/New_York"),
    )
    cfg = _make_config(
        sunrise_require_am_low=True,
        nws_low_deadline_hour=12,
        sunrise_temp_rise_required=1.0,
        sunrise_temp_baseline_minutes=15,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=30,
        sunrise_entry_window_minutes=120,
    )
    gate = SunriseEntryGate(cfg, nws_client=client)
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))
    now_utc = datetime.datetime(2026, 8, 9, 10, 40, tzinfo=datetime.timezone.utc)  # 06:40 EDT
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is True


def test_combined_gate_time_before_open_blocks_regardless(monkeypatch):
    """Time gate blocks before gate_open even if other checks would pass."""
    tz = ZoneInfo("America/New_York")
    local_date = datetime.date(2026, 8, 9)
    forecast = _make_forecast_periods_local_low_before_noon(tz, local_date)
    obs = _obs_features([
        ("2026-08-09T09:50:00+00:00", 20.0),
        ("2026-08-09T10:10:00+00:00", 20.56),
    ])
    client = _FakeNWSClient(
        obs_payload=obs,
        forecast_periods=forecast,
        station_meta=(40.778, -73.969, "https://api.weather.gov/hourly", "America/New_York"),
    )
    cfg = _make_config(
        sunrise_require_am_low=True,
        nws_low_deadline_hour=12,
        sunrise_temp_rise_required=1.0,
        sunrise_temp_baseline_minutes=15,
        sunrise_require_temp_rising=False,
        sunrise_strategy_time=30,
        sunrise_entry_window_minutes=120,
    )
    gate = SunriseEntryGate(cfg, nws_client=client)
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *a, **k: (fixed_sunrise, "astral"))
    now_utc = datetime.datetime(2026, 8, 9, 10, 20, tzinfo=datetime.timezone.utc)  # 06:20 EDT (before gate_open)
    result = gate.evaluate("KXLOWTNYC-26AUG09-B73.5", now_utc=now_utc)
    assert result.allowed is False


# ---------------------------------------------------------------------------
# KXHIGH and NWS_WINDOW modes unchanged
# ---------------------------------------------------------------------------

def test_kxhigh_series_always_allowed():
    """KXHIGH* series are not KXLOW → always allowed by SunriseEntryGate."""
    gate = SunriseEntryGate(_make_config())
    now_utc = datetime.datetime(2026, 8, 9, 12, 0, tzinfo=datetime.timezone.utc)
    result = gate.evaluate("KXHIGHTATL-26AUG09-B95", now_utc=now_utc)
    assert result.allowed is True


def test_nws_window_mode_does_not_invoke_sunrise_gate(monkeypatch):
    """In NWS_WINDOW mode the state machine does not call evaluate(); gate is inert."""
    # The evaluate method itself allows non-KXLOW series. NWS_WINDOW bypasses the
    # gate entirely in state_machine.py. Here we just confirm the gate's own guard.
    cfg = _make_config(entry_gate_mode="NWS_WINDOW")
    gate = SunriseEntryGate(cfg)
    # KXLOW ticker: in SUNRISE mode this would check coords/time. In NWS_WINDOW
    # the state machine never calls this. We call directly to show the code path
    # still works even if mistakenly invoked.
    now_utc = datetime.datetime(2026, 8, 9, 12, 0, tzinfo=datetime.timezone.utc)
    # Without mocking sunrise_local, it will try to compute via astral — that's fine.
    # The gate itself doesn't check entry_gate_mode; it always evaluates.
    # This test just confirms no exception is raised.
    try:
        gate.evaluate("KXHIGHTATL-26AUG09-B95", now_utc=now_utc)
    except Exception as exc:  # noqa: BLE001
        raise AssertionError(f"Unexpected exception from evaluate: {exc}") from exc
