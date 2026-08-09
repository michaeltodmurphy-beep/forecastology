import datetime

from app.config import AppConfig
from core.sunrise_gate import SunriseEntryGate

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]


class _FakeNWSClient:
    def __init__(self, payload):
        self.payload = payload

    def _get_json(self, _url: str):  # noqa: D401
        return self.payload


def _make_config(**overrides) -> AppConfig:
    cfg = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=1,
        monitor_start_price=80,
        buy_trigger_price_low=82,
        buy_trigger_price_high=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=35,
        entry_gate_mode="SUNRISE",
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def test_astral_sunrise_computation_known_city_date():
    gate = SunriseEntryGate(_make_config())
    tz = ZoneInfo("America/New_York")
    sunrise = gate._compute_astral_sunrise_local(39.8729, -75.2437, tz, datetime.date(2026, 8, 9))
    assert sunrise.tzinfo is not None
    assert sunrise.hour in {5, 6}


def test_sunrise_gate_blocks_before_open_and_after_window(monkeypatch):
    cfg = _make_config(sunrise_strategy_time=30, sunrise_entry_window_minutes=120, sunrise_require_temp_rising=False)
    gate = SunriseEntryGate(cfg, nws_client=_FakeNWSClient({"features": []}))
    tz = ZoneInfo("America/Chicago")
    fixed_sunrise = datetime.datetime(2026, 8, 9, 6, 0, tzinfo=tz)
    monkeypatch.setattr(gate, "_get_sunrise_local", lambda *_args, **_kwargs: (fixed_sunrise, "astral"))

    before_open = datetime.datetime(2026, 8, 9, 11, 20, tzinfo=datetime.timezone.utc)  # 06:20 local
    within_window = datetime.datetime(2026, 8, 9, 11, 40, tzinfo=datetime.timezone.utc)  # 06:40 local
    after_close = datetime.datetime(2026, 8, 9, 13, 40, tzinfo=datetime.timezone.utc)  # 08:40 local

    assert gate.evaluate("KXLOWTCHI-26AUG09-B67.5", now_utc=before_open).allowed is False
    assert gate.evaluate("KXLOWTCHI-26AUG09-B67.5", now_utc=within_window).allowed is True
    assert gate.evaluate("KXLOWTCHI-26AUG09-B67.5", now_utc=after_close).allowed is False


def test_temp_rising_pass_fail_and_stale_cases(monkeypatch):
    tz = ZoneInfo("America/Phoenix")
    fixed_sunrise = datetime.datetime(2026, 8, 9, 5, 45, tzinfo=tz)
    now_utc = datetime.datetime(2026, 8, 9, 13, 0, tzinfo=datetime.timezone.utc)

    def _gate_for_payload(payload):
        gate = SunriseEntryGate(
            _make_config(sunrise_strategy_time=0, sunrise_entry_window_minutes=300, sunrise_require_temp_rising=True),
            nws_client=_FakeNWSClient(payload),
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

    assert _gate_for_payload(rising_payload).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is True
    assert _gate_for_payload(falling_payload).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is False
    assert _gate_for_payload(stale_payload).evaluate("KXLOWTPHX-26AUG09-B90.5", now_utc=now_utc).allowed is False


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
