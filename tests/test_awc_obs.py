"""Tests for nws/awc_obs.py: AWC METAR client, NWS parser, fallback logic."""
from __future__ import annotations

import datetime
import os

import pytest

from nws.awc_obs import (
    _parse_awc_response,
    fetch_obs_with_fallback,
    parse_nws_obs_payload,
)


# ---------------------------------------------------------------------------
# AWC response parsing tests
# ---------------------------------------------------------------------------

def _utc(ts: str) -> datetime.datetime:
    dt = datetime.datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def test_parse_awc_unix_epoch():
    """obsTime as Unix integer → correct UTC datetime."""
    data = [{"icaoId": "KBOS", "obsTime": 1723888800, "temp": 22.5}]
    result = _parse_awc_response(data)
    assert len(result) == 1
    ts, temp = result[0]
    assert isinstance(ts, datetime.datetime)
    assert ts.tzinfo == datetime.timezone.utc
    assert temp == pytest.approx(22.5)
    assert ts == datetime.datetime.fromtimestamp(1723888800, tz=datetime.timezone.utc)


def test_parse_awc_iso_string():
    """obsTime as ISO-8601 string → correct UTC datetime."""
    data = [{"icaoId": "KBOS", "obsTime": "2026-08-17T11:05:00Z", "temp": 20.0}]
    result = _parse_awc_response(data)
    assert len(result) == 1
    ts, temp = result[0]
    assert ts == _utc("2026-08-17T11:05:00+00:00")
    assert temp == pytest.approx(20.0)


def test_parse_awc_uses_report_time_when_obs_time_missing():
    """Falls back to reportTime when obsTime is absent."""
    data = [{"icaoId": "KBOS", "reportTime": 1723888800, "temp": 19.0}]
    result = _parse_awc_response(data)
    assert len(result) == 1


def test_parse_awc_skips_missing_temp():
    """Records with temp=None are skipped."""
    data = [
        {"icaoId": "KBOS", "obsTime": 1723888800, "temp": None},
        {"icaoId": "KBOS", "obsTime": 1723885200, "temp": 18.0},
    ]
    result = _parse_awc_response(data)
    assert len(result) == 1
    assert result[0][1] == pytest.approx(18.0)


def test_parse_awc_skips_missing_time():
    """Records with no obsTime or reportTime are skipped."""
    data = [
        {"icaoId": "KBOS", "temp": 20.0},  # no time fields
        {"icaoId": "KBOS", "obsTime": 1723888800, "temp": 21.0},
    ]
    result = _parse_awc_response(data)
    assert len(result) == 1
    assert result[0][1] == pytest.approx(21.0)


def test_parse_awc_skips_invalid_temp():
    """Records with non-numeric temp are skipped."""
    data = [
        {"icaoId": "KBOS", "obsTime": 1723888800, "temp": "bad"},
        {"icaoId": "KBOS", "obsTime": 1723885200, "temp": 17.0},
    ]
    result = _parse_awc_response(data)
    assert len(result) == 1


def test_parse_awc_newest_first_ordering():
    """Results are sorted newest first."""
    data = [
        {"icaoId": "KBOS", "obsTime": 1723885200, "temp": 20.0},
        {"icaoId": "KBOS", "obsTime": 1723888800, "temp": 21.0},
        {"icaoId": "KBOS", "obsTime": 1723882800, "temp": 19.5},
    ]
    result = _parse_awc_response(data)
    assert len(result) == 3
    assert result[0][0] > result[1][0] > result[2][0]
    assert result[0][1] == pytest.approx(21.0)  # newest


def test_parse_awc_empty_list():
    """Empty response → empty list, no error."""
    assert _parse_awc_response([]) == []


def test_parse_awc_non_dict_records_skipped():
    """Non-dict records in array are skipped gracefully."""
    data = [
        "not a dict",
        None,
        {"icaoId": "KBOS", "obsTime": 1723888800, "temp": 22.0},
    ]
    result = _parse_awc_response(data)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# NWS obs payload parsing tests
# ---------------------------------------------------------------------------

def _nws_feature(ts: str, temp_c: float | None) -> dict:
    return {
        "properties": {
            "timestamp": ts,
            "temperature": {"value": temp_c} if temp_c is not None else {"value": None},
        }
    }


def test_parse_nws_obs_payload_basic():
    payload = {
        "features": [
            _nws_feature("2026-08-17T11:05:00+00:00", 22.0),
            _nws_feature("2026-08-17T11:00:00+00:00", 21.5),
        ]
    }
    result = parse_nws_obs_payload(payload)
    assert len(result) == 2
    assert result[0][0] > result[1][0]  # newest first


def test_parse_nws_obs_payload_skips_null_temp():
    payload = {
        "features": [
            _nws_feature("2026-08-17T11:05:00+00:00", None),
            _nws_feature("2026-08-17T11:00:00+00:00", 21.5),
        ]
    }
    result = parse_nws_obs_payload(payload)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# fetch_obs_with_fallback tests
# ---------------------------------------------------------------------------

class _OkNWSClient:
    """NWS client that returns a fixed GeoJSON payload."""

    def __init__(self, features: list):
        self.features = features
        self.called_urls: list[str] = []

    def _get_json(self, url: str):
        self.called_urls.append(url)
        return {"features": self.features}


class _RaisingNWSClient:
    """NWS client that always raises."""

    def _get_json(self, url: str):
        raise RuntimeError("nws network error")


def _nws_features_two() -> list:
    return [
        {
            "properties": {
                "timestamp": "2026-08-17T11:05:00+00:00",
                "temperature": {"value": 22.0},
            }
        },
        {
            "properties": {
                "timestamp": "2026-08-17T11:00:00+00:00",
                "temperature": {"value": 21.5},
            }
        },
    ]


def test_nws_mode_uses_nws_only(monkeypatch):
    """obs_source='nws' calls NWS directly, never touches AWC."""
    import nws.awc_obs as awc_module

    awc_called = []

    def fake_fetch_awc(station_id, **kwargs):
        awc_called.append(station_id)
        raise RuntimeError("should not be called")

    monkeypatch.setattr(awc_module, "fetch_awc_obs", fake_fetch_awc)

    nws_client = _OkNWSClient(_nws_features_two())
    obs, source = fetch_obs_with_fallback(
        "KBOS",
        nws_client=nws_client,
        nws_url="https://api.weather.gov/stations/KBOS/observations?limit=2",
        obs_source="nws",
    )
    assert source == "nws"
    assert len(obs) == 2
    assert not awc_called


def test_awc_success_nws_not_called(monkeypatch):
    """AWC returns ≥2 obs → NWS is not called."""
    import nws.awc_obs as awc_module

    awc_obs = [
        (datetime.datetime(2026, 8, 17, 11, 5, tzinfo=datetime.timezone.utc), 22.0),
        (datetime.datetime(2026, 8, 17, 11, 0, tzinfo=datetime.timezone.utc), 21.5),
    ]

    monkeypatch.setattr(awc_module, "fetch_awc_obs", lambda *a, **k: awc_obs)

    nws_client = _OkNWSClient(_nws_features_two())
    obs, source = fetch_obs_with_fallback(
        "KBOS",
        nws_client=nws_client,
        nws_url="https://api.weather.gov/stations/KBOS/observations?limit=2",
        obs_source="awc",
    )
    assert source == "awc"
    assert len(obs) == 2
    # NWS was NOT called
    assert nws_client.called_urls == []


def test_awc_network_error_falls_back_to_nws(monkeypatch):
    """AWC network error → NWS fallback is used."""
    import nws.awc_obs as awc_module

    def fail(*a, **k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(awc_module, "fetch_awc_obs", fail)

    nws_client = _OkNWSClient(_nws_features_two())
    obs, source = fetch_obs_with_fallback(
        "KBOS",
        nws_client=nws_client,
        nws_url="https://api.weather.gov/stations/KBOS/observations?limit=2",
        obs_source="awc",
    )
    assert source == "nws"
    assert len(obs) == 2
    assert nws_client.called_urls  # NWS was called


def test_awc_insufficient_obs_falls_back_to_nws(monkeypatch):
    """AWC returns <2 usable obs → NWS fallback is used."""
    import nws.awc_obs as awc_module

    single_obs = [
        (datetime.datetime(2026, 8, 17, 11, 5, tzinfo=datetime.timezone.utc), 22.0),
    ]

    monkeypatch.setattr(awc_module, "fetch_awc_obs", lambda *a, **k: single_obs)

    nws_client = _OkNWSClient(_nws_features_two())
    obs, source = fetch_obs_with_fallback(
        "KBOS",
        nws_client=nws_client,
        nws_url="https://api.weather.gov/stations/KBOS/observations?limit=2",
        obs_source="awc",
    )
    assert source == "nws"
    assert len(obs) == 2
    assert nws_client.called_urls


def test_awc_empty_obs_falls_back_to_nws(monkeypatch):
    """AWC returns empty list → NWS fallback is used."""
    import nws.awc_obs as awc_module

    monkeypatch.setattr(awc_module, "fetch_awc_obs", lambda *a, **k: [])

    nws_client = _OkNWSClient(_nws_features_two())
    obs, source = fetch_obs_with_fallback(
        "KBOS",
        nws_client=nws_client,
        nws_url="https://api.weather.gov/stations/KBOS/observations?limit=2",
        obs_source="awc",
    )
    assert source == "nws"


# ---------------------------------------------------------------------------
# Config parsing tests: SUNRISE_OBS_SOURCE
# ---------------------------------------------------------------------------

def test_config_sunrise_obs_source_default(monkeypatch):
    """Default SUNRISE_OBS_SOURCE is 'awc'."""
    from app.config import _parse_sunrise_obs_source
    assert _parse_sunrise_obs_source(None) == "awc"
    assert _parse_sunrise_obs_source("") == "awc"


def test_config_sunrise_obs_source_nws(monkeypatch):
    """Explicit 'nws' is accepted."""
    from app.config import _parse_sunrise_obs_source
    assert _parse_sunrise_obs_source("nws") == "nws"
    assert _parse_sunrise_obs_source("NWS") == "nws"


def test_config_sunrise_obs_source_awc(monkeypatch):
    """Explicit 'awc' is accepted."""
    from app.config import _parse_sunrise_obs_source
    assert _parse_sunrise_obs_source("awc") == "awc"
    assert _parse_sunrise_obs_source("AWC") == "awc"


def test_config_sunrise_obs_source_invalid_defaults_to_awc():
    """Invalid value → warning + default 'awc'."""
    from app.config import _parse_sunrise_obs_source
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        result = _parse_sunrise_obs_source("bad_value")

    assert result == "awc"
    assert any("sunrise_obs_source_invalid" in (e.get("event") or "") for e in logs)


def test_appconfig_sunrise_obs_source_env(monkeypatch):
    """AppConfig picks up SUNRISE_OBS_SOURCE from env var."""
    monkeypatch.setenv("SUNRISE_OBS_SOURCE", "nws")
    monkeypatch.setenv("ENTRY_GATE_MODE", "SUNRISE")
    from app.config import AppConfig

    cfg = AppConfig(
        kalshi_api_key="k",
        kalshi_private_key_path="p",
        mysql_database_url="mysql+mysqlconnector://localhost/t",
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
    assert cfg.sunrise_obs_source == "nws"
