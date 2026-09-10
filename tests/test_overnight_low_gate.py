"""Tests for core.overnight_low_gate.overnight_low_gate.

Uses synthetic NWS hourly periods (no network, no DB) to verify:
  - the LOW 'stays >= X' bracket blocks when the upcoming overnight forecast
    min is below bracket + margin,
  - it allows when the forecast min is safely above bracket + margin,
  - it fails open (never blocks) on HIGH tickers, unknown series, unparseable
    brackets, or when no future in-window periods exist.
"""
import datetime

from zoneinfo import ZoneInfo

from core.overnight_low_gate import overnight_low_gate

TZ_NY = ZoneInfo("America/New_York")  # EDT (UTC-4) in September


def _pkt(local_hour: int, temp_f: float, day: int = 9, month: int = 9, year: int = 2026) -> dict:
    t = datetime.datetime(year, month, day, local_hour, 0, tzinfo=TZ_NY)
    return {"startTime": t.isoformat(), "temperature": temp_f, "temperatureUnit": "F"}


# now_utc = 2026-09-10 02:00Z == 2026-09-09 22:00 EDT (same month/year context).
NOW_UTC = datetime.datetime(2026, 9, 10, 2, 0, tzinfo=datetime.timezone.utc)


def call(ticker, periods, **kw):
    kw.setdefault("now_utc", NOW_UTC)
    kw.setdefault("station_tz_override", TZ_NY)
    return overnight_low_gate(ticker, nws_client=None, periods=periods, **kw)


def test_blocks_when_overnight_forecast_dips_to_bracket():
    # B64 bracket: required = 64 + 1.0 = 65.  Forecast min 63 => below => block.
    periods = [_pkt(23, 64.0), _pkt(0, 63.0, day=10)]
    blocked, ctx = call("KXLOWTNYC-26SEP09-B64", periods, margin_f=1.0)
    assert blocked is True
    assert ctx["reason"] == "forecast_min_too_low"
    assert ctx["blocked"] is True
    assert ctx["bracket_temp_f"] == 64.0
    assert ctx["min_forecast_f"] == 63.0


def test_allows_when_overnight_forecast_min_is_comfortable():
    # Forecast min 66 >= 65 => safe (open).
    periods = [_pkt(23, 67.0), _pkt(0, 66.0, day=10)]
    blocked, ctx = call("KXLOWTNYC-26SEP09-B64", periods, margin_f=1.0)
    assert blocked is False
    assert ctx["reason"] == "safe"
    assert ctx["min_forecast_f"] == 66.0


def test_margin_edge_inclusive():
    # Forecast min == bracket + margin (65) is allowed (>= required).
    periods = [_pkt(0, 65.0, day=10)]
    blocked, _ = call("KXLOWTNYC-26SEP09-B64", periods, margin_f=1.0)
    assert blocked is False


def test_fails_open_for_high_ticker():
    blocked, ctx = call("KXHIGHTNY-26SEP09-T78", [])
    assert blocked is False
    assert ctx["reason"] == "not_low"


def test_fails_open_no_bracket_line():
    # KXLOW series with an unparseable/absent bracket temperature.
    blocked, ctx = call("KXLOWTNYC-26SEP09-XXX", [])
    assert blocked is False
    assert ctx["reason"] == "no_bracket_line"


def test_fails_open_when_no_future_in_window_periods():
    # All periods are before now (already outside the remaining overnight).
    periods = [_pkt(18, 60.0), _pkt(19, 59.0)]
    blocked, ctx = call("KXLOWTNYC-26SEP09-B60", periods)
    assert blocked is False
    assert ctx["reason"] == "no_future_periods"
    assert ctx["min_forecast_f"] is None


def test_ctx_payload_shape():
    periods = [_pkt(0, 63.0, day=10)]
    blocked, ctx = call("KXLOWTNYC-26SEP09-B64", periods)
    assert ctx["ticker"] == "KXLOWTNYC-26SEP09-B64"
    assert ctx["station_code"] == "KNYC"
    assert ctx["market_date"] == "2026-09-09"
    assert ctx["tz_name"] == "America/New_York"
    assert "required_min_f" in ctx
    assert blocked in (True, False)