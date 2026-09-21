"""Tests for the observed-low bracket-reachability gate.

Two layers are covered:

  1. ``core.trade_outcome_utils.bracket_reachability_range`` -- the PURE helper
     that maps a KXLOW bracket to ``(kind, lo, hi)``.

  2. ``core.sunrise_gate.SunriseEntryGate.day_reached_bracket`` -- the gate
     method that combines the day's observed 5-min low (via
     ``day_has_dipped_below``) with that range and decides block / allow.

Ticker semantics here are VERIFIED against the live Kalshi markets API.  A real
KXLOWTBOS-26SEP22 event contains:

    B57.5 -> sub_title "57° to 58°"   (hard window 57-58)
    B55.5 -> sub_title "55° to 56°"   (hard window 55-56)
    B53.5 -> sub_title "53° to 54°"   (hard window 53-54)
    B51.5 -> sub_title "51° to 52°"   (hard window 51-52)
    T58   -> sub_title "59° or above" (top open end, EXEMPT)
    T51   -> sub_title "50° or below" (bottom open end, must reach <= 50)

No network, no DB: the day low is injected by stubbing ``day_has_dipped_below``.
"""
import datetime

from app.config import AppConfig
from core.sunrise_gate import SunriseEntryGate
from core.trade_outcome_utils import bracket_reachability_range


# The event's two open-ended ("T") lines: bottom 51 ("50 or below"), top 58
# ("59 or above").  This is what the state machine passes as ``sibling_lines``.
SIB_T_LINES = [51.0, 58.0]


# ---------------------------------------------------------------------------
# Layer 1: the pure range helper
# ---------------------------------------------------------------------------

def test_b_line_is_a_two_degree_window():
    assert bracket_reachability_range("KXLOWTBOS-26SEP22-B57.5", 57.5, SIB_T_LINES) == ("hard", 57.0, 58.0)
    assert bracket_reachability_range("KXLOWTBOS-26SEP22-B55.5", 55.5, SIB_T_LINES) == ("hard", 55.0, 56.0)
    assert bracket_reachability_range("KXLOWTBOS-26SEP22-B53.5", 53.5, SIB_T_LINES) == ("hard", 53.0, 54.0)
    assert bracket_reachability_range("KXLOWTBOS-26SEP22-B51.5", 51.5, SIB_T_LINES) == ("hard", 51.0, 52.0)


def test_top_t_is_exempt_above():
    kind, lo, hi = bracket_reachability_range("KXLOWTBOS-26SEP22-T58", 58.0, SIB_T_LINES)
    assert kind == "above"
    assert lo == 59.0
    assert hi == float("inf")


def test_bottom_t_is_the_smallest_t():
    kind, lo, hi = bracket_reachability_range("KXLOWTBOS-26SEP22-T51", 51.0, SIB_T_LINES)
    assert kind == "below"
    assert lo == float("-inf")
    assert hi == 50.0  # "50 or below"


def test_single_t_defaults_to_top_exempt():
    # With only one T line it is treated as the top (safe: top is exempt).
    kind, _, _ = bracket_reachability_range("KXLOWTBOS-26SEP22-T58", 58.0, [58.0])
    assert kind == "above"


def test_unparseable_ticker_returns_none():
    assert bracket_reachability_range("BADTICKER", 50.0) is None
    assert bracket_reachability_range("KXLOWTBOS-26SEP22-XXX", 50.0) is None


# ---------------------------------------------------------------------------
# Layer 2: the gate method, with the observed low injected
# ---------------------------------------------------------------------------

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
        sunrise_max_spread=4,
        midam_max_spread=4,
        pm_max_spread=4,
        stop_loss_price=35,
        entry_gate_mode="SUNRISE",
        sunrise_obs_source="nws",
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _gate_with_day_min(day_min_f):
    gate = SunriseEntryGate(_make_config())

    def _fake_dip(ticker, bracket_temp_f, now_utc=None):
        return False, {"below": False, "blocked": False, "day_min_f": day_min_f}

    gate.day_has_dipped_below = _fake_dip  # type: ignore[assignment]
    return gate


NOW = datetime.datetime(2026, 9, 22, 13, 0, tzinfo=datetime.timezone.utc)


def _blocked(ticker, bracket_temp_f, day_min_f):
    gate = _gate_with_day_min(day_min_f)
    blocked, _ctx = gate.day_reached_bracket(
        ticker, bracket_temp_f, now_utc=NOW, sibling_lines=SIB_T_LINES
    )
    return blocked


def test_observed_56_blocks_every_other_window():
    # Observed low = 56 F: only the 55-56 window is tradeable; all others are
    # unreachable (either too cold below, or never-reached the window).
    assert _blocked("KXLOWTBOS-26SEP22-B57.5", 57.5, 56.0) is True   # never reached (57-58)
    assert _blocked("KXLOWTBOS-26SEP22-B53.5", 53.5, 56.0) is True   # never reached (53-54)
    assert _blocked("KXLOWTBOS-26SEP22-B51.5", 51.5, 56.0) is True   # never reached (51-52)
    assert _blocked("KXLOWTBOS-26SEP22-T51", 51.0, 56.0) is True     # "50 or below" not reached


def test_observed_56_allows_its_window():
    assert _blocked("KXLOWTBOS-26SEP22-B55.5", 55.5, 56.0) is False


def test_observed_56_allows_top_open_end():
    assert _blocked("KXLOWTBOS-26SEP22-T58", 58.0, 56.0) is False


def test_window_boundaries_are_inclusive():
    # 55-56 window: 55 and 56 allowed; 54 and 57 blocked.
    assert _blocked("KXLOWTBOS-26SEP22-B55.5", 55.5, 55.0) is False
    assert _blocked("KXLOWTBOS-26SEP22-B55.5", 55.5, 56.0) is False
    assert _blocked("KXLOWTBOS-26SEP22-B55.5", 55.5, 54.0) is True
    assert _blocked("KXLOWTBOS-26SEP22-B55.5", 55.5, 57.0) is True


def test_bottom_open_end_reached_at_or_below_50():
    # "50 or below": 50 or colder reached it; 51 did not.
    assert _blocked("KXLOWTBOS-26SEP22-T51", 51.0, 50.0) is False
    assert _blocked("KXLOWTBOS-26SEP22-T51", 51.0, 45.0) is False
    assert _blocked("KXLOWTBOS-26SEP22-T51", 51.0, 51.0) is True


def test_fails_open_when_no_observation_yet():
    blocked, ctx = _gate_with_day_min(None).day_reached_bracket(
        "KXLOWTBOS-26SEP22-B55.5", 55.5, now_utc=NOW, sibling_lines=SIB_T_LINES
    )
    assert blocked is False
    assert ctx["day_min_f"] is None


def test_fails_open_for_high_ticker():
    blocked, _ctx = _gate_with_day_min(56.0).day_reached_bracket(
        "KXHIGHTNY-26SEP22-T78", 78.0, now_utc=NOW
    )
    assert blocked is False


def test_ctx_payload_shape_on_block():
    blocked, ctx = _gate_with_day_min(56.0).day_reached_bracket(
        "KXLOWTBOS-26SEP22-B51.5", 51.5, now_utc=NOW, sibling_lines=SIB_T_LINES
    )
    assert blocked is True
    assert ctx["kind"] == "hard"
    assert ctx["day_min_f"] == 56.0
    assert ctx["reason"] == "never_reached"
    assert ctx["range_lo"] == 51.0
    assert ctx["range_hi"] == 52.0
