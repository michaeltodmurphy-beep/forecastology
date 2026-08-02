"""
Tests for:
  Part 1 — Hedge buy cap enforcement (bug fix)
  Part 2 — Daily Low-ticker close-out at 22:00 ET
  Part 3 — Low-ticker entry halt after 22:00 ET
"""
import asyncio
import datetime
import os
import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config import AppConfig
from app.models import Position as PositionModel, StopLossLedger
from core.state_machine import (
    TemperatureStrategy,
    hedge_policy,
    is_low_entry_halted_et,
    is_past_closeout_time_et,
    parse_series_and_date,
)
from core.types import MarketBracket, OrderRequest, OrderSide, Phase
from data.ticker_cache import TickerCache
from execution.base import ExecutionResult
from execution.live import LiveTradeExecutor
from execution.paper import PaperTradeExecutor
from execution.factory import create_executor

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

# Import helpers from the main state machine test module
from tests.test_state_machine import (
    InMemoryDB,
    FakeExecutor,
    FakeWSManager,
    make_config,
    make_strategy,
    capture_logs,
)

_ET = ZoneInfo("America/New_York")
_UTC = datetime.timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_live_executor(max_buy_qty: Optional[int] = None) -> LiveTradeExecutor:
    """Create a LiveTradeExecutor with mocked key-loading."""
    import execution.live as live_mod
    # Patch load_private_key at module level so the constructor doesn't read a file.
    original = live_mod.load_private_key
    live_mod.load_private_key = lambda _: object()
    try:
        ex = LiveTradeExecutor(
            base_url="https://example.test",
            api_key="key",
            private_key_path="unused.pem",
            max_buy_qty=max_buy_qty,
        )
    finally:
        live_mod.load_private_key = original
    # Replace the HTTP client so no real requests are made.
    ex._client = None
    return ex


def _make_paper_executor(max_buy_qty: Optional[int] = None) -> PaperTradeExecutor:
    return PaperTradeExecutor(
        ticker_cache=TickerCache(),
        max_buy_qty=max_buy_qty,
    )


def _make_order(ticker: str = "KXLOWTLAX-26JUL30-B60.5", qty: int = 4) -> OrderRequest:
    return OrderRequest(ticker, OrderSide.BUY_YES, price=80, quantity=qty)


def _et(year: int, month: int, day: int, hour: int, minute: int) -> datetime.datetime:
    """Return a UTC datetime equivalent to the given ET wall-clock time."""
    naive = datetime.datetime(year, month, day, hour, minute)
    et_aware = naive.replace(tzinfo=_ET)
    return et_aware.astimezone(_UTC)


# ---------------------------------------------------------------------------
# Part 1 — Hedge Policy Math
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("count,exp_qty,exp_allowed,exp_max", [
    (0, 4, True,  8),   # initial entry: 4 contracts
    (1, 8, True,  8),   # first recovery: 8 contracts (= max)
    (2, 0, False, 8),   # count >= factor=2: BLOCKED
    (3, 0, False, 8),   # still blocked
    (9, 0, False, 8),   # any stale count is blocked
])
def test_hedge_policy_initial4_factor2_all_cases(count, exp_qty, exp_allowed, exp_max):
    """With initial=4, factor=2: max=8, counts 0 and 1 allowed, >=2 blocked."""
    qty, allowed, max_qty = hedge_policy(4, 2, count)
    assert max_qty == exp_max
    assert allowed is exp_allowed
    assert qty == exp_qty


# ---------------------------------------------------------------------------
# Part 1 — Executor-level hard cap (LiveTradeExecutor)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_executor_buy_yes_refuses_oversized_order(monkeypatch):
    """LiveTradeExecutor.buy_yes must refuse qty > max_buy_qty = 8."""
    critical_logged = []
    import execution.live as live_mod
    monkeypatch.setattr(live_mod.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    ex = _make_live_executor(max_buy_qty=8)
    order = _make_order(qty=16)

    result = await ex.buy_yes(order)

    assert result.success is False
    assert result.status == "REJECTED"
    assert result.fill_quantity == 0
    # Must have logged hedge.cap_blocked at CRITICAL
    assert any(ev == "hedge.cap_blocked" for ev, _ in critical_logged), (
        "Expected hedge.cap_blocked to be logged at CRITICAL"
    )
    cap_log = next(kw for ev, kw in critical_logged if ev == "hedge.cap_blocked")
    assert cap_log["proposed_qty"] == 16
    assert cap_log["max_allowed_qty"] == 8
    assert "executor_hard_cap" in cap_log["action"]


@pytest.mark.asyncio
async def test_live_executor_buy_yes_allows_at_cap(monkeypatch):
    """LiveTradeExecutor.buy_yes must allow qty == max_buy_qty without blocking."""
    critical_logged = []
    import execution.live as live_mod
    monkeypatch.setattr(live_mod.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))
    monkeypatch.setattr(live_mod, "build_auth_headers", lambda *_, **__: {})

    class _FakeResp:
        status_code = 201
        def json(self):
            return {
                "fill_count_fp": "8.00",
                "taker_fill_cost_dollars": "6.400000",
                "maker_fill_cost_dollars": "0.000000",
                "yes_price_dollars": "0.8000",
            }

    class _FakeClient:
        async def post(self, *_, **__):
            return _FakeResp()

    ex = _make_live_executor(max_buy_qty=8)
    ex._client = _FakeClient()
    order = _make_order(qty=8)

    result = await ex.buy_yes(order, max_price=90)

    # Must NOT have logged any cap block
    assert not any(ev == "hedge.cap_blocked" for ev, _ in critical_logged)
    assert result.success is True
    assert result.fill_quantity == 8


@pytest.mark.asyncio
async def test_live_executor_buy_yes_no_cap_set_passes_through(monkeypatch):
    """When max_buy_qty is None the executor applies no cap."""
    import execution.live as live_mod
    monkeypatch.setattr(live_mod, "build_auth_headers", lambda *_, **__: {})

    class _FakeResp:
        status_code = 201
        def json(self):
            return {
                "fill_count_fp": "16.00",
                "taker_fill_cost_dollars": "12.800000",
                "maker_fill_cost_dollars": "0.000000",
                "yes_price_dollars": "0.8000",
            }

    class _FakeClient:
        async def post(self, *_, **__):
            return _FakeResp()

    ex = _make_live_executor(max_buy_qty=None)
    ex._client = _FakeClient()
    order = _make_order(qty=16)
    result = await ex.buy_yes(order, max_price=90)
    assert result.success is True
    assert result.fill_quantity == 16


# ---------------------------------------------------------------------------
# Part 1 — Executor-level hard cap (PaperTradeExecutor)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_paper_executor_buy_yes_refuses_oversized_order(monkeypatch):
    """PaperTradeExecutor.buy_yes must refuse qty > max_buy_qty = 8."""
    critical_logged = []
    import execution.paper as paper_mod
    monkeypatch.setattr(paper_mod.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    ex = _make_paper_executor(max_buy_qty=8)
    order = _make_order(qty=9)

    result = await ex.buy_yes(order)

    assert result.success is False
    assert result.status == "REJECTED"
    assert any(ev == "hedge.cap_blocked" for ev, _ in critical_logged)


@pytest.mark.asyncio
async def test_paper_executor_blocks_when_existing_plus_proposed_exceeds_cap(monkeypatch):
    critical_logged = []
    import execution.paper as paper_mod
    monkeypatch.setattr(paper_mod.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    ex = _make_paper_executor(max_buy_qty=10)
    ex.positions["KXLOWTLAX-26JUL30-B60.5"] = {
        "market_ticker": "KXLOWTLAX-26JUL30-B60.5",
        "side": "yes",
        "quantity": 10,
        "avg_entry_price": 80,
    }
    order = _make_order(ticker="KXLOWTLAX-26JUL30-B60.5", qty=10)

    result = await ex.buy_yes(order)

    assert result.success is False
    assert result.status == "REJECTED"
    cap_log = next(kw for ev, kw in critical_logged if ev == "hedge.cap_blocked")
    assert cap_log["action"] == "executor_position_cap_blocked_total"


# ---------------------------------------------------------------------------
# Part 1 — Factory wires max_buy_qty
# ---------------------------------------------------------------------------

def test_create_executor_live_passes_max_buy_qty(monkeypatch):
    """create_executor passes max_buy_qty to LiveTradeExecutor."""
    import execution.live as live_mod
    monkeypatch.setattr(live_mod, "load_private_key", lambda _: object())
    ex = create_executor(
        trading_mode="LIVE",
        ticker_cache=TickerCache(),
        rest_base_url="https://example.test",
        api_key="key",
        private_key_path="unused.pem",
        max_buy_qty=8,
    )
    assert isinstance(ex, LiveTradeExecutor)
    assert ex.max_buy_qty == 8


def test_create_executor_paper_passes_max_buy_qty():
    """create_executor passes max_buy_qty to PaperTradeExecutor."""
    ex = create_executor(
        trading_mode="PAPER",
        ticker_cache=TickerCache(),
        rest_base_url="https://example.test",
        api_key="key",
        private_key_path="unused.pem",
        max_buy_qty=8,
    )
    assert isinstance(ex, PaperTradeExecutor)
    assert ex.max_buy_qty == 8


# ---------------------------------------------------------------------------
# Part 1 — monitor.py _buy_hedge hard cap
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_monitor_buy_hedge_refuses_oversized_order(monkeypatch):
    """monitor._buy_hedge must refuse qty > max_allowed_qty and log hedge.cap_blocked."""
    import monitor as mon

    critical_logged = []
    monkeypatch.setattr(mon.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    config = make_config(initial_contract_count=4, hedge_max_factor=2)  # max=8

    post_called = []

    class _FakeClient:
        async def post(self, *_, **__):
            post_called.append(True)
            return MagicMock(status_code=201)

    result = await mon._buy_hedge(
        ticker="KXLOWTLAX-26JUL30-B60.5",
        price_cents=80,
        qty=16,
        config=config,
        client=_FakeClient(),
    )

    assert result is False, "Expected _buy_hedge to return False when capped"
    assert not post_called, "No HTTP request should be sent when cap blocks the order"
    assert any(ev == "hedge.cap_blocked" for ev, _ in critical_logged)
    cap_log = next(kw for ev, kw in critical_logged if ev == "hedge.cap_blocked")
    assert cap_log["proposed_qty"] == 16
    assert cap_log["max_allowed_qty"] == 8
    assert cap_log["action"] == "monitor_buy_hedge_blocked"


@pytest.mark.asyncio
async def test_monitor_buy_hedge_allows_at_cap(monkeypatch):
    """monitor._buy_hedge must allow qty <= max_allowed_qty."""
    import monitor as mon

    critical_logged = []
    monkeypatch.setattr(mon.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))
    monkeypatch.setattr(mon, "load_private_key", lambda _: object())
    monkeypatch.setattr(mon, "build_auth_headers", lambda *_, **__: {})

    config = make_config(initial_contract_count=4, hedge_max_factor=2)

    class _FakeResp:
        status_code = 201

    class _FakeClient:
        async def post(self, *_, **__):
            return _FakeResp()

    result = await mon._buy_hedge(
        ticker="KXLOWTLAX-26JUL30-B60.5",
        price_cents=80,
        qty=8,
        config=config,
        client=_FakeClient(),
    )

    assert result is True
    assert not any(ev == "hedge.cap_blocked" for ev, _ in critical_logged)


@pytest.mark.asyncio
async def test_monitor_buy_hedge_blocks_when_existing_plus_proposed_exceeds_cap(monkeypatch):
    import monitor as mon

    critical_logged = []
    monkeypatch.setattr(mon.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    config = make_config(initial_contract_count=5, hedge_max_factor=2)  # max=10

    class _FakeClient:
        async def post(self, *_, **__):
            return MagicMock(status_code=201)

    result = await mon._buy_hedge(
        ticker="KXLOWTLAX-26JUL30-B60.5",
        price_cents=80,
        qty=10,
        config=config,
        client=_FakeClient(),
        existing_position_qty=10,
    )

    assert result is False
    cap_log = next(kw for ev, kw in critical_logged if ev == "hedge.cap_blocked")
    assert cap_log["total_position_qty"] == 20
    assert cap_log["max_allowed_qty"] == 10


# ---------------------------------------------------------------------------
# Part 1 — _execute_entry blocks directly-injected quantity=16
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_entry_blocks_injected_quantity_16(monkeypatch):
    """_execute_entry must block qty=16 when initial=4, factor=2 (max=8)."""
    critical_logged = []
    import core.state_machine as sm
    monkeypatch.setattr(sm.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    strategy = make_strategy(
        monkeypatch,
        initial_contract_count=4,
        hedge_max_factor=2,
    )
    bracket = MarketBracket(
        market_ticker="KXLOWTLAX-26JUL30-B60.5",
        event_ticker="KXLOWTLAX-26JUL30",
        series_ticker="KXLOWTLAX",
        bracket_label="B60.5",
        phase=Phase.MONITORING,
        falling_knife_guard=False,
    )
    # Inject a price so _execute_entry doesn't need to fetch
    from core.types import OrderBook, OrderBookLevel
    ob = OrderBook(
        yes_asks=[OrderBookLevel(price=82, quantity=10, order_count=1)],
        yes_bids=[OrderBookLevel(price=78, quantity=10, order_count=1)],
    )

    await strategy._execute_entry(bracket, ob=ob, quantity=16)

    # Bracket must have been reset to MONITORING (not HOLDING)
    assert bracket.phase == Phase.MONITORING
    # No buy order must have been submitted
    assert strategy.executor.orders == []
    # Critical cap-blocked log must exist
    assert any(ev == "hedge.cap_blocked" for ev, _ in critical_logged), (
        "Expected hedge.cap_blocked CRITICAL log"
    )


# ---------------------------------------------------------------------------
# Part 1 — Stale ledger regression: count=3 must NOT produce an order > 8
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_ledger_count3_does_not_produce_oversized_order(monkeypatch):
    """A StopLossLedger row with stop_loss_count=3 must be blocked for initial=4, factor=2."""
    import core.state_machine as sm
    critical_logged = []
    monkeypatch.setattr(sm.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    ticker = "KXLOWTLAX-26JUL30-B60.5"
    series, date_pfx = parse_series_and_date(ticker)

    # Seed a stale ledger row with count=3.
    stale_ledger = StopLossLedger(
        series_ticker=series,
        date_prefix=date_pfx,
        stop_loss_count=3,
        updated_at=datetime.datetime(2025, 1, 1),
    )
    db = InMemoryDB([stale_ledger])

    strategy = make_strategy(
        monkeypatch,
        db=db,
        initial_contract_count=4,
        hedge_max_factor=2,
    )

    # Simulate the watchlist entry evaluation path by calling _get_stop_loss_count
    # and then calling hedge_policy (as _evaluate_watchlist does).
    count = await strategy._get_stop_loss_count_for_market(ticker)
    assert count == 3

    hedge_max = strategy.config.hedge_max_factor  # 2
    next_qty, is_allowed, max_allowed_qty = hedge_policy(
        strategy.config.initial_contract_count, hedge_max, count
    )
    assert is_allowed is False, "count=3 >= factor=2 must be blocked by hedge_policy"
    assert next_qty == 0
    assert max_allowed_qty == 8

    # Even if someone were to call _execute_entry with qty=32 (4*2**3), it must be blocked.
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTLAX-26JUL30",
        series_ticker=series,
        bracket_label="B60.5",
        phase=Phase.MONITORING,
        falling_knife_guard=False,
    )
    from core.types import OrderBook, OrderBookLevel
    ob = OrderBook(
        yes_asks=[OrderBookLevel(price=82, quantity=10, order_count=1)],
        yes_bids=[OrderBookLevel(price=78, quantity=10, order_count=1)],
    )
    await strategy._execute_entry(bracket, ob=ob, quantity=32)

    assert bracket.phase == Phase.MONITORING
    assert strategy.executor.orders == []
    assert any(ev == "hedge.cap_blocked" for ev, _ in critical_logged)


# ---------------------------------------------------------------------------
# Part 1 — stop_loss_count clamp in _increment_stop_loss_count_for_market
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_increment_stop_loss_count_clamped_at_hedge_max_factor(monkeypatch):
    """_increment_stop_loss_count_for_market must clamp to hedge_max_factor."""
    import core.state_machine as sm
    warning_logged = []
    monkeypatch.setattr(sm.logger, "warning",
                        lambda ev, **kw: warning_logged.append((ev, kw)))

    ticker = "KXLOWTLAX-26JUL30-B60.5"
    series, date_pfx = parse_series_and_date(ticker)

    # Seed ledger at count=2 (= hedge_max_factor) — it should be clamped on next increment.
    stale = StopLossLedger(
        series_ticker=series,
        date_prefix=date_pfx,
        stop_loss_count=2,
        updated_at=datetime.datetime(2025, 1, 1),
    )
    db = InMemoryDB([stale])
    strategy = make_strategy(monkeypatch, db=db, initial_contract_count=4, hedge_max_factor=2)

    await strategy._increment_stop_loss_count_for_market(ticker)

    # Count must still be 2 (clamped), not 3.
    count_after = await strategy._get_stop_loss_count_for_market(ticker)
    assert count_after == 2, f"Expected count=2 (clamped), got {count_after}"
    # Clamp warning must have been logged.
    assert any(ev == "hedge.stop_loss_count_clamped" for ev, _ in warning_logged)


# ---------------------------------------------------------------------------
# Part 3 — is_low_entry_halted_et() time gate
# ---------------------------------------------------------------------------

def _make_et_entry_halt_config(**kw) -> AppConfig:
    cfg = make_config(**kw)
    cfg.low_ticker_entry_halt_enabled = kw.get("low_ticker_entry_halt_enabled", True)
    cfg.low_ticker_entry_halt_time_et = kw.get("low_ticker_entry_halt_time_et", "22:00")
    return cfg


@pytest.mark.parametrize("h,m,expected_halted", [
    (21, 59, False),   # one minute before halt: NOT halted
    (22,  0, True),    # exactly at halt time: HALTED
    (22,  1, True),    # just after: HALTED
    (23, 59, True),    # late night: HALTED
])
def test_low_entry_halted_et_basic(h, m, expected_halted):
    """is_low_entry_halted_et returns correct result based on ET wall-clock time."""
    config = _make_et_entry_halt_config()
    # Use a known non-DST date (January 15 2025, UTC-5)
    now_utc = _et(2025, 1, 15, h, m)
    halted, ctx = is_low_entry_halted_et(config, now_utc=now_utc)
    assert halted is expected_halted
    if expected_halted:
        assert "now_et" in ctx
        assert "halt_time_et" in ctx


def test_low_entry_halted_et_disabled():
    """When feature is disabled, gate always returns False."""
    config = _make_et_entry_halt_config(low_ticker_entry_halt_enabled=False)
    now_utc = _et(2025, 1, 15, 23, 0)
    halted, ctx = is_low_entry_halted_et(config, now_utc=now_utc)
    assert halted is False
    assert ctx == {}


def test_low_entry_halted_et_dst_transition_spring_forward():
    """DST spring-forward: 22:00 ET = 03:00 UTC (EDT = UTC-4).

    On 2025-03-09, clocks spring forward at 02:00 EST -> 03:00 EDT.
    After the transition 22:00 EDT = 02:00 UTC.
    """
    config = _make_et_entry_halt_config()
    # 2025-03-09 22:00 EDT = 2025-03-10 02:00 UTC
    now_utc = datetime.datetime(2025, 3, 10, 2, 0, tzinfo=_UTC)
    halted, _ = is_low_entry_halted_et(config, now_utc=now_utc)
    assert halted is True

    # 2025-03-09 21:59 EDT = 2025-03-10 01:59 UTC
    now_utc_before = datetime.datetime(2025, 3, 10, 1, 59, tzinfo=_UTC)
    halted_before, _ = is_low_entry_halted_et(config, now_utc=now_utc_before)
    assert halted_before is False


def test_low_entry_halted_et_dst_transition_fall_back():
    """DST fall-back: 22:00 ET = 03:00 UTC (EST = UTC-5).

    On 2025-11-02, clocks fall back at 02:00 EDT -> 01:00 EST.
    22:00 EST = 03:00 UTC.
    """
    config = _make_et_entry_halt_config()
    # 2025-11-02 22:00 EST = 2025-11-03 03:00 UTC
    now_utc = datetime.datetime(2025, 11, 3, 3, 0, tzinfo=_UTC)
    halted, _ = is_low_entry_halted_et(config, now_utc=now_utc)
    assert halted is True

    # 2025-11-02 21:59 EST = 2025-11-03 02:59 UTC
    now_utc_before = datetime.datetime(2025, 11, 3, 2, 59, tzinfo=_UTC)
    halted_before, _ = is_low_entry_halted_et(config, now_utc=now_utc_before)
    assert halted_before is False


# ---------------------------------------------------------------------------
# Part 3 — _evaluate_watchlist ET gate integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("yes_ask, expected_blocked", [
    (92, True),
    (93, False),
    (94, False),
])
async def test_evaluate_watchlist_low_entry_halt_threshold_after_2200_et(monkeypatch, yes_ask, expected_blocked):
    """After 22:00 ET, Low halt applies only when ask is strictly below threshold."""
    import core.state_machine as sm

    info_logged = []
    monkeypatch.setattr(sm.logger, "info",
                        lambda ev, **kw: info_logged.append((ev, kw)))

    strategy = make_strategy(
        monkeypatch,
        initial_contract_count=4,
        hedge_max_factor=2,
    )
    strategy.executor.buy_success = True
    strategy.config.low_ticker_entry_halt_enabled = True
    strategy.config.low_ticker_entry_halt_time_et = "22:00"
    strategy.config.low_ticker_10pm_max_ask = 93
    strategy.config.spread_monitor_price = 99

    ticker = "KXLOWTLAX-26JUL30-B60.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTLAX-26JUL30",
        series_ticker="KXLOWTLAX",
        bracket_label="B60.5",
        phase=Phase.MONITORING,
        falling_knife_guard=False,
        crossed_buy=False,
    )
    strategy.brackets[ticker] = bracket

    # Simulate post-22:00 ET time by patching is_low_entry_halted_et.
    monkeypatch.setattr(sm, "is_low_entry_halted_et",
                        lambda cfg, **__: (True, {"now_et": "22:01:00", "halt_time_et": "22:00"}))

    # Feed a price that would normally trigger a buy.
    strategy.cache.update_quote(ticker, yes_bid=90, yes_ask=yes_ask)

    await strategy._evaluate_watchlist()

    if expected_blocked:
        assert strategy.executor.orders == []
        assert any(ev == "entry.blocked_low_after_2200_et" for ev, _ in info_logged), (
            "Expected entry.blocked_low_after_2200_et to be logged"
        )
    else:
        assert len(strategy.executor.orders) == 1
        assert not any(ev == "entry.blocked_low_after_2200_et" for ev, _ in info_logged)


@pytest.mark.asyncio
async def test_evaluate_watchlist_allows_high_entry_after_2200_et(monkeypatch):
    """After 22:00 ET, a KXHIGH bracket must still be entered normally."""
    import core.state_machine as sm

    strategy = make_strategy(
        monkeypatch,
        initial_contract_count=4,
        hedge_max_factor=2,
    )
    strategy.config.low_ticker_entry_halt_enabled = True
    strategy.config.low_ticker_entry_halt_time_et = "22:00"
    strategy.executor.buy_success = True

    ticker = "KXHIGHLAX-26JUL30-T90"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHLAX-26JUL30",
        series_ticker="KXHIGHLAX",
        bracket_label="T90",
        phase=Phase.MONITORING,
        falling_knife_guard=False,
        crossed_buy=False,
    )
    strategy.brackets[ticker] = bracket

    # Simulate post-22:00 ET, but KXHIGH should be unaffected.
    monkeypatch.setattr(sm, "is_low_entry_halted_et",
                        lambda cfg, **__: (True, {"now_et": "22:01:00", "halt_time_et": "22:00"}))

    strategy.cache.update_quote(ticker, yes_bid=80, yes_ask=83)

    await strategy._evaluate_watchlist()

    # A buy order should have been placed for the High ticker.
    assert len(strategy.executor.orders) == 1
    placed_ticker = strategy.executor.orders[0][0].market_ticker
    assert "KXHIGH" in placed_ticker


# ---------------------------------------------------------------------------
# Part 2 — is_past_closeout_time_et helper
# ---------------------------------------------------------------------------

def _make_closeout_config(**kw) -> AppConfig:
    cfg = make_config(**kw)
    cfg.low_ticker_daily_closeout_enabled = kw.get("low_ticker_daily_closeout_enabled", True)
    cfg.low_ticker_closeout_time_et = kw.get("low_ticker_closeout_time_et", "22:00")
    cfg.low_ticker_closeout_on_late_start = kw.get("low_ticker_closeout_on_late_start", True)
    return cfg


@pytest.mark.parametrize("h,m,expected_past", [
    (21, 59, False),
    (22,  0, True),
    (22,  1, True),
])
def test_is_past_closeout_time_et(h, m, expected_past):
    config = _make_closeout_config()
    now_utc = _et(2025, 1, 15, h, m)
    past, et_date = is_past_closeout_time_et(config, now_utc=now_utc)
    assert past is expected_past
    assert et_date == datetime.date(2025, 1, 15)


# ---------------------------------------------------------------------------
# Part 2 — _run_low_ticker_closeout flattens only KXLOW positions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_low_ticker_closeout_flattens_only_low(monkeypatch):
    """_run_low_ticker_closeout exits KXLOW positions and leaves KXHIGH untouched."""
    import core.state_machine as sm

    info_logged = []
    monkeypatch.setattr(sm.logger, "info",
                        lambda ev, **kw: info_logged.append((ev, kw)))

    strategy = make_strategy(monkeypatch)
    strategy._reconciliation_complete = True

    low_ticker = "KXLOWTLAX-26JUL30-B60.5"
    high_ticker = "KXHIGHLAX-26JUL30-T90"

    low_bracket = MarketBracket(
        market_ticker=low_ticker,
        event_ticker="KXLOWTLAX-26JUL30",
        series_ticker="KXLOWTLAX",
        bracket_label="B60.5",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
        crossed_buy=True,
        position_quantity=4,
    )
    high_bracket = MarketBracket(
        market_ticker=high_ticker,
        event_ticker="KXHIGHLAX-26JUL30",
        series_ticker="KXHIGHLAX",
        bracket_label="T90",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
        crossed_buy=True,
        position_quantity=4,
    )
    strategy.active_positions[low_ticker] = low_bracket
    strategy.active_positions[high_ticker] = high_bracket
    strategy.brackets[low_ticker] = low_bracket
    strategy.brackets[high_ticker] = high_bracket
    strategy._app_owned_qty[low_ticker] = 4
    strategy._app_owned_qty[high_ticker] = 4
    strategy.cache.update_quote(low_ticker, yes_bid=80, yes_ask=92)
    strategy.cache.update_quote(high_ticker, yes_bid=80, yes_ask=95)

    stop_loss_calls = []

    async def _fake_execute_stop_loss(bracket, **kw):
        stop_loss_calls.append(bracket.market_ticker)
        return False

    monkeypatch.setattr(strategy, "_execute_stop_loss", _fake_execute_stop_loss)

    await strategy._run_low_ticker_closeout()

    assert low_ticker in stop_loss_calls, "KXLOW position must be closed out"
    assert high_ticker not in stop_loss_calls, "KXHIGH position must be untouched"

    complete_logs = [kw for ev, kw in info_logged if ev == "lowticker.daily_closeout_complete"]
    assert complete_logs, "lowticker.daily_closeout_complete must be logged"


@pytest.mark.asyncio
async def test_run_low_ticker_closeout_respects_manage_external_false(monkeypatch):
    """_execute_stop_loss is called (ownership/idempotency is its responsibility)."""
    import core.state_machine as sm

    strategy = make_strategy(monkeypatch, manage_external_positions=False)
    strategy._reconciliation_complete = True

    low_ticker = "KXLOWTLAX-26JUL30-B60.5"
    low_bracket = MarketBracket(
        market_ticker=low_ticker,
        event_ticker="KXLOWTLAX-26JUL30",
        series_ticker="KXLOWTLAX",
        bracket_label="B60.5",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
        crossed_buy=True,
        position_quantity=4,
    )
    strategy.active_positions[low_ticker] = low_bracket
    strategy.brackets[low_ticker] = low_bracket
    # No app-owned qty — ownership guard in _execute_stop_loss will skip it.
    strategy._app_owned_qty[low_ticker] = 0
    strategy.cache.update_quote(low_ticker, yes_bid=80, yes_ask=92)

    stop_loss_calls = []

    async def _fake_execute_stop_loss(bracket, **kw):
        stop_loss_calls.append(bracket.market_ticker)
        return False

    monkeypatch.setattr(strategy, "_execute_stop_loss", _fake_execute_stop_loss)

    await strategy._run_low_ticker_closeout()

    # _execute_stop_loss is invoked; it will skip internally due to manage_external=False.
    assert low_ticker in stop_loss_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("yes_ask, expected_closed", [
    (92, True),
    (93, False),
    (94, False),
])
async def test_run_low_ticker_closeout_applies_only_below_ask_threshold(monkeypatch, yes_ask, expected_closed):
    strategy = make_strategy(monkeypatch)
    strategy._reconciliation_complete = True
    strategy.config.low_ticker_10pm_max_ask = 93

    low_ticker = "KXLOWTLAX-26JUL30-B60.5"
    low_bracket = MarketBracket(
        market_ticker=low_ticker,
        event_ticker="KXLOWTLAX-26JUL30",
        series_ticker="KXLOWTLAX",
        bracket_label="B60.5",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
        crossed_buy=True,
        position_quantity=4,
    )
    strategy.active_positions[low_ticker] = low_bracket
    strategy.brackets[low_ticker] = low_bracket
    strategy._app_owned_qty[low_ticker] = 4
    strategy.cache.update_quote(low_ticker, yes_bid=80, yes_ask=yes_ask)

    stop_loss_calls = []

    async def _fake_execute_stop_loss(bracket, **kw):
        stop_loss_calls.append(bracket.market_ticker)
        return False

    monkeypatch.setattr(strategy, "_execute_stop_loss", _fake_execute_stop_loss)

    await strategy._run_low_ticker_closeout()

    if expected_closed:
        assert low_ticker in stop_loss_calls
    else:
        assert low_ticker not in stop_loss_calls


# ---------------------------------------------------------------------------
# Part 2 — Idempotency: close-out does not repeat on same ET day
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_low_ticker_closeout_loop_idempotent(monkeypatch):
    """The close-out loop runs exactly once per ET day even if called multiple times."""
    import core.state_machine as sm

    strategy = make_strategy(monkeypatch)
    strategy._running = True
    strategy._reconciliation_complete = True
    strategy.config.low_ticker_daily_closeout_enabled = True
    strategy.config.low_ticker_closeout_time_et = "22:00"
    strategy.config.low_ticker_closeout_on_late_start = True

    run_count = []

    async def _fake_run_closeout():
        run_count.append(1)

    monkeypatch.setattr(strategy, "_run_low_ticker_closeout", _fake_run_closeout)

    # Patch is_past_closeout_time_et to always return True for today.
    today_et = datetime.date(2025, 7, 30)

    def _fake_past_closeout(cfg, now_utc=None):
        return True, today_et

    monkeypatch.setattr(sm, "is_past_closeout_time_et", _fake_past_closeout)

    # Also patch asyncio.sleep so the loop runs immediately.
    sleep_calls = []

    async def _fast_sleep(n):
        sleep_calls.append(n)
        if len(sleep_calls) >= 3:
            # Stop the loop after 3 iterations.
            strategy._running = False

    monkeypatch.setattr(sm.asyncio, "sleep", _fast_sleep)

    await strategy._low_ticker_closeout_loop()

    # close-out must have run exactly once despite multiple loop iterations.
    assert len(run_count) == 1, (
        f"Expected 1 close-out run but got {len(run_count)}"
    )


# ---------------------------------------------------------------------------
# Part 2 — Close-out loop is ET-based and unaffected by local timezone
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_low_ticker_closeout_loop_fires_at_et_time_only(monkeypatch):
    """Close-out fires based on ET time regardless of the ticker's local timezone.

    A Pacific-time KXLOW ticker (22:00 ET = 19:00 PT) must be included in the
    close-out that fires at 22:00 ET — the local-time gate for that ticker's
    city (01:00 PT = 04:00 ET) plays no role in the close-out timing.

    Sequence: before 22:00 ET → not fired; at 22:01 ET → fires exactly once;
    same ET day → idempotent, does not fire again.
    """
    import core.state_machine as sm

    strategy = make_strategy(monkeypatch)
    strategy._running = True
    strategy._reconciliation_complete = True
    strategy.config.low_ticker_daily_closeout_enabled = True
    strategy.config.low_ticker_closeout_time_et = "22:00"
    # Use True so the close-out fires on the first crossing (not treated as a skip).
    strategy.config.low_ticker_closeout_on_late_start = True

    run_count = []

    async def _fake_run_closeout():
        run_count.append(1)

    monkeypatch.setattr(strategy, "_run_low_ticker_closeout", _fake_run_closeout)

    sleep_calls = []

    # Sequence: before 22:00 ET → NOT fired; at 22:01 ET → FIRED; same day → idempotent.
    _before_22 = _et(2025, 7, 4, 21, 59)
    _after_22  = _et(2025, 7, 4, 22, 1)
    _still_22  = _et(2025, 7, 4, 23, 0)

    _et_times_iter = iter([_before_22, _after_22, _still_22])

    def _fake_past_closeout(cfg, now_utc=None):
        t = next(_et_times_iter, _still_22)
        et_tz = ZoneInfo("America/New_York")
        now_et = t.astimezone(et_tz)
        closeout_wall = datetime.time(22, 0)
        past = now_et.time() >= closeout_wall
        return past, now_et.date()

    monkeypatch.setattr(sm, "is_past_closeout_time_et", _fake_past_closeout)

    async def _fast_sleep(n):
        sleep_calls.append(n)
        if len(sleep_calls) >= 3:
            strategy._running = False

    monkeypatch.setattr(sm.asyncio, "sleep", _fast_sleep)

    await strategy._low_ticker_closeout_loop()

    assert len(run_count) == 1, (
        f"Close-out should fire exactly once at/after 22:00 ET, got {len(run_count)}"
    )


@pytest.mark.asyncio
async def test_evaluate_watchlist_high_ticker_not_blocked_by_local_gate(monkeypatch):
    """KXHIGH* tickers must proceed through _evaluate_watchlist even when the
    city-local-time settle gate would have blocked them (if applied).

    The local settle gate is only called for is_low=True tickers; KXHIGH tickers
    bypass it entirely.
    """
    import core.state_machine as sm
    from core.local_time_gate import is_entry_allowed

    info_logged = []
    monkeypatch.setattr(sm.logger, "info",
                        lambda ev, **kw: info_logged.append((ev, kw)))

    strategy = make_strategy(
        monkeypatch,
        initial_contract_count=4,
        hedge_max_factor=2,
    )
    strategy.config.low_ticker_entry_halt_enabled = False
    strategy.config.enable_local_settle_gate = True
    strategy.config.default_entry_start_local = "01:00"
    strategy.executor.buy_success = True

    ticker = "KXHIGHNY-26JUL30-T95"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHNY-26JUL30",
        series_ticker="KXHIGHNY",
        bracket_label="T95",
        phase=Phase.MONITORING,
        falling_knife_guard=False,
        crossed_buy=False,
    )
    strategy.brackets[ticker] = bracket

    # Confirm is_entry_allowed itself would block at this UTC time
    # (00:30 ET = 04:30 UTC summer, below 01:00 threshold).
    now_utc = _et(2025, 7, 4, 0, 30)  # 00:30 ET summer
    gate_ok, _ = is_entry_allowed(ticker, strategy.config, now_utc=now_utc)
    assert gate_ok is False, "Precondition: function itself would block at this time"

    # Feed a qualifying price and evaluate; the settle gate must NOT fire for HIGH.
    strategy.cache.update_quote(ticker, yes_bid=80, yes_ask=83)

    await strategy._evaluate_watchlist()

    # entry.blocked_local_settle_gate must NOT appear for a KXHIGH ticker.
    blocked_settle = [ev for ev, kw in info_logged
                      if ev == "entry.blocked_local_settle_gate"
                      and "KXHIGH" in kw.get("ticker", "")]
    assert not blocked_settle, (
        "KXHIGH ticker must never be blocked by the local settle gate"
    )
