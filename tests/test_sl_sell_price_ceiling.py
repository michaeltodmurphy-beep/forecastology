"""Regression tests for the stop-loss sell-price ceiling.

Background / incident
---------------------
A held position was sold at ~92¢ even though STOP_LOSS_PRICE_ASK was 0.62 and
the book bid was ~62.  Root cause: the aggressive fast-SL exit ladder
(``_run_fast_sl_exit``) priced its marketable SELL_YES off ``bracket.last_price``
-- the *last trade price* (92) -- rather than the live order book.  Because a
reactive stop-loss is submitted as an immediate_or_cancel reduce_only "ask",
that decoupled reference could transmit a sell far ABOVE the real bid.

These tests lock in the three-layer fix:

  1. ``_compute_fast_sl_exit_price`` can never return a price above its own
     reference (defense-in-depth).
  2. ``_run_fast_sl_exit`` sources its reference from the live best BID
     (falling back to the trigger), never from ``last_price``.
  3. ``_execute_stop_loss`` hard-clamps any SELL_YES whose price exceeds
     ``STOP_LOSS_PRICE_ASK`` and logs ``sl.exit_price_clamped_above_stop``.
"""

import datetime
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.test_state_machine import (  # noqa: E402
    FakeExecutor,
    InMemoryDB,
    capture_logs,
    make_strategy,
)
from core.types import MarketBracket, Phase  # noqa: E402
from execution.base import ExecutionResult  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Pure function: the ladder price can never exceed its reference.
# ---------------------------------------------------------------------------

def test_compute_fast_sl_exit_price_never_exceeds_reference(monkeypatch):
    strategy = make_strategy(
        monkeypatch,
        stop_loss_price=62,
        sl_exit_aggressive_offset_ticks=2,
        sl_exit_max_slippage=20,
    )
    # For every attempt, the computed price must be <= the reference.
    for attempt in (1, 2, 3, 5, 10):
        p = strategy._compute_fast_sl_exit_price(92, attempt)
        assert p <= 92, f"attempt={attempt} produced {p} > reference 92"
    # And with a 92 reference the first attempt is well BELOW 92 (offset applied).
    assert strategy._compute_fast_sl_exit_price(92, 1) < 92


def test_compute_fast_sl_exit_price_floor_respected(monkeypatch):
    strategy = make_strategy(
        monkeypatch,
        stop_loss_price=62,
        sl_exit_aggressive_offset_ticks=2,
        sl_exit_max_slippage=4,
    )
    # Reference 10, huge offset ladder -> must floor, never go below 1.
    p = strategy._compute_fast_sl_exit_price(10, 99)
    assert p >= 1
    assert p <= 10


# ---------------------------------------------------------------------------
# 2. End-to-end: a stop-loss at a 92 last_price / 62-ish bid must NOT sell at 92.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fast_sl_exit_uses_bid_not_last_price(monkeypatch):
    """The ladder reference must come from the cached best BID, not last_price."""
    ticker = "KXLOWTBOS-26JUN25-B65.5"
    executor = FakeExecutor()
    seen = {}

    async def fake_sell_yes(order):
        seen["price"] = order.price
        seen["qty"] = order.quantity
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="sell-id",
            notes="filled",
        )

    executor.sell_yes = fake_sell_yes
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=InMemoryDB(),
        stop_loss_price=62,
        sl_exit_mode="AGGRESSIVE_LIMIT",
        sl_exit_aggressive_offset_ticks=2,
        sl_exit_max_slippage=20,
        sl_exit_max_attempts=1,
        sl_exit_retry_interval_ms=0,
        enable_fast_sl_exit=True,
    )
    strategy._reconciliation_complete = True
    strategy._app_owned_qty[ticker] = 1

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="",
        series_ticker="",
        bracket_label="",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
    )
    bracket.position_quantity = 1
    # The stale last-trade print that caused the incident:
    bracket.last_price = 92
    strategy.active_positions[ticker] = bracket

    # Live book: bid 60, ask 62 (ask at/below stop -> legitimately triggered).
    strategy.cache.update_quote(ticker, 60, 62)

    await strategy._run_fast_sl_exit(
        bracket,
        trigger_price=62,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    assert "price" in seen, "no sell was submitted"
    # Must be priced off the BID (60) minus offset, NOT off last_price (92).
    assert seen["price"] <= 62, f"sell priced at {seen['price']} exceeded the stop threshold"
    assert seen["price"] < 92, "sell was (wrongly) priced off the stale last_price"


# ---------------------------------------------------------------------------
# 3. The hard ceiling guard in _execute_stop_loss.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_stop_loss_clamps_override_above_stop(monkeypatch):
    """An override_price above STOP_LOSS_PRICE_ASK must be clamped + logged."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN25-B66.5"
    executor = FakeExecutor()
    seen = {}

    async def fake_sell_yes(order):
        seen["price"] = order.price
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="sell-id",
            notes="filled",
        )

    executor.sell_yes = fake_sell_yes
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=InMemoryDB(),
        stop_loss_price=62,
        sl_exit_mode="AGGRESSIVE_LIMIT",
    )
    strategy._reconciliation_complete = True
    strategy._app_owned_qty[ticker] = 1

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="",
        series_ticker="",
        bracket_label="",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
    )
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket

    # A decoupled path hands us an absurd 92 override for a 62 stop.
    await strategy._execute_stop_loss(
        bracket,
        override_price=92,
        bypass_cooldown=True,
    )

    assert seen.get("price") == 62, (
        f"sell price {seen.get('price')} was not clamped to the stop threshold 62"
    )
    assert any(ev == "sl.exit_price_clamped_above_stop" for ev, _ in logged), (
        "expected sl.exit_price_clamped_above_stop to be logged"
    )


@pytest.mark.asyncio
async def test_execute_stop_loss_allows_override_at_or_below_stop(monkeypatch):
    """A legitimate override at/below the stop threshold passes through unchanged."""
    ticker = "KXLOWTBOS-26JUN25-B67.5"
    executor = FakeExecutor()
    seen = {}

    async def fake_sell_yes(order):
        seen["price"] = order.price
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="sell-id",
            notes="filled",
        )

    executor.sell_yes = fake_sell_yes
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=InMemoryDB(),
        stop_loss_price=62,
    )
    strategy._reconciliation_complete = True
    strategy._app_owned_qty[ticker] = 1

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="",
        series_ticker="",
        bracket_label="",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
    )
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket

    await strategy._execute_stop_loss(
        bracket,
        override_price=60,
        bypass_cooldown=True,
    )
    assert seen.get("price") == 60
