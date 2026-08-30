"""Tests for the resting take-profit sell feature.

Covers:
  1. Config: profit_take_sell_enabled and profit_take_sell_price parsing.
  2. ProfitTakeSellManager: place / cancel / check_fill lifecycle with a mocked
     executor.
  3. Integration: state machine honours cancel-before-sell ordering when the
     take-profit order is enabled (it is cancelled alongside the SL backstop
     before any reactive sell).
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# 1. Config parsing
# ---------------------------------------------------------------------------


class TestProfitTakeConfig:
    def setup_method(self):
        for key in ("PROFIT_TAKE_SELL_ENABLED", "PROFIT_TAKE_SELL_PRICE"):
            os.environ.pop(key, None)

    def teardown_method(self):
        for key in ("PROFIT_TAKE_SELL_ENABLED", "PROFIT_TAKE_SELL_PRICE"):
            os.environ.pop(key, None)

    def _cfg(self, **kwargs):
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig

        defaults = dict(
            kalshi_api_key="k",
            kalshi_private_key_path="k.pem",
            mysql_database_url="******localhost:3306/test",
            trading_mode="PAPER",
            initial_contract_count=1,
            monitor_start_price=80,
            buy_trigger_price_low=82,
            buy_trigger_price_high=82,
            spread_monitor_price=90,
            minimum_spread=4,
            stop_loss_price=35,
            no_trade_tickers=set(),
        )
        defaults.update(kwargs)
        return AppConfig(**defaults)

    def test_defaults_disabled_and_99_cents(self):
        cfg = self._cfg()
        assert cfg.profit_take_sell_enabled is False
        assert cfg.profit_take_sell_price == 99

    def test_enabled_via_direct_construct(self):
        cfg = self._cfg(profit_take_sell_enabled=True)
        assert cfg.profit_take_sell_enabled is True

    def test_price_dollars_converted_to_cents(self):
        """Dollar string '0.50' must be converted to 50¢ by the validator."""
        cfg = self._cfg(profit_take_sell_price="0.50")
        assert cfg.profit_take_sell_price == 50

    def test_price_direct_int_is_already_cents(self):
        cfg = self._cfg(profit_take_sell_enabled=True, profit_take_sell_price=95)
        assert cfg.profit_take_sell_enabled is True
        assert cfg.profit_take_sell_price == 95


# ---------------------------------------------------------------------------
# 2. ProfitTakeSellManager unit tests
# ---------------------------------------------------------------------------


class _FakeExecutor:
    """Minimal executor stub for ProfitTakeSellManager tests."""

    def __init__(self):
        self.placed: list[dict] = []
        self.cancelled: list[str] = []
        self.statuses: dict[str, str] = {}  # order_id → status

    async def place_limit_sell(self, order):
        from execution.base import ExecutionResult

        order_id = f"ord_{len(self.placed)}"
        self.placed.append(
            {"ticker": order.market_ticker, "price": order.price, "qty": order.quantity,
             "order_id": order_id, "client_order_id": order.client_order_id}
        )
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            order_id=order_id,
            status="RESTING",
        )

    async def cancel_order(self, order_id: str, market_ticker: str = "") -> bool:
        self.cancelled.append(order_id)
        return True

    async def get_order_status(self, order_id: str):
        return self.statuses.get(order_id)

    async def list_open_sell_orders(self, ticker: str) -> list:
        return []

    async def cancel_open_sell_orders(self, ticker: str, client_prefix: str = "") -> int:
        return 0


def _mgr(exec_, enabled=True, price=99, mode="LIVE"):
    from execution.profit_take_sell import ProfitTakeSellManager

    return ProfitTakeSellManager(
        executor=exec_,
        profit_take_sell_enabled=enabled,
        profit_take_sell_price=price,
        trading_mode=mode,
    )


@pytest.mark.asyncio
async def test_place_stores_order_id_at_high_price():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    order_id = await mgr.place("TICK", 3)
    assert order_id is not None
    assert mgr.get_order_id("TICK") == order_id
    assert len(exec_.placed) == 1
    assert exec_.placed[0]["price"] == 99  # resting take-profit price


@pytest.mark.asyncio
async def test_place_custom_price():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_, price=98)
    await mgr.place("TICK", 2)
    assert exec_.placed[0]["price"] == 98


@pytest.mark.asyncio
async def test_cancel_clears_order_id():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    await mgr.place("TICK", 2)
    assert mgr.get_order_id("TICK") is not None

    ok = await mgr.cancel("TICK", reason="exit")
    assert ok is True
    assert mgr.get_order_id("TICK") is None
    assert len(exec_.cancelled) == 1


@pytest.mark.asyncio
async def test_cancel_when_no_order_returns_true():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    ok = await mgr.cancel("MISSING_TICK")
    assert ok is True
    assert exec_.cancelled == []


@pytest.mark.asyncio
async def test_replace_on_second_place():
    """A second place call cancels the first order before placing a new one."""
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    first_id = await mgr.place("TICK", 2)
    second_id = await mgr.place("TICK", 3)
    assert first_id is not None
    assert second_id is not None
    assert first_id != second_id
    assert first_id in exec_.cancelled
    assert mgr.get_order_id("TICK") == second_id


@pytest.mark.asyncio
async def test_check_fill_detects_filled_order():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    order_id = await mgr.place("TICK", 1)
    exec_.statuses[order_id] = "filled"

    filled = await mgr.check_fill("TICK")
    assert filled is True
    assert mgr.get_order_id("TICK") is None  # cleared on fill detection


@pytest.mark.asyncio
async def test_check_fill_returns_false_for_resting():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    order_id = await mgr.place("TICK", 1)
    exec_.statuses[order_id] = "resting"

    filled = await mgr.check_fill("TICK")
    assert filled is False
    assert mgr.get_order_id("TICK") == order_id  # still active


@pytest.mark.asyncio
async def test_disabled_is_noop():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_, enabled=False)
    order_id = await mgr.place("TICK", 3)
    assert order_id is None
    assert exec_.placed == []

    ok = await mgr.cancel("TICK")
    assert ok is True  # no-op returns True (safe to proceed with sell)


@pytest.mark.asyncio
async def test_paper_mode_is_noop():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_, mode="PAPER")
    order_id = await mgr.place("TICK", 3)
    assert order_id is None
    assert exec_.placed == []


@pytest.mark.asyncio
async def test_set_and_get_order_id():
    exec_ = _FakeExecutor()
    mgr = _mgr(exec_)
    mgr.set_order_id("TICK", "existing_order_123")
    assert mgr.get_order_id("TICK") == "existing_order_123"

    # Placing cancels the existing order first.
    new_id = await mgr.place("TICK", 2)
    assert "existing_order_123" in exec_.cancelled
    assert new_id != "existing_order_123"


# ---------------------------------------------------------------------------
# 3. State machine integration: cancel-before-sell ordering
# ---------------------------------------------------------------------------

# The profit-take order is cancelled via _cancel_sl_backstop (which now also
# cancels the take-profit order).  We verify that when a ProfitTakeSellManager
# is wired onto the strategy, its cancel is awaited before the reactive sell.


def _make_config(**kwargs):
    from app.config import AppConfig

    defaults = dict(
        kalshi_api_key="k",
        kalshi_private_key_path="k.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="LIVE",
        initial_contract_count=1,
        monitor_start_price=80,
        buy_trigger_price_low=82,
        buy_trigger_price_high=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=35,
        no_trade_tickers=set(),
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        profit_take_sell_enabled=True,
        profit_take_sell_price=99,
    )
    defaults.update(kwargs)
    return AppConfig(**defaults)


@pytest.mark.asyncio
async def test_cancel_profit_take_called_before_sell(monkeypatch):
    """When profit_take_sell_enabled=True, its order must be cancelled before
    executor.sell_yes is called inside _execute_stop_loss."""
    from execution.profit_take_sell import ProfitTakeSellManager

    call_order: list[str] = []

    class _TrackingExecutor:
        async def sell_yes(self, order):
            call_order.append("sell_yes")
            from execution.base import ExecutionResult
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=1,
                fill_quantity=order.quantity,
                total_cost_cents=-order.quantity,
                order_id="ord_1",
                status="FILLED",
            )

        async def place_limit_sell(self, order):
            from execution.base import ExecutionResult
            return ExecutionResult(
                success=True, market_ticker=order.market_ticker, side="yes",
                price=order.price, quantity=order.quantity, fill_price=0,
                fill_quantity=0, total_cost_cents=0, order_id="pts_1", status="RESTING",
            )

        async def cancel_order(self, order_id, market_ticker=""):
            call_order.append(f"cancel:{order_id}")
            return True

        async def list_open_sell_orders(self, ticker: str):
            return []

        async def cancel_open_sell_orders(self, ticker: str, client_prefix: str = "") -> int:
            return 0

        async def get_order_status(self, order_id):
            return "resting"

    class _FakeSessionContext:
        async def __aenter__(self):
            return _FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeDb:
        async def get_session(self):
            return _FakeSessionContext()

    class _FakeSession:
        async def execute(self, *a, **kw):
            return _FakeResult()

        async def commit(self):
            pass

        async def rollback(self):
            pass

        def add(self, obj):
            pass

    class _FakeResult:
        def scalar_one_or_none(self):
            return None

        def scalars(self):
            return self

        def all(self):
            return []

    exec_ = _TrackingExecutor()
    mgr = ProfitTakeSellManager(
        executor=exec_,
        profit_take_sell_enabled=True,
        profit_take_sell_price=99,
        trading_mode="LIVE",
    )
    mgr.set_order_id("TICKER", "pts_1")

    original_cancel = mgr.cancel

    async def _tracking_cancel(ticker, *, reason="exit"):
        call_order.append(f"cancel_profit_take:{ticker}")
        return await original_cancel(ticker, reason=reason)

    mgr.cancel = _tracking_cancel

    from core import state_machine
    from core.state_machine import TemperatureStrategy
    from core.types import Phase, MarketBracket
    from unittest.mock import MagicMock

    cfg = _make_config()
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())
    strategy = TemperatureStrategy(
        config=cfg,
        cache=MagicMock(),
        ws_manager=MagicMock(),
        executor=exec_,
        db=_FakeDb(),
    )
    strategy.profit_take = mgr
    strategy._reconciliation_complete = True
    strategy._app_owned_qty["TICKER"] = 2

    bracket = MarketBracket(
        market_ticker="TICKER",
        event_ticker="",
        series_ticker="",
        bracket_label="",
        phase=Phase.HOLDING,
        falling_knife_guard=False,
    )
    bracket.position_quantity = 2
    strategy.active_positions["TICKER"] = bracket

    await strategy._execute_stop_loss(bracket, override_price=1, bypass_cooldown=True)

    cancel_idx = next(
        (i for i, x in enumerate(call_order) if "cancel_profit_take" in x), None
    )
    sell_idx = next(
        (i for i, x in enumerate(call_order) if x == "sell_yes"), None
    )
    assert cancel_idx is not None, "cancel_profit_take was never called"
    assert sell_idx is not None, "sell_yes was never called"
    assert cancel_idx < sell_idx, (
        f"cancel_profit_take({cancel_idx}) must precede sell_yes({sell_idx}); "
        f"call_order={call_order}"
    )


@pytest.mark.asyncio
async def test_profit_take_client_order_ids_are_unique_across_placements():
    """Each profit-take placement gets a unique client_order_id with the
    APP_PTS_ prefix retained for startup reconciliation."""
    from execution.profit_take_sell import (
        profit_take_client_order_id,
        is_profit_take_client_order_id,
    )

    ids = [profit_take_client_order_id("KXLOWTBOS-26JUL16-B70") for _ in range(5)]
    for cid in ids:
        assert is_profit_take_client_order_id(cid), f"{cid!r} missing prefix"
    assert len(set(ids)) == 5, "client_order_ids must all be unique across placements"
