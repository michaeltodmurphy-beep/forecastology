"""Tests for the SL backstop feature.

Covers:
  1. Config: sl_backstop_enabled and sl_backstop_offset parsing.
  2. SlBackstopManager: place / cancel / check_fill lifecycle with a mocked
     executor.
  3. Integration: state machine honours cancel-before-sell ordering when the
     backstop is enabled.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# 1. Config parsing
# ---------------------------------------------------------------------------


class TestSlBackstopConfig:
    def setup_method(self):
        for key in ("SL_BACKSTOP_ENABLED", "SL_BACKSTOP_OFFSET"):
            os.environ.pop(key, None)

    def teardown_method(self):
        for key in ("SL_BACKSTOP_ENABLED", "SL_BACKSTOP_OFFSET"):
            os.environ.pop(key, None)

    def test_defaults_disabled_and_five_cents(self):
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig

        cfg = AppConfig(
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
        assert cfg.sl_backstop_enabled is False
        assert cfg.sl_backstop_offset == 5

    def test_enabled_via_direct_construct(self):
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig

        cfg = AppConfig(
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
            sl_backstop_enabled=True,
        )
        assert cfg.sl_backstop_enabled is True

    def test_offset_dollars_converted_to_cents(self):
        """Dollar string '0.03' must be converted to 3¢ by the validator."""
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig

        # The convert_dollars_to_cents validator fires on string inputs.
        cfg = AppConfig(
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
            sl_backstop_offset="0.03",
        )
        assert cfg.sl_backstop_offset == 3

    def test_offset_direct_int_is_already_cents(self):
        """Programmatic construction with int bypasses the validator."""
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig

        cfg = AppConfig(
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
            sl_backstop_enabled=True,
            sl_backstop_offset=5,
        )
        assert cfg.sl_backstop_enabled is True
        assert cfg.sl_backstop_offset == 5


# ---------------------------------------------------------------------------
# 2. SlBackstopManager unit tests
# ---------------------------------------------------------------------------


class _FakeExecutor:
    """Minimal executor stub for SlBackstopManager tests."""

    def __init__(self):
        self.placed: list[dict] = []
        self.cancelled: list[str] = []
        self.statuses: dict[str, str] = {}  # order_id → status

    async def place_limit_sell(self, order):
        from execution.base import ExecutionResult

        order_id = f"ord_{len(self.placed)}"
        self.placed.append(
            {"ticker": order.market_ticker, "price": order.price, "qty": order.quantity,
             "order_id": order_id}
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


@pytest.mark.asyncio
async def test_backstop_place_stores_order_id():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    order_id = await mgr.place("TICK", 3)
    assert order_id is not None
    assert mgr.get_order_id("TICK") == order_id
    assert len(exec_.placed) == 1
    assert exec_.placed[0]["price"] == 30  # 35 - 5


@pytest.mark.asyncio
async def test_backstop_price_floor_at_one_cent():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=50,  # offset larger than SL price
        stop_loss_price_ask=10,
        trading_mode="LIVE",
    )
    order_id = await mgr.place("TICK", 1)
    assert order_id is not None
    assert exec_.placed[0]["price"] == 1  # floored at 1¢


@pytest.mark.asyncio
async def test_backstop_cancel_clears_order_id():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    await mgr.place("TICK", 2)
    assert mgr.get_order_id("TICK") is not None

    ok = await mgr.cancel("TICK", reason="sl_exit")
    assert ok is True
    assert mgr.get_order_id("TICK") is None
    assert len(exec_.cancelled) == 1


@pytest.mark.asyncio
async def test_backstop_cancel_when_no_order_returns_true():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    ok = await mgr.cancel("MISSING_TICK")
    assert ok is True
    assert exec_.cancelled == []


@pytest.mark.asyncio
async def test_backstop_replace_on_second_place():
    """A second place call cancels the first order before placing a new one."""
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    first_id = await mgr.place("TICK", 2)
    second_id = await mgr.place("TICK", 3)
    assert first_id is not None
    assert second_id is not None
    assert first_id != second_id
    # The old order must have been cancelled.
    assert first_id in exec_.cancelled
    assert mgr.get_order_id("TICK") == second_id


@pytest.mark.asyncio
async def test_backstop_check_fill_detects_filled_order():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    order_id = await mgr.place("TICK", 1)
    exec_.statuses[order_id] = "filled"

    filled = await mgr.check_fill("TICK")
    assert filled is True
    assert mgr.get_order_id("TICK") is None  # cleared on fill detection


@pytest.mark.asyncio
async def test_backstop_check_fill_returns_false_for_resting():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    order_id = await mgr.place("TICK", 1)
    exec_.statuses[order_id] = "resting"

    filled = await mgr.check_fill("TICK")
    assert filled is False
    assert mgr.get_order_id("TICK") == order_id  # still active


@pytest.mark.asyncio
async def test_backstop_disabled_is_noop():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=False,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    order_id = await mgr.place("TICK", 3)
    assert order_id is None
    assert exec_.placed == []

    ok = await mgr.cancel("TICK")
    assert ok is True  # no-op returns True (safe to proceed with sell)


@pytest.mark.asyncio
async def test_backstop_paper_mode_is_noop():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="PAPER",
    )
    order_id = await mgr.place("TICK", 3)
    assert order_id is None
    assert exec_.placed == []


@pytest.mark.asyncio
async def test_backstop_set_and_get_order_id():
    from execution.sl_backstop import SlBackstopManager

    exec_ = _FakeExecutor()
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    mgr.set_order_id("TICK", "existing_order_123")
    assert mgr.get_order_id("TICK") == "existing_order_123"

    # Placing will cancel the existing order first.
    new_id = await mgr.place("TICK", 2)
    assert "existing_order_123" in exec_.cancelled
    assert new_id != "existing_order_123"


# ---------------------------------------------------------------------------
# 3. State machine integration: cancel-before-sell ordering
# ---------------------------------------------------------------------------


def _make_config(**kwargs):
    """Return a minimal AppConfig for testing."""
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
    )
    defaults.update(kwargs)
    return AppConfig(**defaults)


@pytest.mark.asyncio
async def test_cancel_sl_backstop_called_before_sell(monkeypatch):
    """When sl_backstop_enabled=True, _cancel_sl_backstop must be awaited
    before executor.sell_yes is called inside _execute_stop_loss."""
    from execution.sl_backstop import SlBackstopManager

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
                fill_quantity=0, total_cost_cents=0, order_id="bsp_1", status="RESTING",
            )

        async def cancel_order(self, order_id, market_ticker=""):
            call_order.append(f"cancel:{order_id}")
            return True

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
    mgr = SlBackstopManager(
        executor=exec_,
        sl_backstop_enabled=True,
        sl_backstop_offset=5,
        stop_loss_price_ask=35,
        trading_mode="LIVE",
    )
    mgr.set_order_id("TICKER", "bsp_1")

    # Patch cancel to record the call.
    original_cancel = mgr.cancel

    async def _tracking_cancel(ticker, *, reason="exit"):
        call_order.append(f"cancel_backstop:{ticker}")
        return await original_cancel(ticker, reason=reason)

    mgr.cancel = _tracking_cancel

    # Build a minimal TemperatureStrategy with the tracking objects.
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
    strategy.sl_backstop = mgr
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

    # cancel_backstop must come before sell_yes.
    cancel_idx = next(
        (i for i, x in enumerate(call_order) if "cancel_backstop" in x), None
    )
    sell_idx = next(
        (i for i, x in enumerate(call_order) if x == "sell_yes"), None
    )
    assert cancel_idx is not None, "cancel_backstop was never called"
    assert sell_idx is not None, "sell_yes was never called"
    assert cancel_idx < sell_idx, (
        f"cancel_backstop({cancel_idx}) must precede sell_yes({sell_idx}); "
        f"call_order={call_order}"
    )
