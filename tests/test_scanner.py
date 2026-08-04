"""
Unit tests for scanner.py critical-fix changes:
  - _daemon_is_running() correctly detects whether run.py holds the lockfile
  - main() exits immediately when the daemon lockfile is held (Critical #1 guard)
"""

import asyncio
import fcntl
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import scanner as scanner_module
from scanner import _daemon_is_running


# ---------------------------------------------------------------------------
# _daemon_is_running() tests
# ---------------------------------------------------------------------------

def test_daemon_not_running_when_no_lockfile(tmp_path):
    """If the lockfile does not exist, _daemon_is_running() returns False."""
    non_existent = str(tmp_path / "missing.lock")
    with patch.object(scanner_module, "DAEMON_LOCKFILE", non_existent):
        assert _daemon_is_running() is False


def test_daemon_not_running_when_lock_available(tmp_path):
    """If the lockfile exists but no process holds it, _daemon_is_running() returns False."""
    lockfile = str(tmp_path / "test.lock")
    with open(lockfile, "w") as f:
        f.write("")
    with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
        assert _daemon_is_running() is False


def test_daemon_is_running_when_lock_held(tmp_path):
    """If another fd holds an exclusive lock on the lockfile, _daemon_is_running() returns True."""
    lockfile = str(tmp_path / "held.lock")
    # Hold an exclusive lock in this process to simulate run.py
    holder = open(lockfile, "w")
    try:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
            assert _daemon_is_running() is True
    finally:
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        holder.close()


# ---------------------------------------------------------------------------
# main() skips scan when daemon is running (Critical #1 guard)
# ---------------------------------------------------------------------------

def test_main_exits_early_when_daemon_running(tmp_path, capfd):
    """main() must return without running the scan cycle when the daemon lock is held."""
    lockfile = str(tmp_path / "daemon.lock")
    holder = open(lockfile, "w")
    try:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
            with patch.object(scanner_module, "run_scan_cycle", new_callable=AsyncMock) as mock_scan:
                scanner_module.main()
                mock_scan.assert_not_awaited()
    finally:
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        holder.close()


# ---------------------------------------------------------------------------
# Bug B fix: Scanner buy path routed through capped executor
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_scan_cycle_uses_family_specific_buy_triggers(monkeypatch):
    from app.config import AppConfig

    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=1,
        monitor_start_price=80,
        buy_trigger_price_low=82,
        buy_trigger_price_high=90,
        spread_monitor_price=95,
        minimum_spread=2,
        stop_loss_price=25,
        hedge_max_factor=2,
    )

    bought = []

    async def fake_fetch(_config, _client):
        return (
            [
                "KXLOWTLAX-26JUL30-B65.5",
                "KXHIGHTLAX-26JUL30-B95",
                "KXMIDTLAX-26JUL30-B70",
            ],
            {
                "KXLOWTLAX-26JUL30-B65.5": {"best_ask": 83, "best_bid": 82, "spread": 1},
                "KXHIGHTLAX-26JUL30-B95": {"best_ask": 89, "best_bid": 88, "spread": 1},
                "KXMIDTLAX-26JUL30-B70": {"best_ask": 95, "best_bid": 94, "spread": 1},
            },
        )

    class FakeResult:
        def fetchall(self):
            return []

    class FakeSession:
        def add(self, *_args, **_kwargs):
            return None

        async def commit(self):
            return None

        async def execute(self, *_args, **_kwargs):
            return FakeResult()

    class FakeSessionCtx:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class FakeDB:
        async def get_session(self):
            return FakeSessionCtx()

    async def fake_buy_market(_config, ticker, ask, _client):
        bought.append((ticker, ask))
        return True

    monkeypatch.setattr(scanner_module, "_fetch_markets_via_rest", fake_fetch)
    monkeypatch.setattr(scanner_module, "buy_market", fake_buy_market)

    await scanner_module.run_scan_cycle(config, FakeDB())

    assert bought == [("KXLOWTLAX-26JUL30-B65.5", 83)]


@pytest.mark.asyncio
async def test_scanner_buy_executor_cap_blocks_oversized_order(monkeypatch):
    """Scanner buy_market must propagate executor-level hedge.cap_blocked rejections.

    The executor is initialized with max_buy_qty=max_allowed_qty; when the
    executor signals a cap-blocked REJECTED result, buy_market must return False.
    This mirrors test_monitor_buy_hedge_refuses_oversized_order.
    """
    import scanner as scanner_module
    from app.config import AppConfig
    from execution.base import ExecutionResult

    critical_logged = []
    monkeypatch.setattr(scanner_module.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    class CapBlockingExecutor:
        """Simulates an executor that enforces a max_buy_qty cap."""
        def __init__(self, max_buy_qty):
            self.max_buy_qty = max_buy_qty

        async def buy_yes(self, order, max_price=None):
            if order.quantity > self.max_buy_qty:
                import scanner as sm
                sm.logger.critical(
                    "hedge.cap_blocked",
                    ticker=order.market_ticker,
                    proposed_qty=order.quantity,
                    max_allowed_qty=self.max_buy_qty,
                    action="executor_hard_cap_blocked_submission",
                )
                return ExecutionResult(
                    success=False,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=0,
                    fill_quantity=0,
                    total_cost_cents=0,
                    status="REJECTED",
                    notes=f"hard_cap_blocked: qty={order.quantity} exceeds max_buy_qty={self.max_buy_qty}",
                )
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=order.price * order.quantity,
                status="FILLED",
            )

    # Route through a capping executor that will block qty=16 when max_buy_qty=8
    def fake_create_executor(*args, **kwargs):
        return CapBlockingExecutor(max_buy_qty=8)

    monkeypatch.setattr(scanner_module, "create_executor", fake_create_executor)

    # Config with initial=16 — executor cap of 8 will block it
    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=16,
        monitor_start_price=80,
        buy_trigger_price_low=72,
        buy_trigger_price_high=72,
        spread_monitor_price=90,
        minimum_spread=2,
        stop_loss_price=25,
        hedge_max_factor=2,
    )

    result = await scanner_module.buy_market(config, "KXLOWTLAX-26JUL30-B65.5", 80, None)
    assert result is False, "Expected buy_market to return False when executor cap blocks"

    assert any(ev == "hedge.cap_blocked" for ev, _ in critical_logged), (
        "Expected hedge.cap_blocked CRITICAL when executor cap blocks"
    )


@pytest.mark.asyncio
async def test_scanner_buy_uses_executor_buy_yes(monkeypatch):
    """Scanner buy_market must route through the executor's buy_yes, not raw httpx."""
    import scanner as scanner_module
    from app.config import AppConfig
    from execution.base import ExecutionResult

    buy_yes_calls = []

    async def fake_buy_yes(order, max_price=None):
        buy_yes_calls.append((order, max_price))
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=order.price * order.quantity,
            status="FILLED",
        )

    class FakeExecutorInstance:
        async def buy_yes(self, order, max_price=None):
            return await fake_buy_yes(order, max_price)

    def fake_create_executor(*args, **kwargs):
        return FakeExecutorInstance()

    monkeypatch.setattr(scanner_module, "create_executor", fake_create_executor)

    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=4,
        monitor_start_price=80,
        buy_trigger_price_low=72,
        buy_trigger_price_high=72,
        spread_monitor_price=90,
        minimum_spread=2,
        stop_loss_price=25,
        hedge_max_factor=2,
    )

    result = await scanner_module.buy_market(config, "KXLOWTLAX-26JUL30-B65.5", 80, None)
    assert result is True
    assert len(buy_yes_calls) == 1, "buy_yes must be called exactly once"
    order, max_price = buy_yes_calls[0]
    assert order.market_ticker == "KXLOWTLAX-26JUL30-B65.5"
    assert order.quantity == 4
    assert max_price == 90  # spread_monitor_price


@pytest.mark.asyncio
async def test_scanner_buy_logs_cap_blocked_not_buy_yes_when_qty_at_cap(monkeypatch):
    """Scanner passes qty == max_allowed_qty through without cap block."""
    import scanner as scanner_module
    from app.config import AppConfig
    from execution.base import ExecutionResult

    buy_yes_calls = []

    class FakeExecutorInstance:
        async def buy_yes(self, order, max_price=None):
            buy_yes_calls.append(order)
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=order.price * order.quantity,
                status="FILLED",
            )

    monkeypatch.setattr(scanner_module, "create_executor", lambda *a, **kw: FakeExecutorInstance())

    # initial=4, factor=2 → max=8; initial(4) <= max(8) → allowed
    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=4,
        monitor_start_price=80,
        buy_trigger_price_low=72,
        buy_trigger_price_high=72,
        spread_monitor_price=90,
        minimum_spread=2,
        stop_loss_price=25,
        hedge_max_factor=2,
    )

    result = await scanner_module.buy_market(config, "KXLOWTLAX-26JUL30-B65.5", 80, None)
    assert result is True
    assert len(buy_yes_calls) == 1


@pytest.mark.asyncio
async def test_scanner_buy_blocks_when_existing_plus_proposed_exceeds_cap(monkeypatch):
    import scanner as scanner_module
    from app.config import AppConfig

    critical_logged = []
    monkeypatch.setattr(scanner_module.logger, "critical",
                        lambda ev, **kw: critical_logged.append((ev, kw)))

    class FakeExecutorInstance:
        async def get_positions(self):
            return {"KXLOWTLAX-26JUL30-B65.5": {"count": 10}}

        async def buy_yes(self, order, max_price=None):
            raise AssertionError("buy_yes should not run when pre-submit cap blocks")

    monkeypatch.setattr(scanner_module, "create_executor", lambda *a, **kw: FakeExecutorInstance())

    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=5,
        monitor_start_price=80,
        buy_trigger_price_low=72,
        buy_trigger_price_high=72,
        spread_monitor_price=90,
        minimum_spread=2,
        stop_loss_price=25,
        hedge_max_factor=2,
    )

    result = await scanner_module.buy_market(config, "KXLOWTLAX-26JUL30-B65.5", 80, None)
    assert result is False
    cap_log = next(kw for ev, kw in critical_logged if ev == "hedge.cap_blocked")
    assert cap_log["action"] == "scanner_position_cap_blocked_before_submit"
    assert cap_log["total_position_qty"] == 15


def test_scanner_daemon_guard_prevents_buy_when_daemon_running(tmp_path, monkeypatch):
    """When daemon lockfile is held, main() exits without calling buy_market."""
    import scanner as scanner_module
    from unittest.mock import AsyncMock

    lockfile = str(tmp_path / "daemon.lock")
    holder = open(lockfile, "w")
    import fcntl
    try:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
            with patch.object(scanner_module, "run_scan_cycle", new_callable=AsyncMock) as mock_scan:
                with patch.object(scanner_module, "buy_market", new_callable=AsyncMock) as mock_buy:
                    scanner_module.main()
                    mock_scan.assert_not_awaited()
                    mock_buy.assert_not_awaited()
    finally:
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        holder.close()
