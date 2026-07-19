"""Tests for Change B: non-blocking trade DB writes via background writer.

Verifies that:
- _handle_trade updates cache.last_price synchronously and does NOT await a DB
  commit on the hot path.
- Trade records are eventually persisted by the background writer task.
- The bounded queue does not raise or block when saturated (drop policy).
"""
import asyncio
import os
import sys
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config import AppConfig
from app.models import StreamedTrade
from core.state_machine import TemperatureStrategy
from data.ticker_cache import TickerCache


# ---------------------------------------------------------------------------
# Minimal fakes
# ---------------------------------------------------------------------------


class FakeWSManager:
    def on_message(self, *_args, **_kwargs):
        return None

    async def subscribe(self, *_args, **_kwargs):
        return None


class FakeExecutor:
    async def get_active_markets(self):
        return []

    async def buy_yes(self, order, max_price=None):
        return None

    async def sell_yes(self, order):
        return None

    async def get_balance(self):
        return 0

    async def get_positions(self):
        return {}


class RecordingSession:
    """Fake session that records add() calls and commits."""

    def __init__(self, store: list):
        self._store = store

    def add(self, item):
        self._store.append(item)

    async def commit(self):
        pass  # no-op


class RecordingDB:
    """Fake DatabaseManager that uses RecordingSession."""

    def __init__(self):
        self.records: list = []
        # Optionally inject a commit hook for tests that need to await completion.
        self._commit_event: asyncio.Event | None = None

    @asynccontextmanager
    async def _session_ctx(self):
        session = RecordingSession(self.records)
        yield session

    async def get_session(self):
        return self._session_ctx()


def make_config(**overrides):
    defaults = dict(
        trading_mode="DRY_RUN",
        dry_run=True,
        rest_base_url="https://demo-api.kalshi.co/trade-api/v2",
        ws_url="wss://demo-api.kalshi.co/trade-api/ws/v2",
        kalshi_api_key="test",
        kalshi_private_key_path="/dev/null",
        mysql_database_url="******localhost/db",
        monitor_start_price=80,
        buy_trigger_price=75,
        minimum_spread=3,
        spread_monitor_price=70,
        stop_loss_price=40,
        initial_contract_count=1,
        hedge_max_factor=4,
        sl_worker_interval_ms=250,
        sl_exit_mode="PANIC_FLATTEN",
        enable_fast_sl_exit=False,
        sl_panic_sell_price=1,
        sl_panic_retry_ms=0,
        sl_panic_max_retries=3,
        sl_panic_max_quote_age_ms=30000,
        no_trade_tickers=set(),
    )
    defaults.update(overrides)

    config = AppConfig.model_construct(**defaults)
    for key, value in defaults.items():
        setattr(config, key, value)
    return config


def make_strategy(monkeypatch, db=None, **config_overrides):
    import core.state_machine as sm
    import nws.gate as nws_gate

    monkeypatch.setattr(sm, "load_private_key", lambda _path: object())
    monkeypatch.setattr(sm, "is_entry_allowed", lambda *_a, **_k: (True, {}))
    monkeypatch.setattr(nws_gate, "has_forecast", lambda *_a, **_k: True)
    monkeypatch.setattr(nws_gate, "is_trading_gate_open", lambda *_a, **_k: True)
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        db or RecordingDB(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_trade_updates_cache_synchronously(monkeypatch):
    """cache.update_last_price is called synchronously; no DB write happens inline."""
    db = RecordingDB()
    strategy = make_strategy(monkeypatch, db=db)

    msg = {
        "type": "trade",
        "msg": {
            "market_ticker": "KXHIGH-25JUL25-T77",
            "price": 55,
            "quantity": 10,
            "side": "yes",
            "ts": None,
        },
    }

    # No writer task running — queue drains only if the task runs.
    await strategy._handle_trade(msg)

    # Cache must be updated immediately.
    assert strategy.cache.get_last_price("KXHIGH-25JUL25-T77") == 55

    # DB must NOT have been written yet (writer hasn't run).
    assert db.records == [], "DB write should be deferred to background writer"

    # The queue must have exactly one item.
    assert strategy._trade_log_queue.qsize() == 1


@pytest.mark.asyncio
async def test_handle_trade_db_write_via_background_writer(monkeypatch):
    """After the background writer processes the queue, the StreamedTrade is persisted."""
    db = RecordingDB()
    strategy = make_strategy(monkeypatch, db=db)

    msg = {
        "type": "trade",
        "msg": {
            "market_ticker": "KXHIGH-25JUL25-T77",
            "price": 60,
            "quantity": 5,
            "side": "yes",
            "ts": 1_700_000_000_000,
        },
    }

    await strategy._handle_trade(msg)
    assert db.records == [], "no write before writer task runs"

    # Start the writer, let it drain the queue, then stop it.
    writer_task = asyncio.create_task(strategy._trade_log_writer())
    # Give the task a chance to process the one queued item.
    await asyncio.sleep(0.05)
    writer_task.cancel()
    try:
        await writer_task
    except asyncio.CancelledError:
        pass

    assert len(db.records) == 1
    assert isinstance(db.records[0], StreamedTrade)
    assert db.records[0].market_ticker == "KXHIGH-25JUL25-T77"
    assert db.records[0].price == 60
    assert db.records[0].quantity == 5


@pytest.mark.asyncio
async def test_handle_trade_queue_full_does_not_raise_or_block(monkeypatch):
    """When the trade log queue is saturated, _handle_trade drops the record silently
    (rate-limited warning) without raising or blocking."""
    db = RecordingDB()
    strategy = make_strategy(monkeypatch, db=db)

    # Fill the queue to its capacity.
    for i in range(strategy._trade_log_queue.maxsize):
        strategy._trade_log_queue.put_nowait({"market_ticker": "X", "price": i, "quantity": 1, "side": "yes", "trade_ts": None})

    assert strategy._trade_log_queue.full()

    # Calling _handle_trade when the queue is full must not raise.
    msg = {"type": "trade", "msg": {"market_ticker": "TICKER", "price": 1, "quantity": 1, "side": "yes", "ts": None}}
    await strategy._handle_trade(msg)  # should not raise, should not block

    # Cache is still updated (synchronous path is unaffected by the drop).
    assert strategy.cache.get_last_price("TICKER") == 1

    # Queue is still full (nothing was added or removed).
    assert strategy._trade_log_queue.full()


@pytest.mark.asyncio
async def test_trade_log_writer_survives_db_error(monkeypatch):
    """If a DB write fails for one record, the writer continues processing subsequent
    records without crashing."""

    class FailOnceDB(RecordingDB):
        def __init__(self):
            super().__init__()
            self._fail_count = 0

        @asynccontextmanager
        async def _session_ctx(self):
            self._fail_count += 1
            if self._fail_count == 1:
                raise RuntimeError("simulated DB error")
            yield RecordingSession(self.records)

    db = FailOnceDB()
    strategy = make_strategy(monkeypatch, db=db)

    # Enqueue two records — first will fail, second should succeed.
    strategy._trade_log_queue.put_nowait({"market_ticker": "A", "price": 1, "quantity": 1, "side": "yes", "trade_ts": None})
    strategy._trade_log_queue.put_nowait({"market_ticker": "B", "price": 2, "quantity": 1, "side": "yes", "trade_ts": None})

    writer_task = asyncio.create_task(strategy._trade_log_writer())
    await asyncio.sleep(0.05)
    writer_task.cancel()
    try:
        await writer_task
    except asyncio.CancelledError:
        pass

    # Second record must have been persisted despite the first failing.
    assert len(db.records) == 1
    assert db.records[0].market_ticker == "B"
