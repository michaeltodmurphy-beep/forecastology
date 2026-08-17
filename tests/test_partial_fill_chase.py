# tests/test_partial_fill_chase.py
"""
Tests for the partial-fill chaser feature (PARTIAL_FILL_CHASE=yes).

All tests use the same InMemoryDB / FakeExecutor / make_strategy helpers
that the rest of the test suite uses, imported directly here to avoid
circular dependencies from conftest.
"""
import asyncio
import datetime
import os
import sys

import pytest
from unittest.mock import AsyncMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config import AppConfig, _parse_trade_toggle, _parse_positive_int
from app.models import (
    ExecutedTrade,
    Position as PositionModel,
    StopLossLedger,
    OrderAction,
    OrderActionStatus,
    TradeStatus,
)
from core.state_machine import TemperatureStrategy, parse_series_and_date
from core.types import (
    MarketBracket,
    OrderBook,
    OrderBookLevel,
    OrderRequest,
    OrderSide,
    Phase,
)
from data.ticker_cache import TickerCache
from execution.base import ExecutionResult
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList


# ---------------------------------------------------------------------------
# Helpers (mirrors test_state_machine.py)
# ---------------------------------------------------------------------------


class FakeWSManager:
    def on_message(self, *_a, **_kw):
        return None

    async def subscribe(self, *_a, **_kw):
        return None


class FakeSessionResult:
    def __init__(self, items=None):
        self._items = items or []

    def scalar_one_or_none(self):
        return self._items[0] if self._items else None

    def scalars(self):
        return self

    def all(self):
        return list(self._items)


class InMemorySession:
    TABLES = {
        PositionModel.__tablename__: PositionModel,
        StopLossLedger.__tablename__: StopLossLedger,
        ExecutedTrade.__tablename__: ExecutedTrade,
        OrderAction.__tablename__: OrderAction,
    }

    def __init__(self, db):
        self.db = db

    def add(self, item):
        bucket = self.db.store.setdefault(type(item), [])
        if item not in bucket:
            bucket.append(item)

    async def commit(self):
        return None

    async def rollback(self):
        return None

    def _matches(self, item, criterion):
        if isinstance(criterion, BooleanClauseList):
            return all(self._matches(item, clause) for clause in criterion.clauses)
        if isinstance(criterion, BinaryExpression):
            left = getattr(criterion.left, "key", None)
            right = getattr(criterion.right, "value", criterion.right)
            value = getattr(item, left, None)
            if criterion.operator is operators.eq:
                return value == right
            if criterion.operator is operators.gt:
                return value is not None and value > right
            if criterion.operator is operators.ge:
                return value is not None and value >= right
            if criterion.operator is operators.lt:
                return value is not None and value < right
            if criterion.operator is operators.le:
                return value is not None and value <= right
        return True

    async def execute(self, statement, *_a, **_kw):
        visit_name = getattr(statement, "__visit_name__", "")
        if visit_name == "select":
            entity = statement.column_descriptions[0]["entity"]
            items = list(self.db.store.get(entity, []))
            for criterion in statement._where_criteria:
                items = [item for item in items if self._matches(item, criterion)]
            return FakeSessionResult(items)
        if visit_name == "delete":
            entity = self.TABLES[statement.table.name]
            items = list(self.db.store.get(entity, []))
            kept = [
                item
                for item in items
                if not all(
                    self._matches(item, criterion)
                    for criterion in statement._where_criteria
                )
            ]
            self.db.store[entity] = kept
            return FakeSessionResult([])
        if visit_name == "update":
            entity = self.TABLES[statement.table.name]
            new_values = {
                col.key: val.value if hasattr(val, "value") else val
                for col, val in statement._values.items()
            }
            items = list(self.db.store.get(entity, []))
            for item in items:
                if all(
                    self._matches(item, criterion)
                    for criterion in statement._where_criteria
                ):
                    for attr, val in new_values.items():
                        setattr(item, attr, val)
            return FakeSessionResult([])
        return FakeSessionResult([])


class InMemorySessionContext:
    def __init__(self, db):
        self.session = InMemorySession(db)

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class InMemoryDB:
    def __init__(self, items=None):
        self.store = {
            PositionModel: [],
            StopLossLedger: [],
            ExecutedTrade: [],
            OrderAction: [],
        }
        for item in items or []:
            self.store.setdefault(type(item), []).append(item)

    async def get_session(self):
        return InMemorySessionContext(self)


class FakeExecutor:
    """Minimal executor for chase tests with configurable responses."""

    def __init__(self):
        self.orders = []
        self.buy_success = True
        self.sell_success = True
        self.positions = {}
        self.fills = []
        self.balance = 100_000_00
        # Resting order tracking for chase tests
        self._resting_orders: dict = {}
        self._order_fill_info_responses: dict = {}  # order_id -> list of dicts (popped each call)
        self._cancel_calls: list = []

    async def buy_yes(self, order, max_price=None):
        self.orders.append(("buy_yes", order, max_price))
        if self.buy_success:
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=order.price * order.quantity,
                order_id="buy-oid",
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
            notes="rejected",
        )

    async def sell_yes(self, order):
        self.orders.append(("sell_yes", order, None))
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="sell-oid",
        )

    async def get_balance(self):
        return self.balance

    async def get_positions(self):
        return dict(self.positions)

    async def get_active_markets(self, series_prefix=""):
        return []

    async def get_fills(self, ticker=None):
        return list(self.fills)

    async def place_limit_buy(self, order):
        oid = f"chase-oid-{len(self.orders)}"
        self.orders.append(("place_limit_buy", order, None))
        self._resting_orders[oid] = {
            "ticker": order.market_ticker,
            "price": order.price,
            "quantity": order.quantity,
        }
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            order_id=oid,
            status="RESTING",
        )

    async def cancel_order(self, order_id, market_ticker=""):
        self._cancel_calls.append(order_id)
        self._resting_orders.pop(order_id, None)
        return True

    async def get_order_status(self, order_id):
        if order_id in self._resting_orders:
            return "resting"
        return "not_found"

    async def get_order_fill_info(self, order_id):
        responses = self._order_fill_info_responses.get(order_id)
        if responses:
            return responses.pop(0)
        # default: still resting
        return {"status": "resting", "fill_qty": 0, "fill_price": 0}


def make_config(**overrides):
    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=12,
        monitor_start_price=80,
        buy_trigger_price_low=82,
        buy_trigger_price_high=82,
        spread_monitor_price=94,
        minimum_spread=4,
        stop_loss_price=50,
        hedge_max_factor=3,
        dry_run=False,
        sl_exit_mode="AGGRESSIVE_LIMIT",
        enable_fast_sl_exit=False,
        sl_panic_sell_price=1,
        sl_panic_retry_ms=0,
        sl_panic_max_retries=3,
        sl_panic_max_quote_age_ms=30000,
        no_trade_tickers=set(),
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def make_strategy(monkeypatch, db=None, executor=None, **config_overrides):
    import core.state_machine as state_machine
    import nws.gate as nws_gate

    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())
    monkeypatch.setattr(state_machine, "is_entry_allowed", lambda *_a, **_kw: (True, {}))
    monkeypatch.setattr(nws_gate, "has_forecast", lambda *_a, **_kw: True)
    monkeypatch.setattr(nws_gate, "is_trading_gate_open", lambda *_a, **_kw: True)
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        executor or FakeExecutor(),
        db or InMemoryDB(),
    )


def capture_logs(monkeypatch):
    import core.state_machine as state_machine

    logged = []
    for method in ("debug", "info", "warning", "error", "critical"):
        monkeypatch.setattr(
            state_machine.logger,
            method,
            lambda event, _m=method, **kwargs: logged.append((event, kwargs)),
        )
    return logged


def _partial_fill_result(ticker, proposed_qty, fill_qty, fill_price=90):
    return ExecutionResult(
        success=True,
        market_ticker=ticker,
        side="yes",
        price=fill_price,
        quantity=proposed_qty,
        fill_price=fill_price,
        fill_quantity=fill_qty,
        total_cost_cents=fill_price * fill_qty,
        order_id="partial-fill",
        notes=f'{{"fill_count":"{fill_qty}.00","remaining_count":"{proposed_qty-fill_qty}.00"}}',
    )


# ===========================================================================
# Config parse tests
# ===========================================================================


def test_partial_fill_chase_default_is_false():
    """PARTIAL_FILL_CHASE defaults to False (feature off by default)."""
    val = _parse_trade_toggle(None, "PARTIAL_FILL_CHASE", default=False)
    assert val is False


def test_partial_fill_chase_yes():
    val = _parse_trade_toggle("yes", "PARTIAL_FILL_CHASE", default=False)
    assert val is True


def test_partial_fill_chase_no():
    val = _parse_trade_toggle("no", "PARTIAL_FILL_CHASE", default=False)
    assert val is False


def test_chase_interval_seconds_default():
    val = _parse_positive_int(None, "CHASE_INTERVAL_SECONDS", default=60)
    assert val == 60


def test_chase_max_minutes_default():
    val = _parse_positive_int(None, "CHASE_MAX_MINUTES", default=30)
    assert val == 30


def test_chase_interval_seconds_valid():
    val = _parse_positive_int("30", "CHASE_INTERVAL_SECONDS", default=60)
    assert val == 30


def test_chase_max_minutes_valid():
    val = _parse_positive_int("15", "CHASE_MAX_MINUTES", default=30)
    assert val == 15


def test_config_fields_exist():
    cfg = make_config(partial_fill_chase=True, chase_interval_seconds=10, chase_max_minutes=5)
    assert cfg.partial_fill_chase is True
    assert cfg.chase_interval_seconds == 10
    assert cfg.chase_max_minutes == 5


# ===========================================================================
# Partial fill does NOT trigger chaser when disabled (default)
# ===========================================================================


@pytest.mark.asyncio
async def test_partial_fill_no_chaser_when_disabled(monkeypatch):
    """Default config: partial fill → no chaser task started."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    executor = FakeExecutor()

    async def buy_yes(order, max_price=None):
        return _partial_fill_result(ticker, 12, 7)

    executor.buy_yes = buy_yes
    executor.positions = {ticker: {"average_fill_cost_cents": 90}}

    db = InMemoryDB()
    # partial_fill_chase defaults to False
    strategy = make_strategy(monkeypatch, db=db, executor=executor)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket

    await strategy._execute_entry(
        bracket,
        ob=OrderBook(yes_asks=[OrderBookLevel(price=90, quantity=10, order_count=1)]),
        quantity=12,
    )

    assert bracket.phase == Phase.HOLDING
    # No chase task should be running
    assert ticker not in strategy._chase_tasks


# ===========================================================================
# Partial fill DOES trigger chaser when enabled
# ===========================================================================


@pytest.mark.asyncio
async def test_partial_fill_triggers_chaser_when_enabled(monkeypatch):
    """PARTIAL_FILL_CHASE=yes: partial fill → chaser task is created."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    executor = FakeExecutor()

    async def buy_yes(order, max_price=None):
        return _partial_fill_result(ticker, 12, 7)

    executor.buy_yes = buy_yes
    executor.positions = {ticker: {"average_fill_cost_cents": 90}}

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True, chase_interval_seconds=1
    )

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket

    # Intercept _maybe_start_chaser to record that it was called
    chaser_started = {"called": False, "remaining": None}
    orig_maybe = strategy._maybe_start_chaser

    async def fake_start_chaser(bkt, remaining, intended_qty):
        chaser_started["called"] = True
        chaser_started["remaining"] = remaining
        # Don't actually start the loop

    strategy._maybe_start_chaser = fake_start_chaser

    await strategy._execute_entry(
        bracket,
        ob=OrderBook(yes_asks=[OrderBookLevel(price=90, quantity=10, order_count=1)]),
        quantity=12,
    )

    assert bracket.phase == Phase.HOLDING
    assert chaser_started["called"], "Chaser was not started for partial fill"
    assert chaser_started["remaining"] == 5


# ===========================================================================
# Initial bid = best_bid + 1, clamped to ceiling
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_initial_bid_best_bid_plus_one(monkeypatch):
    """Chaser places first bid at best_bid + 1."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    # Set best bid to 88
    cache.quotes[ticker] = (88, 92)

    executor = FakeExecutor()
    place_calls = []
    orig_place = executor.place_limit_buy

    async def tracked_place(order):
        place_calls.append(order.price)
        return await orig_place(order)

    executor.place_limit_buy = tracked_place

    # After first sleep, have it fill
    async def sleep_then_fill(secs):
        if len(place_calls) >= 1:
            oid = list(executor._resting_orders.keys())[0] if executor._resting_orders else None
            if oid:
                executor._order_fill_info_responses[oid] = [
                    {"status": "filled", "fill_qty": 5, "fill_price": 89}
                ]
        if len(place_calls) >= 1:
            raise asyncio.CancelledError()

    import core.state_machine as sm_mod
    monkeypatch.setattr(sm_mod.asyncio, "sleep", sleep_then_fill)

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch,
        db=db,
        executor=executor,
        partial_fill_chase=True,
        chase_interval_seconds=0,
    )
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )

    try:
        await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)
    except (asyncio.CancelledError, Exception):
        pass

    assert len(place_calls) > 0
    # First bid should be 88 + 1 = 89
    assert place_calls[0] == 89


@pytest.mark.asyncio
async def test_chaser_initial_bid_clamped_at_ceiling(monkeypatch):
    """If best_bid+1 > ceiling, bid is clamped to ceiling (94)."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    # best_bid = 94 → desired_bid = min(95, 94) = 94
    cache.quotes[ticker] = (94, 95)

    executor = FakeExecutor()
    place_calls = []

    async def tracked_place(order):
        place_calls.append(order.price)
        return ExecutionResult(
            success=True, market_ticker=order.market_ticker, side="yes",
            price=order.price, quantity=order.quantity,
            fill_price=0, fill_quantity=0, total_cost_cents=0,
            order_id="chase-oid-ceil", status="RESTING",
        )

    executor.place_limit_buy = tracked_place

    import core.state_machine as sm_mod
    call_count = {"n": 0}

    async def sleep_once(secs):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise asyncio.CancelledError()

    monkeypatch.setattr(sm_mod.asyncio, "sleep", sleep_once)

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True, spread_monitor_price=94
    )
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )

    try:
        await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)
    except (asyncio.CancelledError, Exception):
        pass

    assert len(place_calls) > 0
    assert place_calls[0] == 94  # ceiling


# ===========================================================================
# Ceiling rule: best_bid >= ceiling-1 → bid exactly at ceiling
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_ceiling_rule(monkeypatch):
    """best_bid >= ceiling-1 (93) → chaser bids exactly ceiling (94)."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    # best_bid = 93 → ceiling-1 == 93 → bid = 94
    cache.quotes[ticker] = (93, 95)

    executor = FakeExecutor()
    place_calls = []

    async def tracked_place(order):
        place_calls.append(order.price)
        return ExecutionResult(
            success=True, market_ticker=order.market_ticker, side="yes",
            price=order.price, quantity=order.quantity,
            fill_price=0, fill_quantity=0, total_cost_cents=0,
            order_id=f"oid-{len(place_calls)}", status="RESTING",
        )

    executor.place_limit_buy = tracked_place

    import core.state_machine as sm_mod
    call_count = {"n": 0}

    async def sleep_twice(secs):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise asyncio.CancelledError()

    monkeypatch.setattr(sm_mod.asyncio, "sleep", sleep_twice)

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True, spread_monitor_price=94
    )
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )

    try:
        await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)
    except (asyncio.CancelledError, Exception):
        pass

    assert len(place_calls) > 0
    assert place_calls[0] == 94


# ===========================================================================
# Outbid → cancel → re-bid at new_best_bid + 1
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_outbid_cancel_rebid(monkeypatch):
    """When outbid, cancel existing order and re-bid at new best_bid+1."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    cache.quotes[ticker] = (88, 92)

    executor = FakeExecutor()
    place_calls = []

    oid_counter = {"n": 0}

    async def tracked_place(order):
        oid_counter["n"] += 1
        oid = f"oid-{oid_counter['n']}"
        place_calls.append((order.price, oid))
        executor._resting_orders[oid] = {
            "ticker": order.market_ticker,
            "price": order.price,
            "quantity": order.quantity,
        }
        return ExecutionResult(
            success=True, market_ticker=order.market_ticker, side="yes",
            price=order.price, quantity=order.quantity,
            fill_price=0, fill_quantity=0, total_cost_cents=0,
            order_id=oid, status="RESTING",
        )

    executor.place_limit_buy = tracked_place

    import core.state_machine as sm_mod
    # Only count calls to the interval sleep (not the 0.5s confirmation sleep)
    interval_sleep_count = {"n": 0}

    async def sleep_and_advance(secs):
        if secs < 1:
            # short confirmation sleep — don't advance state
            return
        interval_sleep_count["n"] += 1
        if interval_sleep_count["n"] == 1:
            # After first interval: outbid — best_bid now 89 (our bid was 89)
            cache.quotes[ticker] = (89, 93)
        elif interval_sleep_count["n"] >= 2:
            raise asyncio.CancelledError()

    monkeypatch.setattr(sm_mod.asyncio, "sleep", sleep_and_advance)

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True
    )
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )

    try:
        await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)
    except (asyncio.CancelledError, Exception):
        pass

    # Should have placed 2 bids
    assert len(place_calls) >= 2
    # First bid at 89 (88+1), second at 90 (89+1)
    assert place_calls[0][0] == 89
    assert place_calls[1][0] == 90
    # First order should have been cancelled
    assert place_calls[0][1] in executor._cancel_calls


# ===========================================================================
# Fill bookkeeping: position_quantity, avg_entry, ExecutedTrade, PositionModel
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_fill_bookkeeping(monkeypatch):
    """Chaser fill updates bracket.position_quantity and avg_entry correctly."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    cache.quotes[ticker] = (88, 92)

    executor = FakeExecutor()
    oid_ref = {"oid": None}

    async def tracked_place(order):
        oid = "fill-oid"
        oid_ref["oid"] = oid
        executor._resting_orders[oid] = {
            "ticker": order.market_ticker,
            "price": order.price,
            "quantity": order.quantity,
        }
        # Queue up a fill response for next poll
        executor._order_fill_info_responses[oid] = [
            {"status": "filled", "fill_qty": 5, "fill_price": 89}
        ]
        return ExecutionResult(
            success=True, market_ticker=order.market_ticker, side="yes",
            price=order.price, quantity=order.quantity,
            fill_price=0, fill_quantity=0, total_cost_cents=0,
            order_id=oid, status="RESTING",
        )

    executor.place_limit_buy = tracked_place

    import core.state_machine as sm_mod
    call_count = {"n": 0}

    async def sleep_then_stop(secs):
        call_count["n"] += 1
        if call_count["n"] >= 3:
            raise asyncio.CancelledError()

    monkeypatch.setattr(sm_mod.asyncio, "sleep", sleep_then_stop)

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True
    )
    strategy.cache = cache

    # Start with 7 filled @ 90¢
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )
    strategy.active_positions[ticker] = bracket
    strategy._app_owned_qty[ticker] = 7

    try:
        await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)
    except (asyncio.CancelledError, Exception):
        pass

    # After fill of 5 @ 89: new qty=12, blended avg = (90*7 + 89*5)//12
    expected_avg = (90 * 7 + 89 * 5) // 12
    assert bracket.position_quantity == 12
    assert bracket.avg_entry == expected_avg

    # ExecutedTrade row written
    trades = db.store.get(ExecutedTrade, [])
    chase_trades = [t for t in trades if "chase_fill" in (t.notes or "")]
    assert len(chase_trades) >= 1
    assert chase_trades[0].quantity == 5
    assert chase_trades[0].price == 89

    # PositionModel upserted
    positions = db.store.get(PositionModel, [])
    assert len(positions) >= 1
    assert positions[0].quantity == 12


# ===========================================================================
# Hard cap: re-submission blocked when it would exceed max_allowed_qty
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_hard_cap_blocks_submission(monkeypatch):
    """Chase submission is blocked when existing_qty >= max_allowed_qty."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    cache.quotes[ticker] = (88, 92)

    executor = FakeExecutor()
    place_calls = []
    orig = executor.place_limit_buy

    async def tracked(order):
        place_calls.append(order)
        return await orig(order)

    executor.place_limit_buy = tracked

    logged = []
    import core.state_machine as sm_mod

    real_critical = sm_mod.logger.critical
    sm_mod.logger.critical = lambda e, **kw: logged.append((e, kw))

    import core.state_machine as sm_mod
    monkeypatch.setattr(sm_mod.asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError()))

    db = InMemoryDB()
    # hedge_max_factor=3, initial=12 → max_allowed=12*2^2=48
    # Set position to 48 → cap blocked
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True,
        initial_contract_count=12, hedge_max_factor=3,
    )
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=48,  # at the cap
        avg_entry=90,
    )

    await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=53)

    # No chase orders placed — cap blocked
    assert len(place_calls) == 0
    sm_mod.logger.critical = real_critical


# ===========================================================================
# Termination: max-minutes timeout
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_max_minutes_termination(monkeypatch):
    """Chaser exits after max_chase_minutes is exceeded (chase_max_minutes=0 → immediate)."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    cache.quotes[ticker] = (88, 92)

    executor = FakeExecutor()

    logged = []
    import core.state_machine as sm_mod
    real_info = sm_mod.logger.info
    sm_mod.logger.info = lambda e, **kw: logged.append((e, kw))

    db = InMemoryDB()
    # chase_max_minutes=0 → max_seconds=0 → elapsed (≥0) always satisfies elapsed >= 0
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True
    )
    strategy.config.chase_max_minutes = 0
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )

    await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)

    sm_mod.logger.info = real_info
    assert any(e == "chase.cancelled" and kw.get("reason") == "max_minutes_exceeded" for e, kw in logged)


# ===========================================================================
# Termination: falling knife (ask below buy trigger)
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_falling_knife_termination(monkeypatch):
    """Chaser cancels when best ask falls below the buy trigger price."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    cache.quotes[ticker] = (88, 92)

    # Set an orderbook where ask is below the buy trigger (82)
    from core.types import OrderBook, OrderBookLevel
    cache.orderbooks[ticker] = OrderBook(
        yes_asks=[OrderBookLevel(price=70, quantity=5, order_count=1)],
        yes_bids=[OrderBookLevel(price=60, quantity=5, order_count=1)],
    )

    executor = FakeExecutor()
    logged = []
    import core.state_machine as sm_mod
    real_info = sm_mod.logger.info
    sm_mod.logger.info = lambda e, **kw: logged.append((e, kw))

    monkeypatch.setattr(sm_mod.asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError()))

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch, db=db, executor=executor,
        partial_fill_chase=True,
        buy_trigger_price_low=82,
    )
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )

    await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)

    sm_mod.logger.info = real_info
    assert any(e == "chase.cancelled" and kw.get("reason") == "falling_knife" for e, kw in logged)


# ===========================================================================
# SL trigger cancels chaser before sell
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_cancelled_before_stop_loss(monkeypatch):
    """Stop-loss execution cancels the chaser task BEFORE placing the SL sell."""
    ticker = "KXLOWTPHX-26AUG17-T88"

    executor = FakeExecutor()
    executor.sell_success = True

    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTPHX",
            side="yes",
            quantity=7,
            avg_entry_price=90,
            last_price=90,
        )
    ])

    strategy = make_strategy(
        monkeypatch, db=db, executor=executor, partial_fill_chase=True
    )

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy._app_owned_qty[ticker] = 7
    strategy._reconciliation_complete = True

    # Track call ordering via a shared counter
    cancel_chaser_called = {"v": False}
    orig_cancel_chaser = strategy._cancel_chaser_for_ticker

    async def tracked_cancel_chaser(tick, reason):
        cancel_chaser_called["v"] = True
        return await orig_cancel_chaser(tick, reason)

    strategy._cancel_chaser_for_ticker = tracked_cancel_chaser

    # Create a fake already-done chaser task so the branch triggers
    async def instant_done():
        return

    task = asyncio.create_task(instant_done())
    await asyncio.sleep(0)  # let it finish so it's done
    # Now replace with a real-looking task that is NOT done yet
    async def slow_chase():
        await asyncio.sleep(9999)

    chase_task = asyncio.create_task(slow_chase())
    strategy._chase_tasks[ticker] = chase_task

    await strategy._execute_stop_loss(bracket)

    # Cancel the lingering task to avoid test pollution
    if not chase_task.done():
        chase_task.cancel()
        try:
            await chase_task
        except asyncio.CancelledError:
            pass

    assert cancel_chaser_called["v"], "_cancel_chaser_for_ticker was not called during _execute_stop_loss"
    # After SL, chaser should be removed from _chase_tasks
    assert ticker not in strategy._chase_tasks


# ===========================================================================
# One-chaser-per-ticker: new entry cancels old chaser
# ===========================================================================


@pytest.mark.asyncio
async def test_one_chaser_per_ticker(monkeypatch):
    """Starting a new chaser for a ticker cancels the previous one."""
    ticker = "KXLOWTPHX-26AUG17-T88"

    strategy = make_strategy(monkeypatch, partial_fill_chase=True)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )
    strategy.brackets[ticker] = bracket

    # Track the "first" task identity
    first_task_ref = {"task": None}

    async def slow_first_chase(*args, **kwargs):
        await asyncio.sleep(9999)

    async def instant_second_chase(*args, **kwargs):
        return

    call_count = {"n": 0}

    async def mock_chase_loop(bkt, remaining, intended_quantity):
        call_count["n"] += 1
        if call_count["n"] == 1:
            await slow_first_chase()
        else:
            await instant_second_chase()

    monkeypatch.setattr(strategy, "_partial_fill_chase_loop", mock_chase_loop)

    # Start first chaser
    await strategy._maybe_start_chaser(bracket, remaining=5, intended_quantity=12)
    first_task = strategy._chase_tasks.get(ticker)
    assert first_task is not None
    first_task_ref["task"] = first_task

    # Start second chaser — should cancel first
    await strategy._maybe_start_chaser(bracket, remaining=3, intended_quantity=12)

    await asyncio.sleep(0)

    # First task should be cancelled
    assert first_task_ref["task"].cancelled() or first_task_ref["task"].done(), \
        "First chaser task was not cancelled"
    # Only one task per ticker at a time
    current = strategy._chase_tasks.get(ticker)
    assert current is None or current is not first_task_ref["task"]


# ===========================================================================
# chase.started / chase.filled log events
# ===========================================================================


@pytest.mark.asyncio
async def test_chaser_logs_started_and_filled(monkeypatch):
    """chase.started is logged when chaser begins; chase.filled when done."""
    ticker = "KXLOWTPHX-26AUG17-T88"
    cache = TickerCache()
    cache.quotes[ticker] = (88, 92)

    executor = FakeExecutor()
    oid_ref = {}

    async def tracked_place(order):
        oid = "fill-oid-2"
        oid_ref["oid"] = oid
        executor._resting_orders[oid] = {
            "ticker": order.market_ticker,
            "price": order.price,
            "quantity": order.quantity,
        }
        executor._order_fill_info_responses[oid] = [
            {"status": "filled", "fill_qty": 5, "fill_price": 89}
        ]
        return ExecutionResult(
            success=True, market_ticker=order.market_ticker, side="yes",
            price=order.price, quantity=order.quantity,
            fill_price=0, fill_quantity=0, total_cost_cents=0,
            order_id=oid, status="RESTING",
        )

    executor.place_limit_buy = tracked_place

    logged = []
    import core.state_machine as sm_mod
    real_info = sm_mod.logger.info
    sm_mod.logger.info = lambda e, **kw: logged.append((e, kw))

    call_count = {"n": 0}

    async def sleep_limited(secs):
        call_count["n"] += 1
        if call_count["n"] > 3:
            raise asyncio.CancelledError()

    monkeypatch.setattr(sm_mod.asyncio, "sleep", sleep_limited)

    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, db=db, executor=executor, partial_fill_chase=True)
    strategy.cache = cache

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTPHX",
        bracket_label="entry",
        phase=Phase.HOLDING,
        position_quantity=7,
        avg_entry=90,
    )
    strategy.active_positions[ticker] = bracket
    strategy._app_owned_qty[ticker] = 7

    try:
        await strategy._partial_fill_chase_loop(bracket, remaining=5, intended_quantity=12)
    except (asyncio.CancelledError, Exception):
        pass

    sm_mod.logger.info = real_info

    events = [e for e, _ in logged]
    assert "chase.started" in events
    assert "chase.filled" in events


# ===========================================================================
# PaperTradeExecutor: resting order simulation
# ===========================================================================


@pytest.mark.asyncio
async def test_paper_executor_place_limit_buy():
    """PaperTradeExecutor.place_limit_buy places a resting order."""
    from execution.paper import PaperTradeExecutor

    cache = TickerCache()
    exec_ = PaperTradeExecutor(ticker_cache=cache, initial_balance_cents=10_000)

    order = OrderRequest(
        market_ticker="KXLOWTPHX-26AUG17-T88",
        side=OrderSide.BUY_YES,
        price=89,
        quantity=5,
    )
    result = await exec_.place_limit_buy(order)
    assert result.success
    assert result.status == "RESTING"
    assert result.order_id in exec_._resting_buy_orders
    # Balance reduced by reserved cost
    assert exec_.balance_cents == 10_000 - 89 * 5


@pytest.mark.asyncio
async def test_paper_executor_fill_when_ask_crosses():
    """Paper resting buy fills when ask ≤ bid_price."""
    from execution.paper import PaperTradeExecutor

    cache = TickerCache()
    # Initial ask = 95 (above our bid of 89)
    cache.orderbooks["KXLOWTPHX-26AUG17-T88"] = OrderBook(
        yes_asks=[OrderBookLevel(price=95, quantity=10, order_count=1)],
        yes_bids=[OrderBookLevel(price=80, quantity=5, order_count=1)],
    )
    exec_ = PaperTradeExecutor(ticker_cache=cache, initial_balance_cents=10_000)

    order = OrderRequest(
        market_ticker="KXLOWTPHX-26AUG17-T88",
        side=OrderSide.BUY_YES,
        price=89,
        quantity=5,
    )
    result = await exec_.place_limit_buy(order)
    oid = result.order_id

    # Still resting (ask > bid)
    info = await exec_.get_order_fill_info(oid)
    assert info["status"] == "resting"
    assert info["fill_qty"] == 0

    # Ask drops to 87 ≤ 89 → should fill
    cache.orderbooks["KXLOWTPHX-26AUG17-T88"] = OrderBook(
        yes_asks=[OrderBookLevel(price=87, quantity=10, order_count=1)],
        yes_bids=[OrderBookLevel(price=80, quantity=5, order_count=1)],
    )
    info = await exec_.get_order_fill_info(oid)
    assert info["status"] == "filled"
    assert info["fill_qty"] == 5
    assert info["fill_price"] == 87

    # Position updated
    pos = exec_.positions.get("KXLOWTPHX-26AUG17-T88")
    assert pos is not None
    assert pos["quantity"] == 5
    assert pos["avg_entry_price"] == 87


@pytest.mark.asyncio
async def test_paper_executor_cancel_resting_buy():
    """PaperTradeExecutor.cancel_order cancels a resting buy and refunds balance."""
    from execution.paper import PaperTradeExecutor

    cache = TickerCache()
    exec_ = PaperTradeExecutor(ticker_cache=cache, initial_balance_cents=10_000)

    order = OrderRequest(
        market_ticker="KXLOWTPHX-26AUG17-T88",
        side=OrderSide.BUY_YES,
        price=89,
        quantity=5,
    )
    result = await exec_.place_limit_buy(order)
    oid = result.order_id
    assert exec_.balance_cents == 10_000 - 89 * 5

    # Cancel
    ok = await exec_.cancel_order(oid)
    assert ok
    assert oid not in exec_._resting_buy_orders
    # Balance refunded
    assert exec_.balance_cents == 10_000
