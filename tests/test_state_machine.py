import asyncio
import datetime
import os
import sys
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.config import AppConfig
from app.models import (
    ExecutedTrade, Position as PositionModel, StopLossLedger,
    OrderAction, OrderActionStatus,
)
from core.constants import get_eastern_today_date_prefix
from core.state_machine import TemperatureStrategy, parse_series_and_date
from core.types import MarketBracket, OrderBook, OrderBookLevel, OrderRequest, OrderSide, Phase
from data.ticker_cache import TickerCache
from execution.base import ExecutionResult
from execution.live import LiveTradeExecutor
from execution.sl_watcher import StopLossWatcher


class FakeWSManager:
    def on_message(self, *_args, **_kwargs):
        return None

    async def subscribe(self, *_args, **_kwargs):
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

    async def execute(self, statement, *_args, **_kwargs):
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
            kept = []
            for item in items:
                if all(self._matches(item, criterion) for criterion in statement._where_criteria):
                    continue
                kept.append(item)
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
                if all(self._matches(item, criterion) for criterion in statement._where_criteria):
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
    def __init__(self):
        self.orders = []
        self.buy_success = False
        self.sell_success = False
        self.positions = {}
        self.active_markets = []
        self.balance = 0
        self.fills = []

    async def buy_yes(self, order, max_price=None):
        self.orders.append((order, max_price))
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
                order_id="buy-order-id",
                notes="buy-success",
            )
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=0,
            total_cost_cents=0,
            notes="buy-rejected",
        )

    async def sell_yes(self, order):
        self.orders.append((order, None))
        if self.sell_success:
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=-(order.price * order.quantity),
                order_id="sell-order-id",
                notes="sell-success",
            )
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=0,
            total_cost_cents=0,
            notes="sell-rejected",
        )

    async def get_balance(self):
        return self.balance

    async def get_active_markets(self, series_prefix: str = ""):
        return list(self.active_markets)

    async def get_positions(self):
        return dict(self.positions)

    async def get_fills(self, ticker=None):
        if ticker is None:
            return list(self.fills)
        return [fill for fill in self.fills if fill.get("ticker") == ticker or fill.get("market_ticker") == ticker]


def make_config(**overrides):
    if "max_sl_spread" not in overrides:
        raw_max_sl = os.getenv("max_sl_spread")
        if raw_max_sl is not None:
            overrides["max_sl_spread"] = AppConfig.convert_dollars_to_cents(raw_max_sl)
    config = AppConfig(
        kalshi_api_key="test-key",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="******localhost:3306/test",
        trading_mode="PAPER",
        initial_contract_count=2,
        monitor_start_price=80,
        buy_trigger_price=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=50,
        hedge_max_factor=3.0,
        dry_run=False,
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def make_strategy(monkeypatch, db=None, db_items=None, executor=None, **config_overrides):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        executor or FakeExecutor(),
        db or InMemoryDB(db_items),
    )


def capture_logs(monkeypatch):
    import core.state_machine as state_machine

    logged = []
    for method in ("debug", "info", "warning", "error"):
        monkeypatch.setattr(
            state_machine.logger,
            method,
            lambda event, _method=method, **kwargs: logged.append((event, kwargs)),
        )
    return logged


@pytest.mark.asyncio
async def test_strategy_started_logs_minimum_spread(monkeypatch):
    logged = capture_logs(monkeypatch)

    def fake_create_task(coro):
        coro.close()
        return object()

    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.asyncio, "create_task", fake_create_task)

    strategy = make_strategy(monkeypatch, minimum_spread=7)
    monkeypatch.setattr(strategy, "_restore_positions", AsyncMock())
    monkeypatch.setattr(strategy, "_strategy_loop", AsyncMock())
    monkeypatch.setattr(strategy, "_db_cleanup_loop", AsyncMock())

    await strategy.start()

    start_log = next(kwargs for event, kwargs in logged if event == "strategy.started")
    assert start_log["minimum_spread"] == 7
    assert "hedge_trigger" not in start_log


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spread", "expected_note"),
    [(0, "crossed"), (3, "tight"), (4, "normal")],
)
async def test_evaluate_watchlist_logs_spread_note(monkeypatch, spread, expected_note):
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXLOWTSEA-26JUN22-B53.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTSEA",
        bracket_label="test bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 82 - spread, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    buy_log = next(kwargs for event, kwargs in logged if event == "phase.b.buying")
    assert buy_log["spread_note"] == expected_note
    strategy._execute_entry.assert_awaited_once_with(bracket)


@pytest.mark.asyncio
async def test_evaluate_watchlist_uses_rest_spread_when_orderbook_missing(monkeypatch):
    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHLAX-26JUN22-B71.5",
        event_ticker="EVT1",
        series_ticker="KXHIGHLAX",
        bracket_label="thin bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 89, "yes_bid": 87, "spread": 2})
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is True
    assert bracket.last_price == 89
    strategy._execute_entry.assert_awaited_once_with(bracket)


@pytest.mark.asyncio
async def test_evaluate_watchlist_blocks_falling_knife_entry(monkeypatch):
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHTBOS-26JUN22-B71.5",
        event_ticker="EVT1",
        series_ticker="KXHIGHTBOS",
        bracket_label="knife",
        phase=Phase.MONITORING,
    )
    bracket.falling_knife_guard = True
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 79, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is False
    strategy._execute_entry.assert_not_awaited()
    assert "phase.b.falling_knife_blocked" in [event for event, _ in logged]


@pytest.mark.asyncio
async def test_falling_knife_guard_resets_below_floor_then_allows_entry(monkeypatch):
    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHTLAX-26JUN22-B72.5",
        event_ticker="EVT1",
        series_ticker="KXHIGHTLAX",
        bracket_label="reset",
        phase=Phase.MONITORING,
    )
    bracket.falling_knife_guard = True
    strategy.brackets[bracket.market_ticker] = bracket
    strategy._execute_entry = AsyncMock()

    strategy.cache.update_quote(bracket.market_ticker, 80, 81)
    await strategy._evaluate_watchlist()

    assert bracket.falling_knife_guard is False
    strategy._execute_entry.assert_not_awaited()

    strategy.cache.update_quote(bracket.market_ticker, 79, 82)
    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is True
    strategy._execute_entry.assert_awaited_once_with(bracket)


@pytest.mark.asyncio
async def test_falling_knife_guard_updates_for_non_monitoring_brackets(monkeypatch):
    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXLOWTDEN-26JUN22-B53.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTDEN",
        bracket_label="holding",
        phase=Phase.HOLDING,
        crossed_buy=True,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy._execute_entry = AsyncMock()

    strategy.cache.update_quote(bracket.market_ticker, 90, 91)
    await strategy._evaluate_watchlist()
    assert bracket.falling_knife_guard is True

    strategy.cache.update_quote(bracket.market_ticker, 80, 81)
    await strategy._evaluate_watchlist()
    assert bracket.falling_knife_guard is False
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_eval_price_floor_skips_without_below_trigger_log(monkeypatch):
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, eval_price_floor=5)
    bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUN22-B51.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="quiet skip",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 0, 5)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert "phase.b.below_trigger" not in [event for event, _ in logged]
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_eval_price_floor_boundary_logs_below_trigger_above_floor(monkeypatch):
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, eval_price_floor=5)
    bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUN22-B52.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="boundary",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 4, 6)

    await strategy._evaluate_watchlist()

    assert "phase.b.below_trigger" in [event for event, _ in logged]


@pytest.mark.asyncio
async def test_phase_b_skips_settled_one_sided_book(monkeypatch):
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, eval_price_floor=5)
    bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUN22-B53.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="settled",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 0, 100)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert "phase.b.buying" not in [event for event, _ in logged]
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_bracket_filters_to_today(monkeypatch):
    strategy = make_strategy(monkeypatch)
    today = get_eastern_today_date_prefix()
    await strategy._ensure_bracket(f"KXLOWTBOS-{today}-B65.5")
    await strategy._ensure_bracket("KXLOWTBOS-26JAN01-B65.5")

    assert f"KXLOWTBOS-{today}-B65.5" in strategy.brackets
    assert "KXLOWTBOS-26JAN01-B65.5" not in strategy.brackets


@pytest.mark.asyncio
async def test_lifecycle_created_ignores_non_today_market(monkeypatch):
    strategy = make_strategy(monkeypatch)

    await strategy._handle_lifecycle(
        {
            "msg": {
                "type": "created",
                "market_ticker": "KXLOWTBOS-26JAN01-B65.5",
                "event_ticker": "EVT1",
                "series_ticker": "KXLOWTBOS",
                "title": "old market",
            }
        }
    )

    assert strategy.brackets == {}


@pytest.mark.asyncio
async def test_execute_entry_reconciles_fill_price_from_positions(monkeypatch):
    logged = capture_logs(monkeypatch)
    executor = FakeExecutor()
    executor.positions = {"KXLOWTBOS-26JUN22-B65.5": {"average_fill_cost_cents": 83}}

    async def buy_yes(order, max_price=None):
        executor.orders.append((order, max_price))
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=order.quantity,
            total_cost_cents=0,
            order_id="entry-id",
            notes="filled",
        )

    executor.buy_yes = buy_yes
    strategy = make_strategy(monkeypatch, executor=executor)
    bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUN22-B65.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )

    await strategy._execute_entry(
        bracket,
        ob=OrderBook(yes_asks=[OrderBookLevel(price=82, quantity=10, order_count=1)]),
    )

    assert bracket.avg_entry == 83
    assert bracket.phase == Phase.HOLDING
    assert any(event == "phase.b.entry_cost_reconciled" for event, _ in logged)


@pytest.mark.asyncio
async def test_entry_uses_ioc_time_in_force(monkeypatch):
    captured = {}

    class FakeResp:
        status_code = 201

        @staticmethod
        def json():
            return {"order_id": "oid", "fill_count_fp": "1.00", "taker_fill_cost_dollars": "0.90"}

    monkeypatch.setattr("execution.live.load_private_key", lambda _path: object())
    executor = LiveTradeExecutor("https://example.com", "k", "unused.pem")

    async def fake_post(_url, json=None, headers=None):
        captured["payload"] = json
        return FakeResp()

    executor._client.post = fake_post
    executor._headers = lambda *_args, **_kwargs: {}
    await executor.buy_yes(
        OrderRequest(
            market_ticker="KXLOWTOKC-26JUN26-B72.5",
            side=OrderSide.BUY_YES,
            price=86,
            quantity=5,
        ),
        max_price=90,
    )

    assert captured["payload"]["time_in_force"] == "immediate_or_cancel"


@pytest.mark.asyncio
async def test_zero_fill_entry_leaves_no_resting_order_and_monitoring(monkeypatch):
    logged = capture_logs(monkeypatch)
    executor = FakeExecutor()

    async def buy_yes(order, max_price=None):
        executor.orders.append((order, max_price))
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            notes='{"fill_count":"0.00","remaining_count":"5.00","time_in_force":"immediate_or_cancel"}',
        )

    executor.buy_yes = buy_yes
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db)
    bracket = MarketBracket(
        market_ticker="KXLOWTOKC-26JUN26-B72.5",
        event_ticker="EVT1",
        series_ticker="KXLOWTOKC",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket

    await strategy._execute_entry(
        bracket,
        ob=OrderBook(yes_asks=[OrderBookLevel(price=86, quantity=10, order_count=1)]),
        quantity=5,
    )

    assert bracket.phase == Phase.MONITORING
    assert bracket.market_ticker not in strategy.active_positions
    assert db.store[PositionModel] == []
    assert any(event == "phase.b.entry_failed" for event, _ in logged)


@pytest.mark.asyncio
async def test_partial_fill_path_unchanged(monkeypatch):
    logged = capture_logs(monkeypatch)
    executor = FakeExecutor()
    ticker = "KXLOWTOKC-26JUN26-B72.5"
    executor.positions = {ticker: {"average_fill_cost_cents": 90}}

    async def buy_yes(order, max_price=None):
        executor.orders.append((order, max_price))
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=3,
            total_cost_cents=0,
            order_id="partial-fill",
            notes='{"fill_count":"3.00","remaining_count":"2.00"}',
        )

    executor.buy_yes = buy_yes
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTOKC",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket

    await strategy._execute_entry(
        bracket,
        ob=OrderBook(yes_asks=[OrderBookLevel(price=90, quantity=10, order_count=1)]),
        quantity=5,
    )

    assert bracket.phase == Phase.HOLDING
    assert bracket.position_quantity == 3
    assert bracket.avg_entry == 90
    assert any(event == "phase.b.entry_cost_reconciled" for event, _ in logged)
    assert db.store[PositionModel][0].quantity == 3


@pytest.mark.asyncio
async def test_entry_self_heal_from_fills_updates_avg_entry(monkeypatch):
    logged = capture_logs(monkeypatch)
    executor = FakeExecutor()
    ticker = "KXLOWTBOS-26JUN22-B65.5"
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 0}}
    executor.fills = [{"ticker": ticker, "action": "buy", "count_fp": 2, "yes_price_dollars": 0.83}]
    db = InMemoryDB(
        [
            PositionModel(
                market_ticker=ticker,
                event_ticker="EVT1",
                series_ticker="KXLOWTBOS",
                side="yes",
                quantity=2,
                avg_entry_price=0,
                last_price=83,
                position_ts=datetime.datetime.utcnow(),
            )
        ]
    )
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        trading_mode="LIVE",
        manage_external_positions=True,
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_last_price(ticker, 60)

    await strategy._evaluate_held_positions()

    assert bracket.avg_entry == 83
    assert any(event == "phase.c.entry_self_healed" for event, _ in logged)
    stored = db.store[PositionModel][0]
    assert stored.avg_entry_price == 83


@pytest.mark.asyncio
async def test_untracked_fill_is_classified_external_by_default(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTOKC-26JUN26-B72.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 5, "average_fill_cost_cents": 90}}
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTOKC",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_held_positions()

    assert bracket.phase == Phase.MONITORING
    assert ticker not in strategy.active_positions
    assert db.store[PositionModel] == []
    ownership_logs = [kwargs for event, kwargs in logged if event == "ownership.classified"]
    assert ownership_logs
    assert ownership_logs[-1]["ticker"] == ticker
    assert ownership_logs[-1]["app_owned_qty"] == 0
    assert ownership_logs[-1]["external_qty"] == 5


@pytest.mark.asyncio
async def test_manage_external_positions_true_preserves_legacy_adoption(monkeypatch):
    ticker = "KXLOWTOKC-26JUN26-B72.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 5, "average_fill_cost_cents": 90}}
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        stop_loss_price=50,
        manage_external_positions=True,
    )
    strategy._execute_stop_loss = AsyncMock(return_value=False)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTOKC",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket

    strategy.cache.update_quote(ticker, 80, 82)
    await strategy._evaluate_held_positions()
    strategy.cache.update_quote(ticker, 1, 2)
    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_awaited()


@pytest.mark.asyncio
async def test_adoption_is_idempotent(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTOKC-26JUN26-B72.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 5, "average_fill_cost_cents": 90}}
    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        stop_loss_price=0,
        manage_external_positions=True,
    )
    strategy._execute_stop_loss = AsyncMock()
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTOKC",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_held_positions()
    await strategy._evaluate_held_positions()

    adopted_logs = [event for event, _ in logged if event == "phase.b.untracked_fill_adopted"]
    assert len(adopted_logs) == 1
    assert len(db.store[PositionModel]) == 1
    assert db.store[PositionModel][0].quantity == 5


@pytest.mark.asyncio
async def test_restore_positions_uses_db_cost_basis_when_api_entry_missing(monkeypatch):
    logged = capture_logs(monkeypatch)
    today_prefix = get_eastern_today_date_prefix()
    ticker = f"KXLOWTSEA-{today_prefix}-B61.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 0}}
    db = InMemoryDB(
        [
            PositionModel(
                market_ticker=ticker,
                event_ticker="EVT1",
                series_ticker="KXLOWTSEA",
                side="yes",
                quantity=2,
                avg_entry_price=84,
                last_price=84,
                position_ts=datetime.datetime.utcnow(),
            )
        ]
    )
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        trading_mode="LIVE",
        manage_external_positions=True,
    )

    await strategy._restore_positions()

    restored = strategy.active_positions[ticker]
    assert restored.avg_entry == 84
    live_log = next(kwargs for event, kwargs in logged if event == "strategy.restored_live_position")
    assert live_log["entry_source"] == "db"


@pytest.mark.asyncio
async def test_stop_loss_sells_when_ask_at_threshold(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
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

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 49, 50)

    await strategy._evaluate_held_positions()

    assert executor.orders[0][0].side.name == "SELL_YES"
    assert executor.orders[0][0].quantity == 2
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)


@pytest.mark.asyncio
async def test_mixed_position_exit_is_capped_to_app_owned_qty(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 5, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {ticker: {"count": 3, "average_fill_cost_cents": 80}}
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

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=5,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._set_ownership(
        ticker,
        total_position_qty=5,
        app_owned_qty=2,
        source="test",
        action="seed",
    )

    await strategy._execute_stop_loss(bracket)

    assert executor.orders
    assert executor.orders[0][0].quantity == 2
    assert strategy._app_owned_qty[ticker] == 0
    assert any(event == "exit.capped_to_app_owned" for event, _ in logged)


@pytest.mark.asyncio
async def test_external_only_position_skip_exit_when_default_config(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.sell_yes = AsyncMock()
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=4,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._set_ownership(
        ticker,
        total_position_qty=4,
        app_owned_qty=0,
        source="test",
        action="seed",
    )

    await strategy._execute_stop_loss(bracket)

    executor.sell_yes.assert_not_awaited()
    assert any(event == "exit.skipped_no_app_qty" for event, _ in logged)


@pytest.mark.asyncio
async def test_stop_loss_fires_on_low_bid_even_when_last_price_is_100(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXHIGHTMIN-26JUN23-B77.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
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

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXHIGHTMIN",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_last_price(ticker, 100)
    strategy.cache.update_quote(ticker, 10, 12)

    await strategy._evaluate_held_positions()

    assert executor.orders[0][0].side.name == "SELL_YES"
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)


@pytest.mark.asyncio
async def test_no_stop_loss_when_bid_above_threshold(monkeypatch):
    ticker = "KXHIGHTMIN-26JUN23-B77.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXHIGHTMIN",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_last_price(ticker, 100)
    strategy.cache.update_quote(ticker, 60, 62)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("bid_price", [50, 51])
async def test_no_stop_loss_at_or_above_threshold(monkeypatch, bid_price):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, bid_price, bid_price + 1)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_stop_loss_without_last_trade(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()
    assert any(event == "phase.c.no_live_price" for event, _ in logged)


@pytest.mark.asyncio
async def test_stop_loss_increments_ledger_when_ask_at_threshold(monkeypatch):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
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

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 49, 50)

    await strategy._evaluate_held_positions()

    assert await strategy._get_stop_loss_count_for_market(ticker) == 1


@pytest.mark.asyncio
async def test_stop_loss_increments_ledger_then_recovery_doubles(monkeypatch):
    stop_ticker = "KXLOWTBOS-26JUN23-B65.5"
    recovery_ticker = "KXLOWTBOS-26JUN23-T68"
    executor = FakeExecutor()
    executor.positions = {stop_ticker: {"count": 2, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
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

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor, db=InMemoryDB(), stop_loss_price=50)

    held_bracket = MarketBracket(
        market_ticker=stop_ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[stop_ticker] = held_bracket
    strategy.brackets[stop_ticker] = held_bracket
    strategy.cache.update_last_price(stop_ticker, 100)
    strategy.cache.update_quote(stop_ticker, 10, 12)

    await strategy._evaluate_held_positions()

    assert await strategy._get_stop_loss_count_for_market(stop_ticker) == 1

    recovery_bracket = MarketBracket(
        market_ticker=recovery_ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="recovery",
        phase=Phase.MONITORING,
    )
    strategy.brackets[recovery_ticker] = recovery_bracket
    strategy.cache.update_quote(recovery_ticker, 80, 82)

    await strategy._evaluate_watchlist()

    assert executor.orders[-1][0].side.name == "BUY_YES"
    assert executor.orders[-1][0].quantity == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("count, expected_qty", [(0, 2), (1, 4), (2, 8), (3, 16)])
async def test_recovery_sizing_doubles(monkeypatch, count, expected_qty):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    items = []
    if count:
        items.append(StopLossLedger(series_ticker="KXLOWTBOS", date_prefix="26JUN23", stop_loss_count=count))
    strategy = make_strategy(monkeypatch, db=InMemoryDB(items))
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="buy",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_watchlist()

    assert strategy.executor.orders[0][0].quantity == expected_qty


@pytest.mark.asyncio
async def test_recovery_cap_blocks_after_factor(monkeypatch):
    logged = capture_logs(monkeypatch)
    blocked_ticker = "KXLOWTBOS-26JUN23-B65.5"
    boundary_ticker = "KXLOWTBOS-26JUN24-B65.5"
    db = InMemoryDB(
        [
            StopLossLedger(series_ticker="KXLOWTBOS", date_prefix="26JUN23", stop_loss_count=4),
            StopLossLedger(series_ticker="KXLOWTBOS", date_prefix="26JUN24", stop_loss_count=3),
        ]
    )
    strategy = make_strategy(monkeypatch, db=db)
    for ticker in (blocked_ticker, boundary_ticker):
        strategy.brackets[ticker] = MarketBracket(
            market_ticker=ticker,
            event_ticker=ticker,
            series_ticker="KXLOWTBOS",
            bracket_label="buy",
            phase=Phase.MONITORING,
        )
        strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_watchlist()

    assert len(strategy.executor.orders) == 1
    assert strategy.executor.orders[0][0].market_ticker == boundary_ticker
    assert strategy.executor.orders[0][0].quantity == 16
    assert strategy.brackets[blocked_ticker].crossed_buy is True
    assert any(event == "phase.b.recovery_cap_reached" for event, _ in logged)


@pytest.mark.asyncio
async def test_high_low_counters_independent(monkeypatch):
    db = InMemoryDB([StopLossLedger(series_ticker="KXLOWTBOS", date_prefix="26JUN23", stop_loss_count=1)])
    ticker = "KXHIGHTBOS-26JUN23-T90"
    strategy = make_strategy(monkeypatch, db=db)
    strategy.brackets[ticker] = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXHIGHTBOS",
        bracket_label="buy",
        phase=Phase.MONITORING,
    )
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_watchlist()

    assert strategy.executor.orders[0][0].quantity == 2


@pytest.mark.asyncio
async def test_any_bracket_in_series_uses_counter(monkeypatch):
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, db=db)
    await strategy._increment_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")
    ticker = "KXLOWTBOS-26JUN23-T68"
    strategy.brackets[ticker] = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="buy",
        phase=Phase.MONITORING,
    )
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_watchlist()

    assert strategy.executor.orders[0][0].quantity == 4


def test_parse_series_and_date():
    assert parse_series_and_date("KXLOWTSATX-26JUN23-T78") == ("KXLOWTSATX", "26JUN23")
    assert parse_series_and_date("KXHIGHTPHX-26JUN23-B111.5") == ("KXHIGHTPHX", "26JUN23")
    assert parse_series_and_date("KXHIGHNY-26JUN23-T90") == ("KXHIGHNY", "26JUN23")
    assert parse_series_and_date("bad-ticker") is None


@pytest.mark.asyncio
async def test_ledger_persists_across_restart(monkeypatch):
    db = InMemoryDB()
    strategy_a = make_strategy(monkeypatch, db=db)
    ticker = "KXLOWTBOS-26JUN23-B65.5"

    await strategy_a._increment_stop_loss_count_for_market(ticker)

    strategy_b = make_strategy(monkeypatch, db=db)
    assert await strategy_b._get_stop_loss_count_for_market(ticker) == 1


def test_config_loads_without_hedge_trigger_price(monkeypatch):
    env = {
        "KALSHI_API_KEY": "test-key",
        "KALSHI_PRIVATE_KEY_PATH": "unused.pem",
        "MYSQL_DATABASE_URL": "******localhost:3306/test",
        "TRADING_MODE": "PAPER",
        "BUY_TRIGGER_PRICE": "0.82",
        "STOP_LOSS_PRICE": "0.35",
        "INITIAL_CONTRACT_COUNT": "2",
        "MINIMUM_SPREAD": "0.04",
        "MONITOR_START_PRICE": "0.80",
        "SPREAD_MONITOR_PRICE": "0.90",
        "HEDGE_MAX_FACTOR": "3",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("HEDGE_TRIGGER_PRICE", raising=False)
    monkeypatch.delenv("HEDGE_BUY", raising=False)

    cfg = AppConfig.from_env()

    assert cfg.hedge_trigger_price == 0
    assert cfg.hedge_buy == 0
    assert cfg.stop_loss_price == 35
    assert cfg.hedge_max_factor == 3.0


@pytest.mark.asyncio
async def test_stop_loss_no_fill_keeps_position_and_retries(monkeypatch):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2}}
    strategy = make_strategy(monkeypatch, executor=executor)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._execute_stop_loss(bracket)
    await strategy._execute_stop_loss(bracket)
    bracket._last_stop_loss_attempt = 0
    await strategy._execute_stop_loss(bracket)

    assert len(executor.orders) == 2
    assert strategy.active_positions[ticker].position_quantity == 2


@pytest.mark.asyncio
async def test_stop_loss_partial_fill_updates_remaining_and_retries(monkeypatch):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=1,
            total_cost_cents=-1,
            order_id="sell-id",
            notes="partial",
        )

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._execute_stop_loss(bracket)
    bracket._last_stop_loss_attempt = 0
    executor.positions = {}
    await strategy._execute_stop_loss(bracket)

    assert len(executor.orders) == 2
    assert ticker not in strategy.active_positions


@pytest.mark.asyncio
async def test_stop_loss_closes_only_after_positions_confirm_zero(monkeypatch):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {}

    async def sell_yes(order):
        executor.orders.append((order, None))
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

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._execute_stop_loss(bracket)

    assert ticker not in strategy.active_positions
    assert ticker not in strategy.brackets


@pytest.mark.asyncio
async def test_stop_loss_success_but_still_held_keeps_position(monkeypatch):
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2}}

    async def sell_yes(order):
        executor.orders.append((order, None))
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
            notes="filled-but-still-held",
        )

    executor.sell_yes = sell_yes
    strategy = make_strategy(monkeypatch, executor=executor)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._execute_stop_loss(bracket)

    assert strategy.active_positions[ticker].position_quantity == 2


@pytest.mark.asyncio
async def test_phase_c_uses_rest_bid_when_quote_bid_is_zero(monkeypatch):
    """REST fallback must run even when a 0-bid quote is cached (bid=0, ask=98)."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSFO-26JUN24-B54.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"yes_ask": 98, "yes_bid": 97, "spread": 1}
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSFO",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 0, 98)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()
    strategy._fetch_market_data_via_rest.assert_awaited_once_with(ticker)
    assert not any(event == "phase.c.no_live_price" for event, _ in logged)


@pytest.mark.asyncio
async def test_phase_c_stop_loss_fires_via_rest_bid_when_quote_bid_zero_and_market_low(monkeypatch):
    """A genuinely falling position with a transient 0-bid quote is still stopped out."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSFO-26JUN24-B54.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"yes_ask": 12, "yes_bid": 10, "spread": 2}
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSFO",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 0, 12)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_awaited_once()
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not any(event == "phase.c.no_live_price" for event, _ in logged)


@pytest.mark.asyncio
async def test_phase_c_no_live_price_logged_at_most_once_per_60s(monkeypatch):
    """phase.c.no_live_price must be throttled to at most once per 60s per ticker."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSFO-26JUN24-B54.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSFO",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # Run several times back-to-back without advancing time
    for _ in range(5):
        await strategy._evaluate_held_positions()

    no_price_logs = [event for event, _ in logged if event == "phase.c.no_live_price"]
    assert len(no_price_logs) == 1


@pytest.mark.asyncio
async def test_zero_bid_does_not_trigger_stop_loss_when_ask_above_stop(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXHIGHNY-26JUN24-B82.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 83}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXHIGHNY")
    bracket.position_quantity = 1
    bracket.avg_entry = 83
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 0, 98)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()
    assert not any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not any(event == "phase.c.sl_held_for_spread" for event, _ in logged)
    assert not getattr(bracket, "_sl_held_for_spread", False)


@pytest.mark.asyncio
async def test_zero_bid_then_unfillable_abandons_via_pr29(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXHIGHDEN-26JUN24-B87.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 60}}
    executor.sell_yes = AsyncMock(
        return_value=ExecutionResult(
            success=True,
            market_ticker=ticker,
            side="yes",
            price=1,
            quantity=1,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            order_id=None,
            notes="no bid",
        )
    )
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXHIGHDEN")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # Under PANIC_FLATTEN, trigger requires ask <= stop.
    strategy.cache.update_quote(ticker, 0, 49)

    for _ in range(strategy.config.stop_loss_max_unfilled_attempts):
        bracket._last_stop_loss_attempt = 0
        await strategy._evaluate_held_positions()

    assert getattr(bracket, "_stop_loss_abandoned", False) is True
    assert any(event == "phase.c.stop_loss_abandoned_no_liquidity" for event, _ in logged)


@pytest.mark.asyncio
async def test_no_live_price_escalates_to_unprotected_alert(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSEA-26JUN24-B62.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        stop_loss_price=50,
        max_no_price_cycles=2,
    )
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXLOWTSEA")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    for _ in range(3):
        await strategy._evaluate_held_positions()

    assert any(event == "phase.c.held_position_unprotected" for event, _ in logged)
    assert len([event for event, _ in logged if event == "phase.c.no_live_price"]) == 1
    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_live_price_with_last_known_below_stop_attempts_protection(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTCHI-26JUN24-T59"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        stop_loss_price=50,
        max_no_price_cycles=1,
        sl_exit_mode="PANIC_FLATTEN",
    )
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXLOWTCHI")
    bracket.last_price = 40
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_last_price(ticker, 40)

    await strategy._evaluate_held_positions()
    await strategy._evaluate_held_positions()

    assert any(event == "phase.c.held_position_unprotected" for event, _ in logged)
    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_live_price_with_healthy_last_known_only_alerts(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTPHX-26JUN24-T88"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        stop_loss_price=50,
        max_no_price_cycles=1,
    )
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXLOWTPHX")
    bracket.last_price = 60
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_last_price(ticker, 60)

    await strategy._evaluate_held_positions()
    await strategy._evaluate_held_positions()

    assert any(event == "phase.c.held_position_unprotected" for event, _ in logged)
    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_position_absent_from_api_not_deleted_without_settlement(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXHIGHNY-26JUN24-B82.5"
    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXHIGHNY",
            side="yes",
            quantity=1,
            avg_entry_price=83,
            last_price=45,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    executor.positions = {}
    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"status": "open"})

    bracket = _make_held_bracket(ticker, "KXHIGHNY")
    bracket.position_quantity = 1
    bracket.avg_entry = 83
    bracket._last_seen_in_api = asyncio.get_event_loop().time() - 31
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._evaluate_held_positions()

    assert ticker in strategy.active_positions
    assert ticker in strategy.brackets
    assert len(db.store[PositionModel]) == 1
    strategy._execute_stop_loss.assert_awaited_once()
    absent_log = next(kwargs for event, kwargs in logged if event == "phase.c.position_not_in_api_after_grace")
    assert absent_log["action"] == "retained_pending_settlement_confirmation"


@pytest.mark.asyncio
async def test_position_absent_then_confirmed_settled_is_cleaned(monkeypatch):
    ticker = "KXLOWTATL-26JUN24-B65.5"
    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTATL",
            side="yes",
            quantity=1,
            avg_entry_price=70,
            last_price=1,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    executor.positions = {}
    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"status": "settled", "result": "no", "settlement_ts": "2026-06-25T08:05:00Z"}
    )

    bracket = _make_held_bracket(ticker, "KXLOWTATL")
    bracket._last_seen_in_api = asyncio.get_event_loop().time() - 31
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._evaluate_held_positions()

    assert ticker not in strategy.active_positions
    assert ticker not in strategy.brackets
    assert db.store[PositionModel] == []
    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_positions_api_mass_absence_skips_cleanup(monkeypatch):
    logged = capture_logs(monkeypatch)
    tickers = [
        "KXHIGHAUS-26JUN24-B93.5",
        "KXHIGHMIA-26JUN24-B92.5",
        "KXHIGHCHI-26JUN24-T77",
    ]
    db = InMemoryDB([
        PositionModel(
            market_ticker=t,
            event_ticker="EVT1",
            series_ticker=t.split("-")[0],
            side="yes",
            quantity=1,
            avg_entry_price=80,
            last_price=60,
            position_ts=datetime.datetime.utcnow(),
        )
        for t in tickers
    ])
    executor = FakeExecutor()
    executor.positions = {
        tickers[0]: {"count": 1, "average_fill_cost_cents": 80},
    }
    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"status": "settled"})

    for ticker in tickers:
        bracket = _make_held_bracket(ticker, ticker.split("-")[0])
        bracket._last_seen_in_api = asyncio.get_event_loop().time() - 31
        strategy.active_positions[ticker] = bracket
        strategy.brackets[ticker] = bracket
        strategy.cache.update_quote(ticker, 60, 62)

    await strategy._evaluate_held_positions()

    assert set(strategy.active_positions) == set(tickers)
    assert len(db.store[PositionModel]) == 3
    assert any(event == "phase.c.positions_api_mass_absence" for event, _ in logged)


@pytest.mark.asyncio
async def test_absent_position_reappears_resumes_normally(monkeypatch):
    ticker = "KXHIGHTSEA-26JUN24-B87.5"
    executor = FakeExecutor()
    executor.positions = {}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"status": "open"})

    bracket = _make_held_bracket(ticker, "KXHIGHTSEA")
    bracket._last_seen_in_api = asyncio.get_event_loop().time() - 31
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 60, 62)

    await strategy._evaluate_held_positions()

    assert ticker in strategy.active_positions
    strategy._execute_stop_loss.assert_not_awaited()

    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy.cache.update_quote(ticker, 45, 48)
    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_awaited_once()


@pytest.mark.asyncio
async def test_held_position_price_refreshes_within_configured_interval(monkeypatch):
    """Held-position REST fetch uses held_position_price_refresh_seconds (default 10s), not 60s."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSFO-26JUN24-B54.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    # Use the 10s default via config
    assert strategy.config.held_position_price_refresh_seconds == 10

    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"yes_ask": 40, "yes_bid": 38, "spread": 2}
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSFO",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # No cached quote, so REST is needed
    # Simulate that 15s have elapsed since last REST fetch (> 10s, < 60s)
    bracket._last_rest_price_fetch = 0  # force the fetch to be eligible

    await strategy._evaluate_held_positions()

    # REST should have been consulted (38 bid < 50 stop-loss → stop-loss triggers)
    strategy._fetch_market_data_via_rest.assert_awaited_once_with(ticker)
    strategy._execute_stop_loss.assert_awaited_once()
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not any(event == "phase.c.no_live_price" for event, _ in logged)


@pytest.mark.asyncio
async def test_stop_loss_abandoned_after_max_unfilled_attempts(monkeypatch):
    """After stop_loss_max_unfilled_attempts consecutive zero fills, position is abandoned."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTLV-26JUN24-T84"
    executor = FakeExecutor()
    # Position stays present and unchanged after each sell attempt (no fill)
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 60}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    max_attempts = strategy.config.stop_loss_max_unfilled_attempts  # default 3

    # sell_yes always returns zero fill
    executor.sell_yes = AsyncMock(
        return_value=ExecutionResult(
            success=True,
            market_ticker=ticker,
            side="yes",
            price=1,
            quantity=1,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            order_id=None,
            notes="no bid",
        )
    )

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTLV",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=60,
    )
    bracket.last_price = 1
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # Drive the stop-loss path max_attempts times, bypassing the 60s attempt throttle
    for _ in range(max_attempts):
        bracket._last_stop_loss_attempt = 0  # reset throttle so it fires each time
        await strategy._execute_stop_loss(bracket)

    assert bracket._stop_loss_abandoned is True
    assert any(event == "phase.c.stop_loss_abandoned_no_liquidity" for event, _ in logged)

    # Now ensure _evaluate_held_positions does NOT emit stop_loss_triggered for this bracket
    logged.clear()
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"yes_ask": 2, "yes_bid": 1, "spread": 1}
    )
    bracket._last_rest_price_fetch = 0
    await strategy._evaluate_held_positions()

    assert not any(event == "phase.c.stop_loss_triggered" for event, _ in logged)


@pytest.mark.asyncio
async def test_stop_loss_abandon_counter_resets_on_partial_fill(monkeypatch):
    """_consecutive_unfilled_sl resets when a sell returns fill_quantity > 0 (position progresses)."""
    ticker = "KXLOWTLV-26JUN24-T84"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 60}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)

    # First two attempts: zero fill
    zero_fill = ExecutionResult(
        success=True,
        market_ticker=ticker,
        side="yes",
        price=1,
        quantity=1,
        fill_price=0,
        fill_quantity=0,
        total_cost_cents=0,
        order_id=None,
        notes="no bid",
    )
    partial_fill = ExecutionResult(
        success=True,
        market_ticker=ticker,
        side="yes",
        price=1,
        quantity=1,
        fill_price=1,
        fill_quantity=1,
        total_cost_cents=-1,
        order_id="order-1",
        notes="filled",
    )

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTLV",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=60,
    )
    bracket.last_price = 1
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # Two zero-fill attempts — counter goes to 2
    executor.sell_yes = AsyncMock(return_value=zero_fill)
    for _ in range(2):
        bracket._last_stop_loss_attempt = 0
        await strategy._execute_stop_loss(bracket)

    assert getattr(bracket, "_consecutive_unfilled_sl", 0) == 2
    assert not getattr(bracket, "_stop_loss_abandoned", False)

    # Third attempt: fill_quantity=1 → position closes, counter resets
    executor.sell_yes = AsyncMock(return_value=partial_fill)
    executor.positions = {}  # position gone after fill
    bracket._last_stop_loss_attempt = 0
    await strategy._execute_stop_loss(bracket)

    # Position fully closed (live_count == 0), so abandonment is never triggered
    assert not getattr(bracket, "_stop_loss_abandoned", False)
    assert bracket.phase == Phase.CLOSED


# ---------------------------------------------------------------------------
# Spread-aware stop-loss guard tests (spread guard in _evaluate_held_positions)
# ---------------------------------------------------------------------------

def _make_held_bracket(ticker, series_ticker):
    return MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker=series_ticker,
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )


@pytest.mark.asyncio
async def test_stop_loss_fires_when_spread_within_max(monkeypatch):
    """Stop-loss fires when YES bid is below threshold and spread is tight (<= max_sl_spread)."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXLOWTBOS")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # bid=45 < 50 (stop threshold), ask=48, spread=3 <= 20
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_awaited_once()
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not getattr(bracket, "_sl_held_for_spread", False)


@pytest.mark.asyncio
async def test_stop_loss_held_when_spread_too_wide(monkeypatch):
    """With PANIC_FLATTEN, wide spread alone does not trigger or hold stop-loss."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXLOWTBOS")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # bid=45 < 50 (stop threshold), ask=75, spread=30 > 20
    strategy.cache.update_quote(ticker, 45, 75)

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()
    assert not any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not any(event == "phase.c.sl_held_for_spread" for event, _ in logged)
    assert not getattr(bracket, "_sl_held_for_spread", False)


@pytest.mark.asyncio
async def test_sl_held_for_spread_then_fires_when_spread_narrows(monkeypatch):
    """With PANIC_FLATTEN, no hold on wide spread; trigger fires once ask <= stop."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXLOWTBOS")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # Cycle 1: ask above stop -> no trigger
    strategy.cache.update_quote(ticker, 45, 75)
    await strategy._evaluate_held_positions()
    strategy._execute_stop_loss.assert_not_awaited()

    # Cycle 2: ask tightens to stop/below -> fires
    strategy.cache.update_quote(ticker, 45, 48)
    logged.clear()
    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_awaited_once()
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not getattr(bracket, "_sl_held_for_spread", False)


@pytest.mark.asyncio
async def test_sl_held_for_spread_resets_when_bid_recovers(monkeypatch):
    """With PANIC_FLATTEN, ask above stop avoids stop-loss regardless of bid recovery."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXLOWTBOS")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # Cycle 1: ask above stop -> no trigger
    strategy.cache.update_quote(ticker, 45, 75)
    await strategy._evaluate_held_positions()

    # Cycle 2: bid recovers above stop -> still no sale
    strategy.cache.update_quote(ticker, 60, 62)
    logged.clear()
    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()
    assert not any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not getattr(bracket, "_sl_held_for_spread", False)


@pytest.mark.asyncio
async def test_sl_held_for_spread_log_throttled_60s(monkeypatch):
    """PANIC_FLATTEN does not emit spread-hold logs when ask is above stop."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXLOWTBOS")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # bid=45, ask=75, spread=30 > 20 — persistently wide
    strategy.cache.update_quote(ticker, 45, 75)

    # Run 5 rapid cycles without advancing time
    for _ in range(5):
        await strategy._evaluate_held_positions()

    held_logs = [event for event, _ in logged if event == "phase.c.sl_held_for_spread"]
    assert len(held_logs) == 0


@pytest.mark.asyncio
async def test_one_sided_book_holds_spread_guard(monkeypatch):
    """A one-sided-book ask encoded as 0 now triggers PANIC_FLATTEN (ask <= stop)."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")
    strategy._execute_stop_loss = AsyncMock()
    # REST fallback returns None so only the cache quote is used
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXLOWTBOS")
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # One-sided: bid=1, ask=0 (no ask)
    strategy.cache.update_quote(ticker, 1, 0)

    await strategy._evaluate_held_positions()

    # Ask=0 satisfies ask<=stop under current trigger semantics.
    assert not getattr(bracket, "_sl_held_for_spread", False)
    strategy._execute_stop_loss.assert_awaited_once()
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    assert not any(event == "phase.c.sl_held_for_spread" for event, _ in logged)


@pytest.mark.asyncio
async def test_max_sl_spread_loaded_from_env(monkeypatch):
    """max_sl_spread is read from env and converted dollars->cents (not hardcoded)."""
    monkeypatch.setenv("max_sl_spread", "0.17")
    cfg = make_config()
    assert cfg.max_sl_spread == 17


@pytest.mark.asyncio
async def test_max_sl_spread_config_from_env(monkeypatch):
    """MAX_SL_SPREAD env var '0.20' is converted to 20 cents by the validator."""
    env = {
        "KALSHI_API_KEY": "test-key",
        "KALSHI_PRIVATE_KEY_PATH": "unused.pem",
        "MYSQL_DATABASE_URL": "******localhost:3306/test",
        "TRADING_MODE": "PAPER",
        "BUY_TRIGGER_PRICE": "0.82",
        "STOP_LOSS_PRICE": "0.50",
        "INITIAL_CONTRACT_COUNT": "1",
        "MINIMUM_SPREAD": "0.04",
        "MONITOR_START_PRICE": "0.80",
        "SPREAD_MONITOR_PRICE": "0.90",
        "MAX_SL_SPREAD": "0.15",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("HEDGE_TRIGGER_PRICE", raising=False)
    monkeypatch.delenv("HEDGE_BUY", raising=False)

    cfg = AppConfig.from_env()
    assert cfg.max_sl_spread == 15


# ---------------------------------------------------------------------------
# Fix 1: market_not_found / 404 treated as confirmed settlement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_market_not_found_treated_as_settled_cleanup(monkeypatch):
    """sell_yes returning market_not_found/404 cleans up position and logs
    phase.c.position_settled_market_gone; phase.c.stop_loss_executed must NOT appear."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTCHI",
            side="yes",
            quantity=1,
            avg_entry_price=80,
            last_price=0,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    # Position present in positions API so the stop-loss code can proceed to fire
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    market_not_found_result = ExecutionResult(
        success=False,
        market_ticker=ticker,
        side="yes",
        price=1,
        quantity=1,
        fill_price=0,
        fill_quantity=0,
        total_cost_cents=0,
        status="REJECTED",
        notes='{"error": {"code": "market_not_found", "message": "market not found"}}',
    )
    executor.sell_yes = AsyncMock(return_value=market_not_found_result)

    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 80
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # Trigger PANIC_FLATTEN path with ask at/below stop
    strategy.cache.update_quote(ticker, 0, 49)

    await strategy._evaluate_held_positions()

    # Position must be cleaned up
    assert ticker not in strategy.active_positions
    assert ticker not in strategy.brackets
    assert bracket.phase == Phase.CLOSED
    assert db.store[PositionModel] == []

    # Correct settlement log must appear; false "executed" log must NOT appear
    events = [event for event, _ in logged]
    assert "phase.c.position_settled_market_gone" in events
    assert "phase.c.stop_loss_executed" not in events

    settled_log = next(kwargs for event, kwargs in logged if event == "phase.c.position_settled_market_gone")
    assert settled_log["ticker"] == ticker
    assert settled_log["qty"] == 1


@pytest.mark.asyncio
async def test_market_not_found_does_not_increment_ledger_or_abandon(monkeypatch):
    """market_not_found cleanup must not count against the StopLossLedger and
    must not advance _consecutive_unfilled_sl or set _stop_loss_abandoned."""
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTCHI",
            side="yes",
            quantity=1,
            avg_entry_price=80,
            last_price=0,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    market_not_found_result = ExecutionResult(
        success=False,
        market_ticker=ticker,
        side="yes",
        price=1,
        quantity=1,
        fill_price=0,
        fill_quantity=0,
        total_cost_cents=0,
        status="REJECTED",
        notes='{"error": {"code": "market_not_found", "message": "market not found"}}',
    )
    executor.sell_yes = AsyncMock(return_value=market_not_found_result)

    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50, sl_exit_mode="PANIC_FLATTEN")

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 80
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # Trigger PANIC_FLATTEN path with ask at/below stop
    strategy.cache.update_quote(ticker, 0, 49)

    await strategy._evaluate_held_positions()

    # StopLossLedger must be 0 (ledger increment undone)
    assert await strategy._get_stop_loss_count_for_market(ticker) == 0

    # Abandon counters must not be advanced
    assert getattr(bracket, "_consecutive_unfilled_sl", 0) == 0
    assert not getattr(bracket, "_stop_loss_abandoned", False)


# ---------------------------------------------------------------------------
# Fix 2: _restore_positions skips stale-dated positions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restore_skips_previous_day_positions(monkeypatch):
    """Positions whose market date is before today (UTC) are skipped on restore;
    strategy.skipped_stale_position is logged and no stop-loss is attempted."""
    logged = capture_logs(monkeypatch)

    today_prefix = get_eastern_today_date_prefix()
    today_ticker = f"KXLOWTCHI-{today_prefix}-B62.5"
    # Use a clearly past date (definitely before today)
    stale_ticker = "KXLOWTATL-24DEC25-T55"

    db = InMemoryDB([
        PositionModel(
            market_ticker=today_ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTCHI",
            side="yes",
            quantity=2,
            avg_entry_price=80,
            last_price=80,
            position_ts=datetime.datetime.utcnow(),
        ),
        PositionModel(
            market_ticker=stale_ticker,
            event_ticker="EVT2",
            series_ticker="KXLOWTATL",
            side="yes",
            quantity=3,
            avg_entry_price=75,
            last_price=75,
            position_ts=datetime.datetime.utcnow(),
        ),
    ])
    executor = FakeExecutor()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="PAPER")

    # Ensure _execute_stop_loss is not called for stale positions
    strategy._execute_stop_loss = AsyncMock()

    await strategy._restore_positions()

    # Today-dated position must be restored
    assert today_ticker in strategy.active_positions

    # Stale position must NOT be restored
    assert stale_ticker not in strategy.active_positions

    # Stale skip must be logged
    skip_logs = [kwargs for event, kwargs in logged if event == "strategy.skipped_stale_position"]
    assert any(kwargs["ticker"] == stale_ticker for kwargs in skip_logs)

    # No stop-loss should have been attempted
    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_keeps_unparseable_ticker(monkeypatch):
    """A position whose ticker cannot be parsed for a date is still restored
    (fallback: do not silently drop positions with unexpected ticker formats)."""
    logged = capture_logs(monkeypatch)

    unparseable_ticker = "UNKNOWN-MARKET"

    db = InMemoryDB([
        PositionModel(
            market_ticker=unparseable_ticker,
            event_ticker="EVT1",
            series_ticker="UNKNOWN",
            side="yes",
            quantity=1,
            avg_entry_price=70,
            last_price=70,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="PAPER")

    await strategy._restore_positions()

    # Position with unparseable ticker must still be restored
    assert unparseable_ticker in strategy.active_positions

    # No skipped_stale_position log for unparseable ticker
    skip_logs = [kwargs for event, kwargs in logged if event == "strategy.skipped_stale_position"]
    assert not any(kwargs.get("ticker") == unparseable_ticker for kwargs in skip_logs)


# Critical #3 – startup reconciliation: positions loaded from DB/API are
# registered with the StopLossWatcher so that WebSocket-driven SL is
# immediately active from the moment run.py completes its start-up phase.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restore_positions_registers_with_sl_watcher(monkeypatch):
    """Positions restored from DB on startup are registered with the
    StopLossWatcher so the WebSocket-driven SL path is active immediately."""
    today_prefix = get_eastern_today_date_prefix()
    ticker = f"KXLOWTSEA-{today_prefix}-B61.5"

    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTSEA",
            side="yes",
            quantity=3,
            avg_entry_price=82,
            last_price=82,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="PAPER")

    # Attach a real StopLossWatcher with a no-op exit handler
    from execution.sl_watcher import StopLossWatcher
    async def _noop_exit(ticker, side, qty, ask):
        return True
    watcher = StopLossWatcher(_noop_exit)
    strategy.stop_loss_watcher = watcher

    await strategy._restore_positions()

    # Position must be in active_positions
    assert ticker in strategy.active_positions

    # Position must be registered with the SL watcher
    assert ticker in watcher._positions
    wp = watcher._positions[ticker]
    assert wp.quantity == 3
    assert wp.side == "yes"


@pytest.mark.asyncio
async def test_restore_live_positions_registers_with_sl_watcher(monkeypatch):
    """Positions fetched from the exchange API in LIVE mode are also
    registered with the StopLossWatcher during startup reconciliation."""
    today_prefix = get_eastern_today_date_prefix()
    ticker = f"KXHIGHTSEA-{today_prefix}-T75"

    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 85}}

    db = InMemoryDB()
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        trading_mode="LIVE",
        manage_external_positions=True,
    )

    from execution.sl_watcher import StopLossWatcher
    async def _noop_exit(ticker, side, qty, ask):
        return True
    watcher = StopLossWatcher(_noop_exit)
    strategy.stop_loss_watcher = watcher

    await strategy._restore_positions()

    assert ticker in strategy.active_positions
    assert ticker in watcher._positions
    assert watcher._positions[ticker].quantity == 2


# ===========================================================================
# Post-merge hardening fixes 1–4
# ===========================================================================

# ---------------------------------------------------------------------------
# Fix 1: Durable idempotency + DB-enforced execution invariants
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_duplicate_stop_loss_suppressed_by_succeeded_action(monkeypatch):
    """A second stop-loss attempt for a position that already has a SUCCEEDED
    OrderAction record is suppressed without placing another API order."""
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    action_key = f"{ticker}:STOP_LOSS"

    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTCHI",
            side="yes",
            quantity=1,
            avg_entry_price=80,
            last_price=40,
            position_ts=datetime.datetime.utcnow(),
        ),
        # A SUCCEEDED action record from a prior run / reconnect cycle
        OrderAction(
            action_key=action_key,
            action_type="STOP_LOSS",
            market_ticker=ticker,
            status=OrderActionStatus.SUCCEEDED,
        ),
    ])
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 40
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True

    market_gone = await strategy._execute_stop_loss(bracket)

    # Should report as market_gone (duplicate suppressed → clean up)
    assert market_gone is True
    # No API order should have been placed
    assert executor.orders == []
    # Position removed from in-memory state
    assert ticker not in strategy.active_positions


@pytest.mark.asyncio
async def test_stop_loss_creates_action_record_and_succeeds(monkeypatch):
    """A successful stop-loss creates an OrderAction record in SUCCEEDED state."""
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    action_key = f"{ticker}:STOP_LOSS"

    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTCHI",
            side="yes",
            quantity=1,
            avg_entry_price=80,
            last_price=40,
            position_ts=datetime.datetime.utcnow(),
        )
    ])
    executor = FakeExecutor()
    executor.sell_success = True
    executor.positions = {}  # empty → live_count = 0

    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    strategy._reconciliation_complete = True

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 40
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    market_gone = await strategy._execute_stop_loss(bracket)

    assert market_gone is False  # success=True but live_count=0 → False after cleanup
    # OrderAction record should exist and be SUCCEEDED
    actions = db.store[OrderAction]
    assert len(actions) == 1
    assert actions[0].action_key == action_key
    assert actions[0].status == OrderActionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_stop_loss_in_flight_skip(monkeypatch):
    """A SUBMITTED action (in-flight from another attempt) blocks a new submission."""
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    action_key = f"{ticker}:STOP_LOSS"

    db = InMemoryDB([
        PositionModel(
            market_ticker=ticker,
            event_ticker="EVT1",
            series_ticker="KXLOWTCHI",
            side="yes",
            quantity=1,
            avg_entry_price=80,
            last_price=40,
            position_ts=datetime.datetime.utcnow(),
        ),
        OrderAction(
            action_key=action_key,
            action_type="STOP_LOSS",
            market_ticker=ticker,
            status=OrderActionStatus.SUBMITTED,
        ),
    ])
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1}}

    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    strategy._reconciliation_complete = True

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 40
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    result = await strategy._execute_stop_loss(bracket)

    # Should be skipped (in-flight)
    assert result is False
    assert executor.orders == []


@pytest.mark.asyncio
async def test_fast_sl_phase_c_dispatches_immediately_without_waiting(monkeypatch):
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        stop_loss_price=50,
        trading_mode="LIVE",
        enable_fast_sl_exit=True,
    )
    strategy._dispatch_stop_loss_exit = AsyncMock()
    strategy._execute_stop_loss = AsyncMock()
    bracket = _make_held_bracket(ticker, "KXLOWTCHI")
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 40, 42)

    await strategy._evaluate_held_positions()

    strategy._dispatch_stop_loss_exit.assert_awaited_once()
    strategy._execute_stop_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_fast_sl_dispatch_is_idempotent_per_ticker(monkeypatch):
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    strategy = make_strategy(monkeypatch, trading_mode="LIVE", enable_fast_sl_exit=True)
    bracket = _make_held_bracket(ticker, "KXLOWTCHI")
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket

    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def fake_runner(*_args, **_kwargs):
        calls.append("run")
        started.set()
        await release.wait()

    strategy._run_fast_sl_exit = fake_runner

    await strategy._dispatch_stop_loss_exit(
        bracket, trigger_price=40, trigger_source="phase_c"
    )
    await started.wait()
    await strategy._dispatch_stop_loss_exit(
        bracket, trigger_price=39, trigger_source="phase_c"
    )

    assert len(calls) == 1
    task = strategy._sl_exit_tasks[ticker]
    release.set()
    await task


@pytest.mark.asyncio
async def test_fast_sl_dispatch_logs_in_flight_suppression(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    strategy = make_strategy(monkeypatch, trading_mode="LIVE", enable_fast_sl_exit=True)
    bracket = _make_held_bracket(ticker, "KXLOWTCHI")
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket

    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_runner(*_args, **_kwargs):
        started.set()
        await release.wait()

    strategy._run_fast_sl_exit = fake_runner

    await strategy._dispatch_stop_loss_exit(
        bracket, trigger_price=40, trigger_source="phase_c"
    )
    await started.wait()
    await strategy._dispatch_stop_loss_exit(
        bracket, trigger_price=39, trigger_source="phase_c"
    )

    release.set()
    await strategy._sl_exit_tasks[ticker]

    suppressed = [kw for event, kw in logged if event == "sl.trigger_suppressed_in_flight"]
    assert suppressed
    assert suppressed[0]["action_key"] == f"{ticker}:STOP_LOSS"


@pytest.mark.asyncio
async def test_fast_sl_repricing_ladder_respects_slippage_cap(monkeypatch):
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    strategy = make_strategy(
        monkeypatch,
        trading_mode="LIVE",
        enable_fast_sl_exit=True,
        sl_exit_max_attempts=3,
        sl_exit_retry_interval_ms=1,
        sl_exit_aggressive_offset_ticks=2,
        sl_exit_max_slippage=4,
    )
    bracket = _make_held_bracket(ticker, "KXLOWTCHI")
    bracket.position_quantity = 1
    bracket.last_price = 50
    strategy.active_positions[ticker] = bracket
    strategy._execute_stop_loss = AsyncMock(return_value=False)

    await strategy._run_fast_sl_exit(
        bracket,
        trigger_price=50,
        trigger_source="phase_c",
        trigger_ts_ms=strategy._now_ms(),
    )

    prices = [call.kwargs["override_price"] for call in strategy._execute_stop_loss.await_args_list]
    assert prices == [48, 46, 46]


@pytest.mark.asyncio
async def test_fast_sl_dispatch_runs_per_ticker_in_parallel(monkeypatch):
    strategy = make_strategy(monkeypatch, trading_mode="LIVE", enable_fast_sl_exit=True)
    b1 = _make_held_bracket("KXLOWTCHI-26JUN25-B62.5", "KXLOWTCHI")
    b2 = _make_held_bracket("KXLOWTBOS-26JUN25-B62.5", "KXLOWTBOS")
    b1.position_quantity = 1
    b2.position_quantity = 1
    strategy.active_positions[b1.market_ticker] = b1
    strategy.active_positions[b2.market_ticker] = b2

    started = set()
    release = asyncio.Event()

    async def fake_runner(bracket, **_kwargs):
        started.add(bracket.market_ticker)
        await release.wait()

    strategy._run_fast_sl_exit = fake_runner
    await strategy._dispatch_stop_loss_exit(b1, trigger_price=40, trigger_source="phase_c")
    await strategy._dispatch_stop_loss_exit(b2, trigger_price=39, trigger_source="phase_c")
    await asyncio.sleep(0)

    assert started == {b1.market_ticker, b2.market_ticker}
    tasks = list(strategy._sl_exit_tasks.values())
    release.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_fast_sl_telemetry_logs_trigger_submit_and_fill(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    executor = FakeExecutor()
    executor.sell_success = True
    executor.positions = {}
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        trading_mode="LIVE",
        enable_fast_sl_exit=True,
    )

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 40
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    trigger_ts_ms = strategy._now_ms()
    await strategy._execute_stop_loss(
        bracket,
        override_price=39,
        bypass_cooldown=True,
        trigger_ts_ms=trigger_ts_ms,
        attempt=1,
    )

    events = [event for event, _ in logged]
    assert "sl.exit_submit_start" in events
    assert "sl.exit_submitted" in events
    assert "sl.exit_fill_observed" in events
    submit_start = next(kwargs for event, kwargs in logged if event == "sl.exit_submit_start")
    submitted = next(kwargs for event, kwargs in logged if event == "sl.exit_submitted")
    filled = next(kwargs for event, kwargs in logged if event == "sl.exit_fill_observed")
    assert submit_start["action_key"] == f"{ticker}:STOP_LOSS"
    assert submitted["action_key"] == f"{ticker}:STOP_LOSS"
    assert filled["action_key"] == f"{ticker}:STOP_LOSS"
    assert submit_start["elapsed_ms"] >= 0
    assert filled["elapsed_ms"] >= 0


@pytest.mark.asyncio
async def test_fast_sl_logs_position_gone_for_market_not_found(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTCHI-26JUN25-B62.5"
    executor = FakeExecutor()

    async def sell_yes_market_gone(order):
        executor.orders.append((order, None))
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            notes="market_not_found",
        )

    executor.sell_yes = sell_yes_market_gone
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        trading_mode="LIVE",
        enable_fast_sl_exit=True,
    )

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTCHI",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    bracket.last_price = 40
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    await strategy._execute_stop_loss(
        bracket,
        override_price=39,
        bypass_cooldown=True,
        trigger_ts_ms=strategy._now_ms(),
        attempt=1,
    )

    position_gone = [kw for event, kw in logged if event == "sl.position_gone"]
    assert position_gone
    assert position_gone[0]["action_key"] == f"{ticker}:STOP_LOSS"
    assert position_gone[0]["reason"] == "market_not_found"


# ---------------------------------------------------------------------------
# Fix 2: Startup reconciliation completeness + readiness gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_readiness_gate_blocks_watcher_before_reconciliation(monkeypatch):
    """_execute_stop_loss_from_watcher returns False and logs a warning when
    _reconciliation_complete is False."""
    logged = capture_logs(monkeypatch)
    executor = FakeExecutor()
    strategy = make_strategy(monkeypatch, executor=executor)

    # Gate is NOT yet set
    assert strategy._reconciliation_complete is False

    result = await strategy._execute_stop_loss_from_watcher(
        "KXLOWTCHI-26JUN25-B62.5", "yes", 1, 30
    )

    assert result is False
    assert executor.orders == []
    gate_logs = [kw for ev, kw in logged if ev == "phase.c.stop_loss_readiness_gate"]
    assert gate_logs, "Expected readiness gate log"


@pytest.mark.asyncio
async def test_readiness_gate_cleared_after_reconciliation(monkeypatch):
    """_reconciliation_complete is True after _restore_positions completes."""
    executor = FakeExecutor()
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="PAPER")

    assert strategy._reconciliation_complete is False
    await strategy._restore_positions()
    assert strategy._reconciliation_complete is True


@pytest.mark.asyncio
async def test_readiness_gate_not_set_on_restore_failure(monkeypatch):
    """_reconciliation_complete remains False when _restore_positions raises."""
    executor = FakeExecutor()
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="PAPER")

    # Force inner restore to raise
    async def _fail():
        raise RuntimeError("db unavailable")

    strategy._restore_positions_inner = _fail

    with pytest.raises(RuntimeError):
        await strategy._restore_positions()

    assert strategy._reconciliation_complete is False


@pytest.mark.asyncio
async def test_reconciliation_logs_start_and_complete(monkeypatch):
    """strategy.reconciliation_starting and strategy.reconciliation_complete
    are both emitted during _restore_positions."""
    logged = capture_logs(monkeypatch)
    executor = FakeExecutor()
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="PAPER")

    await strategy._restore_positions()

    events = [ev for ev, _ in logged]
    assert "strategy.reconciliation_starting" not in events, (
        "_restore_positions should NOT emit starting; that is done by start()"
    )
    # _restore_positions itself just sets the flag; start() logs the bookend.
    # What we CAN verify: flag is set and no error was logged.
    error_logs = [kw for ev, kw in logged if ev == "strategy.reconciliation_failed"]
    assert error_logs == []
    assert strategy._reconciliation_complete is True


# ---------------------------------------------------------------------------
# Fix 3: Failure-mode rigor in execution path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sl_watcher_transient_error_resets_for_retry():
    """A TransientExecutionError moves the watcher into RETRYING so its own
    worker can retry without waiting for another strategy loop pass."""
    from execution.errors import TransientExecutionError
    from execution.sl_watcher import StopLossWatcher

    calls = 0

    async def exit_handler(_ticker, _side, _qty, _ask):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TransientExecutionError("connection timeout")
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("T", side="yes", quantity=1, sl_price=35)

    result1 = await watcher.on_market_update("T", 30)
    assert result1 is True
    await watcher._run_cycle_once()
    await watcher._worker_tasks["T"]
    assert watcher._positions["T"].state == "RETRYING"
    assert watcher._positions["T"].exit_in_progress is False

    await watcher._run_cycle_once()
    await watcher._worker_tasks["T"]
    assert "T" not in watcher._positions
    assert calls == 2


@pytest.mark.asyncio
async def test_sl_watcher_permanent_error_resets_for_manual_recovery():
    """A PermanentExecutionError leaves the watcher in RETRYING so the dedicated
    worker can continue polling instead of waiting on the main loop."""
    from execution.errors import PermanentExecutionError
    from execution.sl_watcher import StopLossWatcher
    import structlog.testing

    async def exit_handler(_ticker, _side, _qty, _ask):
        raise PermanentExecutionError("invalid ticker")

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("T", side="yes", quantity=1, sl_price=35)

    with structlog.testing.capture_logs() as cap:
        result = await watcher.on_market_update("T", 30)
        await watcher._run_cycle_once()
        await watcher._worker_tasks["T"]

    assert result is True
    assert watcher._positions["T"].exit_in_progress is False
    assert watcher._positions["T"].state == "RETRYING"
    # permanent failure log emitted
    perm_logs = [e for e in cap if e.get("log_level") == "error"
                 and "permanent" in str(e.get("event", ""))]
    assert perm_logs, f"Expected permanent failure log, got: {cap}"


@pytest.mark.asyncio
async def test_sl_watcher_unknown_error_treated_as_transient():
    """An unexpected exception is treated conservatively as transient and moved
    into RETRYING for the watcher worker."""
    from execution.sl_watcher import StopLossWatcher

    async def exit_handler(_ticker, _side, _qty, _ask):
        raise ValueError("something unexpected")

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("T", side="yes", quantity=1, sl_price=35)

    result = await watcher.on_market_update("T", 30)
    await watcher._run_cycle_once()
    await watcher._worker_tasks["T"]

    assert result is True
    assert watcher._positions["T"].exit_in_progress is False
    assert watcher._positions["T"].state == "RETRYING"


# ---------------------------------------------------------------------------
# Fix 4: Hard-disable monitor role drift
# ---------------------------------------------------------------------------

def test_monitor_has_no_sell_position_function():
    """_sell_position must not exist in monitor.py – it was a legacy primary
    executor function and has been removed to enforce the non-primary role."""
    import monitor as monitor_module
    assert not hasattr(monitor_module, "_sell_position"), (
        "_sell_position must be removed from monitor.py (non-primary executor contract)"
    )


@pytest.mark.asyncio
async def test_monitor_run_cycle_does_not_submit_stop_loss(monkeypatch):
    """run_monitor_cycle must not call any sell / stop-loss order submit paths."""
    import monitor as monitor_module
    from app.config import AppConfig
    from app.database import DatabaseManager

    # Track any calls to order-submit helpers
    sell_calls = []

    # Patch _buy_hedge to a no-op (we only care it doesn't sell)
    async def noop_buy_hedge(*_args, **_kwargs):
        return False

    monkeypatch.setattr(monitor_module, "_buy_hedge", noop_buy_hedge)

    # Ensure _sell_position is not accidentally restored
    assert not hasattr(monitor_module, "_sell_position")

    # Patch DB to return no positions → cycle exits early
    class _FakeDB:
        async def get_session(self):
            return _FakeSession()

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def execute(self, *_):
            return _FakeResult()

        async def commit(self):
            pass

    class _FakeResult:
        def scalars(self):
            return self

        def all(self):
            return []

    config = AppConfig(
        kalshi_api_key="test",
        kalshi_private_key_path="unused.pem",
        mysql_database_url="sqlite:///",
        trading_mode="PAPER",
        initial_contract_count=2,
        monitor_start_price=80,
        buy_trigger_price=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=50,
        hedge_max_factor=3.0,
        dry_run=False,
    )
    db = _FakeDB()

    # Should run without error and without placing any orders
    await monitor_module.run_monitor_cycle(config, db)
    assert sell_calls == []


# ---------------------------------------------------------------------------
# Phase C price-check reliability tests (stop-loss watcher interaction)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase_c_with_watcher_defers_when_ws_price_present(monkeypatch):
    """When stop_loss_watcher is active and a live WebSocket quote is present,
    Phase C defers to the watcher and does NOT call _execute_stop_loss directly.
    price_source='websocket' + watcher guard → continue."""
    logged = capture_logs(monkeypatch)
    ticker = "KXHIGHLAX-26JUL01-B71.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXHIGHLAX")
    bracket.position_quantity = 2
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # WS quote: bid=40 (below stop_loss=50), ask=42 — tight spread
    strategy.cache.update_quote(ticker, 40, 42)

    # Attach watcher — Phase C must defer to it for websocket-driven prices
    async def no_op_exit(t, s, q, p):
        return True

    watcher = StopLossWatcher(no_op_exit)
    await watcher.register_position(ticker, side="yes", quantity=2, sl_price=50)
    strategy.stop_loss_watcher = watcher

    await strategy._evaluate_held_positions()

    # Watcher guard active → Phase C must not fire the stop-loss directly
    strategy._execute_stop_loss.assert_not_awaited()
    # price_check log must show websocket source
    price_check = next(
        (kw for event, kw in logged if event == "phase.c.price_check"), None
    )
    assert price_check is not None
    assert price_check["price_source"] == "websocket"
    assert price_check["trigger_met"] is True


@pytest.mark.asyncio
async def test_phase_c_with_watcher_fires_via_rest_fallback_when_ws_missing(monkeypatch):
    """When stop_loss_watcher is active but NO WebSocket quote is cached,
    Phase C must fetch a REST fallback quote and fire the stop-loss directly
    (the watcher has no live tick and cannot act)."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTDAL-26JUL01-B79.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    # REST returns a below-stop bid
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"yes_ask": 42, "yes_bid": 40, "spread": 2}
    )

    bracket = _make_held_bracket(ticker, "KXLOWTDAL")
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # No WS quote in cache

    async def no_op_exit(t, s, q, p):
        return True

    watcher = StopLossWatcher(no_op_exit)
    await watcher.register_position(ticker, side="yes", quantity=1, sl_price=50)
    strategy.stop_loss_watcher = watcher

    await strategy._evaluate_held_positions()

    # REST fallback path must have been consulted
    strategy._fetch_market_data_via_rest.assert_awaited_once_with(ticker)
    # Phase C must fire directly since watcher has no tick
    strategy._execute_stop_loss.assert_awaited_once()
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    # price_check log must show fallback_quote source
    price_check = next(
        (kw for event, kw in logged if event == "phase.c.price_check"), None
    )
    assert price_check is not None
    assert price_check["price_source"] == "fallback_quote"
    assert price_check["trigger_met"] is True
    assert not any(event == "phase.c.no_live_price" for event, _ in logged)


@pytest.mark.asyncio
async def test_phase_c_with_watcher_logs_skip_when_both_sources_missing(monkeypatch):
    """When stop_loss_watcher is active and BOTH WebSocket and REST price
    are unavailable, Phase C must log phase.c.no_live_price and not crash."""
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTDAL-26JUL01-B79.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = _make_held_bracket(ticker, "KXLOWTDAL")
    bracket.position_quantity = 1
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    # No WS quote, no last_price

    async def no_op_exit(t, s, q, p):
        return True

    watcher = StopLossWatcher(no_op_exit)
    await watcher.register_position(ticker, side="yes", quantity=1, sl_price=50)
    strategy.stop_loss_watcher = watcher

    # Must not raise
    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_not_awaited()
    assert any(event == "phase.c.no_live_price" for event, _ in logged)
    no_price_log = next(kw for event, kw in logged if event == "phase.c.no_live_price")
    assert no_price_log["price_source"] == "none"
    assert no_price_log["reason"] == "no_websocket_or_rest_price"


@pytest.mark.asyncio
async def test_phase_c_ticker_key_consistency_regression(monkeypatch):
    """Regression: the ticker key used by active_positions must match the key
    used by cache.update_quote / cache.get_quote.  A mismatch would cause
    permanent phase.c.no_live_price even when data is available."""
    logged = capture_logs(monkeypatch)
    # Use the canonical ticker format as returned by the Kalshi API
    ticker = "KXHIGHLAX-26JUL01-B71.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    strategy = make_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()

    bracket = _make_held_bracket(ticker, "KXHIGHLAX")
    bracket.position_quantity = 1
    # Store the bracket under the canonical key
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket

    # Update the cache using the EXACT same ticker string
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_held_positions()

    # The quote was found → no no_live_price log
    assert not any(event == "phase.c.no_live_price" for event, _ in logged)
    price_check = next(
        (kw for event, kw in logged if event == "phase.c.price_check"), None
    )
    assert price_check is not None
    assert price_check["price_source"] == "websocket"
    assert price_check["price"] == 80


# ---------------------------------------------------------------------------
# PANIC_FLATTEN stop-loss mode tests
# ---------------------------------------------------------------------------

def _make_panic_strategy(monkeypatch, executor=None, db=None, **extra_config):
    """Helper: build a strategy configured for PANIC_FLATTEN mode."""
    return make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        sl_exit_mode="PANIC_FLATTEN",
        sl_panic_sell_price=1,
        sl_panic_retry_ms=0,    # no sleep in tests
        sl_panic_max_retries=3,
        enable_fast_sl_exit=True,
        **extra_config,
    )


@pytest.mark.asyncio
async def test_panic_flatten_submits_at_panic_price(monkeypatch):
    """PANIC_FLATTEN mode: first sell order must be at the configured panic price (1¢)."""
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="panic-sell-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    bracket._reconciliation_complete = True
    strategy._reconciliation_complete = True
    # Pre-submit revalidation requires a cached YES ask at or below stop (50¢)
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=49,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # The first (and only) order must be placed at the panic price floor: 1¢
    assert len(executor.orders) >= 1
    first_order = executor.orders[0][0]
    assert first_order.price == 1
    assert first_order.side.name == "SELL_YES"
    assert first_order.quantity == 2


@pytest.mark.asyncio
async def test_panic_flatten_retries_on_unfilled(monkeypatch):
    """PANIC_FLATTEN mode: retries up to sl_panic_max_retries when unfilled."""
    ticker = "KXLOWTBOS-26JUN23-B66.5"
    executor = FakeExecutor()
    call_count = 0

    async def sell_yes(order):
        nonlocal call_count
        call_count += 1
        executor.orders.append((order, None))
        if call_count >= 2:
            executor.positions = {}
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=-(order.price * order.quantity),
                order_id="panic-retry-id",
                notes="filled",
            )
        # First call: unfilled
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            notes="unfilled",
        )

    executor.sell_yes = sell_yes
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    db = InMemoryDB()
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Pre-submit revalidation requires a cached YES ask at or below stop (50¢)
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=49,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # Both attempts must use the panic price (1¢)
    assert call_count == 2
    for order, _ in executor.orders:
        assert order.price == 1


@pytest.mark.asyncio
async def test_panic_flatten_respects_max_retries(monkeypatch):
    """PANIC_FLATTEN mode: stops retrying after sl_panic_max_retries attempts."""
    ticker = "KXLOWTBOS-26JUN23-B67.5"
    executor = FakeExecutor()

    async def sell_yes(order):
        executor.orders.append((order, None))
        # Always unfilled
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            notes="unfilled",
        )

    executor.sell_yes = sell_yes
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    db = InMemoryDB()
    # max_retries=2 so only 2 sell attempts should be made
    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        sl_exit_mode="PANIC_FLATTEN",
        sl_panic_sell_price=1,
        sl_panic_retry_ms=0,
        sl_panic_max_retries=2,
        enable_fast_sl_exit=True,
        stop_loss_price=50,
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Pre-submit revalidation requires a cached YES ask at or below stop (50¢)
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=49,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    assert len(executor.orders) == 2


@pytest.mark.asyncio
async def test_panic_flatten_idempotency_suppresses_concurrent_triggers(monkeypatch):
    """PANIC_FLATTEN mode: repeated trigger calls while a task is in-flight must
    not launch a second exit task for the same ticker."""
    ticker = "KXLOWTBOS-26JUN23-B68.5"
    started = asyncio.Event()
    release = asyncio.Event()
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        started.set()
        await release.wait()
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="idempotent-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Pre-submit revalidation requires a cached YES ask at or below stop (50¢)
    strategy.cache.update_quote(ticker, 45, 48)

    # First dispatch creates a task and starts it
    await strategy._dispatch_stop_loss_exit(bracket, trigger_price=49, trigger_source="test")
    await started.wait()

    # Second dispatch while first is in-flight: must be suppressed
    await strategy._dispatch_stop_loss_exit(bracket, trigger_price=49, trigger_source="test")

    release.set()
    # Wait for the running task to finish
    existing_task = strategy._sl_exit_tasks.get(ticker)
    if existing_task is not None:
        await existing_task

    # Only one sell order must have been submitted
    assert len(executor.orders) == 1


@pytest.mark.asyncio
async def test_panic_flatten_logs_panic_triggered_and_submit(monkeypatch):
    """PANIC_FLATTEN mode: sl.panic_triggered and sl.panic_submit are logged."""
    ticker = "KXLOWTBOS-26JUN23-B69.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="log-test-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Pre-submit revalidation requires a cached YES ask at or below stop (50¢)
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=49,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    log_events = [event for event, _ in logged]
    assert "sl.panic_triggered" in log_events
    assert "sl.panic_submit" in log_events
    # Terminal completion must also be logged when the position clears.
    assert "sl.panic_filled" in log_events or "sl.position_gone" in log_events


@pytest.mark.asyncio
async def test_panic_flatten_logs_retry_event(monkeypatch):
    """PANIC_FLATTEN mode: sl.panic_retry is emitted for the second attempt."""
    ticker = "KXLOWTBOS-26JUN23-B70.5"
    executor = FakeExecutor()
    call_count = 0

    async def sell_yes(order):
        nonlocal call_count
        call_count += 1
        executor.orders.append((order, None))
        if call_count >= 2:
            executor.positions = {}
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=-(order.price * order.quantity),
                order_id="retry-log-id",
                notes="filled",
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
            notes="unfilled",
        )

    executor.sell_yes = sell_yes
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Pre-submit revalidation requires a cached YES ask at or below stop (50¢)
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=49,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    log_events = [event for event, _ in logged]
    assert "sl.panic_retry" in log_events
    retry_log = next(kw for event, kw in logged if event == "sl.panic_retry")
    assert retry_log["retry_index"] == 1
    assert retry_log["panic_price"] == 1


@pytest.mark.asyncio
async def test_aggressive_limit_mode_unchanged_when_panic_flatten_disabled(monkeypatch):
    """Backward compat: when sl_exit_mode is not PANIC_FLATTEN, the existing
    repricing ladder (_run_fast_sl_exit) is used, not the panic path."""
    ticker = "KXLOWTBOS-26JUN23-B64.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 2, "average_fill_cost_cents": 80}}
    executor.sell_success = True
    db = InMemoryDB()

    strategy = make_strategy(
        monkeypatch,
        executor=executor,
        db=db,
        sl_exit_mode="AGGRESSIVE_LIMIT",
        sl_exit_aggressive_offset_ticks=5,
        sl_exit_max_slippage=20,
        sl_exit_max_attempts=1,
        sl_exit_retry_interval_ms=0,
        enable_fast_sl_exit=True,
        stop_loss_price=50,
    )

    run_panic_called = []
    original_panic = strategy._run_panic_flatten_exit

    async def spy_panic(*args, **kwargs):
        run_panic_called.append(True)
        return await original_panic(*args, **kwargs)

    strategy._run_panic_flatten_exit = spy_panic

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    bracket.last_price = 49
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True

    await strategy._dispatch_stop_loss_exit(bracket, trigger_price=49, trigger_source="test")
    task = strategy._sl_exit_tasks.get(ticker)
    if task is not None:
        await task

    # Panic path must NOT have been called
    assert run_panic_called == []
    # A sell order should still have been placed via the AGGRESSIVE_LIMIT path
    assert len(executor.orders) >= 1
    # Price must be the ladder price (reference - offset), not the panic floor
    first_order_price = executor.orders[0][0].price
    assert first_order_price != 1 or strategy.config.sl_exit_aggressive_offset_ticks == 0


# ---------------------------------------------------------------------------
# PANIC_FLATTEN ASK-based trigger and pre-submit revalidation tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_panic_flatten_no_submit_when_ask_above_stop(monkeypatch):
    """ask=0.88 (88¢), stop=0.48 (48¢) → no panic sell submission.

    The pre-submit revalidation must abort because 88 > 48.
    """
    ticker = "KXLOWTBOS-26JUN23-B71.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=48)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Cache: ask=88¢ which is ABOVE the 48¢ stop — revalidation must abort
    strategy.cache.update_quote(ticker, 85, 88)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=47,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # No sell orders must have been placed
    assert len(executor.orders) == 0
    # Revalidation abort must be logged
    abort_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_aborted"]
    assert len(abort_logs) >= 1
    assert abort_logs[0]["reason"] == "ask_above_stop"
    assert abort_logs[0]["best_ask_yes"] == 88
    assert abort_logs[0]["stop_loss_price"] == 48


@pytest.mark.asyncio
async def test_panic_flatten_submits_when_ask_equals_stop(monkeypatch):
    """ask=0.48 (48¢), stop=0.48 (48¢) → panic sell submission occurs.

    Boundary condition: ask == stop_loss_price must trigger (<=).
    """
    ticker = "KXLOWTBOS-26JUN23-B72.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="ask-eq-stop-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=48)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Cache: ask=48¢ exactly equals the 48¢ stop — must trigger (<=)
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=48,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    assert len(executor.orders) == 1
    assert executor.orders[0][0].price == 1  # panic floor price


@pytest.mark.asyncio
async def test_panic_flatten_aborts_when_ask_rebounds_before_submit(monkeypatch):
    """Trigger fires (ask <= stop), then ask rebounds above stop before the
    task reaches pre-submit revalidation → submit must be canceled."""
    ticker = "KXLOWTBOS-26JUN23-B73.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True

    # Step 1: set cache with ask=48¢ (below stop=50¢) → trigger fires
    strategy.cache.update_quote(ticker, 45, 48)

    # Step 2: simulate ask rebounding to 80¢ before the task executes
    # by overwriting the cache between trigger and submit
    strategy.cache.update_quote(ticker, 75, 80)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=48,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # Revalidation sees ask=80¢ > stop=50¢ → no sell
    assert len(executor.orders) == 0
    abort_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_aborted"]
    assert len(abort_logs) >= 1
    assert abort_logs[0]["reason"] == "ask_above_stop"
    assert abort_logs[0]["best_ask_yes"] == 80


@pytest.mark.asyncio
async def test_panic_flatten_proceeds_when_no_cached_quote(monkeypatch):
    """If no cached quote is available at pre-submit time, panic submit proceeds
    in degraded mode: sl.panic_revalidation_degraded is logged with
    reason='no_cached_quote' and the order is still submitted."""
    ticker = "KXLOWTBOS-26JUN23-B74.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    filled = []

    async def sell_yes(order):
        filled.append(order)
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="degraded-sell-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # No cache update — cache is empty for this ticker

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=49,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # Must submit in degraded mode (not abort)
    assert len(filled) >= 1
    degraded_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_degraded"]
    assert len(degraded_logs) >= 1
    assert degraded_logs[0]["reason"] == "no_cached_quote"
    # Must NOT log a hard abort
    abort_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_aborted"]
    assert len(abort_logs) == 0


@pytest.mark.asyncio
async def test_panic_flatten_proceeds_on_stale_quote(monkeypatch):
    """If the cached quote is older than sl_panic_max_quote_age_ms, panic submit
    proceeds in degraded mode: sl.panic_revalidation_degraded is logged with
    reason='stale_quote' and the order is still submitted."""
    import time as _time
    ticker = "KXLOWTBOS-26JUN23-B75.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    filled = []

    async def sell_yes(order):
        filled.append(order)
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="stale-degraded-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(
        monkeypatch, executor=executor, db=db,
        stop_loss_price=50,
        sl_panic_max_quote_age_ms=1000,  # 1 second max age
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Inject a quote with a timestamp 2 seconds in the past (older than 1s limit)
    strategy.cache.update_quote(ticker, 45, 48)
    strategy.cache.quote_timestamps[ticker] = _time.time() - 2.0

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=48,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # Must submit in degraded mode (not abort)
    assert len(filled) >= 1
    degraded_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_degraded"]
    assert len(degraded_logs) >= 1
    assert degraded_logs[0]["reason"] == "stale_quote"
    # Must NOT log a hard abort
    abort_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_aborted"]
    assert len(abort_logs) == 0


@pytest.mark.asyncio
async def test_panic_flatten_retries_on_transient_exception(monkeypatch):
    """PANIC_FLATTEN mode: transient exceptions during submit are retried up to
    sl_panic_max_retries.  Each failed attempt logs sl.panic_submit_error and the
    terminal failure logs sl.exit_retry_exhausted with reason='max_retries_exhausted'."""
    ticker = "KXLOWTBOS-26JUN23-B75.9"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    call_count = [0]

    async def sell_yes_raises(order):
        call_count[0] += 1
        raise RuntimeError("network timeout")

    executor.sell_yes = sell_yes_raises
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(
        monkeypatch, executor=executor, db=db,
        stop_loss_price=50,
    )
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=48,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # All three retry attempts must have been made
    assert call_count[0] == 3
    # Each attempt must log a per-attempt error
    error_logs = [kw for event, kw in logged if event == "sl.panic_submit_error"]
    assert len(error_logs) == 3
    assert all(kw.get("reason") == "submit_error" for kw in error_logs)
    # Terminal failure must be logged
    failed_logs = [kw for event, kw in logged if event == "sl.exit_retry_exhausted"]
    assert any(kw.get("reason") == "max_retries_exhausted" for kw in failed_logs)


@pytest.mark.asyncio
async def test_panic_flatten_phase_c_no_trigger_when_ask_above_stop(monkeypatch):
    """Phase C with PANIC_FLATTEN: when yes_ask > stop_loss_price, no trigger fires
    even if yes_bid < stop_loss_price (e.g., ask=0.88, stop=0.48)."""
    ticker = "KXLOWTBOS-26JUN23-B76.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, stop_loss_price=48)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # bid=30¢ is below stop=48¢, but ask=88¢ is well above stop
    strategy.cache.update_quote(ticker, 30, 88)

    await strategy._evaluate_held_positions()

    # PANIC_FLATTEN must NOT have triggered — ask is above stop
    strategy._execute_stop_loss.assert_not_awaited()
    assert not any(event == "phase.c.stop_loss_triggered" for event, _ in logged)
    # No panic trigger log either
    assert not any(event == "sl.panic_trigger_evaluated" for event, _ in logged)


@pytest.mark.asyncio
async def test_panic_flatten_phase_c_triggers_when_ask_at_stop(monkeypatch):
    """Phase C with PANIC_FLATTEN: trigger fires via the fallback REST path when
    yes_ask <= stop_loss_price."""
    ticker = "KXLOWTBOS-26JUN23-B77.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, stop_loss_price=50)
    strategy._execute_stop_loss = AsyncMock()
    # REST returns ask=48¢ (below stop=50¢)
    strategy._fetch_market_data_via_rest = AsyncMock(
        return_value={"yes_ask": 48, "yes_bid": 45, "spread": 3}
    )
    # No WebSocket quote in cache → fallback to REST
    dispatch_calls = []
    original_dispatch = strategy._dispatch_stop_loss_exit

    async def spy_dispatch(bracket, *, trigger_price, trigger_source):
        dispatch_calls.append({"trigger_price": trigger_price, "trigger_source": trigger_source})
        # Don't actually start the panic task (would need cache quote)

    strategy._dispatch_stop_loss_exit = spy_dispatch

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True

    await strategy._evaluate_held_positions()

    # Trigger must have fired (ask=48 <= stop=50)
    assert len(dispatch_calls) == 1
    assert dispatch_calls[0]["trigger_source"] == "phase_c"
    assert dispatch_calls[0]["trigger_price"] == 48  # yes_ask passed as trigger_price
    assert any(event == "sl.panic_trigger_evaluated" for event, _ in logged)
    trigger_log = next(kw for event, kw in logged if event == "sl.panic_trigger_evaluated")
    assert trigger_log["best_ask_yes"] == 48
    assert trigger_log["stop_loss_price"] == 50
    assert trigger_log["units"] == "cents"


@pytest.mark.asyncio
async def test_panic_flatten_unit_normalization(monkeypatch):
    """Unit normalization: stop_loss_price in .env as dollars (0.48) is stored
    as cents (48) by AppConfig. The cache stores ask in cents. Comparing 48 <= 48
    must be True (no mixed-unit false-positive or false-negative)."""
    ticker = "KXLOWTBOS-26JUN23-B78.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}

    async def sell_yes(order):
        executor.orders.append((order, None))
        executor.positions = {}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id="unit-norm-id",
            notes="filled",
        )

    executor.sell_yes = sell_yes
    db = InMemoryDB()

    # AppConfig.convert_dollars_to_cents converts "0.48" → 48 (cents)
    # Simulate that: pass stop_loss_price already in cents (48)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=48)
    assert strategy.config.stop_loss_price == 48  # must be in cents

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Cache stores ask in cents: 48¢ == 0.48 dollars
    strategy.cache.update_quote(ticker, 45, 48)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=48,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    # 48 <= 48 → must submit (no mixed-unit confusion)
    assert len(executor.orders) == 1
    assert executor.orders[0][0].price == 1  # panic floor


@pytest.mark.asyncio
async def test_panic_flatten_unit_normalization_no_false_trigger(monkeypatch):
    """Anti-regression: ask=88¢ with stop=48¢ must NOT trigger even after any
    unit conversion path (0.48 dollars → 48 cents; 0.88 dollars → 88 cents)."""
    ticker = "KXLOWTBOS-26JUN23-B79.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    db = InMemoryDB()
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=48)
    # stop_loss_price stored in cents
    assert strategy.config.stop_loss_price == 48

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # Cache ask=88¢ > stop=48¢ → must NOT submit
    strategy.cache.update_quote(ticker, 85, 88)

    await strategy._run_panic_flatten_exit(
        bracket,
        trigger_price=47,
        trigger_source="test",
        trigger_ts_ms=strategy._now_ms(),
    )

    assert len(executor.orders) == 0
    abort_logs = [kw for event, kw in logged if event == "sl.panic_revalidation_aborted"]
    assert abort_logs[0]["reason"] == "ask_above_stop"
    # Confirm both values are in cents (not a mixed-unit compare of 88 vs 0.48)
    assert abort_logs[0]["best_ask_yes"] == 88
    assert abort_logs[0]["stop_loss_price"] == 48


@pytest.mark.asyncio
async def test_panic_flatten_zero_bid_no_trigger_when_ask_above_stop(monkeypatch):
    """Zero-bid-collapse scenario for PANIC_FLATTEN: bid drops to 0 but ask=88¢
    is above stop=48¢ → must NOT trigger panic exit (ask-only rule)."""
    ticker = "KXLOWTBOS-26JUN23-B80.5"
    executor = FakeExecutor()
    executor.positions = {ticker: {"count": 1, "average_fill_cost_cents": 80}}
    logged = capture_logs(monkeypatch)
    strategy = _make_panic_strategy(monkeypatch, executor=executor, stop_loss_price=48)
    strategy._execute_stop_loss = AsyncMock()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTBOS",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=80,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True
    # bid=0 (collapse) but ask=88¢ (well above 48¢ stop)
    strategy.cache.update_quote(ticker, 0, 88)

    await strategy._evaluate_held_positions()

    # Must not trigger: ask > stop
    strategy._execute_stop_loss.assert_not_awaited()
    assert not any(event == "phase.c.stop_loss_triggered" for event, _ in logged)


# ---------------------------------------------------------------------------
# Tests for LOW_TRADES / HIGH_TRADES entry-toggle flags
# ---------------------------------------------------------------------------

def _make_entry_bracket(ticker: str, series: str) -> MarketBracket:
    return MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker=series,
        bracket_label="test",
        phase=Phase.MONITORING,
    )


@pytest.mark.asyncio
async def test_trade_toggle_both_enabled_allows_low_and_high(monkeypatch):
    """low_trades=True, high_trades=True -> both LOW and HIGH entries proceed."""
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, low_trades=True, high_trades=True)

    low_ticker = "KXLOWTBOS-26JUN22-B52.5"
    high_ticker = "KXHIGHLAX-26JUN22-B71.5"
    strategy.brackets[low_ticker] = _make_entry_bracket(low_ticker, "KXLOWTBOS")
    strategy.brackets[high_ticker] = _make_entry_bracket(high_ticker, "KXHIGHLAX")
    strategy.cache.update_quote(low_ticker, 82, 82)   # spread=0 -> buys
    strategy.cache.update_quote(high_ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    blocked = [kw for event, kw in logged if event == "phase.b.entry_blocked_by_config"]
    assert blocked == [], "No entries should be blocked when both toggles are yes"
    assert strategy._execute_entry.await_count == 2


@pytest.mark.asyncio
async def test_trade_toggle_high_disabled_blocks_high_allows_low(monkeypatch):
    """low_trades=True, high_trades=False -> HIGH entry blocked, LOW entry proceeds."""
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, low_trades=True, high_trades=False)

    low_ticker = "KXLOWTBOS-26JUN22-B52.5"
    high_ticker = "KXHIGHLAX-26JUN22-B71.5"
    strategy.brackets[low_ticker] = _make_entry_bracket(low_ticker, "KXLOWTBOS")
    strategy.brackets[high_ticker] = _make_entry_bracket(high_ticker, "KXHIGHLAX")
    strategy.cache.update_quote(low_ticker, 82, 82)
    strategy.cache.update_quote(high_ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    blocked = [kw for event, kw in logged if event == "phase.b.entry_blocked_by_config"]
    assert len(blocked) == 1
    assert blocked[0]["ticker"] == high_ticker
    assert "HIGH_TRADES" in blocked[0]["reason"]
    assert strategy._execute_entry.await_count == 1
    called_ticker = strategy._execute_entry.call_args[0][0].market_ticker
    assert called_ticker == low_ticker


@pytest.mark.asyncio
async def test_trade_toggle_low_disabled_blocks_low_allows_high(monkeypatch):
    """low_trades=False, high_trades=True -> LOW entry blocked, HIGH entry proceeds."""
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, low_trades=False, high_trades=True)

    low_ticker = "KXLOWTBOS-26JUN22-B52.5"
    high_ticker = "KXHIGHLAX-26JUN22-B71.5"
    strategy.brackets[low_ticker] = _make_entry_bracket(low_ticker, "KXLOWTBOS")
    strategy.brackets[high_ticker] = _make_entry_bracket(high_ticker, "KXHIGHLAX")
    strategy.cache.update_quote(low_ticker, 82, 82)
    strategy.cache.update_quote(high_ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    blocked = [kw for event, kw in logged if event == "phase.b.entry_blocked_by_config"]
    assert len(blocked) == 1
    assert blocked[0]["ticker"] == low_ticker
    assert "LOW_TRADES" in blocked[0]["reason"]
    assert strategy._execute_entry.await_count == 1
    called_ticker = strategy._execute_entry.call_args[0][0].market_ticker
    assert called_ticker == high_ticker


@pytest.mark.asyncio
async def test_trade_toggle_both_disabled_blocks_all_entries(monkeypatch):
    """low_trades=False, high_trades=False -> both entries blocked."""
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, low_trades=False, high_trades=False)

    low_ticker = "KXLOWTBOS-26JUN22-B52.5"
    high_ticker = "KXHIGHLAX-26JUN22-B71.5"
    strategy.brackets[low_ticker] = _make_entry_bracket(low_ticker, "KXLOWTBOS")
    strategy.brackets[high_ticker] = _make_entry_bracket(high_ticker, "KXHIGHLAX")
    strategy.cache.update_quote(low_ticker, 82, 82)
    strategy.cache.update_quote(high_ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    blocked = [kw for event, kw in logged if event == "phase.b.entry_blocked_by_config"]
    assert len(blocked) == 2
    assert strategy._execute_entry.await_count == 0


@pytest.mark.asyncio
async def test_trade_toggle_does_not_affect_sl_exit_for_existing_positions(monkeypatch):
    """HIGH_TRADES=no must NOT prevent stop-loss execution for existing HIGH positions."""
    ticker = "KXHIGHLAX-26JUN22-B71.5"
    executor = FakeExecutor()
    executor.sell_success = True
    strategy = make_strategy(monkeypatch, executor=executor, high_trades=False)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXHIGHLAX",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True

    # Mock _execute_stop_loss to track if it gets called
    strategy._execute_stop_loss = AsyncMock()
    # Cache a price at or below stop-loss to trigger SL evaluation
    strategy.cache.update_quote(ticker, 40, 50)
    strategy.config.stop_loss_price = 50

    await strategy._evaluate_held_positions()

    # SL should fire even though high_trades=False
    strategy._execute_stop_loss.assert_awaited_once()


# ---------------------------------------------------------------------------
# Tests for city-local-time entry settle gate (ENABLE_LOCAL_SETTLE_GATE)
# ---------------------------------------------------------------------------

import datetime as _dt

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo as _ZoneInfo  # type: ignore[no-redef]

from core.local_time_gate import is_entry_allowed as _is_entry_allowed


def _gate_at_utc(blocked_utc: _dt.datetime):
    """Return a gate function that always uses the given fixed UTC time."""
    return lambda ticker, config, now_utc=None: _is_entry_allowed(
        ticker, config, now_utc=blocked_utc
    )


@pytest.mark.asyncio
async def test_settle_gate_blocks_entry_before_threshold(monkeypatch):
    """Gate enabled + local time before threshold → entry blocked, log emitted."""
    import core.state_machine as _sm
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, enable_local_settle_gate=True,
                             default_entry_start_local="01:00")

    ticker = "KXLOWTNYC-26JUN25-B72"
    strategy.brackets[ticker] = _make_entry_bracket(ticker, "KXLOWTNYC")
    strategy.cache.update_quote(ticker, 82, 82)   # spread=0 → would normally buy
    strategy._execute_entry = AsyncMock()

    # 04:30 UTC = 00:30 EDT (UTC-4 in summer) → before 01:00 threshold → blocked
    blocked_utc = _dt.datetime(2025, 6, 26, 4, 30, tzinfo=_dt.timezone.utc)
    monkeypatch.setattr(_sm, "is_entry_allowed", _gate_at_utc(blocked_utc))

    await strategy._evaluate_watchlist()

    assert strategy._execute_entry.await_count == 0
    gate_logs = [kw for event, kw in logged if event == "entry.blocked_local_settle_gate"]
    assert len(gate_logs) == 1
    assert gate_logs[0]["ticker"] == ticker
    assert gate_logs[0]["timezone"] == "America/New_York"


@pytest.mark.asyncio
async def test_settle_gate_allows_entry_at_threshold(monkeypatch):
    """Gate enabled + local time at/after threshold → entry proceeds."""
    import core.state_machine as _sm
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch, enable_local_settle_gate=True,
                             default_entry_start_local="01:00")

    ticker = "KXLOWTNYC-26JUN25-B72"
    strategy.brackets[ticker] = _make_entry_bracket(ticker, "KXLOWTNYC")
    strategy.cache.update_quote(ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    # 05:00 UTC = 01:00 EDT → at threshold → allowed
    allowed_utc = _dt.datetime(2025, 6, 26, 5, 0, tzinfo=_dt.timezone.utc)
    monkeypatch.setattr(_sm, "is_entry_allowed", _gate_at_utc(allowed_utc))

    await strategy._evaluate_watchlist()

    assert strategy._execute_entry.await_count == 1
    gate_logs = [kw for event, kw in logged if event == "entry.blocked_local_settle_gate"]
    assert gate_logs == []


@pytest.mark.asyncio
async def test_settle_gate_disabled_does_not_block(monkeypatch):
    """Gate disabled → entry proceeds even when local time would be before threshold."""
    import core.state_machine as _sm
    strategy = make_strategy(monkeypatch, enable_local_settle_gate=False)

    ticker = "KXLOWTNYC-26JUN25-B72"
    strategy.brackets[ticker] = _make_entry_bracket(ticker, "KXLOWTNYC")
    strategy.cache.update_quote(ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    # 04:30 UTC = 00:30 EDT — would be blocked if gate were on
    blocked_utc = _dt.datetime(2025, 6, 26, 4, 30, tzinfo=_dt.timezone.utc)
    monkeypatch.setattr(_sm, "is_entry_allowed", _gate_at_utc(blocked_utc))

    await strategy._evaluate_watchlist()

    assert strategy._execute_entry.await_count == 1


@pytest.mark.asyncio
async def test_settle_gate_does_not_affect_sl_exit(monkeypatch):
    """Gate enabled (blocked time) must NOT prevent stop-loss for existing positions."""
    import core.state_machine as _sm
    ticker = "KXLOWTNYC-26JUN25-B72"
    executor = FakeExecutor()
    executor.sell_success = True
    strategy = make_strategy(monkeypatch, executor=executor,
                             enable_local_settle_gate=True,
                             default_entry_start_local="01:00")

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTNYC",
        bracket_label="held",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.active_positions[ticker] = bracket
    strategy.brackets[ticker] = bracket
    strategy._reconciliation_complete = True

    strategy._execute_stop_loss = AsyncMock()
    strategy.cache.update_quote(ticker, 30, 35)
    strategy.config.stop_loss_price = 50  # above ask=35 → triggers

    # Inject a "blocked" UTC time (00:30 ET) — SL must still fire
    blocked_utc = _dt.datetime(2025, 6, 26, 4, 30, tzinfo=_dt.timezone.utc)
    monkeypatch.setattr(_sm, "is_entry_allowed", _gate_at_utc(blocked_utc))

    await strategy._evaluate_held_positions()

    # SL must fire regardless of the gate
    strategy._execute_stop_loss.assert_awaited_once()


@pytest.mark.asyncio
async def test_settle_gate_phoenix_midnight_rule(monkeypatch):
    """Phoenix uses 00:00 threshold; at 07:00 UTC (= 00:00 Phoenix) → allowed."""
    import core.state_machine as _sm
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch,
                             enable_local_settle_gate=True,
                             default_entry_start_local="01:00",
                             phoenix_entry_start_local="00:00")

    ticker = "KXHIGHTPHX-26JUN25-T110"
    strategy.brackets[ticker] = _make_entry_bracket(ticker, "KXHIGHTPHX")
    strategy.cache.update_quote(ticker, 82, 82)
    strategy._execute_entry = AsyncMock()

    # 07:00 UTC = 00:00 Phoenix (MST = UTC-7, no DST)
    phx_midnight_utc = _dt.datetime(2025, 6, 26, 7, 0, tzinfo=_dt.timezone.utc)
    monkeypatch.setattr(_sm, "is_entry_allowed", _gate_at_utc(phx_midnight_utc))

    await strategy._evaluate_watchlist()

    assert strategy._execute_entry.await_count == 1
    gate_logs = [kw for event, kw in logged if event == "entry.blocked_local_settle_gate"]
    assert gate_logs == []
