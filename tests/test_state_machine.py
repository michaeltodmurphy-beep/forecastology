import datetime
import os
import sys
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.config import AppConfig
from app.models import ExecutedTrade, Position as PositionModel, StopLossLedger
from core.constants import get_eastern_today_date_prefix
from core.state_machine import TemperatureStrategy, parse_series_and_date
from core.types import MarketBracket, OrderBook, OrderBookLevel, Phase
from data.ticker_cache import TickerCache
from execution.base import ExecutionResult


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
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="LIVE")
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
async def test_restore_positions_uses_db_cost_basis_when_api_entry_missing(monkeypatch):
    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSEA-26JUN22-B61.5"
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
    strategy = make_strategy(monkeypatch, executor=executor, db=db, trading_mode="LIVE")

    await strategy._restore_positions()

    restored = strategy.active_positions[ticker]
    assert restored.avg_entry == 84
    live_log = next(kwargs for event, kwargs in logged if event == "strategy.restored_live_position")
    assert live_log["entry_source"] == "db"


@pytest.mark.asyncio
async def test_stop_loss_sells_when_bid_below_threshold(monkeypatch):
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
    strategy.cache.update_quote(ticker, 49, 51)

    await strategy._evaluate_held_positions()

    assert executor.orders[0][0].side.name == "SELL_YES"
    assert executor.orders[0][0].quantity == 2
    assert any(event == "phase.c.stop_loss_triggered" for event, _ in logged)


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
async def test_stop_loss_increments_ledger(monkeypatch):
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
    strategy = make_strategy(monkeypatch, executor=executor, db=db, stop_loss_price=50)
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
    strategy.cache.update_quote(ticker, 49, 51)

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
    strategy.cache.update_quote(ticker, 0, 0)

    # Run several times back-to-back without advancing time
    for _ in range(5):
        await strategy._evaluate_held_positions()

    no_price_logs = [event for event, _ in logged if event == "phase.c.no_live_price"]
    assert len(no_price_logs) == 1
