import os
import sys
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.config import AppConfig
from app.models import StopLossLedger
from core.state_machine import TemperatureStrategy, parse_series_and_date
from core.types import MarketBracket, Phase
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
        return self._items


class FakeSession:
    def __init__(self, store):
        self.store = store

    def add(self, item):
        if isinstance(item, StopLossLedger):
            self.store['ledger'][(item.series_ticker, item.date_prefix)] = item
        else:
            self.store['added'].append(item)

    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def execute(self, statement, *_args, **_kwargs):
        entity = None
        if getattr(statement, 'column_descriptions', None):
            entity = statement.column_descriptions[0].get('entity')

        if entity is StopLossLedger:
            criteria = {}
            for expr in getattr(statement, '_where_criteria', ()): 
                left = getattr(expr, 'left', None)
                right = getattr(expr, 'right', None)
                if left is not None and right is not None and hasattr(left, 'key'):
                    criteria[left.key] = getattr(right, 'value', None)
            key = (criteria.get('series_ticker'), criteria.get('date_prefix'))
            row = self.store['ledger'].get(key)
            return FakeSessionResult([row] if row else [])

        return FakeSessionResult([])


class FakeSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeDB:
    def __init__(self, store=None):
        self.store = store or {'ledger': {}, 'added': []}

    async def get_session(self):
        return FakeSessionContext(FakeSession(self.store))


class FakeExecutor:
    def __init__(self):
        self.orders = []
        self.sell_orders = []
        self.positions = {}

    async def buy_yes(self, order, max_price=None):
        self.orders.append((order, max_price))
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side='yes',
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=order.price * order.quantity,
            order_id='buy-order',
            notes='filled',
        )

    async def sell_yes(self, order):
        self.sell_orders.append(order)
        self.orders.append((order, None))
        self.positions[order.market_ticker] = {'count': 0}
        return ExecutionResult(
            success=True,
            market_ticker=order.market_ticker,
            side='yes',
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=order.quantity,
            total_cost_cents=-(order.price * order.quantity),
            order_id='sell-order',
            notes='filled',
        )

    async def get_balance(self):
        return 0

    async def get_active_markets(self, series_prefix: str = ''):
        return []

    async def get_positions(self):
        return dict(self.positions)


def make_config(**overrides):
    cfg = AppConfig(
        kalshi_api_key='test-key',
        kalshi_private_key_path='unused.pem',
        mysql_database_url='******localhost:3306/test',
        trading_mode='PAPER',
        initial_contract_count=2,
        monitor_start_price=80,
        buy_trigger_price=82,
        spread_monitor_price=90,
        minimum_spread=4,
        stop_loss_price=35,
        hedge_max_factor=3.0,
        dry_run=False,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def make_strategy(monkeypatch, db=None, **config_overrides):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, 'load_private_key', lambda _path: object())
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        db or FakeDB(),
    )


def add_holding(strategy, ticker, qty=2, avg_entry=80):
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker='EVT',
        series_ticker=ticker.split('-')[0],
        bracket_label='test',
        phase=Phase.HOLDING,
        position_quantity=qty,
        avg_entry=avg_entry,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.executor.positions[ticker] = {'count': qty, 'average_fill_cost_cents': avg_entry}
    return bracket


@pytest.mark.asyncio
async def test_stop_loss_sells_when_last_trade_below_threshold(monkeypatch):
    import core.state_machine as state_machine

    logs = []
    monkeypatch.setattr(state_machine.logger, 'warning', lambda event, **kwargs: logs.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, 'info', lambda event, **kwargs: logs.append((event, kwargs)))

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
    ticker = 'KXLOWTBOS-26JUN23-B65.5'
    add_holding(strategy, ticker, qty=2)
    strategy.cache.update_last_price(ticker, 49)

    await strategy._evaluate_held_positions()

    assert len(strategy.executor.sell_orders) == 1
    assert any(event == 'phase.c.stop_loss_triggered' for event, _ in logs)


@pytest.mark.asyncio
@pytest.mark.parametrize('last_price', [50, 51])
async def test_no_stop_loss_at_or_above_threshold(monkeypatch, last_price):
    strategy = make_strategy(monkeypatch, stop_loss_price=50)
    ticker = 'KXLOWTBOS-26JUN23-B65.5'
    add_holding(strategy, ticker, qty=2)
    strategy.cache.update_last_price(ticker, last_price)

    await strategy._evaluate_held_positions()

    assert len(strategy.executor.sell_orders) == 0


@pytest.mark.asyncio
async def test_no_stop_loss_without_last_trade(monkeypatch):
    strategy = make_strategy(monkeypatch, stop_loss_price=50)
    ticker = 'KXLOWTBOS-26JUN23-B65.5'
    add_holding(strategy, ticker, qty=2)

    await strategy._evaluate_held_positions()

    assert len(strategy.executor.sell_orders) == 0


@pytest.mark.asyncio
async def test_stop_loss_increments_ledger(monkeypatch):
    strategy = make_strategy(monkeypatch, stop_loss_price=50)
    ticker = 'KXLOWTBOS-26JUN23-B65.5'
    add_holding(strategy, ticker, qty=2)
    strategy.cache.update_last_price(ticker, 49)

    await strategy._evaluate_held_positions()

    count = await strategy._get_stop_loss_count_for_market(ticker)
    assert count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('count,expected_qty', [(0, 2), (1, 4), (2, 8), (3, 16)])
async def test_recovery_sizing_doubles(monkeypatch, count, expected_qty):
    strategy = make_strategy(monkeypatch)
    ticker = 'KXLOWTBOS-26JUN23-B65.5'

    for _ in range(count):
        await strategy._increment_stop_loss_count_for_market(ticker)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker='EVT',
        series_ticker='KXLOWTBOS',
        bracket_label='test',
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    strategy._execute_entry.assert_awaited_once()
    assert strategy._execute_entry.await_args.kwargs['quantity'] == expected_qty


@pytest.mark.asyncio
async def test_recovery_cap_blocks_after_factor(monkeypatch):
    import core.state_machine as state_machine

    logs = []
    monkeypatch.setattr(state_machine.logger, 'info', lambda event, **kwargs: logs.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    ticker = 'KXLOWTBOS-26JUN23-B65.5'

    for _ in range(3):
        await strategy._increment_stop_loss_count_for_market(ticker)

    bracket_boundary = MarketBracket(
        market_ticker=ticker,
        event_ticker='EVT',
        series_ticker='KXLOWTBOS',
        bracket_label='boundary',
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket_boundary
    strategy.cache.update_quote(ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()
    assert strategy._execute_entry.await_args.kwargs['quantity'] == 16

    strategy2 = make_strategy(monkeypatch, db=strategy.db)
    ticker2 = 'KXLOWTBOS-26JUN23-T68'
    for _ in range(1):
        await strategy2._increment_stop_loss_count_for_market(ticker2)

    bracket_cap = MarketBracket(
        market_ticker=ticker2,
        event_ticker='EVT2',
        series_ticker='KXLOWTBOS',
        bracket_label='cap',
        phase=Phase.MONITORING,
    )
    strategy2.brackets[ticker2] = bracket_cap
    strategy2.cache.update_quote(ticker2, 80, 82)
    strategy2._execute_entry = AsyncMock()

    await strategy2._evaluate_watchlist()

    strategy2._execute_entry.assert_not_awaited()
    assert bracket_cap.crossed_buy is True
    assert any(event == 'phase.b.recovery_cap_reached' for event, _ in logs)


@pytest.mark.asyncio
async def test_high_low_counters_independent(monkeypatch):
    strategy = make_strategy(monkeypatch)
    low_ticker = 'KXLOWTBOS-26JUN23-B65.5'
    high_ticker = 'KXHIGHTBOS-26JUN23-B75.5'

    await strategy._increment_stop_loss_count_for_market(low_ticker)

    bracket = MarketBracket(
        market_ticker=high_ticker,
        event_ticker='EVT',
        series_ticker='KXHIGHTBOS',
        bracket_label='high',
        phase=Phase.MONITORING,
    )
    strategy.brackets[high_ticker] = bracket
    strategy.cache.update_quote(high_ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert strategy._execute_entry.await_args.kwargs['quantity'] == 2


@pytest.mark.asyncio
async def test_any_bracket_in_series_uses_counter(monkeypatch):
    strategy = make_strategy(monkeypatch)
    stop_ticker = 'KXLOWTBOS-26JUN23-B65.5'
    recovery_ticker = 'KXLOWTBOS-26JUN23-T68'

    await strategy._increment_stop_loss_count_for_market(stop_ticker)

    bracket = MarketBracket(
        market_ticker=recovery_ticker,
        event_ticker='EVT',
        series_ticker='KXLOWTBOS',
        bracket_label='recover',
        phase=Phase.MONITORING,
    )
    strategy.brackets[recovery_ticker] = bracket
    strategy.cache.update_quote(recovery_ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert strategy._execute_entry.await_args.kwargs['quantity'] == 4


def test_parse_series_and_date():
    assert parse_series_and_date('KXLOWTSATX-26JUN23-T78') == ('KXLOWTSATX', '26JUN23')
    assert parse_series_and_date('KXHIGHTPHX-26JUN23-B111.5') == ('KXHIGHTPHX', '26JUN23')
    assert parse_series_and_date('KXHIGHNY-26JUN23-T90') == ('KXHIGHNY', '26JUN23')
    assert parse_series_and_date('not-a-market-ticker') is None


@pytest.mark.asyncio
async def test_ledger_persists_across_restart(monkeypatch):
    shared_db = FakeDB()
    ticker = 'KXLOWTBOS-26JUN23-B65.5'

    strategy1 = make_strategy(monkeypatch, db=shared_db)
    await strategy1._increment_stop_loss_count_for_market(ticker)

    strategy2 = make_strategy(monkeypatch, db=shared_db)
    count = await strategy2._get_stop_loss_count_for_market(ticker)

    assert count == 1


def test_config_loads_without_hedge_trigger_price(monkeypatch):
    monkeypatch.delenv('HEDGE_TRIGGER_PRICE', raising=False)
    monkeypatch.delenv('HEDGE_BUY', raising=False)
    cfg = AppConfig(
        kalshi_api_key='test-key',
        kalshi_private_key_path='unused.pem',
        mysql_database_url='******localhost:3306/test',
        trading_mode='PAPER',
        initial_contract_count=2,
        monitor_start_price='0.80',
        buy_trigger_price='0.82',
        spread_monitor_price='0.90',
        minimum_spread='0.04',
        stop_loss_price='0.35',
        hedge_max_factor='3',
        dry_run=False,
    )

    assert cfg.hedge_trigger_price == 0
    assert cfg.hedge_buy == 0
    assert cfg.stop_loss_price == 35
    assert cfg.hedge_max_factor == 3.0
