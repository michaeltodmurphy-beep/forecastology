import os
import sys
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.config import AppConfig
from core.state_machine import TemperatureStrategy
from core.types import MarketBracket, OrderBook, OrderBookLevel, Phase
from data.ticker_cache import TickerCache
from execution.base import ExecutionResult


class FakeWSManager:
    def on_message(self, *_args, **_kwargs):
        return None

    async def subscribe(self, *_args, **_kwargs):
        return None


class FakeSessionResult:
    def scalar_one_or_none(self):
        return None


class FakeSession:
    def __init__(self):
        self.added = []

    def add(self, item):
        self.added.append(item)

    async def commit(self):
        return None

    async def execute(self, *_args, **_kwargs):
        return FakeSessionResult()

    async def rollback(self):
        return None


class FakeSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeDB:
    def __init__(self):
        self.session = FakeSession()

    async def get_session(self):
        return FakeSessionContext(self.session)


class FakeExecutor:
    def __init__(self):
        self.orders = []

    async def buy_yes(self, order, max_price=None):
        self.orders.append((order, max_price))
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=order.price,
            fill_quantity=0,
            total_cost_cents=0,
            notes="rejected-for-test",
        )

    async def sell_yes(self, order):
        raise NotImplementedError

    async def get_balance(self):
        return 0

    async def get_active_markets(self, series_prefix: str = ""):
        return []

    async def get_positions(self):
        return {}


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
        hedge_trigger_price=48,
        stop_loss_price=35,
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def make_strategy(monkeypatch, **config_overrides):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        FakeDB(),
    )


@pytest.mark.asyncio
async def test_strategy_started_logs_minimum_spread(monkeypatch):
    import core.state_machine as state_machine

    logged = []

    def fake_create_task(coro):
        coro.close()
        return object()

    monkeypatch.setattr(state_machine.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch, minimum_spread=7)
    monkeypatch.setattr(strategy, "_restore_positions", AsyncMock())
    monkeypatch.setattr(strategy, "_strategy_loop", AsyncMock())
    monkeypatch.setattr(strategy, "_db_cleanup_loop", AsyncMock())

    await strategy.start()

    start_log = next(kwargs for event, kwargs in logged if event == "strategy.started")
    assert start_log["minimum_spread"] == 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spread", "expected_note"),
    [
        (0, "crossed"),
        (3, "tight"),
        (4, "normal"),
    ],
)
async def test_evaluate_watchlist_logs_spread_note(monkeypatch, spread, expected_note):
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXLOWTSEA-26JUN22-B53.5",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="test bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.orderbooks[bracket.market_ticker] = OrderBook(
        yes_bids=[OrderBookLevel(price=82 - spread, quantity=1, order_count=1)],
        yes_asks=[OrderBookLevel(price=82, quantity=1, order_count=1)],
    )
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
        series_ticker="SER1",
        bracket_label="thin bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"price": 89, "spread": 2})
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is True
    assert bracket.last_price == 89
    strategy._execute_entry.assert_awaited_once_with(bracket)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("avg_entry", "position_quantity", "hedge_price", "expected_qty", "expected_reason"),
    [
        (90, 10, 90, 10, "quantity"),
        (82, 10, 90, 9, "cost"),
    ],
)
async def test_execute_hedge_caps_quantity(monkeypatch, avg_entry, position_quantity, hedge_price, expected_qty, expected_reason):
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_args, **_kwargs: None)

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXLOWTLAX-26JUN20-B60.5",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=position_quantity,
        avg_entry=avg_entry,
    )
    hedge_bracket = MarketBracket(
        market_ticker="KXLOWTLAX-26JUN20-T61",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="hedge",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.brackets[hedge_bracket.market_ticker] = hedge_bracket
    strategy.cache.orderbooks[hedge_bracket.market_ticker] = OrderBook(
        yes_bids=[OrderBookLevel(price=hedge_price - 1, quantity=1, order_count=1)],
        yes_asks=[OrderBookLevel(price=hedge_price, quantity=1, order_count=1)],
    )
    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=hedge_bracket.market_ticker))

    await strategy._execute_hedge(bracket)

    order, max_price = strategy.executor.orders[0]
    hedge_log = next(kwargs for event, kwargs in logged if event == "phase.c.hedge_quantity_calc")

    assert order.quantity == expected_qty
    assert max_price == strategy.config.spread_monitor_price
    assert hedge_log["raw_qty"] > expected_qty
    assert hedge_log["capped_qty"] == expected_qty
    assert hedge_log["cap_reason"] == expected_reason


@pytest.mark.asyncio
async def test_ensure_bracket_filters_non_today_tickers(monkeypatch):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, "get_eastern_today_date_prefix", lambda days_offset=0: "26JUN21")

    strategy = make_strategy(monkeypatch)

    await strategy._ensure_bracket("KXLOWTSEA-26JUN22-B53.5")
    await strategy._ensure_bracket("KXLOWTSEA-26JUN21-B53.5")

    assert "KXLOWTSEA-26JUN22-B53.5" not in strategy.brackets
    assert "KXLOWTSEA-26JUN21-B53.5" in strategy.brackets


@pytest.mark.asyncio
async def test_handle_lifecycle_ignores_non_today_event_markets(monkeypatch):
    import app.signing
    import core.state_machine as state_machine
    import httpx

    class FakeLifecycleResponse:
        status_code = 200

        def json(self):
            return {
                "markets": [
                    {"ticker": "KXLOWTSEA-26JUN21-B53.5", "title": "today primary"},
                    {"ticker": "KXLOWTSEA-26JUN21-T54", "title": "today secondary"},
                    {"ticker": "KXLOWTSEA-26JUN22-B54.5", "title": "tomorrow"},
                    {"ticker": "NOTTEMP-26JUN21-X1", "title": "other"},
                ]
            }

    class FakeLifecycleClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *_args, **_kwargs):
            return FakeLifecycleResponse()

    monkeypatch.setattr(state_machine, "get_eastern_today_date_prefix", lambda days_offset=0: "26JUN21")
    monkeypatch.setattr(app.signing, "load_private_key", lambda _path: object())
    monkeypatch.setattr(app.signing, "build_auth_headers", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(httpx, "AsyncClient", FakeLifecycleClient)

    strategy = make_strategy(monkeypatch, rest_base_url="https://example.test")

    await strategy._handle_lifecycle(
        {
            "msg": {
                "type": "created",
                "market_ticker": "KXLOWTSEA-26JUN21-B53.5",
                "event_ticker": "KXLOWTSEA-26JUN21",
                "series_ticker": "KXLOWTSEA",
                "title": "created market",
            }
        }
    )

    assert "KXLOWTSEA-26JUN21-B53.5" in strategy.brackets
    assert "KXLOWTSEA-26JUN21-T54" in strategy.brackets
    assert "KXLOWTSEA-26JUN22-B54.5" not in strategy.brackets
    assert "NOTTEMP-26JUN21-X1" not in strategy.brackets
