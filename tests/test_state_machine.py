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
    def __init__(self, items=None):
        self._items = items or []

    def scalar_one_or_none(self):
        return None

    def scalars(self):
        return self

    def all(self):
        return self._items


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
        self.succeed = False  # set True to make buy_yes return success

    async def buy_yes(self, order, max_price=None):
        self.orders.append((order, max_price))
        if self.succeed:
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=order.price * order.quantity,
                order_id="fake-order-id",
                notes="test-success",
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
    # Drive prices via ticker-quote cache (yes_ask=82, yes_bid=82-spread)
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
async def test_evaluate_watchlist_ticker_quote_triggers_entry(monkeypatch):
    """Market with yes_ask >= buy_trigger and tight spread enters via ticker quote."""
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHTPHX-26JUN22-B84.5",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="phoenix",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    # yes_ask=84 >= buy_trigger=82, spread=6 <= minimum_spread=7 (override below)
    strategy.cache.update_quote(bracket.market_ticker, 78, 84)
    strategy._execute_entry = AsyncMock()
    # Use minimum_spread=7 so spread of 6 passes
    strategy.config.minimum_spread = 7

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is True
    strategy._execute_entry.assert_awaited_once_with(bracket)
    events = [event for event, _ in logged]
    assert "phase.b.buying" in events


@pytest.mark.asyncio
async def test_evaluate_watchlist_wide_spread_blocked(monkeypatch):
    """Market with wide spread is blocked by spread gate."""
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHTPHX-26JUN22-B84.5",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="phoenix wide",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    # yes_ask=84 >= buy_trigger=82, but spread=10 > minimum_spread=4
    strategy.cache.update_quote(bracket.market_ticker, 74, 84)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is False
    strategy._execute_entry.assert_not_awaited()
    events = [event for event, _ in logged]
    assert "phase.b.spread_too_wide" in events


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
    # Use YES ask from ticker-quote cache (authoritative source, not orderbook best_ask)
    strategy.cache.update_quote(hedge_bracket.market_ticker, hedge_price - 1, hedge_price)
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
        def __init__(self, **_kwargs):
            pass

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


# ---------------------------------------------------------------------------
# New tests: hedge price source, multi-hedge, top-off, circuit-breaker, independence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hedge_uses_yes_ask_from_ticker_quote_not_orderbook(monkeypatch):
    """_execute_hedge reads the YES ask from the ticker-quote cache, not orderbook.best_ask."""
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_args, **_kwargs: None)

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHTPHX-26JUN20-B84.5",
        event_ticker="KXHIGHTPHX-26JUN20",
        series_ticker="KXHIGHTPHX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=85,
    )
    hedge_bracket = MarketBracket(
        market_ticker="KXHIGHTPHX-26JUN20-T85",
        event_ticker="KXHIGHTPHX-26JUN20",
        series_ticker="KXHIGHTPHX",
        bracket_label="hedge",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.brackets[hedge_bracket.market_ticker] = hedge_bracket

    # Deliberately set a WRONG price in the orderbook — hedge must NOT use this
    strategy.cache.orderbooks[hedge_bracket.market_ticker] = OrderBook(
        yes_bids=[OrderBookLevel(price=40, quantity=5, order_count=1)],
        yes_asks=[OrderBookLevel(price=40, quantity=5, order_count=1)],  # wrong price
    )
    # Set the CORRECT YES ask in the ticker-quote cache
    strategy.cache.update_quote(hedge_bracket.market_ticker, 59, 60)

    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=hedge_bracket.market_ticker))

    await strategy._execute_hedge(bracket)

    assert len(strategy.executor.orders) == 1
    order, _ = strategy.executor.orders[0]
    # Must use the ticker-quote YES ask (60), not the orderbook price (40)
    assert order.price == 60


@pytest.mark.asyncio
async def test_event_can_be_hedged_multiple_times(monkeypatch):
    """Removing the single-hedge block allows the same bracket to be re-hedged."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXLOWTMIA-26JUN20-B75.5",
        event_ticker="KXLOWTMIA-26JUN20",
        series_ticker="KXLOWTMIA",
        bracket_label="origin",
        phase=Phase.HEDGED,  # already hedged once
        position_quantity=3,
        avg_entry=84,
    )
    # Simulate that a previous hedge bracket was stop-lossed (no longer in active_positions)
    bracket.hedge_market = "KXLOWTMIA-26JUN20-T76"

    hedge_bracket2 = MarketBracket(
        market_ticker="KXLOWTMIA-26JUN20-T77",
        event_ticker="KXLOWTMIA-26JUN20",
        series_ticker="KXLOWTMIA",
        bracket_label="second hedge",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.brackets[hedge_bracket2.market_ticker] = hedge_bracket2
    # Provide a YES ask for the new hedge target
    strategy.cache.update_quote(hedge_bracket2.market_ticker, 61, 62)

    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=hedge_bracket2.market_ticker))

    # First re-hedge attempt
    await strategy._execute_hedge(bracket)
    # Second re-hedge attempt (no permanent block)
    await strategy._execute_hedge(bracket)

    # Both orders were placed (no single-hedge block)
    assert len(strategy.executor.orders) == 2
    assert strategy.executor.orders[0][0].market_ticker == hedge_bracket2.market_ticker
    assert strategy.executor.orders[1][0].market_ticker == hedge_bracket2.market_ticker


@pytest.mark.asyncio
async def test_topoff_fires_when_sibling_closed_and_ask_high(monkeypatch):
    """Phase-2 top-off fires when YES ask >= buy_trigger and all siblings are closed."""
    import core.state_machine as state_machine

    strategy = make_strategy(monkeypatch)
    event_ticker = "KXHIGHTDEN-26JUN20"

    # Surviving bracket (the likely winner, recovering to 85¢)
    survivor = MarketBracket(
        market_ticker="KXHIGHTDEN-26JUN20-T96",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDEN",
        bracket_label="survivor",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=85,
    )
    strategy.brackets[survivor.market_ticker] = survivor
    strategy.active_positions[survivor.market_ticker] = survivor

    # Mark event as hedged (ledger/top-off logic active)
    strategy._hedged_events.add(event_ticker)

    # YES ask at 85¢ (above buy_trigger=82)
    strategy.cache.update_quote(survivor.market_ticker, 83, 85)

    # Mock _execute_topoff so we can verify it was called
    topoff_called = []

    async def fake_topoff(b, ask):
        topoff_called.append((b.market_ticker, ask))

    monkeypatch.setattr(strategy, "_execute_topoff", fake_topoff)

    # Simulate API positions response
    strategy.executor.positions = {
        survivor.market_ticker: {"count": 2, "last_price_cents": 85}
    }

    async def fake_get_positions():
        return {survivor.market_ticker: {"count": 2, "last_price_cents": 85}}

    monkeypatch.setattr(strategy.executor, "get_positions", fake_get_positions)

    await strategy._evaluate_held_positions()

    assert len(topoff_called) == 1
    assert topoff_called[0] == (survivor.market_ticker, 85)


@pytest.mark.asyncio
async def test_topoff_does_not_fire_when_sibling_still_open(monkeypatch):
    """Phase-2 top-off must NOT fire if a sibling bracket is still open."""
    import core.state_machine as state_machine

    strategy = make_strategy(monkeypatch)
    event_ticker = "KXHIGHTDEN-26JUN20"

    survivor = MarketBracket(
        market_ticker="KXHIGHTDEN-26JUN20-T96",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDEN",
        bracket_label="survivor",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=85,
    )
    # Sibling still open (not yet stop-lossed)
    sibling = MarketBracket(
        market_ticker="KXHIGHTDEN-26JUN20-B95.5",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDEN",
        bracket_label="sibling",
        phase=Phase.HEDGED,
        position_quantity=1,
        avg_entry=84,
    )
    strategy.brackets[survivor.market_ticker] = survivor
    strategy.brackets[sibling.market_ticker] = sibling
    strategy.active_positions[survivor.market_ticker] = survivor
    strategy.active_positions[sibling.market_ticker] = sibling

    strategy._hedged_events.add(event_ticker)
    strategy.cache.update_quote(survivor.market_ticker, 83, 85)

    topoff_called = []

    async def fake_topoff(b, ask):
        topoff_called.append((b.market_ticker, ask))

    monkeypatch.setattr(strategy, "_execute_topoff", fake_topoff)

    async def fake_get_positions():
        return {
            survivor.market_ticker: {"count": 2, "last_price_cents": 85},
            sibling.market_ticker: {"count": 1, "last_price_cents": 40},
        }

    monkeypatch.setattr(strategy.executor, "get_positions", fake_get_positions)

    await strategy._evaluate_held_positions()

    assert len(topoff_called) == 0


@pytest.mark.asyncio
async def test_topoff_break_even_qty_rounded_up(monkeypatch):
    """_execute_topoff computes break-even quantity rounded up from the ledger."""
    import core.state_machine as state_machine
    from app.models import ExecutedTrade as ET, TradeAction, TradeStatus

    strategy = make_strategy(monkeypatch)
    event_ticker = "KXHIGHTPHX-26JUN20"

    survivor = MarketBracket(
        market_ticker="KXHIGHTPHX-26JUN20-T106",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTPHX",
        bracket_label="survivor",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=85,
    )
    strategy.brackets[survivor.market_ticker] = survivor
    strategy.active_positions[survivor.market_ticker] = survivor
    strategy._hedged_events.add(event_ticker)

    # Simulate ledger: initial BUY 2×85=170, HEDGE 2×60=120, total gross=290
    # remaining_deficit = 290 - (2 * 100) = 90
    # yes_ask = 85 => profit_per_contract = 15
    # topoff_qty = ceil(90/15) = 6
    mock_ledger = {
        "initial_cost_cents": 170,
        "gross_spend_cents": 290,
        "stop_loss_proceeds_cents": 0,
        "open_tickers": {survivor.market_ticker},
        "closed_tickers": set(),
    }

    async def fake_ledger(et):
        return mock_ledger

    monkeypatch.setattr(strategy, "_event_ledger", fake_ledger)

    await strategy._execute_topoff(survivor, yes_ask=85)

    assert len(strategy.executor.orders) == 1
    order, max_price = strategy.executor.orders[0]
    assert order.market_ticker == survivor.market_ticker
    assert order.price == 85
    assert order.quantity == 6  # ceil(90/15)
    assert max_price == strategy.config.spread_monitor_price


@pytest.mark.asyncio
async def test_topoff_case_b_hedge_premium_covered_by_ledger(monkeypatch):
    """Case B: original bracket recovers while hedge will lose — ledger covers both costs."""
    import core.state_machine as state_machine

    strategy = make_strategy(monkeypatch)
    event_ticker = "KXHIGHTATL-26JUN20"

    # Original bracket recovering (the one that was originally bought)
    original = MarketBracket(
        market_ticker="KXHIGHTATL-26JUN20-B84.5",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTATL",
        bracket_label="original",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=85,
    )
    strategy.brackets[original.market_ticker] = original
    strategy.active_positions[original.market_ticker] = original
    strategy._hedged_events.add(event_ticker)

    # Ledger: initial BUY=200 (2×100¢ = 200), HEDGE=120 (2×60¢), gross=320
    # With yes_ask=85 => profit/contract=15
    # remaining_deficit = 320 - (2*100) = 120, topoff_qty = ceil(120/15) = 8
    mock_ledger = {
        "initial_cost_cents": 200,
        "gross_spend_cents": 320,
        "stop_loss_proceeds_cents": 0,
        "open_tickers": {original.market_ticker},
        "closed_tickers": set(),
    }

    async def fake_ledger(et):
        return mock_ledger

    monkeypatch.setattr(strategy, "_event_ledger", fake_ledger)

    await strategy._execute_topoff(original, yes_ask=85)

    assert len(strategy.executor.orders) == 1
    order, _ = strategy.executor.orders[0]
    assert order.quantity == 8  # ceil(120/15) — hedge premium automatically covered


@pytest.mark.asyncio
async def test_circuit_breaker_blocks_hedge_when_cap_exceeded(monkeypatch):
    """Circuit-breaker stops hedge when gross spend would exceed HEDGE_MAX_FACTOR × initial cost."""
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "warning", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_max_factor=2.0)
    bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUN20-B60.5",
        event_ticker="KXLOWTBOS-26JUN20",
        series_ticker="KXLOWTBOS",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=5,
        avg_entry=84,
    )
    hedge_bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUN20-T61",
        event_ticker="KXLOWTBOS-26JUN20",
        series_ticker="KXLOWTBOS",
        bracket_label="hedge",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.brackets[hedge_bracket.market_ticker] = hedge_bracket
    strategy.cache.update_quote(hedge_bracket.market_ticker, 59, 60)
    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=hedge_bracket.market_ticker))

    # Ledger: initial_cost=420 (5×84), gross_spend already at 600
    # max_event_spend = 2.0 × 420 = 840
    # hedge order cost = ceil(...) × 60 would push over 840
    mock_ledger = {
        "initial_cost_cents": 420,
        "gross_spend_cents": 800,  # already close to cap
        "stop_loss_proceeds_cents": 0,
        "open_tickers": {bracket.market_ticker},
        "closed_tickers": set(),
    }

    async def fake_ledger(et):
        return mock_ledger

    monkeypatch.setattr(strategy, "_event_ledger", fake_ledger)

    await strategy._execute_hedge(bracket)

    # No order placed; circuit-breaker warning logged; event added to cap_reached
    assert len(strategy.executor.orders) == 0
    assert "KXLOWTBOS-26JUN20" in strategy._cap_reached_events
    cap_logs = [ev for ev, _ in logged if ev == "phase.c.hedge_cap_reached"]
    assert len(cap_logs) == 1
    cap_kw = next(kw for ev, kw in logged if ev == "phase.c.hedge_cap_reached")
    assert cap_kw["event_ticker"] == "KXLOWTBOS-26JUN20"
    assert cap_kw["gross_spend_cents"] == 800
    assert cap_kw["max_event_spend_cents"] == 840


@pytest.mark.asyncio
async def test_circuit_breaker_does_not_affect_other_events(monkeypatch):
    """A cap_reached event must not affect a different event_ticker."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)

    # Mark the HIGH event as cap-reached
    high_event = "KXHIGHTDFW-26JUN20"
    strategy._cap_reached_events.add(high_event)

    # The LOW event should be unaffected
    low_event = "KXLOWTDFW-26JUN20"
    bracket_low = MarketBracket(
        market_ticker="KXLOWTDFW-26JUN20-B75.5",
        event_ticker=low_event,
        series_ticker="KXLOWTDFW",
        bracket_label="low origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    hedge_bracket_low = MarketBracket(
        market_ticker="KXLOWTDFW-26JUN20-T76",
        event_ticker=low_event,
        series_ticker="KXLOWTDFW",
        bracket_label="low hedge",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket_low.market_ticker] = bracket_low
    strategy.brackets[hedge_bracket_low.market_ticker] = hedge_bracket_low
    strategy.cache.update_quote(hedge_bracket_low.market_ticker, 61, 62)
    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=hedge_bracket_low.market_ticker))

    await strategy._execute_hedge(bracket_low)

    # LOW event hedge proceeds normally (order placed)
    assert len(strategy.executor.orders) == 1
    assert strategy.executor.orders[0][0].market_ticker == hedge_bracket_low.market_ticker
    assert low_event not in strategy._cap_reached_events


@pytest.mark.asyncio
async def test_independent_events_high_and_low_tracked_separately(monkeypatch):
    """KXHIGHT and KXLOWT of the same city are independent events with separate ledger state."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    strategy.executor.succeed = True

    high_event = "KXHIGHTLAX-26JUN20"
    low_event = "KXLOWTLAX-26JUN20"

    # Set up the HIGH event bracket and its sibling
    bracket_high = MarketBracket(
        market_ticker="KXHIGHTLAX-26JUN20-B84.5",
        event_ticker=high_event,
        series_ticker="KXHIGHTLAX",
        bracket_label="high",
        phase=Phase.HOLDING,
        position_quantity=1,
        avg_entry=85,
    )
    sibling_high = MarketBracket(
        market_ticker="KXHIGHTLAX-26JUN20-T85",
        event_ticker=high_event,
        series_ticker="KXHIGHTLAX",
        bracket_label="high sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket_high.market_ticker] = bracket_high
    strategy.brackets[sibling_high.market_ticker] = sibling_high
    strategy.cache.update_quote(sibling_high.market_ticker, 59, 60)

    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=sibling_high.market_ticker))

    # Hedge the HIGH event
    await strategy._execute_hedge(bracket_high)

    # HIGH event is now hedged
    assert high_event in strategy._hedged_events
    # LOW event remains unaffected — no ledger or hedge state
    assert low_event not in strategy._hedged_events
    assert low_event not in strategy._cap_reached_events


@pytest.mark.asyncio
async def test_evaluate_watchlist_skips_below_floor_quietly(monkeypatch):
    """MONITORING bracket with price <= eval_price_floor is skipped without logging below_trigger."""
    import core.state_machine as state_machine

    debug_logged = []
    monkeypatch.setattr(state_machine.logger, "debug", lambda event, **kwargs: debug_logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXHIGHTPHX-26JUN22-T98",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="near-dead bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    # yes_ask=1 <= eval_price_floor=5
    strategy.cache.update_quote(bracket.market_ticker, 0, 1)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    # last_price should still be updated
    assert bracket.last_price == 1
    # No below_trigger log emitted for floor-skipped brackets
    events = [event for event, _ in debug_logged]
    assert "phase.b.below_trigger" not in events
    # Entry must not have been triggered
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_evaluate_watchlist_logs_below_trigger_above_floor(monkeypatch):
    """MONITORING bracket with price above floor but below buy_trigger still logs below_trigger."""
    import core.state_machine as state_machine

    debug_logged = []
    monkeypatch.setattr(state_machine.logger, "debug", lambda event, **kwargs: debug_logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    bracket = MarketBracket(
        market_ticker="KXLOWTDC-26JUN22-T71",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="live-but-below-trigger bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    # yes_ask=45 > eval_price_floor=5, but < buy_trigger=82
    strategy.cache.update_quote(bracket.market_ticker, 2, 45)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    # below_trigger should be logged for above-floor brackets
    events = [event for event, _ in debug_logged]
    assert "phase.b.below_trigger" in events
    # Entry must not have been triggered
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("price,expect_log", [
    (5, False),   # exactly at floor — silently skipped
    (6, True),    # one cent above floor — logged as below_trigger
])
async def test_evaluate_watchlist_floor_boundary(monkeypatch, price, expect_log):
    """Verify the floor boundary: price==floor is skipped; price==floor+1 is logged."""
    import core.state_machine as state_machine

    debug_logged = []
    monkeypatch.setattr(state_machine.logger, "debug", lambda event, **kwargs: debug_logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch, eval_price_floor=5)
    bracket = MarketBracket(
        market_ticker="KXHIGHTDC-26JUN22-B88.5",
        event_ticker="EVT1",
        series_ticker="SER1",
        bracket_label="boundary bracket",
        phase=Phase.MONITORING,
    )
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 0, price)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    events = [event for event, _ in debug_logged]
    if expect_log:
        assert "phase.b.below_trigger" in events
    else:
        assert "phase.b.below_trigger" not in events
    strategy._execute_entry.assert_not_awaited()
