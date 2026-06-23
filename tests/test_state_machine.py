import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.config import AppConfig
from core.state_machine import TemperatureStrategy, RECOVERY_MAX_CONSECUTIVE_FAILURES
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
        return self._items


class FakeSession:
    def __init__(self, items=None):
        self.added = []
        self._items = items or []

    def add(self, item):
        self.added.append(item)

    async def commit(self):
        return None

    async def execute(self, *_args, **_kwargs):
        return FakeSessionResult(self._items)

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
    def __init__(self, items=None):
        self.session = FakeSession(items)

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
        self.orders.append((order, None))
        if self.succeed:
            return ExecutionResult(
                success=True,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=order.price,
                fill_quantity=order.quantity,
                total_cost_cents=-(order.price * order.quantity),
                order_id="fake-sell-id",
                notes="test-sell-success",
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
            notes="rejected-sell-for-test",
        )

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
        dry_run=False,
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def make_strategy(monkeypatch, db_items=None, **config_overrides):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        FakeDB(db_items),
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

    strategy = make_strategy(monkeypatch, hedge_buy=95)  # gate above test's hedge_price (90)
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
async def test_execute_hedge_backfills_entry_from_positions(monkeypatch):
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    sibling = "KXLOWTSATX-26JUN23-T79"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
    )
    sibling_bracket = MarketBracket(
        market_ticker=sibling,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.brackets[sibling] = sibling_bracket
    strategy.cache.update_quote(sibling, 47, 48)
    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=sibling))
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"average_fill_cost_cents": 86}}))

    await strategy._execute_hedge(bracket)

    assert bracket.avg_entry == 86
    assert len(strategy.executor.orders) == 1
    assert any(event == "phase.c.hedge_entry_backfilled" and kwargs["source"] == "positions" and kwargs["cents"] == 86
               for event, kwargs in logged)
    assert all(event != "phase.c.hedge_no_entry_price" for event, _ in logged)


@pytest.mark.asyncio
async def test_execute_hedge_backfills_entry_from_fills(monkeypatch):
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    sibling = "KXLOWTSATX-26JUN23-T79"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
    )
    sibling_bracket = MarketBracket(
        market_ticker=sibling,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.brackets[sibling] = sibling_bracket
    strategy.cache.update_quote(sibling, 47, 48)
    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=sibling))
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {}}))
    monkeypatch.setattr(
        strategy.executor,
        "get_fills",
        AsyncMock(return_value=[
            {"market_ticker": ticker, "action": "buy", "count_fp": "2", "yes_price_dollars": "0.85"},
            {"market_ticker": ticker, "action": "buy", "count_fp": "1", "yes_price_dollars": "0.85"},
        ]),
        raising=False,
    )

    await strategy._execute_hedge(bracket)

    assert bracket.avg_entry == 85
    assert len(strategy.executor.orders) == 1
    assert any(event == "phase.c.hedge_entry_backfilled" and kwargs["source"] == "fills" and kwargs["cents"] == 85
               for event, kwargs in logged)


@pytest.mark.asyncio
async def test_execute_hedge_falls_back_to_full_qty_when_entry_unknown(monkeypatch):
    import core.state_machine as state_machine

    logged_warnings = []
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda event, **kwargs: logged_warnings.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    sibling = "KXLOWTSATX-26JUN23-T79"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=3,
        avg_entry=0,
    )
    sibling_bracket = MarketBracket(
        market_ticker=sibling,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.brackets[sibling] = sibling_bracket
    strategy.cache.update_quote(sibling, 49, 50)
    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=sibling))
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {}}))
    monkeypatch.setattr(strategy.executor, "get_fills", AsyncMock(return_value=[]), raising=False)

    await strategy._execute_hedge(bracket)

    assert len(strategy.executor.orders) == 1
    order, _ = strategy.executor.orders[0]
    assert order.quantity == 3
    assert any(event == "phase.c.hedge_size_fallback_no_entry" and kwargs["qty"] == 3
               for event, kwargs in logged_warnings)


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

    strategy = make_strategy(monkeypatch, hedge_buy=65)  # 62 ≤ 65, so hedge fires
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
    # Provide a YES ask for the new hedge target (62 ≤ hedge_buy=65)
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

    strategy = make_strategy(monkeypatch, hedge_buy=65)  # 62 ≤ 65, so hedge fires

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
    # YES ask=62 ≤ hedge_buy=65, so hedge fires
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


# ---------------------------------------------------------------------------
# Tests for: price-source fix, HEDGE_BUY gate, deferred hedge, guaranteed
# stop-loss, and top-off reconciliation (NYC scenario and variants).
# ---------------------------------------------------------------------------

def _make_fake_get_positions(positions_map: dict):
    """Helper: build a coroutine factory that returns a fixed positions dict."""
    async def _fake():
        return positions_map
    return _fake


@pytest.mark.asyncio
async def test_phase_c_price_from_yes_bid_not_stale_last_price(monkeypatch):
    """
    Phase C must resolve current_price from the YES bid/ask quote (cache.get_quote),
    NOT from the stale ticker last_price.  When the real bid is below stop_loss but
    last_price is high, stop-loss must fire.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    strategy.executor.succeed = True

    ticker = "KXHIGHTNYC-26JUN22-B72.5"
    event_ticker = "KXHIGHTNYC-26JUN22"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 72.5",
        phase=Phase.HOLDING,
        position_quantity=5,
        avg_entry=83,
        last_price=83,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket

    # Stale last_price is high — would fool the old implementation
    strategy.cache.update_last_price(ticker, 80)
    # Real YES quote: bid=25, ask=26 — both below stop_loss=35 and hedge_trigger=48
    strategy.cache.update_quote(ticker, 25, 26)

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 5, "last_price_cents": 80}}))

    await strategy._evaluate_held_positions()

    # Stop-loss must have been triggered because YES bid=25 <= stop_loss=35
    stop_loss_events = [ev for ev, _ in warn_logged if ev == "phase.c.stop_loss_triggered"]
    assert len(stop_loss_events) >= 1, (
        "stop_loss_triggered must fire when YES bid=25 <= stop_loss=35, "
        "even though stale last_price=80 is above all triggers"
    )


@pytest.mark.asyncio
async def test_phase_c_stop_loss_fires_when_cost_basis_unknown(monkeypatch):
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTCHI-26JUN22-T59"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTCHI-26JUN22",
        series_ticker="KXLOWTCHI",
        bracket_label="chi low 59",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
        last_price=20,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 25, 26)
    strategy.executor.succeed = True

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 25}}))

    await strategy._evaluate_held_positions()

    events = [event for event, _ in warn_logged]
    assert "phase.c.stop_loss_triggered" in events
    assert "phase.c.stop_loss_skipped_no_cost_basis" not in events
    sell_orders = [o for o, _ in strategy.executor.orders
                   if o.market_ticker == ticker and o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1
    assert sell_orders[0].price == 1  # sells at 1¢ to guarantee fill


@pytest.mark.asyncio
async def test_phase_c_stop_loss_fires_with_zero_entry_seattle_scenario(monkeypatch):
    """Regression test: replays the exact production failure where KXHIGHTSEA-26JUN22-B83.5
    had avg_entry=0 and rode from 35¢ to 1¢ because the stop-loss was skipped."""
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTSEA-26JUN22-B83.5"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTSEA-26JUN22",
        series_ticker="KXHIGHTSEA",
        bracket_label="sea high 83.5",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
        last_price=30,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 29, 30)
    strategy.executor.succeed = True

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 29}}))

    await strategy._evaluate_held_positions()

    events = [event for event, _ in warn_logged]
    assert "phase.c.stop_loss_triggered" in events
    sell_orders = [o for o, _ in strategy.executor.orders
                   if o.market_ticker == ticker and o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1
    assert sell_orders[0].price == 1  # sells at 1¢ to guarantee fill


@pytest.mark.asyncio
async def test_phase_c_stop_loss_skips_resolved_market(monkeypatch):
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, eval_price_floor=5)
    ticker = "KXLOWTSEA-26JUN22-T59"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSEA-26JUN22",
        series_ticker="KXLOWTSEA",
        bracket_label="sea low 59",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
        last_price=10,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 1, 2)

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 1}}))

    await strategy._evaluate_held_positions()

    events = [event for event, _ in warn_logged]
    assert "phase.c.stop_loss_skipped_resolved_market" in events
    assert "phase.c.stop_loss_triggered" not in events
    assert len(strategy.executor.orders) == 0


@pytest.mark.asyncio
async def test_phase_c_no_live_price_skips_trading_no_invented_fallback(monkeypatch):
    """
    When no real price is available, log phase.c.no_live_price and skip trading.
    The old 'avg_entry or 83' invented fallback must NOT be used.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)

    ticker = "KXHIGHTNYC-26JUN22-B72.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTNYC-26JUN22",
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 72.5",
        phase=Phase.HOLDING,
        position_quantity=3,
        avg_entry=83,
        last_price=None,   # no last known price
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket

    # No quote, no last_price in cache, REST returns nothing
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 3}}))

    await strategy._evaluate_held_positions()

    # Must log no_live_price
    no_price_events = [ev for ev, _ in warn_logged if ev == "phase.c.no_live_price"]
    assert len(no_price_events) == 1
    strategy._fetch_market_data_via_rest.assert_awaited_once_with(ticker)

    # Must NOT have placed any order (no invented price above triggers)
    assert len(strategy.executor.orders) == 0


@pytest.mark.asyncio
async def test_entry_reconciles_fill_price_zero_from_positions(monkeypatch):
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.MONITORING,
    )

    monkeypatch.setattr(
        strategy.executor,
        "buy_yes",
        AsyncMock(return_value=ExecutionResult(
            success=True,
            market_ticker=ticker,
            side="yes",
            price=82,
            quantity=2,
            fill_price=0,
            fill_quantity=2,
            total_cost_cents=0,
            order_id="x",
            notes="ok",
        )),
    )
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"average_fill_cost_cents": 84}}))

    await strategy._execute_entry(bracket, ob=OrderBook(yes_asks=[OrderBookLevel(price=82, quantity=10, order_count=1)]))

    assert bracket.avg_entry == 84
    assert ticker in strategy.active_positions
    assert any(event == "phase.b.entry_cost_reconciled" and kwargs["source"] == "positions" and kwargs["cents"] == 84
               for event, kwargs in logged)


@pytest.mark.asyncio
async def test_entry_reconciles_fill_price_zero_from_fills(monkeypatch):
    import core.state_machine as state_machine

    logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda event, **kwargs: logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.MONITORING,
    )

    monkeypatch.setattr(
        strategy.executor,
        "buy_yes",
        AsyncMock(return_value=ExecutionResult(
            success=True,
            market_ticker=ticker,
            side="yes",
            price=82,
            quantity=2,
            fill_price=0,
            fill_quantity=2,
            total_cost_cents=0,
            order_id="x",
            notes="ok",
        )),
    )
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {}}))
    monkeypatch.setattr(
        strategy.executor,
        "get_fills",
        AsyncMock(return_value=[
            {"market_ticker": ticker, "action": "buy", "count_fp": "2", "yes_price_dollars": "0.83"},
            {"market_ticker": ticker, "action": "buy", "count_fp": "2", "yes_price_dollars": "0.85"},
        ]),
        raising=False,
    )

    await strategy._execute_entry(bracket, ob=OrderBook(yes_asks=[OrderBookLevel(price=82, quantity=10, order_count=1)]))

    assert bracket.avg_entry == 84
    assert any(event == "phase.b.entry_cost_reconciled" and kwargs["source"] == "fills" and kwargs["cents"] == 84
               for event, kwargs in logged)


@pytest.mark.asyncio
async def test_entry_fill_price_zero_reconcile_failure_is_non_fatal(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.MONITORING,
    )

    monkeypatch.setattr(
        strategy.executor,
        "buy_yes",
        AsyncMock(return_value=ExecutionResult(
            success=True,
            market_ticker=ticker,
            side="yes",
            price=82,
            quantity=2,
            fill_price=0,
            fill_quantity=2,
            total_cost_cents=0,
            order_id="x",
            notes="ok",
        )),
    )
    monkeypatch.setattr(strategy.executor, "get_positions", AsyncMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(strategy.executor, "get_fills", AsyncMock(side_effect=RuntimeError("boom2")), raising=False)

    await strategy._execute_entry(bracket, ob=OrderBook(yes_asks=[OrderBookLevel(price=82, quantity=10, order_count=1)]))

    assert ticker in strategy.active_positions
    assert bracket.position_quantity == 2
    assert bracket.avg_entry == 0


@pytest.mark.asyncio
async def test_ask_price_can_trigger_hedge_even_when_bid_is_healthy(monkeypatch):
    import core.state_machine as state_machine

    debug_logged = []
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda event, **kwargs: debug_logged.append((event, kwargs)))

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 94, 47)
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 94}}))
    strategy._execute_hedge = AsyncMock(return_value=True)

    await strategy._evaluate_held_positions()

    strategy._execute_hedge.assert_awaited_once_with(bracket)
    hedge_eval = next(kwargs for event, kwargs in debug_logged if event == "phase.c.hedge_eval")
    assert hedge_eval["bid_price"] == 94
    assert hedge_eval["ask_price"] == 47


@pytest.mark.asyncio
async def test_no_hedge_when_bid_and_ask_are_healthy(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 94, 94)
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 94}}))
    strategy._execute_hedge = AsyncMock(return_value=True)

    await strategy._evaluate_held_positions()

    strategy._execute_hedge.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_loss_still_uses_realistic_price(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 30, 48)
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 30}}))
    strategy._execute_hedge = AsyncMock(return_value=True)
    strategy._execute_stop_loss = AsyncMock()

    await strategy._evaluate_held_positions()

    strategy._execute_stop_loss.assert_awaited_once_with(bracket)


@pytest.mark.asyncio
async def test_only_ask_price_available_does_not_skip_with_no_live_price(monkeypatch):
    import core.state_machine as state_machine

    warning_events = []
    monkeypatch.setattr(state_machine.logger, "warning", lambda event, **kwargs: warning_events.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXLOWTSATX-26JUN23-T78"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTSATX-26JUN23",
        series_ticker="KXLOWTSATX",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy._execute_hedge = AsyncMock(return_value=True)
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 46, "yes_bid": None, "price": None})
    monkeypatch.setattr(strategy.executor, "get_positions", _make_fake_get_positions({ticker: {"count": 2}}))

    await strategy._evaluate_held_positions()

    strategy._execute_hedge.assert_awaited_once_with(bracket)
    assert all(event != "phase.c.no_live_price" for event, _ in warning_events)


@pytest.mark.asyncio
async def test_restore_positions_uses_db_cost_basis_when_api_entry_missing(monkeypatch):
    import core.state_machine as state_machine

    info_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "error", lambda *_a, **_kw: None)

    ticker = "KXLOWTBOS-26JUN22-T59"
    db_position = SimpleNamespace(
        market_ticker=ticker,
        event_ticker="KXLOWTBOS-26JUN22",
        series_ticker="KXLOWTBOS",
        quantity=2,
        avg_entry_price=82,
        last_price=80,
        hedge_market_ticker=None,
        hedge_quantity=0,
        hedge_pending=0,
    )

    strategy = make_strategy(monkeypatch, db_items=[db_position], trading_mode="LIVE")
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "average_fill_cost_cents": 0}}))

    await strategy._restore_positions()

    restored = strategy.active_positions[ticker]
    live_log = next(kwargs for event, kwargs in info_logged if event == "strategy.restored_live_position")
    assert restored.avg_entry == 82
    assert live_log["entry"] == 82
    assert live_log["entry_source"] == "db"


@pytest.mark.asyncio
async def test_hedge_deferred_when_all_siblings_weak(monkeypatch):
    """
    DC scenario: original at ≤ hedge_trigger (45¢), all siblings priced at 30¢
    (below HEDGE_TRIGGER_PRICE=48¢) — no credible winner yet.  No order should be
    placed; event must be added to _pending_hedge_events; phase.c.hedge_deferred
    must be logged.  (Previously, when the gate was inverted, the bot would have
    bought this 30¢ sibling — that was the bug.)
    """
    import core.state_machine as state_machine

    info_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)

    original = MarketBracket(
        market_ticker="KXHIGHTNYC-26JUN22-B72.5",
        event_ticker="KXHIGHTNYC-26JUN22",
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 72.5",
        phase=Phase.HOLDING,
        position_quantity=3,
        avg_entry=83,
    )
    sibling = MarketBracket(
        market_ticker="KXHIGHTNYC-26JUN22-T73",
        event_ticker="KXHIGHTNYC-26JUN22",
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 73",
        phase=Phase.MONITORING,
    )
    strategy.brackets[original.market_ticker] = original
    strategy.brackets[sibling.market_ticker] = sibling

    # Sibling priced below HEDGE_TRIGGER_PRICE (48¢) — no credible winner
    strategy.cache.update_quote(sibling.market_ticker, 28, 30)

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sibling.market_ticker))

    result = await strategy._execute_hedge(original)

    # No order should be placed
    assert result is False
    assert len(strategy.executor.orders) == 0

    # Event must be armed
    assert "KXHIGHTNYC-26JUN22" in strategy._pending_hedge_events

    # phase.c.hedge_deferred must be logged
    deferred_events = [ev for ev, _ in info_logged if ev == "phase.c.hedge_deferred"]
    assert len(deferred_events) >= 1


@pytest.mark.asyncio
async def test_deferred_hedge_fills_when_sibling_drops_to_hedge_buy(monkeypatch):
    """
    Event is already armed (_pending_hedge_events).  A sibling now has YES ask = 60¢
    (exactly at HEDGE_BUY).  _execute_hedge must place the buy, clear the armed
    state, and add the event to _hedged_events.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTNYC-26JUN22"
    original = MarketBracket(
        market_ticker="KXHIGHTNYC-26JUN22-B72.5",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 72.5",
        phase=Phase.HOLDING,
        position_quantity=3,
        avg_entry=83,
    )
    sibling = MarketBracket(
        market_ticker="KXHIGHTNYC-26JUN22-T73",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 73",
        phase=Phase.MONITORING,
    )
    strategy.brackets[original.market_ticker] = original
    strategy.brackets[sibling.market_ticker] = sibling

    # Event is already armed
    strategy._pending_hedge_events.add(event_ticker)

    # Sibling has now dropped to exactly HEDGE_BUY
    strategy.cache.update_quote(sibling.market_ticker, 58, 60)

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sibling.market_ticker))

    result = await strategy._execute_hedge(original)

    assert result is True
    assert len(strategy.executor.orders) == 1
    order, _ = strategy.executor.orders[0]
    assert order.market_ticker == sibling.market_ticker
    assert order.price == 60

    # Armed state must be cleared
    assert event_ticker not in strategy._pending_hedge_events
    # Hedged state must be set
    assert event_ticker in strategy._hedged_events


@pytest.mark.asyncio
async def test_guaranteed_stop_loss_fires_even_when_hedge_deferred(monkeypatch):
    """
    Original bracket collapses to ≤ stop_loss (25¢) while the event is armed
    and no sibling is within HEDGE_BUY (60¢).  Stop-loss must fire for the
    original; event must remain armed (_pending_hedge_events).
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTNYC-26JUN22"
    ticker = "KXHIGHTNYC-26JUN22-B72.5"
    sibling_ticker = "KXHIGHTNYC-26JUN22-T73"

    original = MarketBracket(
        market_ticker=ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 72.5",
        phase=Phase.HOLDING,
        position_quantity=3,
        avg_entry=83,
    )
    sibling = MarketBracket(
        market_ticker=sibling_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTNYC",
        bracket_label="nyc high 73",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = original
    strategy.brackets[sibling_ticker] = sibling
    strategy.active_positions[ticker] = original

    # Event is already armed
    strategy._pending_hedge_events.add(event_ticker)

    # Original collapses below stop_loss; sibling still weak (below HEDGE_TRIGGER=48¢)
    strategy.cache.update_quote(ticker, 25, 27)           # bid=25 <= stop_loss=35
    strategy.cache.update_quote(sibling_ticker, 28, 30)   # below HEDGE_TRIGGER=48¢ → stays deferred

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sibling_ticker))
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 3, "last_price_cents": 80}}))

    await strategy._evaluate_held_positions()

    # Stop-loss order must have been placed (sell_yes)
    sell_orders = [o for o, _ in strategy.executor.orders if o.market_ticker == ticker]
    assert len(sell_orders) >= 1, "stop-loss sell order must be placed for the original bracket"

    # Event must remain armed — recovery can still fire later
    assert event_ticker in strategy._pending_hedge_events


@pytest.mark.asyncio
async def test_hedge_gate_fires_when_strong_defers_when_weak(monkeypatch):
    """
    Conditional hedge gate:
    Case A: best sibling at 55¢ (≥ HEDGE_TRIGGER_PRICE=48¢) → hedge fires immediately.
    Case B: best sibling at 30¢ (< HEDGE_TRIGGER_PRICE=48¢) → all siblings weak,
            hedge deferred (event armed).
    """
    import core.state_machine as state_machine

    # --- Case A: sibling at 55¢ → should hedge ---
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy_a = make_strategy(monkeypatch, hedge_buy=60)
    strategy_a.executor.succeed = True

    orig_a = MarketBracket(
        market_ticker="KXHIGHTCHI-26JUN22-B75.5",
        event_ticker="KXHIGHTCHI-26JUN22",
        series_ticker="KXHIGHTCHI",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sib_a = MarketBracket(
        market_ticker="KXHIGHTCHI-26JUN22-T76",
        event_ticker="KXHIGHTCHI-26JUN22",
        series_ticker="KXHIGHTCHI",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy_a.brackets[orig_a.market_ticker] = orig_a
    strategy_a.brackets[sib_a.market_ticker] = sib_a
    strategy_a.cache.update_quote(sib_a.market_ticker, 53, 55)  # ask=55 >= HEDGE_TRIGGER=48
    monkeypatch.setattr(strategy_a, "_find_next_bracket",
                        AsyncMock(return_value=sib_a.market_ticker))

    result_a = await strategy_a._execute_hedge(orig_a)

    assert result_a is True
    assert len(strategy_a.executor.orders) == 1
    assert strategy_a.executor.orders[0][0].price == 55
    assert "KXHIGHTCHI-26JUN22" not in strategy_a._pending_hedge_events

    # --- Case B: sibling at 30¢ (< HEDGE_TRIGGER=48¢) → should defer ---
    strategy_b = make_strategy(monkeypatch, hedge_buy=60)

    orig_b = MarketBracket(
        market_ticker="KXHIGHTCHI-26JUN22-B75.5",
        event_ticker="KXHIGHTCHI-26JUN22",
        series_ticker="KXHIGHTCHI",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sib_b = MarketBracket(
        market_ticker="KXHIGHTCHI-26JUN22-T76",
        event_ticker="KXHIGHTCHI-26JUN22",
        series_ticker="KXHIGHTCHI",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy_b.brackets[orig_b.market_ticker] = orig_b
    strategy_b.brackets[sib_b.market_ticker] = sib_b
    strategy_b.cache.update_quote(sib_b.market_ticker, 28, 30)  # ask=30 < HEDGE_TRIGGER=48 → defer
    monkeypatch.setattr(strategy_b, "_find_next_bracket",
                        AsyncMock(return_value=sib_b.market_ticker))

    result_b = await strategy_b._execute_hedge(orig_b)

    assert result_b is False
    assert len(strategy_b.executor.orders) == 0
    assert "KXHIGHTCHI-26JUN22" in strategy_b._pending_hedge_events


@pytest.mark.asyncio
async def test_topoff_reconciles_stop_loss_and_recovery_ledger(monkeypatch):
    """
    Top-off at 82¢ reconciles the realized 35¢ stop-loss loss + 60¢ recovery buy
    via the ledger-based break-even formula, exactly as the NYC lifecycle specifies.
    """
    import core.state_machine as state_machine

    strategy = make_strategy(monkeypatch)
    event_ticker = "KXHIGHTNYC-26JUN22"

    survivor = MarketBracket(
        market_ticker="KXHIGHTNYC-26JUN22-T73",
        event_ticker=event_ticker,
        series_ticker="KXHIGHTNYC",
        bracket_label="recovery survivor",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=60,
    )
    strategy.brackets[survivor.market_ticker] = survivor
    strategy.active_positions[survivor.market_ticker] = survivor
    strategy._hedged_events.add(event_ticker)

    # Ledger:
    #   initial BUY 2×83 = 166 (original entry)
    #   HEDGE/recovery 2×60 = 120
    #   gross_spend = 286
    # current quantity = 2 (only recovery bracket, original was stop-lossed)
    # remaining_deficit = 286 - (2*100) = 86
    # yes_ask = 82 => profit_per_contract = 18
    # topoff_qty = ceil(86/18) = ceil(4.78) = 5
    mock_ledger = {
        "initial_cost_cents": 166,
        "gross_spend_cents": 286,
        "stop_loss_proceeds_cents": 35 * 2,  # 2 contracts stopped at 35¢
        "open_tickers": {survivor.market_ticker},
        "closed_tickers": set(),
    }

    async def fake_ledger(et):
        return mock_ledger

    monkeypatch.setattr(strategy, "_event_ledger", fake_ledger)

    await strategy._execute_topoff(survivor, yes_ask=82)

    assert len(strategy.executor.orders) == 1
    order, max_price = strategy.executor.orders[0]
    assert order.market_ticker == survivor.market_ticker
    assert order.price == 82
    assert order.quantity == 5   # ceil(86/18) = 5
    assert max_price == strategy.config.spread_monitor_price


@pytest.mark.asyncio
async def test_evaluate_held_positions_retries_deferred_hedge_on_subsequent_cycle(monkeypatch):
    """
    When the event is armed (_pending_hedge_events) and the original bracket is
    still active, the next evaluation cycle must retry the hedge and fire it
    once a qualifying sibling appears.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTCHI-26JUN22"
    ticker = "KXHIGHTCHI-26JUN22-B75.5"
    sib_ticker = "KXHIGHTCHI-26JUN22-T76"

    original = MarketBracket(
        market_ticker=ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTCHI",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sibling = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTCHI",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = original
    strategy.brackets[sib_ticker] = sibling
    strategy.active_positions[ticker] = original

    # Arm the event (deferred hedge from a prior cycle)
    strategy._pending_hedge_events.add(event_ticker)

    # Original is still above stop_loss (40¢) but below hedge_trigger (48¢)
    # Sibling is now at 58¢ ≤ HEDGE_BUY=60¢ → deferred hedge should fire
    strategy.cache.update_quote(ticker, 40, 42)
    strategy.cache.update_quote(sib_ticker, 56, 58)

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sib_ticker))
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions(
                            {ticker: {"count": 2, "last_price_cents": 45}}))

    await strategy._evaluate_held_positions()

    # Hedge order must have been placed
    buy_orders = [o for o, _ in strategy.executor.orders if o.market_ticker == sib_ticker]
    assert len(buy_orders) >= 1, "deferred hedge must fire when sibling drops to ≤ HEDGE_BUY"

    # Armed state must be cleared
    assert event_ticker not in strategy._pending_hedge_events
    assert event_ticker in strategy._hedged_events
    assert event_ticker in strategy._hedged_events


# ---------------------------------------------------------------------------
# New tests: conditional hedge gate, DC disaster replay, guardrails
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dc_disaster_replay(monkeypatch):
    """
    DC disaster replay (the key regression test):
    1. Original bracket weakens to hedge trigger; ALL siblings at 1¢ (below
       eval_price_floor=5) → NO hedge placed, event armed, hedge_deferred logged.
    2. Original hits stop-loss (≤ stop_loss_price) → stop-loss order placed,
       event STAYS armed.
    3. One sibling's YES ask rises to 70¢ (> HEDGE_BUY=60¢) while others stay
       at 1¢ → exactly ONE recovery BUY for the 70¢ sibling; 1¢ corpses never
       touched.
    """
    import core.state_machine as state_machine

    info_logged = []
    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTDC-26JUN22"
    orig_ticker = "KXHIGHTDC-26JUN22-B94.5"
    sib_dead1 = "KXHIGHTDC-26JUN22-B87"
    sib_dead2 = "KXHIGHTDC-26JUN22-B88.5"
    sib_dead3 = "KXHIGHTDC-26JUN22-B90.5"
    sib_dead4 = "KXHIGHTDC-26JUN22-B92.5"
    sib_winner = "KXHIGHTDC-26JUN22-T96"

    def _make_bracket(ticker, event, label):
        return MarketBracket(
            market_ticker=ticker,
            event_ticker=event,
            series_ticker="KXHIGHTDC",
            bracket_label=label,
            phase=Phase.MONITORING,
        )

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDC",
        bracket_label="dc 94-95",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=83,
    )
    strategy.brackets[orig_ticker] = original
    strategy.active_positions[orig_ticker] = original
    for t, lbl in [(sib_dead1, "dead1"), (sib_dead2, "dead2"),
                   (sib_dead3, "dead3"), (sib_dead4, "dead4")]:
        strategy.brackets[t] = _make_bracket(t, event_ticker, lbl)
    strategy.brackets[sib_winner] = _make_bracket(sib_winner, event_ticker, "winner")

    # Step 1: all siblings at 1¢ (below eval_price_floor=5)
    strategy.cache.update_quote(orig_ticker, 45, 47)     # above stop_loss but at hedge trigger
    for t in [sib_dead1, sib_dead2, sib_dead3, sib_dead4, sib_winner]:
        strategy.cache.update_quote(t, 0, 1)

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sib_dead1))
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({orig_ticker: {"count": 2, "last_price_cents": 45}}))

    await strategy._evaluate_held_positions()

    # No hedge order should be placed — all siblings are floor-priced (1¢ <= eval_price_floor=5)
    buy_orders = [o for o, _ in strategy.executor.orders if o.side.name == "BUY_YES"]
    assert len(buy_orders) == 0, "No hedge should fire when all siblings are at 1¢"
    assert event_ticker in strategy._pending_hedge_events, "Event must be armed"
    deferred = [ev for ev, _ in info_logged if ev == "phase.c.hedge_deferred"]
    assert len(deferred) >= 1

    # Step 2: original falls to stop_loss (30¢ bid ≤ stop_loss=35)
    strategy.executor.orders.clear()
    strategy.cache.update_quote(orig_ticker, 30, 32)  # bid=30 <= stop_loss=35

    # Reset cooldown so stop-loss fires immediately
    original._last_hedge_attempt = 0

    monkeypatch.setattr(
        strategy.executor,
        "get_positions",
        _make_sequenced_get_positions(
            [
                {orig_ticker: {"count": 2, "last_price_cents": 30}},
                {},
            ]
        ),
    )

    await strategy._evaluate_held_positions()

    sell_orders = [o for o, _ in strategy.executor.orders if o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1, "Stop-loss sell must fire when original bid <= stop_loss"
    assert event_ticker in strategy._pending_hedge_events, "Event must stay armed after stop-loss"

    # Step 3: winner (sib_winner) rises to 70¢ (> HEDGE_BUY=60¢); corpses stay at 1¢
    strategy.executor.orders.clear()
    strategy.cache.update_quote(sib_winner, 68, 70)  # 70 > hedge_buy=60 → qualifies

    # Reset per-event recovery cooldown
    strategy._pending_hedge_last_attempt.clear()

    await strategy._evaluate_held_positions()

    recovery_orders = [o for o, _ in strategy.executor.orders
                       if o.side.name == "BUY_YES"]
    assert len(recovery_orders) == 1, "Exactly ONE recovery buy should be placed"
    assert recovery_orders[0].market_ticker == sib_winner, "Recovery must target the 70¢ winner"
    for dead in [sib_dead1, sib_dead2, sib_dead3, sib_dead4]:
        assert not any(o.market_ticker == dead for o, _ in strategy.executor.orders), \
            f"Dead bracket {dead} must never be bought"
    assert event_ticker not in strategy._pending_hedge_events
    assert event_ticker in strategy._hedged_events


@pytest.mark.asyncio
async def test_normal_hedge_fires_immediately_when_strong_sibling(monkeypatch):
    """
    Normal hedge path: original weakens to trigger; best sibling at 52¢ (≥
    HEDGE_TRIGGER_PRICE=48¢) → hedge into the 52¢ sibling immediately using
    break-even sizing; event is NOT left armed.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTBOS-26JUN22"
    orig_ticker = "KXHIGHTBOS-26JUN22-B84.5"
    sib_ticker = "KXHIGHTBOS-26JUN22-T85"

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTBOS",
        bracket_label="origin",
        phase=Phase.HOLDING,
        position_quantity=4,
        avg_entry=84,
    )
    sibling = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTBOS",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[orig_ticker] = original
    strategy.brackets[sib_ticker] = sibling
    strategy.cache.update_quote(sib_ticker, 50, 52)  # 52 >= HEDGE_TRIGGER=48 → normal hedge

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sib_ticker))

    result = await strategy._execute_hedge(original)

    assert result is True
    assert len(strategy.executor.orders) == 1
    order, max_price = strategy.executor.orders[0]
    assert order.market_ticker == sib_ticker
    assert order.price == 52
    assert max_price == strategy.config.spread_monitor_price
    # Event is hedged, not armed
    assert event_ticker in strategy._hedged_events
    assert event_ticker not in strategy._pending_hedge_events


@pytest.mark.asyncio
async def test_deferred_recovery_respects_90_cent_ceiling(monkeypatch):
    """
    Armed event; a sibling rises to 95¢ (> HEDGE_BUY=60¢ but > SPREAD_MONITOR=90¢).
    The order is submitted at max_price=90¢ ceiling; if it cannot fill at ≤ 90¢, the
    executor returns failure and no recovery is recorded — event stays armed.
    The test verifies: one order is attempted at the correct max_price ceiling;
    after failure the event is still in _pending_hedge_events.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, spread_monitor_price=90)
    # Executor returns FAILURE (simulates no fill at ≤ 90¢ when ask=95¢)
    strategy.executor.succeed = False

    event_ticker = "KXHIGHTMIA-26JUN22"
    sib_ticker = "KXHIGHTMIA-26JUN22-T96"

    sib = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTMIA",
        bracket_label="winner",
        phase=Phase.MONITORING,
    )
    strategy.brackets[sib_ticker] = sib

    # Event already armed; sibling at 95¢ (> 60¢ but cannot fill at ≤ 90¢)
    strategy._pending_hedge_events.add(event_ticker)
    strategy.cache.update_quote(sib_ticker, 93, 95)  # ask=95 > hedge_buy=60 → qualifies

    # No active original bracket → secondary recovery loop handles this
    strategy._pending_hedge_last_attempt.clear()

    await strategy._evaluate_held_positions()

    # One buy attempt was made (at max_price = spread_monitor_price = 90)
    buy_orders = [o for o, mp in strategy.executor.orders if o.side.name == "BUY_YES"]
    assert len(buy_orders) == 1, "One buy attempt must be made"
    _, max_price = strategy.executor.orders[0]
    assert max_price == strategy.config.spread_monitor_price, "Buy must use spread_monitor_price ceiling"

    # Since executor returned failure, event stays armed (not hedged)
    assert event_ticker in strategy._pending_hedge_events, "Event must stay armed on fill failure"
    assert event_ticker not in strategy._hedged_events, "Event must NOT be hedged on fill failure"


@pytest.mark.asyncio
async def test_single_order_per_event_guard(monkeypatch):
    """
    Once a recovery/hedge has filled for an event (event in _hedged_events), no
    further hedge/recovery order is placed for that event on subsequent cycles,
    even if another sibling crosses 60¢.  Only top-off may act.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTORD-26JUN22"
    orig_ticker = "KXHIGHTORD-26JUN22-B84.5"
    sib1_ticker = "KXHIGHTORD-26JUN22-T85"
    sib2_ticker = "KXHIGHTORD-26JUN22-T86"

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTORD",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sib1 = MarketBracket(
        market_ticker=sib1_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTORD",
        bracket_label="sib1",
        phase=Phase.MONITORING,
    )
    sib2 = MarketBracket(
        market_ticker=sib2_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTORD",
        bracket_label="sib2",
        phase=Phase.MONITORING,
    )
    strategy.brackets[orig_ticker] = original
    strategy.brackets[sib1_ticker] = sib1
    strategy.brackets[sib2_ticker] = sib2
    strategy.active_positions[orig_ticker] = original

    # Mark event as already hedged (recovery already filled)
    strategy._hedged_events.add(event_ticker)

    # sib2 crosses 60¢ — but event is already hedged; no new order should fire
    strategy.cache.update_quote(orig_ticker, 40, 42)
    strategy.cache.update_quote(sib1_ticker, 50, 55)
    strategy.cache.update_quote(sib2_ticker, 65, 70)

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({orig_ticker: {"count": 2, "last_price_cents": 40}}))
    monkeypatch.setattr(strategy, "_execute_topoff", AsyncMock())

    await strategy._evaluate_held_positions()

    hedge_buys = [o for o, _ in strategy.executor.orders if o.side.name == "BUY_YES"]
    assert len(hedge_buys) == 0, "No hedge/recovery order after event already hedged"


@pytest.mark.asyncio
async def test_floor_guard_never_selects_floor_priced_sibling(monkeypatch):
    """
    Floor guard: a sibling at exactly eval_price_floor and one just above are
    evaluated.  The floor sibling is never chosen as a hedge/recovery target.
    Only the sibling above the floor (and above hedge_trigger_price) is selected.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTDEN-26JUN22"
    orig_ticker = "KXHIGHTDEN-26JUN22-B84.5"
    sib_floor = "KXHIGHTDEN-26JUN22-B85.5"   # at exactly eval_price_floor
    sib_valid = "KXHIGHTDEN-26JUN22-T86"      # just above floor and above hedge_trigger

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDEN",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sibling_floor_bracket = MarketBracket(
        market_ticker=sib_floor,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDEN",
        bracket_label="floor sib",
        phase=Phase.MONITORING,
    )
    sibling_valid_bracket = MarketBracket(
        market_ticker=sib_valid,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTDEN",
        bracket_label="valid sib",
        phase=Phase.MONITORING,
    )
    strategy.brackets[orig_ticker] = original
    strategy.brackets[sib_floor] = sibling_floor_bracket
    strategy.brackets[sib_valid] = sibling_valid_bracket

    # Floor sibling at exactly eval_price_floor (5¢) — must be ignored
    strategy.cache.update_quote(sib_floor, 4, 5)
    # Valid sibling at 52¢ (> floor=5¢ and >= hedge_trigger=48¢) — must be chosen
    strategy.cache.update_quote(sib_valid, 50, 52)

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sib_valid))

    result = await strategy._execute_hedge(original)

    assert result is True
    assert len(strategy.executor.orders) == 1
    order, _ = strategy.executor.orders[0]
    assert order.market_ticker == sib_valid, "Must select the valid sibling, not the floor-priced one"
    assert order.market_ticker != sib_floor, "Floor-priced sibling must never be the target"


@pytest.mark.asyncio
async def test_stop_loss_fires_while_event_armed_no_qualifying_sibling(monkeypatch):
    """
    Stop-loss fires independently of armed/deferred state.
    Event armed, no sibling > HEDGE_BUY (all at 30¢ < 48¢), original price ≤
    stop_loss_price → stop-loss order placed for the original regardless.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTPHX-26JUN22"
    orig_ticker = "KXHIGHTPHX-26JUN22-B84.5"
    sib_ticker = "KXHIGHTPHX-26JUN22-T85"

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTPHX",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=3,
        avg_entry=84,
    )
    sibling = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTPHX",
        bracket_label="sib",
        phase=Phase.MONITORING,
    )
    strategy.brackets[orig_ticker] = original
    strategy.brackets[sib_ticker] = sibling
    strategy.active_positions[orig_ticker] = original

    # Event already armed
    strategy._pending_hedge_events.add(event_ticker)

    # Original at stop_loss level (bid=30 ≤ stop_loss=35); sibling weak (30¢ < 48¢)
    strategy.cache.update_quote(orig_ticker, 30, 32)
    strategy.cache.update_quote(sib_ticker, 28, 30)

    monkeypatch.setattr(strategy, "_find_next_bracket",
                        AsyncMock(return_value=sib_ticker))
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({orig_ticker: {"count": 3, "last_price_cents": 30}}))

    await strategy._evaluate_held_positions()

    # Stop-loss sell must have been placed
    sell_orders = [o for o, _ in strategy.executor.orders
                   if o.market_ticker == orig_ticker and o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1, "Stop-loss must fire while event is armed"
    assert sell_orders[0].price == 1, "Stop-loss must sell at 1¢ (marketable)"

    # No BUY orders for weak sibling
    buy_orders = [o for o, _ in strategy.executor.orders
                  if o.market_ticker == sib_ticker and o.side.name == "BUY_YES"]
    assert len(buy_orders) == 0, "Weak sibling (30¢ < 48¢) must never be bought"


def _make_sequenced_get_positions(sequence):
    calls = {"idx": 0}

    async def _fake():
        idx = calls["idx"]
        calls["idx"] += 1
        if idx >= len(sequence):
            return sequence[-1]
        return sequence[idx]

    return _fake


@pytest.mark.asyncio
async def test_stop_loss_no_fill_keeps_position_and_retries(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTSEA-26JUN22-B72.5"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTSEA-26JUN22",
        series_ticker="KXHIGHTSEA",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 20, 22)

    monkeypatch.setattr(
        strategy.executor,
        "sell_yes",
        AsyncMock(
            return_value=ExecutionResult(
                success=False,
                market_ticker=ticker,
                side="yes",
                price=1,
                quantity=2,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="NO_FILL",
                notes="{}",
            )
        ),
    )
    monkeypatch.setattr(
        strategy.executor,
        "get_positions",
        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 20}}),
    )

    await strategy._evaluate_held_positions()
    assert ticker in strategy.active_positions
    assert strategy.executor.sell_yes.await_count == 1

    bracket._last_stop_loss_attempt = 0
    await strategy._evaluate_held_positions()
    assert ticker in strategy.active_positions
    assert strategy.executor.sell_yes.await_count == 2


@pytest.mark.asyncio
async def test_stop_loss_partial_fill_updates_remaining_and_retries(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTSEA-26JUN22-B72.5"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTSEA-26JUN22",
        series_ticker="KXHIGHTSEA",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 20, 22)

    attempted_quantities = []

    async def _fake_sell(order):
        attempted_quantities.append(order.quantity)
        if len(attempted_quantities) == 1:
            return ExecutionResult(
                success=True,
                market_ticker=ticker,
                side="yes",
                price=1,
                quantity=order.quantity,
                fill_price=6,
                fill_quantity=1,
                total_cost_cents=-6,
                status="FILLED",
                notes="{}",
            )
        return ExecutionResult(
            success=False,
            market_ticker=ticker,
            side="yes",
            price=1,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            status="NO_FILL",
            notes="{}",
        )

    monkeypatch.setattr(strategy.executor, "sell_yes", _fake_sell)
    monkeypatch.setattr(
        strategy.executor,
        "get_positions",
        _make_fake_get_positions({ticker: {"count": 1, "last_price_cents": 20}}),
    )

    await strategy._evaluate_held_positions()
    assert ticker in strategy.active_positions
    assert bracket.position_quantity == 1

    bracket._last_stop_loss_attempt = 0
    await strategy._evaluate_held_positions()
    assert attempted_quantities == [2, 1]


@pytest.mark.asyncio
async def test_stop_loss_closes_only_after_positions_confirm_zero(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTSEA-26JUN22-B72.5"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTSEA-26JUN22",
        series_ticker="KXHIGHTSEA",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 20, 22)

    monkeypatch.setattr(
        strategy.executor,
        "sell_yes",
        AsyncMock(
            return_value=ExecutionResult(
                success=True,
                market_ticker=ticker,
                side="yes",
                price=1,
                quantity=2,
                fill_price=6,
                fill_quantity=2,
                total_cost_cents=-12,
                status="FILLED",
                notes="{}",
            )
        ),
    )
    monkeypatch.setattr(
        strategy.executor,
        "get_positions",
        _make_sequenced_get_positions(
            [
                {ticker: {"count": 2, "last_price_cents": 20}},
                {},
            ]
        ),
    )

    await strategy._evaluate_held_positions()

    assert ticker not in strategy.active_positions
    assert ticker not in strategy.brackets
    assert bracket.phase == Phase.CLOSED


@pytest.mark.asyncio
async def test_stop_loss_success_but_still_held_keeps_position(monkeypatch):
    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTSEA-26JUN22-B72.5"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTSEA-26JUN22",
        series_ticker="KXHIGHTSEA",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=80,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_quote(ticker, 20, 22)

    monkeypatch.setattr(
        strategy.executor,
        "sell_yes",
        AsyncMock(
            return_value=ExecutionResult(
                success=True,
                market_ticker=ticker,
                side="yes",
                price=1,
                quantity=2,
                fill_price=6,
                fill_quantity=1,
                total_cost_cents=-6,
                status="FILLED",
                notes="{}",
            )
        ),
    )
    monkeypatch.setattr(
        strategy.executor,
        "get_positions",
        _make_sequenced_get_positions(
            [
                {ticker: {"count": 2, "last_price_cents": 20}},
                {ticker: {"count": 1, "last_price_cents": 20}},
            ]
        ),
    )

    await strategy._evaluate_held_positions()

    assert ticker in strategy.active_positions
    assert bracket.position_quantity == 1


# ---------------------------------------------------------------------------
# _find_next_bracket — KXHIGHT? regex fix (covers no-T high cities)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("origin_ticker,next_ticker,event_ticker,series_ticker", [
    # No-T high cities (the 7 affected cities — bug being fixed)
    ("KXHIGHCHI-26JUN22-B72.5", "KXHIGHCHI-26JUN22-T73",  "KXHIGHCHI-26JUN22",  "KXHIGHCHI"),
    ("KXHIGHNY-26JUN22-B72.5",  "KXHIGHNY-26JUN22-T73",   "KXHIGHNY-26JUN22",   "KXHIGHNY"),
    ("KXHIGHMIA-26JUN22-B93.5", "KXHIGHMIA-26JUN22-T94",  "KXHIGHMIA-26JUN22",  "KXHIGHMIA"),
    ("KXHIGHLAX-26JUN22-B71.5", "KXHIGHLAX-26JUN22-T72",  "KXHIGHLAX-26JUN22",  "KXHIGHLAX"),
    ("KXHIGHAUS-26JUN22-B88.5", "KXHIGHAUS-26JUN22-T89",  "KXHIGHAUS-26JUN22",  "KXHIGHAUS"),
    ("KXHIGHDEN-26JUN22-B95.5", "KXHIGHDEN-26JUN22-T96",  "KXHIGHDEN-26JUN22",  "KXHIGHDEN"),
    ("KXHIGHPHIL-26JUN22-B86.5","KXHIGHPHIL-26JUN22-T87", "KXHIGHPHIL-26JUN22", "KXHIGHPHIL"),
    # With-T high cities (regression guard)
    ("KXHIGHTHOU-26JUN22-B93.5","KXHIGHTHOU-26JUN22-T94", "KXHIGHTHOU-26JUN22", "KXHIGHTHOU"),
    ("KXHIGHTSEA-26JUN22-B72.5","KXHIGHTSEA-26JUN22-T73", "KXHIGHTSEA-26JUN22", "KXHIGHTSEA"),
    ("KXHIGHTDC-26JUN22-B88.5", "KXHIGHTDC-26JUN22-T89",  "KXHIGHTDC-26JUN22",  "KXHIGHTDC"),
    # Low cities (regression guard — KXLOWT unchanged)
    ("KXLOWTSEA-26JUN22-B53.5", "KXLOWTSEA-26JUN22-T54",  "KXLOWTSEA-26JUN22",  "KXLOWTSEA"),
    ("KXLOWTBOS-26JUN22-T59",   "KXLOWTBOS-26JUN22-T60",  "KXLOWTBOS-26JUN22",  "KXLOWTBOS"),
])
async def test_find_next_bracket_primary_path(monkeypatch, origin_ticker, next_ticker, event_ticker, series_ticker):
    """Primary path: the expected next-bracket ticker is pre-registered in strategy.brackets."""
    strategy = make_strategy(monkeypatch)

    origin = MarketBracket(
        market_ticker=origin_ticker,
        event_ticker=event_ticker,
        series_ticker=series_ticker,
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sibling = MarketBracket(
        market_ticker=next_ticker,
        event_ticker=event_ticker,
        series_ticker=series_ticker,
        bracket_label="next",
        phase=Phase.MONITORING,
    )
    strategy.brackets[origin_ticker] = origin
    strategy.brackets[next_ticker] = sibling

    result = await strategy._find_next_bracket(origin)
    assert result == next_ticker


@pytest.mark.asyncio
async def test_find_next_bracket_no_t_high_t_bracket_increment(monkeypatch):
    """T-bracket for a no-T city increments correctly: KXHIGHCHI-...-T84 → T85."""
    strategy = make_strategy(monkeypatch)
    origin_ticker = "KXHIGHCHI-26JUN22-T84"
    next_ticker   = "KXHIGHCHI-26JUN22-T85"
    event_ticker  = "KXHIGHCHI-26JUN22"

    origin = MarketBracket(
        market_ticker=origin_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHCHI",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sibling = MarketBracket(
        market_ticker=next_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHCHI",
        bracket_label="next",
        phase=Phase.MONITORING,
    )
    strategy.brackets[origin_ticker] = origin
    strategy.brackets[next_ticker] = sibling

    result = await strategy._find_next_bracket(origin)
    assert result == next_ticker


@pytest.mark.asyncio
async def test_find_next_bracket_no_t_high_fallback_path(monkeypatch):
    """Fallback path: only a non-immediate higher sibling is registered for a no-T city.

    Registers KXHIGHNY-...-T78 (not the immediate T73) and verifies the fallback
    candidate scan — which also uses the KXHIGHT? regex — finds and returns it.
    """
    strategy = make_strategy(monkeypatch)
    origin_ticker  = "KXHIGHNY-26JUN22-B72.5"
    higher_ticker  = "KXHIGHNY-26JUN22-T78"
    event_ticker   = "KXHIGHNY-26JUN22"

    origin = MarketBracket(
        market_ticker=origin_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHNY",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    # Only register the higher (non-immediate) sibling — forces fallback scan
    higher = MarketBracket(
        market_ticker=higher_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHNY",
        bracket_label="higher",
        phase=Phase.MONITORING,
    )
    strategy.brackets[origin_ticker] = origin
    strategy.brackets[higher_ticker] = higher

    result = await strategy._find_next_bracket(origin)
    assert result == higher_ticker


# ---------------------------------------------------------------------------
# New tests: abandoned-event / permanent-failure handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recovery_hedge_abandons_event_on_market_not_found(monkeypatch):
    """
    When buy_yes returns success=False with market_not_found in notes (permanent
    failure), the secondary recovery loop must abandon the event:
    - remove from _pending_hedge_events
    - add to _abandoned_events
    - log phase.c.recovery_hedge_abandoned
    A second call to _evaluate_held_positions must NOT place any further buy orders.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)

    event_ticker = "KXHIGHTLV-26JUN22"
    sib_ticker = "KXHIGHTLV-26JUN22-T110"

    sib = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[sib_ticker] = sib

    # Arm the event; bypass cooldown
    strategy._pending_hedge_events.add(event_ticker)
    strategy._pending_hedge_last_attempt[event_ticker] = 0

    # Sibling ask above hedge_buy so it gets selected
    strategy.cache.update_quote(sib_ticker, 65, 70)

    # _market_is_active fails open (returns True) so order is attempted
    monkeypatch.setattr(strategy, "_market_is_active", AsyncMock(return_value=True))

    # buy_yes returns permanent failure
    permanent_notes = '{"error": {"code": "market_not_found", "message": "market not found"}}'
    monkeypatch.setattr(
        strategy.executor,
        "buy_yes",
        AsyncMock(return_value=ExecutionResult(
            success=False,
            market_ticker=sib_ticker,
            side="yes",
            price=70,
            quantity=1,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            status="REJECTED",
            notes=permanent_notes,
        )),
    )

    await strategy._evaluate_held_positions()

    # Event must be abandoned after first permanent failure
    assert event_ticker not in strategy._pending_hedge_events, \
        "Event must be removed from _pending_hedge_events after permanent failure"
    assert event_ticker in strategy._abandoned_events, \
        "Event must be in _abandoned_events after permanent failure"
    abandoned_logs = [ev for ev, _ in warn_logged if ev == "phase.c.recovery_hedge_abandoned"]
    assert len(abandoned_logs) >= 1, "recovery_hedge_abandoned must be logged"

    # Second cycle: no additional buy orders should be placed
    initial_order_count = len(strategy.executor.orders)
    # Reset cooldown so the secondary loop would run if not abandoned
    strategy._pending_hedge_last_attempt.clear()
    await strategy._evaluate_held_positions()
    assert len(strategy.executor.orders) == initial_order_count, \
        "No further buy_yes orders after event is abandoned"


@pytest.mark.asyncio
async def test_recovery_hedge_skips_inactive_target_market(monkeypatch):
    """
    When _market_is_active returns False for the chosen best_ticker, the secondary
    loop must abandon the event (reason=target_market_inactive) without placing any
    order.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)

    event_ticker = "KXHIGHTLV-26JUN22"
    sib_ticker = "KXHIGHTLV-26JUN22-T110"

    sib = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[sib_ticker] = sib

    strategy._pending_hedge_events.add(event_ticker)
    strategy._pending_hedge_last_attempt[event_ticker] = 0

    strategy.cache.update_quote(sib_ticker, 65, 70)

    # _market_is_active returns False → inactive target
    monkeypatch.setattr(strategy, "_market_is_active", AsyncMock(return_value=False))

    await strategy._evaluate_held_positions()

    # No buy order must have been placed
    buy_orders = [o for o, _ in strategy.executor.orders if o.side.name == "BUY_YES"]
    assert len(buy_orders) == 0, "No buy order when target market is inactive"

    # Event must be abandoned
    assert event_ticker not in strategy._pending_hedge_events
    assert event_ticker in strategy._abandoned_events

    abandoned_logs = [ev for ev, kw in warn_logged
                      if ev == "phase.c.recovery_hedge_abandoned"
                      and kw.get("reason") == "target_market_inactive"]
    assert len(abandoned_logs) >= 1, "recovery_hedge_abandoned with reason=target_market_inactive must be logged"


@pytest.mark.asyncio
async def test_recovery_hedge_transient_failure_caps_after_n(monkeypatch):
    """
    Transient (non-permanent) failures increment the per-event counter.
    After RECOVERY_MAX_CONSECUTIVE_FAILURES attempts the event is abandoned
    with reason=max_failures; subsequent cycles place no further orders.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)

    event_ticker = "KXHIGHTLV-26JUN22"
    sib_ticker = "KXHIGHTLV-26JUN22-T110"

    sib = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[sib_ticker] = sib

    strategy._pending_hedge_events.add(event_ticker)
    strategy.cache.update_quote(sib_ticker, 65, 70)

    # _market_is_active is active
    monkeypatch.setattr(strategy, "_market_is_active", AsyncMock(return_value=True))

    # buy_yes returns transient failure (generic note, NOT permanent)
    monkeypatch.setattr(
        strategy.executor,
        "buy_yes",
        AsyncMock(return_value=ExecutionResult(
            success=False,
            market_ticker=sib_ticker,
            side="yes",
            price=70,
            quantity=1,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            status="REJECTED",
            notes="connection reset",
        )),
    )

    for i in range(RECOVERY_MAX_CONSECUTIVE_FAILURES - 1):
        # Reset cooldown so each iteration runs
        strategy._pending_hedge_last_attempt[event_ticker] = 0
        await strategy._evaluate_held_positions()
        # Event must still be pending (not yet abandoned)
        assert event_ticker in strategy._pending_hedge_events, \
            f"Event must stay armed after {i + 1} transient failures (below cap)"
        assert event_ticker not in strategy._abandoned_events

    # Final (Nth) attempt should trigger abandonment
    strategy._pending_hedge_last_attempt[event_ticker] = 0
    await strategy._evaluate_held_positions()

    assert event_ticker not in strategy._pending_hedge_events, \
        "Event must be removed from _pending_hedge_events after cap"
    assert event_ticker in strategy._abandoned_events, \
        "Event must be in _abandoned_events after cap"

    abandoned_logs = [ev for ev, kw in warn_logged
                      if ev == "phase.c.recovery_hedge_abandoned"
                      and kw.get("reason") == "max_failures"]
    assert len(abandoned_logs) >= 1, "recovery_hedge_abandoned with reason=max_failures must be logged"

    # Further cycle must not produce more orders
    order_count_after_abandon = len(strategy.executor.orders)
    strategy._pending_hedge_last_attempt.clear()
    await strategy._evaluate_held_positions()
    assert len(strategy.executor.orders) == order_count_after_abandon, \
        "No further orders after event is abandoned by transient cap"


@pytest.mark.asyncio
async def test_recovery_hedge_success_still_clears_and_resets(monkeypatch):
    """
    Regression: happy path still works after the failure-tracking changes.
    buy_yes succeeds → event removed from _pending_hedge_events, added to
    _hedged_events, recovery_hedge_filled logged, and _pending_hedge_failures /
    _abandoned_events do NOT contain the event.
    """
    import core.state_machine as state_machine

    info_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)
    strategy.executor.succeed = True

    event_ticker = "KXHIGHTLV-26JUN22"
    sib_ticker = "KXHIGHTLV-26JUN22-T110"

    sib = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="sibling",
        phase=Phase.MONITORING,
    )
    strategy.brackets[sib_ticker] = sib

    strategy._pending_hedge_events.add(event_ticker)
    strategy._pending_hedge_last_attempt[event_ticker] = 0
    # Pre-seed a transient failure count to verify it's cleared on success
    strategy._pending_hedge_failures[event_ticker] = 2

    strategy.cache.update_quote(sib_ticker, 65, 70)

    monkeypatch.setattr(strategy, "_market_is_active", AsyncMock(return_value=True))

    await strategy._evaluate_held_positions()

    # Happy path: event hedged, armed state cleared
    assert event_ticker not in strategy._pending_hedge_events
    assert event_ticker in strategy._hedged_events
    assert event_ticker not in strategy._abandoned_events
    assert event_ticker not in strategy._pending_hedge_failures

    filled_logs = [ev for ev, _ in info_logged if ev == "phase.c.recovery_hedge_filled"]
    assert len(filled_logs) >= 1, "recovery_hedge_filled must be logged"


@pytest.mark.asyncio
async def test_abandoned_event_not_retriggered_in_main_loop(monkeypatch):
    """
    An event in _abandoned_events must NOT cause _execute_hedge to be called in
    the main position loop, even if the active bracket price is at/below
    hedge_trigger_price.
    """
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60)

    event_ticker = "KXHIGHTLV-26JUN22"
    orig_ticker = "KXHIGHTLV-26JUN22-B84.5"

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    strategy.brackets[orig_ticker] = original
    strategy.active_positions[orig_ticker] = original

    # Mark event as abandoned
    strategy._abandoned_events.add(event_ticker)

    # Price at hedge trigger so hedge_triggered would fire if not abandoned
    strategy.cache.update_quote(orig_ticker, 45, 47)

    execute_hedge_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(strategy, "_execute_hedge", execute_hedge_mock)
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({orig_ticker: {"count": 2, "last_price_cents": 45}}))

    await strategy._evaluate_held_positions()

    execute_hedge_mock.assert_not_awaited()
    buy_orders = [o for o, _ in strategy.executor.orders if o.side.name == "BUY_YES"]
    assert len(buy_orders) == 0, "No hedge buy for abandoned event"


@pytest.mark.asyncio
async def test_execute_hedge_abandons_on_market_not_found(monkeypatch):
    """
    _execute_hedge: when buy_yes returns success=False with market_not_found notes,
    it must return False, log phase.c.hedge_failed (existing), add the event to
    _abandoned_events (via recovery_hedge_abandoned), and not produce further orders
    on a subsequent main-loop cycle.
    """
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, hedge_buy=60, eval_price_floor=5)

    event_ticker = "KXHIGHTLV-26JUN22"
    orig_ticker = "KXHIGHTLV-26JUN22-B84.5"
    sib_ticker = "KXHIGHTLV-26JUN22-T110"

    original = MarketBracket(
        market_ticker=orig_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="orig",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=84,
    )
    sibling = MarketBracket(
        market_ticker=sib_ticker,
        event_ticker=event_ticker,
        series_ticker="KXHIGHTLV",
        bracket_label="sib",
        phase=Phase.MONITORING,
    )
    strategy.brackets[orig_ticker] = original
    strategy.brackets[sib_ticker] = sibling

    strategy.cache.update_quote(sib_ticker, 50, 52)

    monkeypatch.setattr(strategy, "_find_next_bracket", AsyncMock(return_value=sib_ticker))
    monkeypatch.setattr(strategy, "_market_is_active", AsyncMock(return_value=True))

    permanent_notes = '{"error": {"code": "market_not_found", "message": "market not found"}}'
    monkeypatch.setattr(
        strategy.executor,
        "buy_yes",
        AsyncMock(return_value=ExecutionResult(
            success=False,
            market_ticker=sib_ticker,
            side="yes",
            price=52,
            quantity=2,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            status="REJECTED",
            notes=permanent_notes,
        )),
    )

    result = await strategy._execute_hedge(original)

    assert result is False
    # Existing hedge_failed log must still be present
    hedge_failed_logs = [ev for ev, _ in warn_logged if ev == "phase.c.hedge_failed"]
    assert len(hedge_failed_logs) >= 1, "phase.c.hedge_failed must be logged"
    # Event must be abandoned
    assert event_ticker in strategy._abandoned_events, \
        "Event must be in _abandoned_events after permanent failure in _execute_hedge"

    # Second call via main loop: abandoned event must not re-trigger hedge
    strategy.active_positions[orig_ticker] = original
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({orig_ticker: {"count": 2, "last_price_cents": 45}}))
    strategy.cache.update_quote(orig_ticker, 43, 45)
    initial_orders = len(strategy.executor.orders)

    await strategy._evaluate_held_positions()

    assert len(strategy.executor.orders) == initial_orders, \
        "No further buy_yes orders for abandoned event in main loop"
