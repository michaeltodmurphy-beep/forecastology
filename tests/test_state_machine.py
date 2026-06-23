import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.config import AppConfig
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
        self.succeed = False  # set True to make buy_yes/sell_yes return success

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
        stop_loss_price=50,
        dry_run=False,
        hedge_max_factor=3,
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


# ---------------------------------------------------------------------------
# Async SQLite DB fixture for ledger persistence tests
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def real_db():
    """Provide a real async SQLite in-memory DB for ledger tests."""
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from app.models import Base

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    class RealDB:
        async def get_session(self):
            return session_factory()

    yield RealDB()
    await engine.dispose()


def make_strategy_with_real_db(monkeypatch, real_db_instance, **config_overrides):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())
    return TemperatureStrategy(
        make_config(**config_overrides),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db_instance,
    )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_fake_get_positions(positions_map: dict):
    async def _fake():
        return positions_map
    return _fake


def _make_sequenced_get_positions(sequence):
    calls = {"idx": 0}

    async def _fake():
        idx = calls["idx"]
        calls["idx"] += 1
        if idx >= len(sequence):
            return sequence[-1]
        return sequence[idx]

    return _fake


# ---------------------------------------------------------------------------
# Start-up / lifecycle tests
# ---------------------------------------------------------------------------

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
    # No hedge_trigger in new start log
    assert "hedge_trigger" not in start_log


# ---------------------------------------------------------------------------
# _evaluate_watchlist tests
# ---------------------------------------------------------------------------

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
    strategy.cache.update_quote(bracket.market_ticker, 82 - spread, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    buy_log = next(kwargs for event, kwargs in logged if event == "phase.b.buying")
    assert buy_log["spread_note"] == expected_note
    strategy._execute_entry.assert_awaited_once()


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
    strategy._execute_entry.assert_awaited_once()


@pytest.mark.asyncio
async def test_evaluate_watchlist_ticker_quote_triggers_entry(monkeypatch):
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
    strategy.cache.update_quote(bracket.market_ticker, 78, 84)
    strategy._execute_entry = AsyncMock()
    strategy.config.minimum_spread = 7

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is True
    strategy._execute_entry.assert_awaited_once()
    events = [event for event, _ in logged]
    assert "phase.b.buying" in events


@pytest.mark.asyncio
async def test_evaluate_watchlist_wide_spread_blocked(monkeypatch):
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
    strategy.cache.update_quote(bracket.market_ticker, 74, 84)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert bracket.crossed_buy is False
    strategy._execute_entry.assert_not_awaited()
    events = [event for event, _ in logged]
    assert "phase.b.spread_too_wide" in events


@pytest.mark.asyncio
async def test_evaluate_watchlist_skips_below_floor_quietly(monkeypatch):
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
    strategy.cache.update_quote(bracket.market_ticker, 0, 1)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert bracket.last_price == 1
    events = [event for event, _ in debug_logged]
    assert "phase.b.below_trigger" not in events
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_evaluate_watchlist_logs_below_trigger_above_floor(monkeypatch):
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
    strategy.cache.update_quote(bracket.market_ticker, 2, 45)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    events = [event for event, _ in debug_logged]
    assert "phase.b.below_trigger" in events
    strategy._execute_entry.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("price,expect_log", [
    (5, False),
    (6, True),
])
async def test_evaluate_watchlist_floor_boundary(monkeypatch, price, expect_log):
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
# _execute_entry fill-price reconciliation tests
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# _restore_positions tests
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# _ensure_bracket / lifecycle tests
# ---------------------------------------------------------------------------

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
# Stop-loss tests (new last-trade-based logic)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_loss_sells_when_last_trade_below_threshold(monkeypatch):
    """Held qty 2, stop_loss_price=50; last_traded_price=49 → sell fires."""
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
    strategy.executor.succeed = True

    ticker = "KXLOWTBOS-26JUN23-B65.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTBOS-26JUN23",
        series_ticker="KXLOWTBOS",
        bracket_label="bos low",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket

    # Last traded price below stop-loss threshold
    strategy.cache.update_last_price(ticker, 49)

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_sequenced_get_positions([
                            {ticker: {"count": 2, "last_price_cents": 49}},
                            {},  # post-sell: position gone
                        ]))

    await strategy._evaluate_held_positions()

    # sell_yes must have been called
    sell_orders = [o for o, _ in strategy.executor.orders if o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1, "Stop-loss sell must fire when last_traded_price < stop_loss_price"
    # Log must have fired
    stop_logs = [ev for ev, _ in warn_logged if ev == "phase.c.stop_loss_triggered"]
    assert len(stop_logs) >= 1


@pytest.mark.asyncio
async def test_no_stop_loss_at_or_above_threshold(monkeypatch):
    """last_price=50 (equal) and last_price=51 → no sell (strictly less-than)."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    for last_price in (50, 51):
        strategy = make_strategy(monkeypatch, stop_loss_price=50)
        ticker = "KXLOWTBOS-26JUN23-B65.5"
        bracket = MarketBracket(
            market_ticker=ticker,
            event_ticker="KXLOWTBOS-26JUN23",
            series_ticker="KXLOWTBOS",
            bracket_label="bos low",
            phase=Phase.HOLDING,
            position_quantity=2,
            avg_entry=82,
        )
        strategy.brackets[ticker] = bracket
        strategy.active_positions[ticker] = bracket
        strategy.cache.update_last_price(ticker, last_price)
        monkeypatch.setattr(strategy.executor, "get_positions",
                            _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": last_price}}))

        await strategy._evaluate_held_positions()

        sell_orders = [o for o, _ in strategy.executor.orders if o.side.name == "SELL_YES"]
        assert len(sell_orders) == 0, f"No stop-loss should fire at last_price={last_price}"


@pytest.mark.asyncio
async def test_no_stop_loss_without_last_trade(monkeypatch):
    """No last-traded price → no sell, no crash."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
    ticker = "KXLOWTBOS-26JUN23-B65.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTBOS-26JUN23",
        series_ticker="KXLOWTBOS",
        bracket_label="bos low",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    # NO update_last_price call → get_last_price returns None

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2}}))

    # Must not raise
    await strategy._evaluate_held_positions()

    sell_orders = [o for o, _ in strategy.executor.orders if o.side.name == "SELL_YES"]
    assert len(sell_orders) == 0, "No stop-loss when no last-traded price exists"


@pytest.mark.asyncio
async def test_stop_loss_increments_ledger(monkeypatch, real_db):
    """Trigger stop-loss for KXLOWTBOS-26JUN23-B65.5; ledger (KXLOWTBOS, 26JUN23) count → 1."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    strategy = TemperatureStrategy(
        make_config(stop_loss_price=50),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )

    ticker = "KXLOWTBOS-26JUN23-B65.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTBOS-26JUN23",
        series_ticker="KXLOWTBOS",
        bracket_label="bos low",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_last_price(ticker, 49)

    # Mock _execute_stop_loss so it doesn't try to insert into executed_trades
    # The increment happens BEFORE _execute_stop_loss is called
    async def fake_execute_stop_loss(b):
        pass  # don't actually run the sell logic

    monkeypatch.setattr(strategy, "_execute_stop_loss", fake_execute_stop_loss)
    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 49}}))

    await strategy._evaluate_held_positions()

    count = await strategy._get_stop_loss_count_for_market(ticker)
    assert count == 1, f"Expected ledger count=1 after stop-loss, got {count}"


@pytest.mark.asyncio
async def test_recovery_sizing_doubles(monkeypatch, real_db):
    """Seed ledger counts 0/1/2/3; at BUY_TRIGGER assert entry quantity is 2/4/8/16."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    # Use separate city-per-count to avoid shared DB state within a single test
    test_cases = [
        (0, 2, "KXLOWTBOSA-26JUN23-T68"),
        (1, 4, "KXLOWTBOSB-26JUN23-T68"),
        (2, 8, "KXLOWTBOSC-26JUN23-T68"),
        (3, 16, "KXLOWTBOSD-26JUN23-T68"),
    ]
    for seed_count, expected_qty, ticker in test_cases:
        strategy = TemperatureStrategy(
            make_config(stop_loss_price=50, initial_contract_count=2, hedge_max_factor=3),
            TickerCache(),
            FakeWSManager(),
            FakeExecutor(),
            real_db,
        )
        # Seed the ledger with the given count (using a sibling bracket of same series)
        series = ticker.rsplit("-", 2)[0]  # e.g. KXLOWTBOSA
        seed_ticker = f"{series}-26JUN23-B65.5"
        for _ in range(seed_count):
            await strategy._increment_stop_loss_count_for_market(seed_ticker)

        bracket = MarketBracket(
            market_ticker=ticker,
            event_ticker=f"{series}-26JUN23",
            series_ticker=series,
            bracket_label="test",
            phase=Phase.MONITORING,
        )
        strategy.brackets[ticker] = bracket
        strategy.cache.update_quote(ticker, 82, 82)  # spread=0 → tight

        captured = []

        async def fake_execute_entry(b, quantity=None):
            captured.append(quantity)

        strategy._execute_entry = fake_execute_entry

        await strategy._evaluate_watchlist()

        assert len(captured) == 1, f"Expected entry call for seed_count={seed_count}"
        assert captured[0] == expected_qty, \
            f"seed_count={seed_count}: expected qty={expected_qty}, got {captured[0]}"


@pytest.mark.asyncio
async def test_recovery_cap_blocks_after_factor(monkeypatch, real_db):
    """count=4 (> HEDGE_MAX_FACTOR=3) → no order; count=3 → buys 16."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    # --- count=4: no buy ---
    strategy_blocked = TemperatureStrategy(
        make_config(stop_loss_price=50, initial_contract_count=2, hedge_max_factor=3),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )
    # Use a different series from the boundary test to avoid shared state
    for _ in range(4):
        await strategy_blocked._increment_stop_loss_count_for_market("KXLOWTBOSCAP-26JUN23-B65.5")

    info_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))

    ticker = "KXLOWTBOSCAP-26JUN23-T68"
    bracket_blocked = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTBOSCAP-26JUN23",
        series_ticker="KXLOWTBOSCAP",
        bracket_label="bos cap 68",
        phase=Phase.MONITORING,
    )
    strategy_blocked.brackets[ticker] = bracket_blocked
    strategy_blocked.cache.update_quote(ticker, 82, 82)
    strategy_blocked._execute_entry = AsyncMock()

    await strategy_blocked._evaluate_watchlist()

    strategy_blocked._execute_entry.assert_not_awaited()
    cap_logs = [ev for ev, _ in info_logged if ev == "phase.b.recovery_cap_reached"]
    assert len(cap_logs) >= 1, "phase.b.recovery_cap_reached must be logged when count > max_doublings"

    # --- count=3: buys 16 ---
    strategy_boundary = TemperatureStrategy(
        make_config(stop_loss_price=50, initial_contract_count=2, hedge_max_factor=3),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )
    # Use a different series for boundary to avoid interference
    for _ in range(3):
        await strategy_boundary._increment_stop_loss_count_for_market("KXLOWTBOSBND-26JUN23-B65.5")

    ticker2 = "KXLOWTBOSBND-26JUN23-T69"
    bracket_boundary = MarketBracket(
        market_ticker=ticker2,
        event_ticker="KXLOWTBOSBND-26JUN23",
        series_ticker="KXLOWTBOSBND",
        bracket_label="bos bnd 69",
        phase=Phase.MONITORING,
    )
    strategy_boundary.brackets[ticker2] = bracket_boundary
    strategy_boundary.cache.update_quote(ticker2, 82, 82)

    captured = []
    async def fake_entry(b, quantity=None):
        captured.append(quantity)

    strategy_boundary._execute_entry = fake_entry

    await strategy_boundary._evaluate_watchlist()

    assert len(captured) == 1, "Should place order at count=3 (boundary: count <= max_doublings)"
    assert captured[0] == 16, f"count=3 should give 2 * 2^3 = 16, got {captured[0]}"


@pytest.mark.asyncio
async def test_high_low_counters_independent(monkeypatch, real_db):
    """Stop-loss on KXLOWTBOS does not change KXHIGHTBOS sizing."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    strategy = TemperatureStrategy(
        make_config(stop_loss_price=50, initial_contract_count=2, hedge_max_factor=3),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )

    # Increment KXLOWTBOS counter (LOW)
    await strategy._increment_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")

    # HIGH counter should be unaffected
    high_count = await strategy._get_stop_loss_count_for_market("KXHIGHTBOS-26JUN23-T90")
    assert high_count == 0, f"HIGH counter should be 0, got {high_count}"

    # LOW counter should be 1
    low_count = await strategy._get_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")
    assert low_count == 1, f"LOW counter should be 1, got {low_count}"

    # A KXHIGHTBOS bracket at BUY_TRIGGER should buy base size 2
    high_ticker = "KXHIGHTBOS-26JUN23-T90"
    high_bracket = MarketBracket(
        market_ticker=high_ticker,
        event_ticker="KXHIGHTBOS-26JUN23",
        series_ticker="KXHIGHTBOS",
        bracket_label="bos high 90",
        phase=Phase.MONITORING,
    )
    strategy.brackets[high_ticker] = high_bracket
    strategy.cache.update_quote(high_ticker, 82, 82)

    captured = []
    async def fake_entry(b, quantity=None):
        captured.append(quantity)

    strategy._execute_entry = fake_entry

    await strategy._evaluate_watchlist()

    assert len(captured) == 1
    assert captured[0] == 2, f"KXHIGHTBOS should buy base size 2 (unaffected by KXLOWTBOS), got {captured[0]}"


@pytest.mark.asyncio
async def test_any_bracket_in_series_uses_counter(monkeypatch, real_db):
    """Stop-loss on KXLOWTBOS-...-B65.5 → KXLOWTBOS-...-T68 at BUY_TRIGGER buys doubled."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    strategy = TemperatureStrategy(
        make_config(stop_loss_price=50, initial_contract_count=2, hedge_max_factor=3),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )

    # Trigger stop-loss on B65.5 bracket to increment counter
    await strategy._increment_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")

    # A DIFFERENT bracket in the same series should pick up the doubled size
    t68_ticker = "KXLOWTBOS-26JUN23-T68"
    t68_bracket = MarketBracket(
        market_ticker=t68_ticker,
        event_ticker="KXLOWTBOS-26JUN23",
        series_ticker="KXLOWTBOS",
        bracket_label="bos low 68",
        phase=Phase.MONITORING,
    )
    strategy.brackets[t68_ticker] = t68_bracket
    strategy.cache.update_quote(t68_ticker, 82, 82)

    captured = []
    async def fake_entry(b, quantity=None):
        captured.append(quantity)

    strategy._execute_entry = fake_entry

    await strategy._evaluate_watchlist()

    assert len(captured) == 1
    assert captured[0] == 4, f"T68 should buy doubled size 4 (count=1 → 2*2^1), got {captured[0]}"


# ---------------------------------------------------------------------------
# parse_series_and_date tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ticker,expected", [
    ("KXLOWTSATX-26JUN23-T78", ("KXLOWTSATX", "26JUN23")),
    ("KXHIGHTPHX-26JUN23-B111.5", ("KXHIGHTPHX", "26JUN23")),
    ("KXHIGHNY-26JUN23-T90", ("KXHIGHNY", "26JUN23")),
    ("not-a-valid-ticker", None),
    ("", None),
    ("KXHIGHNY-26JUN23", None),
])
def test_parse_series_and_date(ticker, expected):
    result = parse_series_and_date(ticker)
    assert result == expected, f"parse_series_and_date({ticker!r}) = {result!r}, expected {expected!r}"


# ---------------------------------------------------------------------------
# Ledger persistence test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ledger_persists_across_restart(monkeypatch, real_db):
    """Increment via helper; re-create strategy against same DB; assert count read back."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    strategy1 = TemperatureStrategy(
        make_config(),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )
    await strategy1._increment_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")
    await strategy1._increment_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")

    # Re-create strategy using the same DB
    strategy2 = TemperatureStrategy(
        make_config(),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )
    count = await strategy2._get_stop_loss_count_for_market("KXLOWTBOS-26JUN23-B65.5")
    assert count == 2, f"Expected count=2 after restart, got {count}"


# ---------------------------------------------------------------------------
# Config loading test
# ---------------------------------------------------------------------------

def test_config_loads_without_hedge_trigger_price(monkeypatch):
    """AppConfig from env WITHOUT hedge_trigger_price/hedge_buy loads with defaults."""
    import os

    # Clear any env pollution from test_config.py
    monkeypatch.delenv("HEDGE_TRIGGER_PRICE", raising=False)
    monkeypatch.delenv("HEDGE_BUY", raising=False)

    # Simulate .env without hedge_trigger_price / hedge_buy
    env = {
        "KALSHI_API_KEY": "test-key",
        "KALSHI_PRIVATE_KEY_PATH": "key.pem",
        "MYSQL_DATABASE_URL": "******localhost:3306/db",
        "TRADING_MODE": "PAPER",
        "INITIAL_CONTRACT_COUNT": "2",
        "BUY_TRIGGER_PRICE": "0.82",
        "MINIMUM_SPREAD": "0.04",
        "HEDGE_MAX_FACTOR": "3",
        "STOP_LOSS_PRICE": "0.50",
        "SPREAD_MONITOR_PRICE": "0.90",
        "MONITOR_START_PRICE": "0.80",
        "EVAL_PRICE_FLOOR": "0.05",
    }
    config = AppConfig(**{k.lower(): v for k, v in env.items()})

    # Must load without error; fields that are absent default to 0
    assert config.hedge_trigger_price == 0
    assert config.hedge_buy == 0
    # Correct parsing of provided fields
    assert config.stop_loss_price == 50  # 0.50 * 100
    assert config.hedge_max_factor == 3.0
    assert config.buy_trigger_price == 82


# ---------------------------------------------------------------------------
# Stop-loss retry tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_loss_no_fill_keeps_position_and_retries(monkeypatch):
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
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
    strategy.cache.update_last_price(ticker, 20)

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
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
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
    strategy.cache.update_last_price(ticker, 20)

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
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
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
    strategy.cache.update_last_price(ticker, 20)

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
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
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
    strategy.cache.update_last_price(ticker, 20)

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
# Stop-loss does NOT fire for above-stop_loss prices (test using stop_loss=50)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase_c_stop_loss_fires_when_cost_basis_unknown(monkeypatch):
    """Stop-loss fires even when avg_entry=0 (cost basis unknown)."""
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
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
    # last_traded_price=25 < stop_loss=50
    strategy.cache.update_last_price(ticker, 25)
    strategy.executor.succeed = True

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 25}}))

    await strategy._evaluate_held_positions()

    events = [event for event, _ in warn_logged]
    assert "phase.c.stop_loss_triggered" in events
    sell_orders = [o for o, _ in strategy.executor.orders
                   if o.market_ticker == ticker and o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1
    assert sell_orders[0].price == 1


@pytest.mark.asyncio
async def test_phase_c_stop_loss_fires_with_zero_entry_seattle_scenario(monkeypatch):
    """Regression: stop-loss fires even when avg_entry=0 (original Seattle failure scenario)."""
    import core.state_machine as state_machine

    warn_logged = []
    monkeypatch.setattr(state_machine.logger, "warning",
                        lambda event, **kwargs: warn_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch, stop_loss_price=50)
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
    # last_traded_price=29 < stop_loss=50
    strategy.cache.update_last_price(ticker, 29)
    strategy.executor.succeed = True

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 29}}))

    await strategy._evaluate_held_positions()

    events = [event for event, _ in warn_logged]
    assert "phase.c.stop_loss_triggered" in events
    sell_orders = [o for o, _ in strategy.executor.orders
                   if o.market_ticker == ticker and o.side.name == "SELL_YES"]
    assert len(sell_orders) >= 1
    assert sell_orders[0].price == 1


# ---------------------------------------------------------------------------
# Self-heal tests (still valid in new Phase C)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_self_heal_when_cost_unrecoverable(monkeypatch):
    """When positions API returns 0 and fills return [], avg_entry stays 0."""
    import core.state_machine as state_machine

    info_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTOKC-26JUN23-T86"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTOKC-26JUN23",
        series_ticker="KXHIGHTOKC",
        bracket_label="okc high 86",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "average_fill_cost_cents": 0}}))
    monkeypatch.setattr(strategy.executor, "get_fills", AsyncMock(return_value=[]), raising=False)

    # No last_price set → stop-loss skip branch, but self-heal runs first
    await strategy._evaluate_held_positions()

    assert bracket.avg_entry == 0
    heal_logs = [ev for ev, _ in info_logged if ev == "phase.c.entry_self_healed"]
    assert len(heal_logs) == 0


@pytest.mark.asyncio
async def test_self_heal_fills_fallback_throttled_to_60s(monkeypatch):
    """Fills fallback is called at most once per 60s per bracket."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTOKC-26JUN23-T86"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTOKC-26JUN23",
        series_ticker="KXHIGHTOKC",
        bracket_label="okc high 86",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=0,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "average_fill_cost_cents": 0}}))

    fills_call_count = []

    async def fake_get_fills(**_kwargs):
        fills_call_count.append(1)
        return [
            {"market_ticker": ticker, "action": "buy", "count_fp": "2", "yes_price_dollars": "0.83"},
        ]

    monkeypatch.setattr(strategy.executor, "get_fills", fake_get_fills, raising=False)

    # No last_price → stop-loss skip, but self-heal runs
    await strategy._evaluate_held_positions()
    # Heal happened; avg_entry is now 83 → subsequent cycles skip self-heal
    await strategy._evaluate_held_positions()

    assert len(fills_call_count) <= 1
    assert bracket.avg_entry == 83


@pytest.mark.asyncio
async def test_existing_healthy_entry_untouched(monkeypatch):
    """Bracket with valid avg_entry is not touched by self-heal."""
    import core.state_machine as state_machine

    info_logged = []
    monkeypatch.setattr(state_machine.logger, "info",
                        lambda event, **kwargs: info_logged.append((event, kwargs)))
    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)

    strategy = make_strategy(monkeypatch)
    ticker = "KXHIGHTOKC-26JUN23-T86"

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXHIGHTOKC-26JUN23",
        series_ticker="KXHIGHTOKC",
        bracket_label="okc high 86",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=86,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket

    monkeypatch.setattr(strategy.executor, "get_positions",
                        _make_fake_get_positions({ticker: {"count": 2, "average_fill_cost_cents": 86}}))

    fills_call_count = []

    async def fake_get_fills(**_kwargs):
        fills_call_count.append(1)
        return []

    monkeypatch.setattr(strategy.executor, "get_fills", fake_get_fills, raising=False)

    await strategy._evaluate_held_positions()

    assert bracket.avg_entry == 86
    heal_logs = [ev for ev, _ in info_logged if ev == "phase.c.entry_self_healed"]
    assert len(heal_logs) == 0
    assert len(fills_call_count) == 0


# ---------------------------------------------------------------------------
# Stop-loss count increment guard: not double-counted on retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_loss_increment_only_once_across_retries(monkeypatch, real_db):
    """The ledger increments exactly once even when stop-loss retries on the 60s throttle."""
    import core.state_machine as state_machine

    monkeypatch.setattr(state_machine.logger, "warning", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine.logger, "debug", lambda *_a, **_kw: None)
    monkeypatch.setattr(state_machine, "load_private_key", lambda _path: object())

    strategy = TemperatureStrategy(
        make_config(stop_loss_price=50),
        TickerCache(),
        FakeWSManager(),
        FakeExecutor(),
        real_db,
    )

    # Use a unique series to avoid interference with other tests in same real_db
    ticker = "KXLOWTRETRY-26JUN23-B65.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="KXLOWTRETRY-26JUN23",
        series_ticker="KXLOWTRETRY",
        bracket_label="retry test",
        phase=Phase.HOLDING,
        position_quantity=2,
        avg_entry=82,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    strategy.cache.update_last_price(ticker, 49)

    # Mock _execute_stop_loss so it keeps the bracket in active_positions
    # (simulating a failed/throttled sell without touching the executed_trades table)
    execute_sl_calls = []
    async def fake_execute_stop_loss(b):
        execute_sl_calls.append(b.market_ticker)
        # Don't remove from active_positions so the retry can happen

    monkeypatch.setattr(strategy, "_execute_stop_loss", fake_execute_stop_loss)
    monkeypatch.setattr(
        strategy.executor,
        "get_positions",
        _make_fake_get_positions({ticker: {"count": 2, "last_price_cents": 49}}),
    )

    # First cycle: stop-loss triggered, increment fires once
    await strategy._evaluate_held_positions()
    count_after_first = await strategy._get_stop_loss_count_for_market(ticker)
    assert count_after_first == 1, f"Expected count=1 after first trigger, got {count_after_first}"
    assert len(execute_sl_calls) == 1

    # Second cycle: retry fires (bracket still in active_positions), but _stop_loss_counted is True
    await strategy._evaluate_held_positions()
    count_after_retry = await strategy._get_stop_loss_count_for_market(ticker)
    assert count_after_retry == 1, "Ledger must not double-count on retry"
    assert len(execute_sl_calls) == 2, "Stop-loss execute should be called again on retry"
