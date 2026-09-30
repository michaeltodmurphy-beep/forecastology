"""
Tests for the five High-severity audit fixes:
  1. Buy price ceiling enforced at submission time (payload, executor,
     state machine fresh-ask re-check, scanner signal).
  2. monitor._buy_hedge routes through the shared executor / V2 endpoint and
     never performs HTTP in PAPER or DRY_RUN.
  3. Monitor reconciles the ACTUAL hedge fill quantity (FILLED / PARTIAL /
     retry on zero fill) instead of treating HTTP 200 as filled.
  4. Per-event (series + date) aggregate exposure cap, DB-backed.
  5. LiveTradeExecutor position cap fails closed when the positions API errors.

All tests exercise the production functions directly.
"""
import os
import sys
from typing import Optional

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import core.event_exposure as event_exposure_mod
import execution.live as live_mod
import monitor as mon
import scanner as scanner_module
from app.models import ExecutedTrade, Position as PositionModel, TradeStatus
from core.constants import REST_PORTFOLIO_ORDERS
from core.event_exposure import event_exposure_allows_buy, event_key, resolve_event_caps
from core.types import MarketBracket, OrderBook, OrderBookLevel, OrderRequest, OrderSide, Phase
from execution.base import ExecutionResult
from execution.live import LiveTradeExecutor
from tests.test_state_machine import (
    FakeExecutor,
    InMemoryDB,
    capture_logs,
    make_config,
    make_strategy,
)

TICKER = "KXLOWTLAX-26JUL30-B60.5"
SIBLING = "KXLOWTLAX-26JUL30-B62.5"
OTHER_EVENT = "KXLOWTLAX-26JUL31-B60.5"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _RecordingClient:
    """Fake httpx client for LiveTradeExecutor recording every request."""

    def __init__(self, post_payload: Optional[dict] = None, positions: Optional[list] = None,
                 get_exc: Optional[Exception] = None):
        self.posts: list[tuple[str, dict]] = []
        self.gets: list[str] = []
        self._post_payload = post_payload or {}
        self._positions = positions or []
        self._get_exc = get_exc

    async def post(self, url, json=None, headers=None):
        self.posts.append((url, json))
        return _Resp(201, self._post_payload)

    async def get(self, url, headers=None, params=None):
        self.gets.append(url)
        if self._get_exc is not None:
            raise self._get_exc
        return _Resp(200, {"market_positions": self._positions})


def _make_live_executor(monkeypatch, client, max_buy_qty=None, dry_run=False) -> LiveTradeExecutor:
    monkeypatch.setattr(live_mod, "load_private_key", lambda _p: object())
    monkeypatch.setattr(live_mod, "build_auth_headers", lambda *_a, **_k: {})
    ex = LiveTradeExecutor("https://example.test", "key", "unused.pem",
                           dry_run=dry_run, max_buy_qty=max_buy_qty)
    ex._client = client
    return ex


def _filled_payload(count: int, price_cents: int) -> dict:
    return {
        "order_id": "oid-1",
        "fill_count_fp": f"{count}.00",
        "taker_fill_cost_dollars": f"{count * price_cents / 100:.6f}",
        "maker_fill_cost_dollars": "0.000000",
    }


def _capture(monkeypatch, logger_obj):
    logged = []
    for method in ("debug", "info", "warning", "error", "critical"):
        monkeypatch.setattr(
            logger_obj, method,
            lambda event, _m=method, **kw: logged.append((event, kw)),
        )
    return logged


def _block_all_http(monkeypatch):
    """Make ANY real httpx request fail the test loudly."""
    calls = []

    async def _no_http(self, request, *args, **kwargs):
        calls.append(str(request.url))
        raise AssertionError(f"unexpected HTTP request: {request.method} {request.url}")

    monkeypatch.setattr(httpx.AsyncClient, "send", _no_http)
    return calls


# ---------------------------------------------------------------------------
# 1. Buy price ceiling
# ---------------------------------------------------------------------------

def test_payload_max_price_is_a_true_cap():
    order = OrderRequest(TICKER, OrderSide.BUY_YES, price=85, quantity=1)
    # Ceiling above requested price: submit the requested price, NOT the ceiling.
    assert order.to_kalshi_payload(max_price=90)["price"] == "0.8500"
    # Requested price above the ceiling: clamp to the ceiling.
    high = OrderRequest(TICKER, OrderSide.BUY_YES, price=95, quantity=1)
    assert high.to_kalshi_payload(max_price=90)["price"] == "0.9000"
    # Sells are never affected by max_price.
    sell = OrderRequest(TICKER, OrderSide.SELL_YES, price=95, quantity=1)
    assert sell.to_kalshi_payload(max_price=90)["price"] == "0.9500"


@pytest.mark.asyncio
async def test_live_executor_rejects_price_above_ceiling(monkeypatch):
    client = _RecordingClient(post_payload=_filled_payload(1, 95))
    ex = _make_live_executor(monkeypatch, client)
    result = await ex.buy_yes(OrderRequest(TICKER, OrderSide.BUY_YES, 95, 1), max_price=90)
    assert result.success is False
    assert result.status == "REJECTED"
    assert "price_above_ceiling" in result.notes
    assert client.posts == []


@pytest.mark.asyncio
async def test_live_executor_submits_limit_at_or_below_ceiling(monkeypatch):
    client = _RecordingClient(post_payload=_filled_payload(1, 85))
    ex = _make_live_executor(monkeypatch, client)
    result = await ex.buy_yes(OrderRequest(TICKER, OrderSide.BUY_YES, 85, 1), max_price=90)
    assert result.success is True
    assert len(client.posts) == 1
    url, payload = client.posts[0]
    assert payload["price"] == "0.8500"
    assert url.endswith(REST_PORTFOLIO_ORDERS)


@pytest.mark.asyncio
async def test_execute_entry_rechecks_fresh_ask_against_ceiling(monkeypatch):
    executor = FakeExecutor()
    executor.buy_success = True
    strategy = make_strategy(monkeypatch, executor=executor, spread_monitor_price=90)
    logged = capture_logs(monkeypatch)

    async def fresh_prices(tickers):
        return {TICKER: OrderBook(yes_asks=[OrderBookLevel(price=95, quantity=5, order_count=1)])}

    monkeypatch.setattr(strategy, "_fetch_live_prices", fresh_prices)
    bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b",
                            phase=Phase.ENTERING, crossed_buy=True)

    await strategy._execute_entry(bracket)

    assert executor.orders == [], "no order may be submitted above the ceiling"
    assert bracket.phase == Phase.MONITORING
    assert bracket.crossed_buy is False
    block = next(kw for ev, kw in logged if ev == "phase.b.entry_blocked_above_ceiling")
    assert block["fresh_ask"] == 95 and block["max_price"] == 90


@pytest.mark.asyncio
async def test_execute_entry_submits_when_fresh_ask_within_ceiling(monkeypatch):
    executor = FakeExecutor()
    executor.buy_success = True
    strategy = make_strategy(monkeypatch, executor=executor, spread_monitor_price=90)

    async def fresh_prices(tickers):
        return {TICKER: OrderBook(yes_asks=[OrderBookLevel(price=88, quantity=5, order_count=1)])}

    monkeypatch.setattr(strategy, "_fetch_live_prices", fresh_prices)
    bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b", phase=Phase.ENTERING)

    await strategy._execute_entry(bracket)

    assert len(executor.orders) == 1
    order, max_price = executor.orders[0]
    assert order.price == 88 and max_price == 90
    assert order.to_kalshi_payload(max_price)["price"] == "0.8800"


def _scanner_fake_db():
    class _Result:
        def fetchall(self):
            return []

        def scalars(self):
            return self

        def all(self):
            return []

    class _Session:
        def add(self, *_a, **_k):
            return None

        async def commit(self):
            return None

        async def execute(self, *_a, **_k):
            return _Result()

    class _Ctx:
        async def __aenter__(self):
            return _Session()

        async def __aexit__(self, *_a):
            return None

    class _DB:
        async def get_session(self):
            return _Ctx()

    return _DB()


@pytest.mark.asyncio
async def test_scanner_requires_ask_at_or_below_ceiling(monkeypatch):
    config = make_config(buy_trigger_price_low=82, spread_monitor_price=90,
                         sunrise_max_spread=5, midam_max_spread=5, pm_max_spread=5)
    above = "KXLOWTLAX-26JUL30-B65.5"
    at = "KXLOWTLAX-26JUL30-B67.5"

    async def fake_fetch(_config, _client):
        return [above, at], {
            above: {"best_ask": 95, "best_bid": 94, "spread": 1},
            at: {"best_ask": 90, "best_bid": 89, "spread": 1},
        }

    bought = []

    async def fake_buy_market(_config, ticker, ask, _client):
        bought.append((ticker, ask))
        return True

    monkeypatch.setattr(scanner_module, "_fetch_markets_via_rest", fake_fetch)
    monkeypatch.setattr(scanner_module, "buy_market", fake_buy_market)

    await scanner_module.run_scan_cycle(config, _scanner_fake_db())

    assert bought == [(at, 90)]


# ---------------------------------------------------------------------------
# 2. Monitor hedges route through the shared executor
# ---------------------------------------------------------------------------

class _NoHttpClient:
    def __init__(self):
        self.calls = []

    async def post(self, *a, **k):
        self.calls.append(("post", a, k))
        raise AssertionError("monitor must not POST directly")

    async def get(self, *a, **k):
        self.calls.append(("get", a, k))
        raise AssertionError("monitor must not GET in _buy_hedge")


@pytest.mark.asyncio
async def test_monitor_buy_hedge_paper_never_performs_http(monkeypatch):
    http_calls = _block_all_http(monkeypatch)
    config = make_config(trading_mode="PAPER", dry_run=False)
    client = _NoHttpClient()

    result = await mon._buy_hedge(TICKER, 60, 2, config, client)

    assert isinstance(result, ExecutionResult)
    assert result.success is True and result.status == "FILLED"
    assert client.calls == []
    assert http_calls == []


@pytest.mark.asyncio
async def test_monitor_buy_hedge_dry_run_never_performs_http(monkeypatch):
    http_calls = _block_all_http(monkeypatch)
    monkeypatch.setattr(live_mod, "load_private_key", lambda _p: object())
    config = make_config(trading_mode="LIVE", dry_run=True)
    client = _NoHttpClient()

    result = await mon._buy_hedge(TICKER, 60, 2, config, client)

    assert isinstance(result, ExecutionResult)
    assert result.success is False
    assert result.status == "DRY_RUN"
    assert result.fill_quantity == 0
    assert client.calls == []
    assert http_calls == []


@pytest.mark.asyncio
async def test_monitor_buy_hedge_live_uses_v2_events_orders_endpoint(monkeypatch):
    live_client = _RecordingClient(post_payload=_filled_payload(2, 60))
    ex = _make_live_executor(monkeypatch, live_client, max_buy_qty=8)
    config = make_config(trading_mode="LIVE", dry_run=False)

    result = await mon._buy_hedge(TICKER, 60, 2, config, _NoHttpClient(), executor=ex)

    assert result.success is True and result.fill_quantity == 2
    assert len(live_client.posts) == 1
    url, payload = live_client.posts[0]
    assert url.endswith("/trade-api/v2/portfolio/events/orders")
    assert payload["price"] == "0.6000"  # capped at requested price, never 0.90
    assert payload["time_in_force"] == "immediate_or_cancel"


@pytest.mark.asyncio
async def test_monitor_buy_hedge_blocks_above_ceiling(monkeypatch):
    config = make_config(trading_mode="PAPER", spread_monitor_price=90)
    result = await mon._buy_hedge(TICKER, 95, 2, config, _NoHttpClient())
    assert result is False


# ---------------------------------------------------------------------------
# 3. Hedge fill reconciliation in run_monitor_cycle
# ---------------------------------------------------------------------------

class _HedgeExecutor:
    def __init__(self, fills: list[int], fill_price: int = 60):
        self._fills = list(fills)
        self.fill_price = fill_price
        self.orders = []

    async def buy_yes(self, order, max_price=None):
        self.orders.append((order, max_price))
        filled = min(self._fills.pop(0), order.quantity)
        return ExecutionResult(
            success=filled > 0,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=self.fill_price if filled else 0,
            fill_quantity=filled,
            total_cost_cents=self.fill_price * filled,
            order_id="hedge-oid" if filled else "",
            status="FILLED" if filled else "NO_FILL",
        )


def _setup_monitor(monkeypatch, executor, qty=4):
    held = PositionModel(market_ticker=TICKER, event_ticker="KXLOWTLAX-26JUL30",
                         series_ticker="KXLOWTLAX", side="yes", quantity=qty,
                         avg_entry_price=80, last_price=80)
    db = InMemoryDB([held])
    config = make_config(trading_mode="LIVE", hedge_trigger_price=50,
                         initial_contract_count=4, hedge_max_factor=2)

    async def fake_price(ticker, *_a, **_k):
        if ticker == TICKER:
            return {"last_price": 40, "yes_ask": 40, "yes_bid": 38}
        return {"last_price": 60, "yes_ask": 60, "yes_bid": 58}

    async def fake_find(*_a, **_k):
        return SIBLING

    monkeypatch.setattr(mon, "load_private_key", lambda _p: object())
    monkeypatch.setattr(mon, "_get_market_price_rest", fake_price)
    monkeypatch.setattr(mon, "_find_hedge_bracket", fake_find)
    monkeypatch.setattr(mon, "create_executor", lambda **_k: executor)
    return db, config, held


@pytest.mark.asyncio
async def test_monitor_full_hedge_fill_marks_filled_and_hedged(monkeypatch):
    executor = _HedgeExecutor([4])
    db, config, held = _setup_monitor(monkeypatch, executor)

    await mon.run_monitor_cycle(config, db)

    trades = db.store[ExecutedTrade]
    assert len(trades) == 1
    assert trades[0].status == TradeStatus.FILLED
    assert trades[0].quantity == 4
    assert held.hedge_market_ticker == SIBLING
    assert held.hedge_quantity == 4


@pytest.mark.asyncio
async def test_monitor_partial_hedge_fill_records_partial_and_retries_remainder(monkeypatch):
    executor = _HedgeExecutor([1, 3])
    db, config, held = _setup_monitor(monkeypatch, executor)

    await mon.run_monitor_cycle(config, db)

    trades = db.store[ExecutedTrade]
    assert len(trades) == 1
    assert trades[0].status == TradeStatus.PARTIAL
    assert trades[0].quantity == 1
    assert trades[0].total_cost_cents == 60
    assert held.hedge_market_ticker is None, "partial fill must NOT mark the position hedged"
    assert held.hedge_quantity == 1

    # Next cycle retries ONLY the remaining quantity.
    await mon.run_monitor_cycle(config, db)
    assert executor.orders[1][0].quantity == 3
    trades = db.store[ExecutedTrade]
    assert [t.status for t in trades] == [TradeStatus.PARTIAL, TradeStatus.FILLED]
    assert held.hedge_market_ticker == SIBLING
    assert held.hedge_quantity == 4


@pytest.mark.asyncio
async def test_monitor_zero_hedge_fill_is_not_recorded_as_hedged(monkeypatch):
    executor = _HedgeExecutor([0, 4])
    db, config, held = _setup_monitor(monkeypatch, executor)

    await mon.run_monitor_cycle(config, db)

    assert db.store[ExecutedTrade] == []
    assert held.hedge_market_ticker is None
    assert not held.hedge_quantity

    # Retried next cycle.
    await mon.run_monitor_cycle(config, db)
    assert len(executor.orders) == 2
    assert held.hedge_market_ticker == SIBLING


@pytest.mark.asyncio
async def test_monitor_records_fill_even_when_result_not_success(monkeypatch):
    executor = _HedgeExecutor([2])
    orig = executor.buy_yes

    async def failing_but_filled(order, max_price=None):
        result = await orig(order, max_price)
        result.success = False
        result.status = "ERROR"
        return result

    executor.buy_yes = failing_but_filled
    db, config, held = _setup_monitor(monkeypatch, executor)

    await mon.run_monitor_cycle(config, db)

    trades = db.store[ExecutedTrade]
    assert len(trades) == 1 and trades[0].quantity == 2
    assert trades[0].status == TradeStatus.PARTIAL
    assert held.hedge_quantity == 2
    assert held.hedge_market_ticker is None


# ---------------------------------------------------------------------------
# 4. Per-event aggregate exposure cap
# ---------------------------------------------------------------------------

def test_event_key_and_default_caps():
    assert event_key(TICKER) == "KXLOWTLAX-26JUL30"
    assert event_key(SIBLING) == event_key(TICKER)
    assert event_key(OTHER_EVENT) != event_key(TICKER)
    cfg = make_config(initial_contract_count=2, hedge_max_factor=3, spread_monitor_price=90)
    assert resolve_event_caps(cfg) == (8, 720)
    cfg.event_max_contracts, cfg.event_max_cost_cents = 5, -1
    assert resolve_event_caps(cfg) == (5, None)


def _pos(ticker, qty, price=80):
    return PositionModel(market_ticker=ticker, side="yes", quantity=qty, avg_entry_price=price)


@pytest.mark.asyncio
async def test_event_exposure_counts_sibling_brackets_from_db(monkeypatch):
    logged = _capture(monkeypatch, event_exposure_mod.logger)
    cfg = make_config(initial_contract_count=2, hedge_max_factor=2)  # auto cap = 4
    db = InMemoryDB([_pos(SIBLING, 3), _pos(OTHER_EVENT, 4)])

    assert await event_exposure_allows_buy(cfg, db, TICKER, 1, 85) is True
    assert await event_exposure_allows_buy(cfg, db, TICKER, 2, 85) is False
    block = next(kw for ev, kw in logged if ev == "entry.event_exposure_cap_blocked")
    assert block["existing_qty"] == 3  # other event's position is NOT counted
    assert block["reason"] == "contracts"


@pytest.mark.asyncio
async def test_event_exposure_cost_cap_and_inflight():
    cfg = make_config(initial_contract_count=2, hedge_max_factor=2)
    cfg.event_max_contracts = 100
    cfg.event_max_cost_cents = 200
    db = InMemoryDB([_pos(SIBLING, 1, price=80)])
    assert await event_exposure_allows_buy(cfg, db, TICKER, 1, 85) is True   # 165
    assert await event_exposure_allows_buy(cfg, db, TICKER, 2, 85) is False  # 250
    # In-flight (e.g. a working chaser) counts at its price.
    assert await event_exposure_allows_buy(
        cfg, db, TICKER, 1, 85, inflight={"KXLOWTLAX-26JUL30-B64.5": (1, 90)}
    ) is False  # 80 + 90 + 85 = 255


@pytest.mark.asyncio
async def test_event_exposure_fails_closed_on_db_error(monkeypatch):
    logged = _capture(monkeypatch, event_exposure_mod.logger)

    class _BrokenDB:
        async def get_session(self):
            raise RuntimeError("db down")

    cfg = make_config()
    assert await event_exposure_allows_buy(cfg, _BrokenDB(), TICKER, 1, 85) is False
    assert any(ev == "entry.event_exposure_unverifiable" for ev, _ in logged)


@pytest.mark.asyncio
async def test_state_machine_entry_blocked_by_event_cap_across_cycles(monkeypatch):
    """A sibling bracket held from an earlier cycle (DB) blocks a new entry,
    even though the per-cycle duplicate-entry set is reset every sweep."""
    executor = FakeExecutor()
    executor.buy_success = True
    db = InMemoryDB([_pos(SIBLING, 4)])
    strategy = make_strategy(monkeypatch, executor=executor, db=db,
                             initial_contract_count=2, hedge_max_factor=2)  # cap 4
    ob = OrderBook(yes_asks=[OrderBookLevel(price=85, quantity=10, order_count=1)])

    for _cycle in range(2):
        strategy._entry_step_seen = set()  # what each watchlist sweep does
        bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b",
                                phase=Phase.ENTERING)
        await strategy._execute_entry(bracket, ob=ob)
        assert bracket.phase == Phase.MONITORING

    assert executor.orders == []

    # A different event (other day) is unaffected.
    other = MarketBracket(OTHER_EVENT, "KXLOWTLAX-26JUL31", "KXLOWTLAX", "b",
                          phase=Phase.ENTERING)
    await strategy._execute_entry(other, ob=ob)
    assert len(executor.orders) == 1


@pytest.mark.asyncio
async def test_state_machine_entry_counts_in_memory_positions(monkeypatch):
    executor = FakeExecutor()
    executor.buy_success = True
    strategy = make_strategy(monkeypatch, executor=executor,
                             initial_contract_count=2, hedge_max_factor=2)  # cap 4
    held = MarketBracket(SIBLING, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "h",
                         phase=Phase.HOLDING, position_quantity=3, avg_entry=80)
    strategy.active_positions[SIBLING] = held
    bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b", phase=Phase.ENTERING)

    await strategy._execute_entry(
        bracket, ob=OrderBook(yes_asks=[OrderBookLevel(price=85, quantity=10, order_count=1)])
    )

    assert executor.orders == []


# ---------------------------------------------------------------------------
# 5. Position cap fails closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_position_cap_fails_closed_on_lookup_error(monkeypatch):
    logged = _capture(monkeypatch, live_mod.logger)
    client = _RecordingClient(post_payload=_filled_payload(1, 85),
                              get_exc=RuntimeError("positions API down"))
    ex = _make_live_executor(monkeypatch, client, max_buy_qty=8)

    result = await ex.buy_yes(OrderRequest(TICKER, OrderSide.BUY_YES, 85, 1), max_price=90)

    assert result.success is False
    assert result.status == "REJECTED"
    assert "position_cap_unverifiable" in result.notes
    assert client.posts == []
    assert any(ev == "live.position_cap_unverifiable" for ev, _ in logged)


@pytest.mark.asyncio
async def test_live_position_cap_fails_closed_on_malformed_count(monkeypatch):
    client = _RecordingClient(post_payload=_filled_payload(1, 85))
    ex = _make_live_executor(monkeypatch, client, max_buy_qty=8)

    async def bad_positions():
        return {TICKER: {"count": "not-a-number"}}

    monkeypatch.setattr(ex, "get_positions", bad_positions)
    result = await ex.buy_yes(OrderRequest(TICKER, OrderSide.BUY_YES, 85, 1), max_price=90)
    assert result.success is False
    assert client.posts == []


@pytest.mark.asyncio
async def test_live_position_cap_uses_verified_position(monkeypatch):
    client = _RecordingClient(
        post_payload=_filled_payload(1, 85),
        positions=[{"ticker": TICKER, "position_fp": "7", "average_fill_cost_dollars": "0.80"}],
    )
    ex = _make_live_executor(monkeypatch, client, max_buy_qty=8)

    ok = await ex.buy_yes(OrderRequest(TICKER, OrderSide.BUY_YES, 85, 1), max_price=90)
    assert ok.success is True
    blocked = await ex.buy_yes(OrderRequest(TICKER, OrderSide.BUY_YES, 85, 2), max_price=90)
    assert blocked.success is False and "position_cap_blocked" in blocked.notes
    assert len(client.posts) == 1
