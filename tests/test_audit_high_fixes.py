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
import datetime
import os
import sys
from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock

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
    # Bid at the ceiling to cross the spread, without ever exceeding it.
    assert order.to_kalshi_payload(max_price=90)["price"] == "0.9000"
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
    assert payload["price"] == "0.9000"
    assert url.endswith(REST_PORTFOLIO_ORDERS)


@pytest.mark.asyncio
async def test_execute_entry_rechecks_fresh_ask_against_ceiling(monkeypatch):
    executor = FakeExecutor()
    executor.buy_success = True
    strategy = make_strategy(monkeypatch, executor=executor, spread_monitor_price=90)
    logged = capture_logs(monkeypatch)

    async def fresh_prices(ticker):
        assert ticker == TICKER
        return {"yes_ask": 95}

    monkeypatch.setattr(strategy, "_fetch_market_data_via_rest", fresh_prices)
    bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b",
                            phase=Phase.ENTERING, crossed_buy=True)

    await strategy._execute_entry(bracket)

    assert executor.orders == [], "no order may be submitted above the ceiling"
    assert bracket.phase == Phase.MONITORING
    assert bracket.crossed_buy is True
    assert bracket.pending_entry is True
    block = next(kw for ev, kw in logged if ev == "phase.b.entry_blocked_above_ceiling")
    assert block["ask"] == 95 and block["max_price"] == 90
    assert block["quote_source"] == "rest"


@pytest.mark.asyncio
async def test_execute_entry_submits_when_fresh_ask_within_ceiling(monkeypatch):
    executor = FakeExecutor()
    executor.buy_success = True
    strategy = make_strategy(monkeypatch, executor=executor, spread_monitor_price=90)

    async def fresh_prices(ticker):
        assert ticker == TICKER
        return {"yes_ask": 88}

    monkeypatch.setattr(strategy, "_fetch_market_data_via_rest", fresh_prices)
    bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b", phase=Phase.ENTERING)

    await strategy._execute_entry(bracket)

    assert len(executor.orders) == 1
    order, max_price = executor.orders[0]
    assert order.price == 88 and max_price == 90
    assert order.to_kalshi_payload(max_price)["price"] == "0.9000"


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
    assert payload["price"] == "0.9000"  # marketable, never above the ceiling
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
    assert held.hedge_market_ticker == SIBLING, "partial fill must retain its target"
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
    assert held.hedge_market_ticker == SIBLING


# ---------------------------------------------------------------------------
# 4. Per-event aggregate exposure cap
# ---------------------------------------------------------------------------

def test_event_key_and_default_caps():
    assert event_key(TICKER) == "KXLOWTLAX-26JUL30"
    assert event_key(SIBLING) == event_key(TICKER)
    assert event_key(OTHER_EVENT) != event_key(TICKER)
    cfg = make_config(initial_contract_count=2, hedge_max_factor=3, spread_monitor_price=90)
    assert resolve_event_caps(cfg) == (None, None)
    cfg.event_max_contracts, cfg.event_max_cost_cents = 5, -1
    assert resolve_event_caps(cfg) == (5, None)


@pytest.mark.parametrize("value", [0, None, -1])
def test_event_caps_nonpositive_values_disable_caps(value):
    assert resolve_event_caps(SimpleNamespace(
        event_max_contracts=value, event_max_cost_cents=value,
        initial_contract_count=6, hedge_max_factor=1, spread_monitor_price=90,
    )) == (None, None)
    assert resolve_event_caps(SimpleNamespace()) == (None, None)


@pytest.mark.parametrize("contracts, cost, expected", [
    (4, 0, (4, None)), (0, 200, (None, 200)), (4, 200, (4, 200)),
])
def test_event_caps_positive_values_are_independent(contracts, cost, expected):
    assert resolve_event_caps(SimpleNamespace(
        event_max_contracts=contracts, event_max_cost_cents=cost,
    )) == expected


@pytest.fixture
def exposure_today(monkeypatch):
    class _Clock(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 30, 12, tzinfo=datetime.timezone.utc).astimezone(tz)

    monkeypatch.setattr(event_exposure_mod, "datetime", SimpleNamespace(datetime=_Clock))


def _pos(ticker, qty, price=80):
    return PositionModel(market_ticker=ticker, side="yes", quantity=qty, avg_entry_price=price)


@pytest.mark.asyncio
async def test_event_exposure_counts_sibling_brackets_from_db(monkeypatch, exposure_today):
    logged = _capture(monkeypatch, event_exposure_mod.logger)
    cfg = make_config(event_max_contracts=4)
    db = InMemoryDB([_pos(SIBLING, 3), _pos(OTHER_EVENT, 4)])

    assert await event_exposure_allows_buy(cfg, db, TICKER, 1, 85) is True
    assert await event_exposure_allows_buy(cfg, db, TICKER, 2, 85) is False
    block = next(kw for ev, kw in logged if ev == "entry.event_exposure_cap_blocked")
    assert block["existing_qty"] == 3  # other event's position is NOT counted
    assert block["reason"] == "contracts"


@pytest.mark.asyncio
async def test_event_exposure_cost_cap_and_inflight(exposure_today):
    cfg = make_config(initial_contract_count=2, hedge_max_factor=2)
    cfg.event_max_contracts = 100
    cfg.event_max_cost_cents = 200
    db = InMemoryDB([_pos(SIBLING, 1, price=80)])
    assert await event_exposure_allows_buy(cfg, db, TICKER, 1, 85) is True   # 170
    assert await event_exposure_allows_buy(cfg, db, TICKER, 2, 85) is False  # 260
    # In-flight (e.g. a working chaser) counts at its price.
    assert await event_exposure_allows_buy(
        cfg, db, TICKER, 1, 85, inflight={"KXLOWTLAX-26JUL30-B64.5": (1, 90)}
    ) is False  # 80 + 90 + 90 = 260


@pytest.mark.asyncio
@pytest.mark.parametrize("crossing", [None, True, False])
async def test_event_exposure_cost_reserves_effective_submission_limit(
    monkeypatch, exposure_today, crossing,
):
    cfg = SimpleNamespace(event_max_cost_cents=500, spread_monitor_price=96)
    if crossing is not None:
        cfg.entry_cross_spread_to_ceiling = crossing
    logged = _capture(monkeypatch, event_exposure_mod.logger)
    allows = await event_exposure_allows_buy(cfg, InMemoryDB(), TICKER, 6, 80)
    assert allows is (crossing is False)
    if crossing is not False:
        block = next(kw for ev, kw in logged if ev == "entry.event_exposure_cap_blocked")
        assert block["proposed_cost_cents"] == 576
        assert block["total_cost_cents"] == 576
        assert block["reason"] == "cost"


@pytest.mark.asyncio
async def test_event_exposure_cost_preserves_stored_cost_basis(exposure_today):
    cfg = SimpleNamespace(event_max_cost_cents=576, spread_monitor_price=96)
    db = InMemoryDB([_pos(SIBLING, 6, price=80)])
    assert await event_exposure_allows_buy(cfg, db, TICKER, 1, 80)
    assert db.store[PositionModel][0].avg_entry_price == 80


@pytest.mark.asyncio
@pytest.mark.parametrize("crossing", [True, False])
async def test_scanner_event_exposure_reserves_submission_limit(
    monkeypatch, exposure_today, crossing,
):
    cfg = make_config(initial_contract_count=6, hedge_max_factor=1,
                      spread_monitor_price=96, buy_trigger_price_low=80,
                      event_max_cost_cents=500, entry_cross_spread_to_ceiling=crossing)
    bought = []

    async def fake_fetch(*_args):
        return [TICKER], {TICKER: {"best_ask": 80, "best_bid": 79, "spread": 1}}

    async def fake_buy(_config, ticker, ask, _client):
        bought.append((ticker, ask))
        return True

    monkeypatch.setattr(scanner_module, "_fetch_markets_via_rest", fake_fetch)
    monkeypatch.setattr(scanner_module, "buy_market", fake_buy)
    await scanner_module.run_scan_cycle(cfg, _scanner_fake_db())
    assert bought == ([] if crossing else [(TICKER, 80)])


@pytest.mark.asyncio
@pytest.mark.parametrize("crossing", [True, False])
async def test_monitor_event_exposure_reserves_submission_limit(exposure_today, crossing):
    cfg = make_config(initial_contract_count=6, hedge_max_factor=1,
                      spread_monitor_price=96, event_max_cost_cents=500,
                      entry_cross_spread_to_ceiling=crossing)
    executor = FakeExecutor()
    executor.buy_success = True
    result = await mon._buy_hedge(
        TICKER, 80, 6, cfg, executor=executor, db=InMemoryDB(),
    )
    assert len(executor.orders) == (0 if crossing else 1)
    if crossing:
        assert result is False
    else:
        assert result.success


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_closed", [None, False, True])
@pytest.mark.parametrize("failure_stage", ["session", "execute"])
async def test_event_exposure_db_error_policy(monkeypatch, exposure_today,
                                            fail_closed, failure_stage):
    logged = []
    monkeypatch.setattr(event_exposure_mod.logger, "critical",
                        lambda event, **kw: logged.append((event, kw)))

    class _BrokenDB:
        async def get_session(self):
            if failure_stage == "session":
                raise RuntimeError("db down")
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def execute(self, *_args):
            raise RuntimeError("db down")

    cfg = SimpleNamespace(event_max_contracts=4)
    if fail_closed is not None:
        cfg.event_exposure_fail_closed = fail_closed
    assert await event_exposure_allows_buy(cfg, _BrokenDB(), TICKER, 1, 85) is not bool(fail_closed)
    assert logged == [("entry.event_exposure_unverifiable", {
        "ticker": TICKER, "event_ticker": event_key(TICKER), "source": "",
        "error": "db down",
        "action": "event_exposure_cap_blocked" if fail_closed else "event_exposure_cap_bypassed",
    })]


@pytest.mark.asyncio
async def test_event_exposure_disabled_does_not_query_db():
    class _UnexpectedDB:
        async def get_session(self):
            pytest.fail("disabled caps must not query the DB")

    assert await event_exposure_allows_buy(
        SimpleNamespace(event_exposure_fail_closed=True), _UnexpectedDB(), TICKER, 6, 85
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("date_prefix, counted", [
    ("26JUL29", False), ("26JUL30", True), ("26JUL31", True), ("UNKNOWN", True),
    ("26JUL32", True), ("26JUL3", True),
])
async def test_event_exposure_excludes_only_expired_positive_positions(
    exposure_today, date_prefix, counted,
):
    ticker = f"KXLOWTLAX-{date_prefix}-B60.5"
    sibling = f"KXLOWTLAX-{date_prefix}-B62.5"
    db = InMemoryDB([_pos(sibling, 4), _pos(ticker, 0)])
    cfg = SimpleNamespace(event_max_contracts=4)
    assert await event_exposure_allows_buy(cfg, db, ticker, 1, 85) is not counted
    assert await event_exposure_allows_buy(
        cfg, InMemoryDB(), ticker, 1, 85,
        extra_holdings={sibling: (4, 80)}, inflight={ticker: (4, 80)},
        target_existing_qty=4,
    ) is False
    assert await event_exposure_mod.load_event_holdings(db, ticker, 90) == (
        {sibling: (4, 80)} if counted else {}
    )


@pytest.mark.asyncio
async def test_event_exposure_zeroed_settled_positions_do_not_count(exposure_today):
    cfg = SimpleNamespace(event_max_contracts=4)
    db = InMemoryDB([_pos(SIBLING, 0)])
    assert await event_exposure_allows_buy(cfg, db, TICKER, 4, 85)


@pytest.mark.asyncio
@pytest.mark.parametrize("series, counted", [
    ("KXLOWTLAX", True), ("KXLOWTSEA", True), ("KXLOWTPHX", True),
    ("KXLOWTATL", False), ("UNKNOWN", False),
])
async def test_event_exposure_city_local_date_at_eastern_rollover(
    monkeypatch, series, counted,
):
    class _Clock(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            # July 31 Eastern, but July 30 21:30 Pacific and Phoenix.
            return cls(2026, 7, 31, 4, 30, tzinfo=datetime.timezone.utc).astimezone(tz)

    monkeypatch.setattr(event_exposure_mod, "datetime", SimpleNamespace(datetime=_Clock))
    ticker = f"{series}-26JUL30-B60.5"
    sibling = f"{series}-26JUL30-B62.5"
    cfg = SimpleNamespace(event_max_contracts=4)
    db = InMemoryDB([_pos(sibling, 4)])
    assert await event_exposure_allows_buy(cfg, db, ticker, 1, 85) is not counted


@pytest.mark.asyncio
@pytest.mark.parametrize("ticker", ["KXLOWTLAX-26JUL29-B60.5", "GENERIC-26JUL29-B60.5"])
@pytest.mark.parametrize("caps", [
    {"event_max_contracts": 4}, {"event_max_cost_cents": 400},
])
async def test_event_exposure_expired_holdings_never_bypass_proposed_caps(
    exposure_today, ticker, caps,
):
    assert not await event_exposure_allows_buy(
        SimpleNamespace(**caps), InMemoryDB([_pos(ticker, 10)]), ticker, 5, 85
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("known_source", ["target", "memory", "inflight"])
@pytest.mark.parametrize("caps", [
    {"event_max_contracts": 4}, {"event_max_cost_cents": 400},
])
async def test_event_exposure_expired_db_rows_do_not_hide_known_live_risk(
    exposure_today, known_source, caps,
):
    ticker = "KXLOWTLAX-26JUL29-B60.5"
    sibling = "KXLOWTLAX-26JUL29-B62.5"
    cfg = SimpleNamespace(spread_monitor_price=90, **caps)
    db = InMemoryDB([_pos(sibling, 100)])
    assert await event_exposure_allows_buy(cfg, db, ticker, 1, 80)
    known = {
        "target": {"target_existing_qty": 4},
        "memory": {"extra_holdings": {sibling: (4, 80)}},
        "inflight": {"inflight": {sibling: (4, 80)}},
    }[known_source]
    assert not await event_exposure_allows_buy(cfg, db, ticker, 1, 80, **known)


@pytest.mark.asyncio
async def test_state_machine_two_same_event_six_contract_entries_with_caps_unset(
    monkeypatch, exposure_today,
):
    executor = FakeExecutor()
    executor.buy_success = True
    db = InMemoryDB()
    strategy = make_strategy(monkeypatch, executor=executor, db=db,
                             initial_contract_count=6, hedge_max_factor=1)
    monkeypatch.setattr(strategy, "_fetch_market_data_via_rest",
                        AsyncMock(return_value={"yes_ask": 85}))
    ob = OrderBook(yes_asks=[OrderBookLevel(price=85, quantity=20, order_count=1)])
    for ticker in (TICKER, SIBLING):
        strategy._entry_step_seen = set()
        bracket = MarketBracket(ticker, event_key(ticker), "KXLOWTLAX", "b",
                                phase=Phase.ENTERING)
        await strategy._execute_entry(bracket, ob=ob)
        assert bracket.phase == Phase.HOLDING
        assert bracket.position_quantity == 6
    assert [order.quantity for order, _ in executor.orders] == [6, 6]
    assert sum(pos.quantity for pos in db.store[PositionModel]) == 12


@pytest.mark.asyncio
async def test_state_machine_entry_blocked_by_event_cap_across_cycles(monkeypatch, exposure_today):
    """A sibling bracket held from an earlier cycle (DB) blocks a new entry,
    even though the per-cycle duplicate-entry set is reset every sweep."""
    executor = FakeExecutor()
    executor.buy_success = True
    db = InMemoryDB([_pos(SIBLING, 4)])
    strategy = make_strategy(monkeypatch, executor=executor, db=db,
                             initial_contract_count=2, hedge_max_factor=2,
                             event_max_contracts=4)
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
async def test_state_machine_entry_counts_in_memory_positions(monkeypatch, exposure_today):
    executor = FakeExecutor()
    executor.buy_success = True
    strategy = make_strategy(monkeypatch, executor=executor,
                             initial_contract_count=2, hedge_max_factor=2,
                             event_max_contracts=4)
    held = MarketBracket(SIBLING, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "h",
                         phase=Phase.HOLDING, position_quantity=3, avg_entry=80)
    strategy.active_positions[SIBLING] = held
    bracket = MarketBracket(TICKER, "KXLOWTLAX-26JUL30", "KXLOWTLAX", "b", phase=Phase.ENTERING)

    await strategy._execute_entry(
        bracket, ob=OrderBook(yes_asks=[OrderBookLevel(price=85, quantity=10, order_count=1)])
    )

    assert executor.orders == []


# ---------------------------------------------------------------------------
# 5. Strict position cap is opt-in
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_position_cap_fails_closed_on_lookup_error(monkeypatch):
    logged = _capture(monkeypatch, live_mod.logger)
    client = _RecordingClient(post_payload=_filled_payload(1, 85),
                              get_exc=RuntimeError("positions API down"))
    ex = _make_live_executor(monkeypatch, client, max_buy_qty=8)
    ex.position_cap_fail_closed = True

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
    ex.position_cap_fail_closed = True

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
