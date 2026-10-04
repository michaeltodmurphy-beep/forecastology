"""Scanner fill persistence and monitor aggregate hedge-position caps."""

from unittest.mock import AsyncMock

import pytest

import monitor
import scanner
from app.models import ExecutedTrade, Position, TradeStatus
from execution.base import ExecutionResult
from tests.test_audit_high_fixes import _HedgeExecutor, _setup_monitor, SIBLING, TICKER
from tests.test_state_machine import InMemoryDB, make_config


class ScannerDB(InMemoryDB):
    async def get_session(self):
        context = await super().get_session()
        original_enter = context.__aenter__

        class Context:
            async def __aenter__(self):
                session = await original_enter()
                original_execute = session.execute

                async def execute(statement):
                    result = await original_execute(statement)
                    result.fetchall = lambda: [(p.market_ticker,) for p in result.all()]
                    return result

                session.execute = execute
                return session

            async def __aexit__(self, *args):
                return await context.__aexit__(*args)

        return Context()


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity,success", [(2, True), (0, True), (2, False), (0, False)])
async def test_scanner_persists_only_actual_fills(monkeypatch, quantity, success):
    config = make_config(initial_contract_count=4, spread_monitor_price=96)
    db = ScannerDB()
    result = ExecutionResult(
        success, TICKER, "yes", 85, 4, 94 if quantity else 0, quantity,
        187 if quantity else 0, order_id="actual-fill", status="PARTIAL",
    )
    executor = type("Executor", (), {
        "get_positions": AsyncMock(return_value={}),
        "buy_yes": AsyncMock(return_value=result),
    })()
    monkeypatch.setattr(scanner, "create_executor", lambda **kwargs: executor)
    monkeypatch.setattr(scanner, "_fetch_markets_via_rest", AsyncMock(return_value=(
        [TICKER], {TICKER: {"best_ask": 85, "best_bid": 84, "spread": 1}},
    )))
    monkeypatch.setattr(scanner, "get_max_spread_for_entry", lambda *_: (2, "test"))
    await scanner.run_scan_cycle(config, db)
    if quantity == 0:
        assert not db.store[Position]
        assert not db.store[ExecutedTrade]
        return
    position, = db.store[Position]
    trade, = db.store[ExecutedTrade]
    assert (position.quantity, position.avg_entry_price, position.last_price) == (2, 94, 94)
    assert (trade.quantity, trade.price, trade.total_cost_cents) == (2, 94, 187)
    assert trade.status == TradeStatus.PARTIAL
    assert trade.kalshi_order_id == "actual-fill"


def parent(ticker, quantity=4, hedged=0, target=None):
    return Position(
        market_ticker=ticker, side="yes", quantity=quantity, avg_entry_price=80,
        hedge_quantity=hedged, hedge_market_ticker=target,
    )


def trigger_all_parents(monkeypatch):
    async def price(ticker, *_args, **_kwargs):
        ask = 60 if ticker == SIBLING else 40
        return {"last_price": ask, "yes_ask": ask, "yes_bid": ask - 2}

    monkeypatch.setattr(monitor, "_get_market_price_rest", price)


@pytest.mark.asyncio
@pytest.mark.parametrize("standalone", [0, 5])
async def test_monitor_counts_parent_hedges_without_double_counting(monkeypatch, standalone):
    executor = _HedgeExecutor([3])
    db, config, held = _setup_monitor(monkeypatch, executor)
    held.quantity = 3
    db.store[Position].extend([
        parent("KXLOWTLAX-26JUL30-B55", quantity=2, hedged=2, target=SIBLING),
        parent("KXLOWTLAX-26JUL30-B57", quantity=3, hedged=3, target=SIBLING),
    ])
    if standalone:
        db.store[Position].append(parent(SIBLING, quantity=standalone))
    await monitor.run_monitor_cycle(config, db)
    order, _ = executor.orders[0]
    assert len(executor.orders) == 1
    assert order.known_position_qty == 5
    assert held.hedge_quantity == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("first_fill", [2, 3])
async def test_monitor_counts_same_cycle_fills_even_partial(monkeypatch, first_fill):
    executor = _HedgeExecutor([first_fill, 3])
    db, config, held = _setup_monitor(monkeypatch, executor)
    trigger_all_parents(monkeypatch)
    held.quantity = 3
    second = parent("KXLOWTLAX-26JUL30-B55", quantity=6)
    db.store[Position].append(second)
    await monitor.run_monitor_cycle(config, db)
    # Cap = 8: 2 + 6 fits exactly, whereas 3 + 6 must be blocked.
    assert len(executor.orders) == (2 if first_fill == 2 else 1)
    if first_fill == 2:
        assert executor.orders[1][0].known_position_qty == 2
        assert second.hedge_quantity == 3
    else:
        assert second.hedge_quantity == 0
    assert held.hedge_quantity == first_fill
    assert held.hedge_market_ticker == SIBLING


@pytest.mark.asyncio
async def test_monitor_partial_retry_keeps_target_and_true_quantity(monkeypatch):
    executor = _HedgeExecutor([1, 3])
    db, config, held = _setup_monitor(monkeypatch, executor)
    await monitor.run_monitor_cycle(config, db)
    monkeypatch.setattr(monitor, "_find_hedge_bracket", AsyncMock(return_value="wrong-target"))
    await monitor.run_monitor_cycle(config, db)
    order, _ = executor.orders[1]
    assert (order.market_ticker, order.quantity, order.known_position_qty) == (SIBLING, 3, 1)
    assert held.hedge_quantity == 4
    assert sum(trade.quantity for trade in db.store[ExecutedTrade]) == 4


@pytest.mark.asyncio
async def test_monitor_includes_legacy_partial_parent_quantities(monkeypatch):
    executor = _HedgeExecutor([4])
    db, config, held = _setup_monitor(monkeypatch, executor)
    trigger_all_parents(monkeypatch)
    legacy = parent("KXLOWTLAX-26JUL30-B55", quantity=6, hedged=5)
    db.store[Position].append(legacy)
    await monitor.run_monitor_cycle(config, db)
    # The first requested 4 contracts is blocked by the legacy parent's 5.
    assert len(executor.orders) == 1
    order, _ = executor.orders[0]
    assert (order.quantity, order.known_position_qty) == (1, 5)
    assert not held.hedge_quantity
    assert legacy.hedge_quantity == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("event_cap,expected_orders", [(9, 1), (0, 2)])
async def test_monitor_event_cap_includes_same_cycle_different_hedge_targets(
    monkeypatch, event_cap, expected_orders,
):
    import core.event_exposure as exposure

    monkeypatch.setattr(exposure, "_event_is_expired", lambda _: False)
    executor = _HedgeExecutor([3, 3])
    db, config, held = _setup_monitor(monkeypatch, executor)
    held.quantity = 3
    second = parent("KXLOWTLAX-26JUL30-B55", quantity=3)
    db.store[Position].append(second)
    config.event_max_contracts = event_cap
    targets = iter([SIBLING, "KXLOWTLAX-26JUL30-B58"])

    async def find(*_args):
        return next(targets)

    monkeypatch.setattr(monitor, "_find_hedge_bracket", find)
    trigger_all_parents(monkeypatch)
    await monitor.run_monitor_cycle(config, db)
    # Six primary contracts plus the first hedge of three exhaust a cap of nine.
    assert len(executor.orders) == expected_orders
    assert held.hedge_quantity == 3
    assert second.hedge_quantity == (3 if event_cap == 0 else 0)


@pytest.mark.asyncio
async def test_monitor_event_cost_includes_parent_only_hedges(monkeypatch):
    import core.event_exposure as exposure

    monkeypatch.setattr(exposure, "_event_is_expired", lambda _: False)
    executor = _HedgeExecutor([3])
    db, config, held = _setup_monitor(monkeypatch, executor)
    held.quantity = 3
    db.store[Position].append(parent(
        "KXLOWTLAX-26JUL30-B55", quantity=2, hedged=2,
        target="KXLOWTLAX-26JUL30-B58",
    ))
    # Primary cost 5 * 80 = 400; proposed 3 * 90 = 270 would fit alone.
    # Parent-only hedge cost (unknown) reserves 2 * ceiling 90 = 180.
    config.event_max_cost_cents = 700
    config.spread_monitor_price = 90
    await monitor.run_monitor_cycle(config, db)
    assert not executor.orders
    assert not held.hedge_quantity


@pytest.mark.asyncio
async def test_direct_monitor_buy_merges_known_target_without_overriding_db(monkeypatch):
    import core.event_exposure as exposure

    monkeypatch.setattr(exposure, "_event_is_expired", lambda _: False)
    config = make_config(event_max_contracts=3)
    executor = _HedgeExecutor([1])
    db = InMemoryDB()
    result = await monitor._buy_hedge(
        SIBLING, 60, 1, config, executor=executor, db=db, existing_position_qty=3,
    )
    assert result is False
    assert not executor.orders
    db.store[Position].append(parent(SIBLING, quantity=3))
    result = await monitor._buy_hedge(
        SIBLING, 60, 1, config, executor=executor, db=db, existing_position_qty=0,
    )
    assert result is False
    assert not executor.orders


@pytest.mark.asyncio
async def test_monitor_event_cost_preserves_standalone_actual_basis(monkeypatch):
    import core.event_exposure as exposure

    monkeypatch.setattr(exposure, "_event_is_expired", lambda _: False)
    executor = _HedgeExecutor([3])
    db, config, held = _setup_monitor(monkeypatch, executor)
    held.quantity = 3
    standalone = parent(SIBLING, quantity=2)
    standalone.avg_entry_price = 30
    db.store[Position].append(standalone)
    config.spread_monitor_price = 90
    # Actual existing cost 240 + 60; submission reservation 270 fits exactly.
    config.event_max_cost_cents = 570
    await monitor.run_monitor_cycle(config, db)
    assert len(executor.orders) == 1
    assert held.hedge_quantity == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("cost_cap,expected_orders", [(930, 2), (929, 1)])
async def test_monitor_event_cost_tracks_actual_same_cycle_fill_cost(
    monkeypatch, cost_cap, expected_orders,
):
    import core.event_exposure as exposure

    monkeypatch.setattr(exposure, "_event_is_expired", lambda _: False)
    executor = _HedgeExecutor([3, 3], fill_price=60)
    db, config, held = _setup_monitor(monkeypatch, executor)
    held.quantity = 3
    db.store[Position].append(parent("KXLOWTLAX-26JUL30-B55", quantity=3))
    config.event_max_cost_cents = cost_cap
    config.spread_monitor_price = 90
    targets = iter([SIBLING, "KXLOWTLAX-26JUL30-B58"])

    async def find(*_args):
        return next(targets)

    monkeypatch.setattr(monitor, "_find_hedge_bracket", find)
    trigger_all_parents(monkeypatch)
    await monitor.run_monitor_cycle(config, db)
    # Six primary contracts at 80, first actual hedge at 60, next limit at 90.
    assert len(executor.orders) == expected_orders
