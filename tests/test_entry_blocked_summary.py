"""Entry-cycle observability and submission-boundary quote regressions."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import core.state_machine as state_machine
from core.types import MarketBracket, OrderBook, OrderBookLevel, Phase
from execution.base import ExecutionResult
from tests.test_state_machine import capture_logs, make_strategy


def bracket_for(ticker="KXHIGHUNKNOWN-26OCT04-B80.5", **kwargs):
    return MarketBracket(ticker, "EVT1", ticker.split("-")[0], "entry", **kwargs)


def ask_book(price):
    return OrderBook(yes_asks=[OrderBookLevel(price, 10, 1)])


def summaries(logged):
    return [fields for event, fields in logged if event == "entry.blocked_summary"]


@pytest.mark.asyncio
async def test_empty_and_failed_cycles_always_emit_summary(monkeypatch):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    await strategy._evaluate_watchlist()
    assert summaries(logged)[0]["counts_by_reason"] == {}
    assert summaries(logged)[0]["cycle_completed"] is True

    async def broken():
        strategy._entry_cycle_ticker = "KXHIGHUNKNOWN-26OCT04-B80.5"
        raise RuntimeError("entry failed")

    strategy._evaluate_watchlist_cycle = broken
    with pytest.raises(RuntimeError):
        await strategy._evaluate_watchlist()
    assert summaries(logged)[1]["counts_by_reason"] == {"entry_error": 1}
    assert summaries(logged)[1]["cycle_completed"] is False
    assert strategy._entry_cycle_blocks is None


@pytest.mark.asyncio
async def test_cancelled_cycle_emits_summary(monkeypatch):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    strategy._evaluate_watchlist_cycle = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await strategy._evaluate_watchlist()
    assert summaries(logged)[0]["cycle_completed"] is False


@pytest.mark.asyncio
async def test_summary_counts_ledger_blocks_per_cycle_not_per_log(monkeypatch):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    ticker = bracket_for().market_ticker

    async def cycle():
        strategy._record_gate(ticker, "spread", "BLOCKED")
        strategy._record_gate(ticker, "spread", "BLOCKED")
        strategy._record_gate("other-pass", "price_trigger", "PASS")
        strategy._record_gate("other-skip", "entry_eligibility", "SKIPPED")
        strategy._record_gate("other-held", "hedge_cap", "BLOCKED", reason="already_holding_app_owned_qty")

    strategy._evaluate_watchlist_cycle = cycle
    await strategy._evaluate_watchlist()
    await strategy._evaluate_watchlist()
    assert [item["counts_by_reason"] for item in summaries(logged)] == [{"spread": 1}] * 2
    assert summaries(logged)[0]["total_blocks"] == 1
    assert summaries(logged)[0]["blocked_ticker_count"] == 1
    assert len([1 for event, fields in logged if event == "phase.b.decision" and fields["gate"] == "spread"]) == 1


@pytest.mark.asyncio
async def test_am_low_counts_candidates_and_unique_cities_not_held_skips(monkeypatch):
    strategy = make_strategy(monkeypatch, am_low_forecast_keywords={"rain"}, entry_gate_mode="NWS_WINDOW")
    logged = capture_logs(monkeypatch)
    strategy._am_low_brief_gate.get_block = lambda *args, **kwargs: (True, {"rain"})
    for suffix, phase, crossed in (("B70.5", Phase.MONITORING, False),
                                    ("B72.5", Phase.MONITORING, False),
                                    ("B74.5", Phase.HOLDING, True)):
        bracket = bracket_for(f"KXLOWTSEA-26OCT04-{suffix}", phase=phase, crossed_buy=crossed)
        strategy.brackets[bracket.market_ticker] = bracket
        strategy.cache.update_quote(bracket.market_ticker, 83, 85)
    await strategy._evaluate_watchlist()
    summary = summaries(logged)[0]
    assert summary["counts_by_reason"] == {"am_low_keyword": 2}
    assert summary["am_low_blocked_city_count"] == 1
    assert summary["blocked_ticker_count"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind, expected", [
    ("missing_quote", {"no_price": 1}),
    ("missing_spread", {"no_spread": 1}),
    ("invalid_book", {"settled_one_sided_book": 1}),
    ("unknown_family", {"unknown_family": 1}),
    ("below_floor", {}),
    ("held_missing_quote", {}),
    ("held_quote", {}),
])
async def test_candidate_feed_skips_count_but_normal_floor_and_held_skips_do_not(monkeypatch, kind, expected):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    bracket = bracket_for("OTHER-26OCT04-B80.5" if kind == "unknown_family" else bracket_for().market_ticker)
    if kind.startswith("held"):
        bracket.phase = Phase.HOLDING
        bracket.crossed_buy = True
    strategy.brackets[bracket.market_ticker] = bracket
    rest_data = {"yes_ask": 85} if kind == "missing_spread" else None
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=rest_data)
    if kind in {"unknown_family", "held_quote"}:
        strategy.cache.update_quote(bracket.market_ticker, 83, 85)
    elif kind == "invalid_book":
        strategy.cache.update_quote(bracket.market_ticker, 0, 99)
    elif kind == "below_floor":
        strategy.cache.update_quote(bracket.market_ticker, 0, 1)
    await strategy._evaluate_watchlist()
    assert summaries(logged)[0]["counts_by_reason"] == expected
    assert strategy.executor.orders == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fresh", [{"yes_ask": 88}, {"no_bid": 12}])
async def test_submission_refreshes_supplied_book_and_uses_best_ask(monkeypatch, fresh):
    strategy = make_strategy(monkeypatch)
    bracket = bracket_for(position_quantity=1)
    strategy.executor.positions[bracket.market_ticker] = {"count": 3}
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=fresh)
    await strategy._execute_entry(bracket, ob=ask_book(83))
    strategy._fetch_market_data_via_rest.assert_awaited_once_with(bracket.market_ticker)
    order, ceiling = strategy.executor.orders[0]
    assert order.price == 88
    assert ceiling == 90
    assert order.known_position_qty == 3


@pytest.mark.asyncio
async def test_fresh_ceiling_block_preserves_crossed_and_retries_pending_entry(monkeypatch):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    bracket = bracket_for()
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 84, 85)
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 95})
    await strategy._evaluate_watchlist()
    assert strategy.executor.orders == []
    assert bracket.crossed_buy is True
    assert bracket.pending_entry is True
    assert bracket.phase == Phase.MONITORING
    assert summaries(logged)[0]["counts_by_reason"] == {"price_ceiling": 1}
    assert any(event == "phase.b.decision" and fields["gate"] == "price_ceiling"
               and fields["verdict"] == "BLOCKED" and fields["reason"] == "submission_ask_above_ceiling"
               for event, fields in logged)

    strategy._fetch_market_data_via_rest.return_value = {"yes_ask": 88}
    await strategy._evaluate_watchlist()
    assert len(strategy.executor.orders) == 1
    assert strategy.executor.orders[0][0].price == 88
    assert bracket.crossed_buy is True
    assert bracket.falling_knife_guard is False


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["no_fill", "event_exposure", "execution_error", "rejected"])
async def test_unfilled_or_exposure_blocked_entry_remains_retryable(monkeypatch, kind):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    bracket = bracket_for()
    strategy.brackets[bracket.market_ticker] = bracket
    strategy.cache.update_quote(bracket.market_ticker, 83, 85)
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 85})
    if kind == "event_exposure":
        monkeypatch.setattr(
            state_machine, "event_exposure_allows_buy",
            AsyncMock(side_effect=[False, True]),
        )
    elif kind in {"no_fill", "execution_error"}:
        original_buy = strategy.executor.buy_yes
        first = True

        async def first_failure(order, max_price=None):
            nonlocal first
            if first:
                first = False
                if kind == "execution_error":
                    raise RuntimeError("exchange unavailable")
                return ExecutionResult(
                    False, order.market_ticker, "yes", order.price, order.quantity,
                    0, 0, 0, status="NO_FILL",
                )
            return await original_buy(order, max_price)

        strategy.executor.buy_yes = first_failure
    if kind == "execution_error":
        with pytest.raises(RuntimeError):
            await strategy._evaluate_watchlist()
    else:
        await strategy._evaluate_watchlist()
    assert bracket.crossed_buy is True
    assert bracket.pending_entry is True
    assert bracket.phase == Phase.MONITORING
    await strategy._evaluate_watchlist()
    assert summaries(logged)[1]["counts_by_reason"] == {"execution_rejected": 1}
    assert len(strategy.executor.orders) == (2 if kind == "rejected" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_price, expected_orders", [(86, 1), (95, 0), (None, 0)])
async def test_missing_fresh_ask_only_allows_bounded_actual_cached_ask(monkeypatch, cached_price, expected_orders):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    bracket = bracket_for()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"price": 84})
    # A last trade is not an ask, and must not become a trigger-price order.
    strategy.cache.get_last_price = lambda ticker: 84
    await strategy._execute_entry(bracket, ob=ask_book(cached_price) if cached_price else None)
    assert len(strategy.executor.orders) == expected_orders
    if expected_orders:
        order, ceiling = strategy.executor.orders[0]
        assert order.price == cached_price <= ceiling
        assert any(event == "phase.b.decision" and fields.get("reason") == "cached_ask_bounded_limit"
                   for event, fields in logged)
    else:
        assert bracket.pending_entry is True


@pytest.mark.asyncio
async def test_missing_rest_quote_uses_no_derived_cached_best_ask(monkeypatch):
    strategy = make_strategy(monkeypatch)
    bracket = bracket_for()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value=None)
    await strategy._execute_entry(
        bracket, ob=OrderBook(no_bids=[OrderBookLevel(14, 10, 1)]),
    )
    assert strategy.executor.orders[0][0].price == 86


@pytest.mark.asyncio
@pytest.mark.parametrize("market, expected", [
    ({"yes_ask_dollars": "0.88", "yes_ask": 83, "yes_bid_dollars": "0.84"}, 88),
    ({"yes_ask": 88, "yes_bid": 84}, 88),
    ({"no_bid_dollars": "0.12"}, 88),
    ({"no_bid": 12}, 88),
])
async def test_rest_submission_quotes_parse_dollars_legacy_cents_and_no_bids(monkeypatch, market, expected):
    strategy = make_strategy(monkeypatch)
    monkeypatch.setattr("app.signing.build_auth_headers", lambda *args: {})
    response = SimpleNamespace(status_code=200, json=lambda: {"market": market})
    strategy._http_client = SimpleNamespace(is_closed=False, get=AsyncMock(return_value=response))
    quote = await strategy._fetch_market_data_via_rest(bracket_for().market_ticker)
    assert quote["yes_ask"] == expected


@pytest.mark.asyncio
async def test_position_lookup_failure_carries_known_quantity_without_false_block(monkeypatch):
    strategy = make_strategy(monkeypatch)
    bracket = bracket_for(position_quantity=3)
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 85})
    strategy.executor.get_positions = AsyncMock(side_effect=RuntimeError("lookup unavailable"))
    await strategy._execute_entry(bracket)
    assert strategy.executor.orders[0][0].known_position_qty == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("status, notes, gate", [
    ("NO_FILL", "", "no_fill_ioc"),
    ("FILLED", '{"time_in_force":"immediate_or_cancel","fill_count":0}', "no_fill_ioc"),
    ("REJECTED", "position_cap_unverifiable: lookup failed", "position_lookup_error"),
    ("REJECTED", "position_cap_blocked", "position_cap"),
    ("REJECTED", "price_above_ceiling", "price_ceiling"),
    ("REJECTED", "exchange refused", "execution_rejected"),
])
async def test_executor_blocks_are_in_cycle_summary(monkeypatch, status, notes, gate):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    bracket = bracket_for()
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 85})
    strategy.executor.buy_yes = AsyncMock(return_value=ExecutionResult(
        False, bracket.market_ticker, "yes", 85, 2, 0, 0, 0, status=status, notes=notes,
    ))
    strategy._evaluate_watchlist_cycle = lambda: strategy._execute_entry(bracket)
    await strategy._evaluate_watchlist()
    assert summaries(logged)[0]["counts_by_reason"] == {gate: 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind, gate", [
    ("quantity_cap", "position_cap"),
    ("total_cap", "position_cap"),
    ("event_exposure", "event_exposure"),
    ("executor_error", "execution_error"),
    ("final_gate", "nws_temp_window_final"),
    ("final_gate_error", "nws_temp_window_final"),
    ("sunrise_final", "sunrise_final"),
])
async def test_submission_guards_record_actual_blocks(monkeypatch, kind, gate):
    strategy = make_strategy(monkeypatch)
    logged = capture_logs(monkeypatch)
    bracket = bracket_for("KXHIGHTSEA-26OCT04-B80.5")
    strategy._fetch_market_data_via_rest = AsyncMock(return_value={"yes_ask": 85})
    quantity = 9 if kind == "quantity_cap" else 2
    if kind == "total_cap":
        strategy.executor.positions[bracket.market_ticker] = {"count": 7}
    elif kind == "event_exposure":
        monkeypatch.setattr(state_machine, "event_exposure_allows_buy", AsyncMock(return_value=False))
    elif kind == "executor_error":
        strategy.executor.buy_yes = AsyncMock(side_effect=RuntimeError("exchange failed"))
    elif kind == "final_gate":
        monkeypatch.setattr("nws.gate.is_trading_gate_open", lambda *args: False)
    elif kind == "final_gate_error":
        def fail(*args):
            raise RuntimeError("forecast lookup failed")
        monkeypatch.setattr("nws.gate.is_trading_gate_open", fail)
    elif kind == "sunrise_final":
        bracket = bracket_for("KXLOWTSEA-26OCT04-B80.5")
        strategy.config.entry_gate_mode = "SUNRISE"
        strategy._sunrise_entry_gate.evaluate = lambda **kwargs: SimpleNamespace(allowed=False)
    strategy._evaluate_watchlist_cycle = lambda: strategy._execute_entry(bracket, quantity=quantity)
    if kind == "executor_error":
        with pytest.raises(RuntimeError):
            await strategy._evaluate_watchlist()
    else:
        await strategy._evaluate_watchlist()
    assert summaries(logged)[0]["counts_by_reason"] == {gate: 1}
    assert strategy.executor.orders == []
