"""Regression tests: AM_LOW_FORECAST daily-brief keyword gate is CONTINUOUS.

These tests prove the "Definition A" fix: the ``AM_LOW_FORECAST`` keyword gate
must be enforced on **every** KXLOW* add path regardless of whether a bracket
is a first-cross entry, a restored/``crossed_buy`` HOLDING bracket, a recovery
(re-hedge) buy, or a partial-fill chaser top-up.  Existing held quantity is
left untouched; only *new* buys are suppressed.

Motivating production incident
------------------------------
``KXLOWTSATX-26SEP18-B76.5`` was held (15 shares, adopted via
``periodic_reconciliation`` with ``crossed_buy=True``) on a day whose NWS daily
brief contained "thunderstorms".  The old gate lived *inside* the
``should_evaluate_entry`` block, so it was structurally skipped for restored /
crossed_buy brackets and never emitted an ``am_low_brief.evaluated`` line for
SAT.  These tests lock the fixed behaviour in place.
"""
import os
import sys

import pytest
from unittest.mock import AsyncMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.state_machine import TemperatureStrategy  # noqa: E402
from core.types import MarketBracket, Phase  # noqa: E402

from tests.test_state_machine import (  # noqa: E402
    FakeExecutor,
    InMemoryDB,
    capture_logs,
    make_strategy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _install_blocking_daily_brief(monkeypatch, strategy, *, matched=("thunderstorms",)):
    """Force the strategy's DailyBriefGate to report BLOCKED for any series.

    The production gate does a network fetch + DB snapshot; here we stub the
    gate object's ``get_block`` so the test is hermetic and deterministic.
    """
    matched = set(matched)

    def fake_get_block(series, now_utc=None):  # noqa: ANN001
        return True, set(matched)

    monkeypatch.setattr(strategy._am_low_brief_gate, "get_block", fake_get_block)


def _install_passing_daily_brief(monkeypatch, strategy):
    monkeypatch.setattr(
        strategy._am_low_brief_gate,
        "get_block",
        lambda series, now_utc=None: (False, set()),
    )


def _enable_keywords(strategy):
    # The config attribute is a set; the gate only runs when non-empty.
    strategy.config.am_low_forecast_keywords = {"thunderstorm", "thunderstorms"}


# ---------------------------------------------------------------------------
# Test 1: restored / crossed_buy bracket is blocked even though
#         should_evaluate_entry is False
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_am_low_keyword_blocks_restored_crossed_buy_bracket(monkeypatch):
    """A restored HOLDING bracket (crossed_buy=True, phase=HOLDING) must be
    keyword-gated — this is the exact KXLOWTSATX case.

    The continuous gate must fire even though ``should_evaluate_entry`` is
    False, emit ``phase.b.entry_blocked_am_low_forecast``, and place no order.
    """
    logged = capture_logs(monkeypatch)
    strategy = make_strategy(monkeypatch)
    _enable_keywords(strategy)
    _install_blocking_daily_brief(monkeypatch, strategy)

    ticker = "KXLOWTSATX-26SEP18-B76.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="restored",
        phase=Phase.HOLDING,
        crossed_buy=True,
        position_quantity=15,
    )
    strategy.brackets[ticker] = bracket
    strategy.active_positions[ticker] = bracket
    # Give it a tradeable quote so ONLY the keyword gate can stop it.
    strategy.cache.update_quote(ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    strategy._execute_entry.assert_not_awaited()
    blocked = [kw for ev, kw in logged if ev == "phase.b.entry_blocked_am_low_forecast"]
    assert blocked, "restored/crossed_buy bracket must emit am_low block log"
    assert blocked[-1]["ticker"] == ticker
    assert blocked[-1]["series_prefix"] == "KXLOWTSATX"
    assert blocked[-1]["reason"] == "continuous"
    assert blocked[-1]["held_qty"] == 15
    # Definition A: existing quantity is NOT touched.
    assert bracket.position_quantity == 15
    assert bracket.phase == Phase.HOLDING


# ---------------------------------------------------------------------------
# Test 2: the gate is evaluated even when should_evaluate_entry is False
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_am_low_keyword_evaluated_even_when_should_evaluate_entry_false(monkeypatch):
    """The DailyBriefGate.get_block must be CALLED for a crossed_buy bracket.

    Before the fix the gate lived inside the ``should_evaluate_entry`` block,
    so ``get_block`` was never called for restored brackets (which is why SAT
    produced no ``am_low_brief.evaluated`` line all day).  We assert the gate
    is now consulted regardless of phase/crossed_buy.
    """
    strategy = make_strategy(monkeypatch)
    _enable_keywords(strategy)

    calls = []

    def spy_get_block(series, now_utc=None):  # noqa: ANN001
        calls.append(series)
        return True, {"thunderstorms"}

    monkeypatch.setattr(strategy._am_low_brief_gate, "get_block", spy_get_block)

    ticker = "KXLOWTSATX-26SEP18-B76.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="restored",
        phase=Phase.HOLDING,
        crossed_buy=True,
        position_quantity=15,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert calls == ["KXLOWTSATX"], (
        "get_block must be consulted for a crossed_buy/HOLDING bracket "
        f"(got calls={calls})"
    )


# ---------------------------------------------------------------------------
# Test 3: recovery (re-hedge) add is blocked
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_am_low_keyword_blocks_recovery_add(monkeypatch):
    """A martingale recovery buy must be suppressed when the series is blocked.

    Setup: stop-loss count=1 for the series (so a recovery buy would normally
    be placed at 2x).  With the keyword gate blocking the series, no BUY order
    may be placed.
    """
    from app.models import StopLossLedger

    logged = capture_logs(monkeypatch)
    ticker = "KXLOWTSATX-26SEP18-T80"
    db = InMemoryDB([
        StopLossLedger(
            series_ticker="KXLOWTSATX",
            date_prefix="26SEP18",
            stop_loss_count=1,
        )
    ])
    strategy = make_strategy(monkeypatch, db=db)
    _enable_keywords(strategy)
    _install_blocking_daily_brief(monkeypatch, strategy)

    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="recovery",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)

    await strategy._evaluate_watchlist()

    assert strategy.executor.orders == [], (
        "a keyword-blocked series must not place a recovery buy"
    )
    assert any(
        ev == "phase.b.entry_blocked_am_low_forecast"
        for ev, _ in logged
    )


# ---------------------------------------------------------------------------
# Test 4: partial-fill chaser top-up is blocked
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_am_low_keyword_blocks_chaser_topup(monkeypatch):
    """_chase_entry_gate_open must return False for a keyword-blocked series.

    This covers BOTH the partial-fill chaser loop and
    ``_resume_chasers_for_underfilled_positions`` (startup resume), which share
    this helper.
    """
    strategy = make_strategy(monkeypatch)
    _enable_keywords(strategy)
    _install_blocking_daily_brief(monkeypatch, strategy)

    ticker = "KXLOWTSATX-26SEP18-B76.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="held",
        phase=Phase.HOLDING,
        crossed_buy=True,
        position_quantity=5,
    )

    gate_open = await strategy._chase_entry_gate_open(bracket)

    assert gate_open is False, (
        "chaser top-up must be blocked when the AM-low keyword gate matches"
    )


# ---------------------------------------------------------------------------
# Test 5: non-matching keyword does NOT block (guard against over-blocking)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_am_low_keyword_allows_entry_when_no_match(monkeypatch):
    """A clean brief (no keyword match) must let a normal entry proceed."""
    strategy = make_strategy(monkeypatch)
    _enable_keywords(strategy)
    _install_passing_daily_brief(monkeypatch, strategy)

    ticker = "KXLOWTSATX-26SEP18-B76.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    strategy._execute_entry.assert_awaited_once_with(bracket)


# ---------------------------------------------------------------------------
# Test 6: gate is inert when AM_LOW_FORECAST is unset (default config)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_am_low_keyword_inert_when_config_unset(monkeypatch):
    """With no keywords configured, get_block must not even be consulted and
    a normal entry proceeds (feature-disabled behaviour)."""
    strategy = make_strategy(monkeypatch)
    # Ensure the attribute is empty (default from make_config/env isolation).
    strategy.config.am_low_forecast_keywords = set()

    calls = []
    monkeypatch.setattr(
        strategy._am_low_brief_gate,
        "get_block",
        lambda series, now_utc=None: (calls.append(series) or (False, set())),
    )

    ticker = "KXLOWTSATX-26SEP18-B76.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="entry",
        phase=Phase.MONITORING,
    )
    strategy.brackets[ticker] = bracket
    strategy.cache.update_quote(ticker, 80, 82)
    strategy._execute_entry = AsyncMock()

    await strategy._evaluate_watchlist()

    assert calls == [], "gate must not be consulted when keywords are unset"
    strategy._execute_entry.assert_awaited_once_with(bracket)


# ---------------------------------------------------------------------------
# Test 7: chaser gate is inert when AM_LOW_FORECAST is unset
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chase_gate_inert_when_config_unset(monkeypatch):
    """_chase_entry_gate_open must not consult the gate when disabled.

    The NWS station lookup path is stubbed to a station so we exercise the
    keyword branch specifically; with no keywords we must reach the NWS part.
    """
    strategy = make_strategy(monkeypatch)
    strategy.config.am_low_forecast_keywords = set()

    calls = []
    monkeypatch.setattr(
        strategy._am_low_brief_gate,
        "get_block",
        lambda series, now_utc=None: (calls.append(series) or (True, set())),
    )

    ticker = "KXLOWTSATX-26SEP18-B76.5"
    bracket = MarketBracket(
        market_ticker=ticker,
        event_ticker="EVT1",
        series_ticker="KXLOWTSATX",
        bracket_label="held",
        phase=Phase.HOLDING,
        crossed_buy=True,
        position_quantity=5,
    )

    await strategy._chase_entry_gate_open(bracket)

    assert calls == [], "keyword gate must be skipped entirely when unset"
