"""Tests for ASK_SPREAD_PROTECTION (order-book spoof guard).

The stop-loss ask trigger must not fire on a lone thin ask placed far below
the rest of the book (a fake-out). When the gap between the lowest YES ask and
the next-best YES ask exceeds ASK_SPREAD_PROTECTION, the top ask is treated as
an outlier and the next-best (corroborated) ask is used for SL evaluation.
A genuine full-book collapse still triggers.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.ticker_cache import TickerCache


# ---------------------------------------------------------------------------
# TickerCache.get_confirmed_ask unit tests
# ---------------------------------------------------------------------------

def _prime_cache(ticker: str, no_bids: list[tuple[float, float]]) -> TickerCache:
    """Populate a TickerCache orderbook from NO-bid (price_dollars, qty) pairs."""
    cache = TickerCache()
    cache.update_orderbook_snapshot(
        ticker,
        {
            "yes_dollars_fp": [],
            "no_dollars_fp": [[str(p), str(q)] for p, q in no_bids],
        },
    )
    return cache


def test_confirmed_ask_returns_top_when_gap_within_threshold():
    """Book 65c/68c (gap 3 <= 5): top ask is corroborated -> return 65."""
    ticker = "T"
    # NO bids 35c (=YES 65c) and 32c (=YES 68c)
    cache = _prime_cache(ticker, [(0.35, 5), (0.32, 5)])
    assert cache.get_confirmed_ask(ticker, 5) == 65


def test_confirmed_ask_uses_next_when_top_is_spoof():
    """Book 65c/93c (gap 28 > 5): top ask is a spoof -> return next-best 93."""
    ticker = "T"
    # NO bids 35c (=YES 65c) and 7c (=YES 93c)
    cache = _prime_cache(ticker, [(0.35, 1), (0.07, 10)])
    assert cache.get_confirmed_ask(ticker, 5) == 93


def test_confirmed_ask_exactly_at_threshold_is_not_spoof():
    """Gap == threshold is NOT treated as a spoof (strict '>' comparison)."""
    ticker = "T"
    # NO bids 35c (=YES 65c) and 30c (=YES 70c) -> gap 5
    cache = _prime_cache(ticker, [(0.35, 5), (0.30, 5)])
    assert cache.get_confirmed_ask(ticker, 5) == 65


def test_confirmed_ask_disabled_when_threshold_zero():
    """Threshold 0 disables the guard -> always return the raw top ask."""
    ticker = "T"
    cache = _prime_cache(ticker, [(0.35, 1), (0.07, 10)])
    assert cache.get_confirmed_ask(ticker, 0) == 65


def test_confirmed_ask_single_level_falls_back_to_top():
    """Only one NO-bid level -> no corroboration; return the lone top ask."""
    ticker = "T"
    cache = _prime_cache(ticker, [(0.35, 5)])
    assert cache.get_confirmed_ask(ticker, 5) == 65


def test_confirmed_ask_returns_none_without_book():
    cache = TickerCache()
    assert cache.get_confirmed_ask("MISSING", 5) is None


def test_confirmed_ask_ignores_zero_quantity_levels():
    """Zero-quantity NO bids must not count as corroboration."""
    ticker = "T"
    cache = _prime_cache(ticker, [(0.35, 1), (0.07, 10)])
    # Drop the 35c level's quantity to 0 via a delta.
    cache.update_orderbook_delta(
        ticker, {"price_dollars": "0.35", "delta_fp": "-1", "side": "no"}
    )
    # Only the 7c NO bid remains -> YES ask 93 -> single level -> 93.
    assert cache.get_confirmed_ask(ticker, 5) == 93


# ---------------------------------------------------------------------------
# Strategy handler tests: the guarded ask is what reaches the SL watcher
# ---------------------------------------------------------------------------

class _FakeWatcher:
    def __init__(self):
        self.calls = []

    async def on_market_update(self, ticker, ask=None, **kwargs):
        self.calls.append((ticker, kwargs.get("best_ask", ask)))
        return False


@pytest.mark.asyncio
async def test_snapshot_spoof_ask_is_suppressed(monkeypatch):
    """A lone 65c ask in a 93c book must NOT be forwarded to the SL watcher."""
    from test_state_machine import make_strategy

    strategy = make_strategy(monkeypatch, stop_loss_price=69, ask_spread_protection=5)
    watcher = _FakeWatcher()
    strategy.stop_loss_watcher = watcher

    ticker = "KXLOWTMIA-26SEP28-T74"
    msg = {
        "market_ticker": ticker,
        # NO bid 35c (YES ask 65c, the spoof), NO bid 7c (YES ask 93c, real)
        "yes_dollars_fp": [],
        "no_dollars_fp": [["0.35", "1"], ["0.07", "10"]],
    }
    await strategy._handle_orderbook_snapshot({"msg": msg})

    assert watcher.calls, "watcher should still be called (with the guarded ask)"
    _, forwarded_ask = watcher.calls[-1]
    assert forwarded_ask == 93, "spoofed 65c ask must be replaced by 93c"


@pytest.mark.asyncio
async def test_snapshot_real_collapse_still_triggers(monkeypatch):
    """A genuine collapse (65c/68c, small gap) is forwarded so the SL can fire."""
    from test_state_machine import make_strategy

    strategy = make_strategy(monkeypatch, stop_loss_price=69, ask_spread_protection=5)
    watcher = _FakeWatcher()
    strategy.stop_loss_watcher = watcher

    ticker = "KXLOWTMIA-26SEP28-T74"
    msg = {
        "market_ticker": ticker,
        "yes_dollars_fp": [],
        "no_dollars_fp": [["0.35", "5"], ["0.32", "5"]],  # asks 65c / 68c
    }
    await strategy._handle_orderbook_snapshot({"msg": msg})

    _, forwarded_ask = watcher.calls[-1]
    assert forwarded_ask == 65, "real collapse must be forwarded unchanged"


@pytest.mark.asyncio
async def test_delta_spoof_ask_is_suppressed(monkeypatch):
    """A spoof introduced via an orderbook delta is also guarded."""
    from test_state_machine import make_strategy

    strategy = make_strategy(monkeypatch, stop_loss_price=69, ask_spread_protection=5)
    watcher = _FakeWatcher()
    strategy.stop_loss_watcher = watcher

    ticker = "KXLOWTMIA-26SEP28-T74"
    # Real book first: NO bid 7c -> YES ask 93c
    await strategy._handle_orderbook_snapshot(
        {"msg": {"market_ticker": ticker, "yes_dollars_fp": [], "no_dollars_fp": [["0.07", "10"]]}}
    )
    watcher.calls.clear()

    # Spoofer adds a 1-lot NO bid at 35c -> derived YES ask collapses to 65c
    await strategy._handle_orderbook_delta(
        {"msg": {"market_ticker": ticker, "price_dollars": "0.35", "delta_fp": "1", "side": "no"}}
    )

    assert watcher.calls
    _, forwarded_ask = watcher.calls[-1]
    assert forwarded_ask == 93, "delta-introduced spoof must be suppressed"
