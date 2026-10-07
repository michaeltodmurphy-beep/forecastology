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


# ---------------------------------------------------------------------------
# get_confirmed_bid: the BUY side (guards marketable sells / bid triggers)
# ---------------------------------------------------------------------------

def _prime_cache_bids(ticker: str, yes_bids: list[tuple[float, float]]) -> TickerCache:
    """Populate a TickerCache orderbook from YES-bid (price_dollars, qty) pairs."""
    cache = TickerCache()
    cache.update_orderbook_snapshot(
        ticker,
        {
            "yes_dollars_fp": [[str(p), str(q)] for p, q in yes_bids],
            "no_dollars_fp": [],
        },
    )
    return cache


def test_confirmed_bid_returns_top_when_gap_within_threshold():
    """Book bid 60c/58c (gap 2 <= 5): top bid corroborated -> 60."""
    ticker = "T"
    cache = _prime_cache_bids(ticker, [(0.60, 5), (0.58, 5)])
    assert cache.get_confirmed_bid(ticker, 5) == 60


def test_confirmed_bid_uses_next_when_top_is_spoof():
    """Book bid 92c/60c (gap 32 > 5): top bid is a spoof -> next-best 60."""
    ticker = "T"
    cache = _prime_cache_bids(ticker, [(0.92, 1), (0.60, 10)])
    assert cache.get_confirmed_bid(ticker, 5) == 60


def test_confirmed_bid_single_level_falls_back_to_top():
    ticker = "T"
    cache = _prime_cache_bids(ticker, [(0.92, 1)])
    assert cache.get_confirmed_bid(ticker, 5) == 92


def test_confirmed_bid_disabled_when_threshold_zero():
    ticker = "T"
    cache = _prime_cache_bids(ticker, [(0.92, 1), (0.60, 10)])
    assert cache.get_confirmed_bid(ticker, 0) == 92


# ---------------------------------------------------------------------------
# get_effective_ask / get_effective_bid: single source of truth
# ---------------------------------------------------------------------------

def test_effective_ask_prefers_guarded_book_over_raw_quote():
    """A spoofed 52c ticker quote must lose to the corroborated 92c book ask."""
    ticker = "T"
    cache = _prime_cache(ticker, [(0.48, 1), (0.08, 10)])  # asks 52c / 92c
    # Raw ticker channel carries the spoofed 52c ask.
    cache.update_quote(ticker, 91, 52)
    assert cache.get_effective_ask(ticker, 5) == 92


def test_effective_ask_falls_back_to_quote_without_book():
    ticker = "T"
    cache = TickerCache()
    cache.update_quote(ticker, 91, 92)
    assert cache.get_effective_ask(ticker, 5) == 92


def test_effective_bid_falls_back_to_quote_without_book():
    ticker = "T"
    cache = TickerCache()
    cache.update_quote(ticker, 60, 62)
    assert cache.get_effective_bid(ticker, 5) == 60


def test_effective_bid_guards_spoofed_high_bid():
    """A lone 92c bid in a 60c book must not be used for a marketable sell."""
    ticker = "T"
    cache = _prime_cache_bids(ticker, [(0.92, 1), (0.60, 10)])
    cache.update_quote(ticker, 92, 93)
    assert cache.get_effective_bid(ticker, 5) == 60


# ---------------------------------------------------------------------------
# Phase-C loop: the ACTUAL bug path -- stops firing off the raw ticker quote
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase_c_loop_ignores_spoofed_quote_ask(monkeypatch):
    """The Phase-C stop-loss loop must NOT trigger on a spoofed raw quote ask.

    Reproduces the shake-out: the ticker channel reports a lone 52c ask while
    the corroborated orderbook ask is 92c.  With stop_loss_price=62, the raw
    quote (52 <= 62) would fire the SL; the guarded ask (92) must not.
    """
    from test_state_machine import make_strategy

    strategy = make_strategy(
        monkeypatch,
        stop_loss_price=62,
        ask_spread_protection=5,
    )
    ticker = "KXLOWTBOS-26OCT06-B47.5"

    # Corroborated book: YES asks 92c / 93c (NO bids 8c / 7c).
    strategy.cache.update_orderbook_snapshot(
        ticker,
        {"yes_dollars_fp": [], "no_dollars_fp": [["0.08", "10"], ["0.07", "10"]]},
    )
    # Tick channel carries the spoofed lone 52c ask.
    strategy.cache.update_quote(ticker, 91, 52)

    # The guarded effective ask must be the corroborated 92c.
    assert strategy.cache.get_effective_ask(ticker, 5) == 92
    assert strategy.cache.get_confirmed_ask(ticker, 5) == 92

    # Sanity: the RAW quote ask (52c) WOULD have tripped a 62c stop, but the
    # guarded ask (92c) does not -- this is the exact fake-out we must defeat.
    raw_quote = strategy.cache.get_quote(ticker)
    assert raw_quote is not None and raw_quote[1] == 52
    assert raw_quote[1] <= 62, "raw quote ask would have fired the SL (the bug)"
    assert strategy.cache.get_effective_ask(ticker, 5) == 92 > 62

