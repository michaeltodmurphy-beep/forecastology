import asyncio

import pytest

from execution.sl_watcher import StopLossWatcher


@pytest.mark.asyncio
async def test_register_trigger_and_unregister_flow():
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=3, sl_price=35)

    assert await watcher.on_market_update("TICKER", 40) is False
    assert await watcher.on_market_update("TICKER", 35) is True
    assert calls == [("TICKER", "yes", 3, 35)]
    assert "TICKER" not in watcher._positions


@pytest.mark.asyncio
async def test_duplicate_trigger_suppression():
    calls = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        started.set()
        await release.wait()
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=2, sl_price=35)

    first_trigger = asyncio.create_task(watcher.on_market_update("TICKER", 34))
    await started.wait()
    duplicate_trigger = await watcher.on_market_update("TICKER", 33)
    release.set()

    assert await first_trigger is True
    assert duplicate_trigger is False
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_failed_exit_resets_idempotency_for_retry():
    attempts = 0

    async def exit_handler(_ticker, _side, _quantity, _best_ask):
        nonlocal attempts
        attempts += 1
        return attempts > 1

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=1, sl_price=35)

    assert await watcher.on_market_update("TICKER", 34) is False
    assert watcher._positions["TICKER"].exit_in_progress is False

    assert await watcher.on_market_update("TICKER", 34) is True
    assert attempts == 2
    assert "TICKER" not in watcher._positions
