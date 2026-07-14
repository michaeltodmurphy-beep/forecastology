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
    await watcher._run_cycle_once()
    task = watcher._worker_tasks["TICKER"]
    await task
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

    assert await watcher.on_market_update("TICKER", 34) is True
    await watcher._run_cycle_once()
    await started.wait()
    duplicate_trigger = await watcher.on_market_update("TICKER", 33)
    release.set()

    assert duplicate_trigger is False
    await watcher._worker_tasks["TICKER"]
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

    assert await watcher.on_market_update("TICKER", 34) is True
    await watcher._run_cycle_once()
    await watcher._worker_tasks["TICKER"]
    assert watcher._positions["TICKER"].state == "RETRYING"
    assert watcher._positions["TICKER"].exit_in_progress is False

    await watcher._run_cycle_once()
    await watcher._worker_tasks["TICKER"]
    assert attempts == 2
    assert "TICKER" not in watcher._positions


@pytest.mark.asyncio
async def test_worker_runs_independently_of_main_loop():
    started = asyncio.Event()
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        started.set()
        return True

    watcher = StopLossWatcher(exit_handler, poll_interval_ms=5)
    await watcher.register_position("TICKER", side="yes", quantity=1, sl_price=35)

    worker_task = asyncio.create_task(watcher.run())
    try:
        assert await watcher.on_market_update("TICKER", 34) is True
        await asyncio.sleep(0.05)
        await asyncio.wait_for(started.wait(), timeout=0.2)
    finally:
        await watcher.stop()
        await worker_task

    assert calls == [("TICKER", "yes", 1, 34)]


@pytest.mark.asyncio
async def test_rearm_position_resets_retrying_state():
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=1, sl_price=35)
    watcher._positions["TICKER"].state = "RETRYING"
    watcher._positions["TICKER"].exit_in_progress = False

    assert await watcher.rearm_position("TICKER", trigger_price=34) is True
    await watcher._run_cycle_once()
    await watcher._worker_tasks["TICKER"]

    assert calls == [("TICKER", "yes", 1, 34)]
