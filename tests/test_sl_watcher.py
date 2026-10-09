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


# ---------------------------------------------------------------------------
# New tests for Change A: inline exit dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_market_update_fires_exit_inline_without_run_cycle_once():
    """Exit handler is invoked immediately when on_market_update detects the trigger,
    without any call to _run_cycle_once."""
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=5, sl_price=40)

    # Trigger fires inline — no explicit _run_cycle_once call.
    triggered = await watcher.on_market_update("TICKER", 38)
    assert triggered is True

    # The worker task is already scheduled; allow it to run.
    task = watcher._worker_tasks.get("TICKER")
    assert task is not None, "worker task should have been spawned inline"
    await task

    assert calls == [("TICKER", "yes", 5, 38)]
    assert "TICKER" not in watcher._positions


@pytest.mark.asyncio
async def test_no_double_dispatch_inline_then_run_cycle_once():
    """When on_market_update triggers inline dispatch, a subsequent _run_cycle_once
    must not spawn a second worker for the same position."""
    calls = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        started.set()
        await release.wait()
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=2, sl_price=50)

    # Inline dispatch on trigger.
    assert await watcher.on_market_update("TICKER", 45) is True
    await started.wait()  # worker is in flight

    # Poll backstop runs while worker is still alive — must not duplicate.
    await watcher._run_cycle_once()

    release.set()
    task = watcher._worker_tasks.get("TICKER")
    if task is not None:
        await task

    # Give any stray task a chance to run.
    await asyncio.sleep(0)

    assert len(calls) == 1, f"expected exactly 1 call, got {len(calls)}: {calls}"


@pytest.mark.asyncio
async def test_poll_loop_backstop_processes_rearmed_position():
    """A position that reaches TRIGGERED via rearm_position (no on_market_update
    inline trigger) is still picked up by the poll loop."""
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        return True

    watcher = StopLossWatcher(exit_handler, poll_interval_ms=5)
    await watcher.register_position("TICKER", side="yes", quantity=3, sl_price=60)

    # Simulate a partial-fill rearm (no on_market_update trigger).
    watcher._positions["TICKER"].state = "RETRYING"
    watcher._positions["TICKER"].exit_in_progress = False
    assert await watcher.rearm_position("TICKER", trigger_price=55) is True

    # Poll loop picks it up.
    worker_task = asyncio.create_task(watcher.run())
    try:
        await asyncio.wait_for(
            asyncio.shield(
                asyncio.get_event_loop().run_until_complete(asyncio.sleep(0))
                if False else asyncio.sleep(0.05)
            ),
            timeout=1.0,
        )
    except asyncio.TimeoutError:
        pass
    finally:
        await watcher.stop()
        await worker_task

    assert calls == [("TICKER", "yes", 3, 55)]


@pytest.mark.asyncio
async def test_no_double_dispatch_after_inline_and_subsequent_market_updates():
    """Multiple market updates below sl_price while a worker is in flight must all
    be suppressed and must not spawn additional workers."""
    calls = []
    release = asyncio.Event()

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        await release.wait()
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=1, sl_price=50)

    assert await watcher.on_market_update("TICKER", 48) is True  # inline dispatch

    # Further price drops while in flight — all must be suppressed.
    assert await watcher.on_market_update("TICKER", 46) is False
    assert await watcher.on_market_update("TICKER", 44) is False
    assert await watcher.on_market_update("TICKER", 42) is False

    release.set()
    task = watcher._worker_tasks.get("TICKER")
    if task:
        await task

    await asyncio.sleep(0)  # let cleanup callbacks fire

    assert len(calls) == 1


# ---------------------------------------------------------------------------
# F4: no-bid latch must STOP the watcher respawn loop (live thrash regression)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_suppressed_result_stops_respawn_loop():
    """A handler returning 'suppressed' must park the position in SUPPRESSED and
    MUST NOT be re-spawned by the poll backstop (this was the live ~4 Hz
    sl.exit_order_failed thrash for a stranded no-bid ticker)."""
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        # Bottomed-out / no-bid latch: no order attempted.
        return "suppressed"

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=6, sl_price=62)

    assert await watcher.on_market_update("TICKER", 1) is True  # inline dispatch
    await watcher._worker_tasks["TICKER"]
    await asyncio.sleep(0)

    # One attempt, then parked SUPPRESSED.
    assert len(calls) == 1
    assert watcher._positions["TICKER"].state == "SUPPRESSED"
    assert watcher._positions["TICKER"].exit_in_progress is False

    # The poll backstop must NOT re-spawn the worker for a SUPPRESSED position,
    # even across many cycles.
    for _ in range(20):
        await watcher._run_cycle_once()
        await asyncio.sleep(0)

    assert len(calls) == 1, (
        f"suppressed position must not be respawned; got {len(calls)} attempts"
    )
    assert watcher._positions["TICKER"].state == "SUPPRESSED"


@pytest.mark.asyncio
async def test_suppressed_market_updates_do_not_respawn():
    """Repeated WS ticks below sl_price while SUPPRESSED must not spawn workers."""
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        return "suppressed"

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=6, sl_price=62)

    assert await watcher.on_market_update("TICKER", 1) is True
    await watcher._worker_tasks["TICKER"]
    await asyncio.sleep(0)

    for _ in range(10):
        fired = await watcher.on_market_update("TICKER", 1)
        assert fired is False, "suppressed position must not re-trigger"
        await asyncio.sleep(0)

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_rearm_resumes_from_suppressed_when_bid_returns():
    """When a live bid returns, rearm_position must move SUPPRESSED -> TRIGGERED
    so the poll backstop resumes the exit."""
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        # First attempt is suppressed; the rearmed attempt succeeds.
        return "suppressed" if len(calls) == 1 else True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=6, sl_price=62)

    assert await watcher.on_market_update("TICKER", 1) is True
    await watcher._worker_tasks["TICKER"]
    await asyncio.sleep(0)
    assert watcher._positions["TICKER"].state == "SUPPRESSED"

    # Bid returns -> state machine re-arms.
    assert await watcher.rearm_position("TICKER", trigger_price=40) is True
    assert watcher._positions["TICKER"].state == "TRIGGERED"

    await watcher._run_cycle_once()
    await watcher._worker_tasks["TICKER"]
    await asyncio.sleep(0)

    assert len(calls) == 2
    assert "TICKER" not in watcher._positions  # terminal after success


@pytest.mark.asyncio
async def test_suppressed_does_not_log_exit_order_failed(capsys):
    """The suppressed path must NOT emit an sl.exit_order_failed line."""
    calls = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append((ticker, side, quantity, best_ask))
        return "suppressed"

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=6, sl_price=62)
    assert await watcher.on_market_update("TICKER", 1) is True
    await watcher._worker_tasks["TICKER"]
    await asyncio.sleep(0)

    assert watcher._positions["TICKER"].state == "SUPPRESSED"
    assert len(calls) == 1
