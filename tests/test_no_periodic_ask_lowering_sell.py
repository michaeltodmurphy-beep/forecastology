"""
Regression tests proving that no 15-minute (or any other) periodic
ask-price-lowering sell mechanism exists or can trigger in this codebase.

Audit findings (see PR description for full details):
  - There is NO periodic ask-lowering sell check in production code.
  - The StopLossWatcher fires only when ask <= sl_price (threshold), not
    when the ask is merely declining.
  - The _held_positions_loop (~250 ms) evaluates the same threshold; it does
    not track ask history or ask trends.
  - The NWS APScheduler runs every HIGH_LOW_UPDATE minutes (default 60)
    and updates weather forecasts only — it contains zero sell/exit logic.
    A whitelist guard in start_scheduler() enforces this at runtime.
  - monitor.py, run via a ~30 s systemd timer, contains zero sell/exit logic.
  - No 900-second (15-minute), timedelta(minutes=15), or cron */15 timer
    is defined anywhere in the production codebase.
  - _log_snapshot() (renamed from _log_periodic_snapshot) uses wall-clock
    time throttling, not a counter-modulo pattern, and contains zero sell logic.

Production changes made in this PR:
  - core/state_machine.py: removed _snapshot_counter attribute and counter%60
    pattern from the snapshot method; replaced with explicit timestamp throttle
    (_last_snapshot_ts / _snapshot_interval_s) and renamed to _log_snapshot().
  - nws/scheduler.py: added _ALLOWED_JOB_IDS whitelist guard in start_scheduler()
    to raise RuntimeError if any sell/exit job is accidentally added.

These tests act as regression guards: if anyone re-introduces a periodic
ask-lowering sell path, they must update or delete these tests first.
"""

import asyncio
import inspect
import os
import pathlib

import pytest

from execution.sl_watcher import StopLossWatcher


# ---------------------------------------------------------------------------
# Helper: collect source of all production Python modules
# ---------------------------------------------------------------------------

def _production_sources() -> dict[str, str]:
    """Return {relative_path: source} for all production Python modules."""
    repo_root = pathlib.Path(__file__).parent.parent
    production_dirs = ["core", "execution", "nws", "app", "data"]
    top_level_files = ["run.py", "monitor.py", "scanner.py", "bracket_scanner.py"]
    sources: dict[str, str] = {}
    for d in production_dirs:
        for path in (repo_root / d).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            rel = str(path.relative_to(repo_root))
            sources[rel] = path.read_text(encoding="utf-8")
    for f in top_level_files:
        p = repo_root / f
        if p.exists():
            sources[f] = p.read_text(encoding="utf-8")
    return sources


# ---------------------------------------------------------------------------
# 1.  Declining ask ABOVE sl_price must never trigger a sell
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_declining_ask_above_sl_price_never_triggers_sell():
    """
    Ask prices drop through 80 → 70 → 60 → 50 → 40, all above sl_price=35.
    No sell should fire at any point.
    """
    calls: list = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append(best_ask)
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=5, sl_price=35)

    for ask in (80, 70, 60, 50, 40):
        fired = await watcher.on_market_update("TICKER", ask)
        assert not fired, f"Sell fired at ask={ask} (above sl_price=35); should not have"

    # Also run the backstop poll cycle several times to be sure.
    for _ in range(5):
        await watcher._run_cycle_once()

    # Give any stray tasks a chance to execute.
    await asyncio.sleep(0)

    assert calls == [], (
        f"Sell handler was called with asks={calls}; no sell should fire above sl_price=35"
    )


# ---------------------------------------------------------------------------
# 2.  Sell fires ONLY when ask crosses the threshold, not on decline alone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sell_fires_only_at_threshold_not_on_declining_trend():
    """
    Asks decline from 80 → 60 → 40 → 36 (all above sl_price=35 → no sell),
    then finally to 35 (at threshold → sell fires exactly once).
    """
    calls: list = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append(best_ask)
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=3, sl_price=35)

    for ask in (80, 60, 40, 36):
        fired = await watcher.on_market_update("TICKER", ask)
        assert not fired, f"Sell fired at ask={ask} (above sl_price=35); wrong"

    # Now cross the threshold.
    fired = await watcher.on_market_update("TICKER", 35)
    assert fired is True, "Expected sell to fire when ask==sl_price=35"

    task = watcher._worker_tasks.get("TICKER")
    assert task is not None
    await task

    assert calls == [35], f"Expected exactly one call at ask=35, got {calls}"


# ---------------------------------------------------------------------------
# 3.  Poll loop does NOT introduce periodic sells on a declining-but-safe ask
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_loop_does_not_sell_on_declining_ask_above_sl():
    """
    Run many _run_cycle_once iterations while the ask is declining but
    always stays above sl_price.  No sell should ever fire.
    """
    calls: list = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append(best_ask)
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=2, sl_price=20)

    # Simulate a steadily declining ask (70 → 21) – still above sl_price=20.
    for ask in range(70, 20, -1):
        await watcher.on_market_update("TICKER", ask)

    # Run the poll backstop many times to ensure no spurious fire.
    for _ in range(50):
        await watcher._run_cycle_once()

    await asyncio.sleep(0)

    assert calls == [], (
        "Poll loop triggered a sell during ask decline above sl_price=20; "
        f"unexpected calls: {calls}"
    )


# ---------------------------------------------------------------------------
# 4.  Recovery: ask rises back above sl_price clears state without selling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ask_recovery_above_sl_price_does_not_trigger_sell():
    """
    Ask starts below sl_price (→ would normally trigger), then rises above it
    before the worker is spawned.  No sell should fire.
    """
    calls: list = []

    async def exit_handler(ticker, side, quantity, best_ask):
        calls.append(best_ask)
        return True

    watcher = StopLossWatcher(exit_handler)
    await watcher.register_position("TICKER", side="yes", quantity=1, sl_price=35)

    # Ask drops below threshold — normally triggers.
    fired_low = await watcher.on_market_update("TICKER", 30)
    assert fired_low is True

    # Wait for the inline-dispatched worker to finish.
    task = watcher._worker_tasks.get("TICKER")
    if task is not None:
        await task

    # Position is unregistered after successful exit — re-register to test recovery.
    await watcher.register_position("TICKER", side="yes", quantity=1, sl_price=35)
    watcher._positions["TICKER"].state = "IDLE"
    watcher._positions["TICKER"].exit_in_progress = False
    calls.clear()

    # Ask recovers to well above sl_price.
    fired_high = await watcher.on_market_update("TICKER", 80)
    assert fired_high is False, "Ask above sl_price should not fire a sell"

    await watcher._run_cycle_once()
    await asyncio.sleep(0)

    assert calls == [], f"Sell fired on recovered ask; unexpected calls: {calls}"


# ---------------------------------------------------------------------------
# 5.  No 15-minute (900-second) timer in StopLossWatcher run loop
# ---------------------------------------------------------------------------


def test_sl_watcher_run_loop_has_no_15_minute_interval():
    """
    Introspect StopLossWatcher to confirm the poll interval is far shorter
    than 900 seconds (15 minutes).  The poll interval controls retry cadence
    only, not ask-price checks.
    """
    watcher = StopLossWatcher(lambda *a, **k: None, poll_interval_ms=250)
    assert watcher._poll_interval_s < 900, (
        f"StopLossWatcher._poll_interval_s={watcher._poll_interval_s} is >= 900 s (15 min); "
        "no periodic 15-minute timer should exist"
    )
    # The watcher source code must not contain literal '900' as a timer value.
    source = inspect.getsource(StopLossWatcher)
    assert "900" not in source, (
        "StopLossWatcher source contains '900' which may indicate a 15-min timer"
    )


# ---------------------------------------------------------------------------
# 6.  No ask-history / ask-trend comparison in StopLossWatcher
# ---------------------------------------------------------------------------


def test_sl_watcher_does_not_compare_historical_ask_values():
    """
    StopLossWatcher should store at most one 'last_best_ask' for deduplication
    purposes.  It must NOT compare the current ask against a rolling history
    to decide whether the ask is 'lowering', which would be a trend-based sell.
    """
    from execution.sl_watcher import WatchedPosition

    # WatchedPosition stores last_best_ask as a scalar (not a list/queue).
    pos = WatchedPosition(sl_price=35, side="yes", quantity=1)
    assert not isinstance(pos.last_best_ask, (list, tuple)), (
        "last_best_ask should be a scalar, not a history list"
    )
    # There should be no 'ask_history', 'ask_trend', or similar attribute.
    pos_fields = {f.name for f in pos.__dataclass_fields__.values()}
    for forbidden in ("ask_history", "ask_series", "ask_trend", "prev_ask", "prior_ask"):
        assert forbidden not in pos_fields, (
            f"WatchedPosition has a '{forbidden}' field, which would enable "
            "trend-based sell logic — this must not exist"
        )


# ---------------------------------------------------------------------------
# 7.  monitor.py cycle contains zero sell/exit order calls
# ---------------------------------------------------------------------------


def test_monitor_cycle_contains_no_sell_calls():
    """
    monitor.run_monitor_cycle() must not call any sell/exit order API.
    The comment in monitor.py declares 'Role: READ-ONLY / NON-PRIMARY EXECUTOR'.
    We verify the source does not reference stop-loss or sell-order helpers.
    """
    import monitor as monitor_module

    source = inspect.getsource(monitor_module)

    # These are the only order-submission helpers; must NOT appear in monitor.
    forbidden_calls = [
        "sell_yes(",
        "sell_no(",
        "_execute_stop_loss(",
        "dispatch_stop_loss",
        "stop_loss_watcher",
    ]
    for call in forbidden_calls:
        assert call not in source, (
            f"monitor.py contains '{call}', which would mean it can submit "
            "stop-loss/exit orders — this violates the role boundary"
        )


# ---------------------------------------------------------------------------
# 8.  No 15-minute / 900-second timer in any production module
# ---------------------------------------------------------------------------


def test_no_900_second_or_15_minute_timer_in_any_production_module():
    """
    Scan all production Python source files for patterns indicating a
    15-minute (900-second) periodic timer.  None should exist.
    """
    sources = _production_sources()
    # Patterns that would indicate a 900-second or 15-minute periodic timer
    forbidden_patterns = [
        "asyncio.sleep(900",
        "time.sleep(900",
        "timedelta(minutes=15)",
        "timedelta(minutes = 15)",
        "minutes=15,",
        "minutes = 15,",
        "interval=900",
        "interval = 900",
        "*/15",          # cron syntax
    ]
    violations: list[str] = []
    for module_path, source in sources.items():
        for pattern in forbidden_patterns:
            if pattern in source:
                violations.append(f"{module_path}: contains '{pattern}'")

    assert not violations, (
        "Found 15-minute/900-second periodic timer patterns in production code:\n"
        + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# 9.  No ask-trend comparison keywords in any production module
# ---------------------------------------------------------------------------


def test_no_ask_lowering_trend_logic_in_production_modules():
    """
    Scan all production Python source files for attribute names that would
    indicate ask-trend tracking or ask-history comparison for sell decisions.
    """
    sources = _production_sources()
    forbidden_attrs = [
        "ask_history",
        "ask_series",
        "ask_trend",
        "prev_ask",
        "prior_ask",
        "ask_lower",
        "lowering_ask",
    ]
    violations: list[str] = []
    for module_path, source in sources.items():
        for attr in forbidden_attrs:
            if attr in source:
                violations.append(f"{module_path}: contains '{attr}'")

    assert not violations, (
        "Found ask-trend/ask-history attributes in production code:\n"
        + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# 10.  _log_snapshot uses timestamp throttle, NOT a counter-modulo pattern
# ---------------------------------------------------------------------------


def test_strategy_snapshot_uses_time_throttle_not_counter_modulo():
    """
    TemperatureStrategy._log_snapshot() must use wall-clock time throttling
    (self._last_snapshot_ts) rather than a counter % N pattern.
    A counter-modulo periodic check could be confused with a periodic sell
    trigger; the timestamp approach makes the intent unambiguous.
    """
    import core.state_machine as sm_module

    source = inspect.getsource(sm_module)

    # The old _snapshot_counter approach must be gone.
    assert "_snapshot_counter" not in source, (
        "core/state_machine.py still contains '_snapshot_counter'; the counter-modulo "
        "periodic pattern must be replaced with the timestamp-based throttle."
    )
    # The old method name must be gone.
    assert "_log_periodic_snapshot" not in source, (
        "core/state_machine.py still contains '_log_periodic_snapshot'; this method "
        "was renamed to '_log_snapshot' to remove 'periodic' from the name."
    )
    # The new timestamp-based throttle must be present.
    assert "_last_snapshot_ts" in source, (
        "core/state_machine.py is missing '_last_snapshot_ts'; the new timestamp "
        "throttle must be used instead of the counter-modulo approach."
    )
    # The new method name must exist.
    assert "def _log_snapshot(" in source, (
        "core/state_machine.py is missing 'def _log_snapshot('; the method was "
        "renamed from '_log_periodic_snapshot' to remove the confusing 'periodic' label."
    )


# ---------------------------------------------------------------------------
# 11.  NWS scheduler job whitelist enforces no sell jobs at runtime
# ---------------------------------------------------------------------------


def test_nws_scheduler_has_job_whitelist_guard():
    """
    nws/scheduler.start_scheduler() must contain the _ALLOWED_JOB_IDS
    whitelist guard so that any accidental addition of a sell/exit job
    raises a RuntimeError at startup.
    """
    import nws.scheduler as sched_module

    source = inspect.getsource(sched_module)

    assert "_ALLOWED_JOB_IDS" in source, (
        "nws/scheduler.py is missing '_ALLOWED_JOB_IDS'; the whitelist guard "
        "that prevents accidental sell/exit scheduler jobs must be present."
    )
    assert "nws_high_low_updater" in source, (
        "nws/scheduler.py must reference the 'nws_high_low_updater' job id "
        "as the only allowed scheduled job."
    )

