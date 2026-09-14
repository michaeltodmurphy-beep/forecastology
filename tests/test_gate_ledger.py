"""Tests for the Ticker Gate Ledger (Stage 1, Option Z — observability only).

Covers:
  * DedupeLogger-backed verdict-change emission semantics used by _record_gate.
  * scripts/ledger_report.py names the SPECIFIC sunrise CHILD that blocked,
    never the bare parent, even though the child is reconstructed from the
    existing sunrise.* logs (Option Z).
"""
from __future__ import annotations

import datetime

import pytest

from core.log_dedupe import DedupeLogger

# Import the report module from scripts/ (pure functions, no side effects).
import importlib.util
import pathlib

_REPORT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "ledger_report.py"
_spec = importlib.util.spec_from_file_location("ledger_report", _REPORT_PATH)
ledger_report = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
import sys as _sys
_sys.modules["ledger_report"] = ledger_report
_spec.loader.exec_module(ledger_report)


class _CaptureLogger:
    def __init__(self):
        self.events = []

    def info(self, event, **kwargs):
        self.events.append(("info", event, kwargs))

    def debug(self, event, **kwargs):
        self.events.append(("debug", event, kwargs))


# ---------------------------------------------------------------------------
# 1. _record_gate emission semantics (verdict-change only)
# ---------------------------------------------------------------------------

def _record_gate(logger, dedupe, ticker, gate, verdict, **payload):
    """Mirror of TemperatureStrategy._record_gate (kept in sync manually)."""
    dedupe.log(
        logger, "info", "phase.b.decision", ticker,
        day="2026-09-14",
        ticker=ticker, gate=gate, gate_type="continuous",
        verdict=verdict, parent="", terminal=False, **payload,
    )


def test_record_gate_emits_once_per_verdict_state():
    logger = _CaptureLogger()
    dedupe = DedupeLogger(summary_interval_seconds=300, monotonic_fn=lambda: 0.0)

    # Same verdict+payload twice -> one decision line (the repeat is
    # suppressed; a pending-repeat summary may flush on the NEXT change).
    _record_gate(logger, dedupe, "KXLOWTBOS-26SEP14-B70.5", "price_trigger",
                 "BLOCKED", price=72, buy_trigger=85)
    _record_gate(logger, dedupe, "KXLOWTBOS-26SEP14-B70.5", "price_trigger",
                 "BLOCKED", price=72, buy_trigger=85)

    decisions = [e for e in logger.events if e[1] == "phase.b.decision"]
    assert len(decisions) == 1
    assert decisions[0][2]["gate"] == "price_trigger"
    assert decisions[0][2]["verdict"] == "BLOCKED"


def test_record_gate_emits_on_verdict_change():
    logger = _CaptureLogger()
    dedupe = DedupeLogger(summary_interval_seconds=300, monotonic_fn=lambda: 0.0)

    _record_gate(logger, dedupe, "KXLOWTBOS-26SEP14-B70.5", "price_trigger",
                 "BLOCKED", price=72, buy_trigger=85)
    _record_gate(logger, dedupe, "KXLOWTBOS-26SEP14-B70.5", "price_trigger",
                 "PASS", price=86, buy_trigger=85)

    decisions = [e for e in logger.events if e[1] == "phase.b.decision"]
    assert [e[2]["verdict"] for e in decisions] == ["BLOCKED", "PASS"]


# ---------------------------------------------------------------------------
# 2. Report resolves to the SPECIFIC sunrise child (leaf), not the parent
# ---------------------------------------------------------------------------

def _write_log(tmp_path, lines):
    p = tmp_path / "run.log"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def test_report_names_specific_sunrise_child_not_parent(tmp_path):
    ticker = "KXLOWTBOS-26SEP14-B70.5"
    day = "2026-09-14"
    lines = [
        # Parent sunrise blocked (recorded by the new code).
        f'2026-09-14 06:50:00 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=sunrise verdict=BLOCKED gate_type=once parent= terminal=False '
        f'reason=sunrise_gate_closed',
        # The specific CHILD that failed (existing sunrise_gate.py log).
        f'2026-09-14 06:50:00 [info     ] sunrise.temp_rising_blocked  '
        f'ticker={ticker} station=KBOS rise=0.4 required=1.0',
    ]
    glob_pattern = _write_log(tmp_path, lines)

    rows, children = ledger_report.collect(ticker, day, glob_pattern)
    verdict = ledger_report.decisive_block(rows, children)

    assert verdict is not None
    # MUST name the child leaf, not the bare parent.
    assert "sunrise.temp_rise_1deg" in verdict
    assert verdict.strip() != "sunrise"


def test_report_keeps_parent_when_no_child_known(tmp_path):
    """If only the parent is logged, the report still names the parent."""
    ticker = "KXLOWTBOS-26SEP14-B70.5"
    day = "2026-09-14"
    lines = [
        f'2026-09-14 06:50:00 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=sunrise verdict=BLOCKED gate_type=once parent= terminal=False '
        f'reason=sunrise_gate_closed',
    ]
    glob_pattern = _write_log(tmp_path, lines)

    rows, children = ledger_report.collect(ticker, day, glob_pattern)
    verdict = ledger_report.decisive_block(rows, children)
    assert verdict == "sunrise"


def test_report_orders_gates_and_picks_first_decisive_block(tmp_path):
    ticker = "KXLOWTBOS-26SEP14-B70.5"
    day = "2026-09-14"
    lines = [
        f'2026-09-14 07:10:00 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=sunrise verdict=OPEN gate_type=once parent= terminal=False',
        f'2026-09-14 07:10:01 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=price_trigger verdict=BLOCKED gate_type=continuous parent= '
        f'terminal=False price=72 buy_trigger=85',
        f'2026-09-14 07:10:02 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=price_ceiling verdict=PASS gate_type=continuous parent= '
        f'terminal=False price=72 max_price=90',
    ]
    glob_pattern = _write_log(tmp_path, lines)

    rows, children = ledger_report.collect(ticker, day, glob_pattern)
    gates_in_order = [r.gate for r in rows]
    assert gates_in_order == ["sunrise", "price_trigger", "price_ceiling"]
    assert ledger_report.decisive_block(rows, children) == "price_trigger"


def test_report_ignores_other_tickers_and_dates(tmp_path):
    ticker = "KXLOWTBOS-26SEP14-B70.5"
    day = "2026-09-14"
    lines = [
        f'2026-09-14 07:10:00 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=spread verdict=BLOCKED gate_type=continuous parent= terminal=False '
        f'spread=8',
        # Different ticker -> ignored.
        f'2026-09-14 07:10:00 [info     ] phase.b.decision  ticker=KXHIGHNY-26SEP14-T90 '
        f'gate=spread verdict=PASS gate_type=continuous parent= terminal=False',
        # Different date -> ignored.
        f'2026-09-15 07:10:00 [info     ] phase.b.decision  ticker={ticker} '
        f'gate=spread verdict=PASS gate_type=continuous parent= terminal=False',
    ]
    glob_pattern = _write_log(tmp_path, lines)

    rows, children = ledger_report.collect(ticker, day, glob_pattern)
    assert [r.gate for r in rows] == ["spread"]
    assert rows[0].verdict == "BLOCKED"


def test_report_empty_when_no_data(tmp_path):
    glob_pattern = _write_log(tmp_path, ["2026-09-14 07:10:00 [info] unrelated"])
    rows, children = ledger_report.collect("KXLOWTBOS-26SEP14-B70.5", "2026-09-14", glob_pattern)
    assert rows == []
    assert children == []
    text = ledger_report.render("KXLOWTBOS-26SEP14-B70.5", "2026-09-14", rows, children)
    assert "No ledger data found" in text
