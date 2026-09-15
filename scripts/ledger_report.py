#!/usr/bin/env python3
"""Ticker Gate Ledger report (Stage 1, Option Z - read-only).

Prints the full entry-gate lifecycle for a single ticker on a given day, in
gate order, naming the SPECIFIC gate that blocked - including when the
responsible gate is a CHILD of the sunrise gate (e.g. sunrise.am_low_before_9am
or sunrise.temp_rise_1deg).

Data sources (both from logs/run.log* by default):

  * phase.b.decision lines emitted by TemperatureStrategy._record_gate
    (the top-level gates plus the sunrise parent verdict).
  * Existing sunrise.* child lines emitted by core/sunrise_gate.py
    (used, per Option Z, to name WHICH sunrise child failed without editing
    any strategy file).

USAGE:
    python scripts/ledger_report.py TICKER [DATE] [--glob 'logs/run.log*']
    python scripts/ledger_report.py KXLOWTBOS-26SEP14-B70.5 2026-09-14
    python scripts/ledger_report.py KXLOWTBOS            # defaults to today

This script never writes anything.  It is a pure reader.
"""
from __future__ import annotations

import argparse
import datetime
import glob
import re
import sys
from dataclasses import dataclass, field
from typing import Iterable, Optional


# ---------------------------------------------------------------------------
# Gate ordering / metadata (mirrors _evaluate_watchlist execution order)
# ---------------------------------------------------------------------------
GATE_ORDER = [
    ("sunrise", "SUNRISE (start gate)", "once"),
    ("sunrise.time_window", "  child: time window open", "once"),
    ("sunrise.am_low_before_9am", "  child: AM low before deadline", "once"),
    ("sunrise.temp_rise_1deg", "  child: temp rise after sunrise", "once"),
    ("am_low_keyword", "AM-low daily-brief keyword", "once"),
    ("overnight_9pm_1am_low", "Overnight 9pm-1am low (forecast)", "continuous"),
    ("price_trigger", "Price >= buy trigger", "continuous"),
    ("price_ceiling", "Price <= max ceiling", "continuous"),
    ("falling_knife", "Falling-knife guard", "continuous"),
    ("spread", "Spread <= minimum", "continuous"),
]
_GATE_ORDER_INDEX = {gid: i for i, (gid, _l, _t) in enumerate(GATE_ORDER)}

# Map child events emitted by sunrise_gate.py to canonical child gate ids.
SUNRISE_CHILD_EVENTS = {
    "sunrise.gate_blocked": ("sunrise.time_window", "window not yet open"),
    "sunrise.gate_window_closed": ("sunrise.time_window", "window closed"),
    "sunrise.am_low_blocked": ("sunrise.am_low_before_9am", "AM-low deadline"),
    "sunrise.temp_rising_blocked": ("sunrise.temp_rise_1deg", "temp not rising"),
    "sunrise.temp_rise_latched": ("sunrise.temp_rise_1deg", "temp rose (latched)"),
    "sunrise.obs_unavailable": ("sunrise.temp_rise_1deg", "obs unavailable"),
}

_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")
# key=value where value is a quoted string or a run of non-space chars.
_KV_RE = re.compile(r"""([\w.]+)=("[^"]*"|'[^']*'|\S+)""")
# ANSI SGR/erase escape sequences (present in legacy colored log lines).
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[mK]")


@dataclass
class GateRow:
    gate: str
    verdict: str
    ts: str
    terminal: bool = False
    parent: str = ""
    payload: dict = field(default_factory=dict)


@dataclass
class SunriseChild:
    gate: str
    reason: str
    ts: str
    payload: dict = field(default_factory=dict)


def _iter_log_lines(log_glob: str) -> Iterable[tuple[str, str]]:
    files = sorted(glob.glob(log_glob))
    if not files:
        files = sorted(glob.glob(log_glob + "*"))
    for path in files:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    # Strip ANSI escapes: legacy log lines were written with
                    # color codes (e.g. \x1b[36mgate\x1b[0m=...) which would
                    # otherwise corrupt key=value parsing.
                    yield path, _ANSI_RE.sub("", line.rstrip("\n"))
        except OSError:
            continue


def _parse_kv(line: str) -> dict:
    out: dict = {}
    for m in _KV_RE.finditer(line):
        key = m.group(1)
        val = m.group(2)
        if (val.startswith('"') and val.endswith('"')) or (
            val.startswith("'") and val.endswith("'")
        ):
            val = val[1:-1]
        out[key] = val
    return out


def _ts_of(line: str) -> str:
    m = _LINE_RE.match(line.strip())
    return m.group("ts") if m else ""


def _verdict_rank(verdict: str) -> int:
    return {"BLOCKED": 3, "SKIPPED": 2, "OPEN": 1, "PASS": 0}.get(verdict, 0)


def _match_line(line: str, ticker_u: str, day: str) -> bool:
    up = line.upper()
    if ticker_u not in up:
        return False
    # Whole-token match so the series prefix doesn't collide with longer ids.
    if not re.search(re.escape(ticker_u) + r"(?![A-Za-z0-9.\-])", up):
        return False
    line_day = _ts_of(line)[:10]
    if line_day:
        return line_day == day
    # Lines without a parseable timestamp (e.g. older phase.b.decision lines
    # emitted before the logger added a timestamp) cannot be date-filtered.
    # Accept them: the ticker string usually carries the date (e.g.
    # KXLOWTATL-26SEP15-B70.5), so the day is still effectively scoped.
    return True


def collect(ticker: str, day: str, log_glob: str) -> tuple[list[GateRow], list[SunriseChild]]:
    ticker_u = ticker.upper()
    latest: dict[str, GateRow] = {}
    sunrise_children: list[SunriseChild] = []

    for _path, line in _iter_log_lines(log_glob):
        if not _match_line(line, ticker_u, day):
            continue

        kv = _parse_kv(line)
        ts = _ts_of(line) or "?"

        if "phase.b.decision" in line:
            gate = kv.get("gate")
            verdict = kv.get("verdict")
            if not gate or not verdict:
                continue
            row = GateRow(
                gate=gate,
                verdict=verdict,
                ts=ts,
                terminal=str(kv.get("terminal", "false")).lower() == "true",
                parent=kv.get("parent", ""),
                payload={
                    k: v
                    for k, v in kv.items()
                    if k
                    not in {
                        "ticker", "gate", "verdict", "gate_type",
                        "parent", "terminal",
                    }
                },
            )
            prev = latest.get(gate)
            if prev is None or _verdict_rank(verdict) >= _verdict_rank(prev.verdict):
                latest[gate] = row
            continue

        for ev, (child_gate, reason) in SUNRISE_CHILD_EVENTS.items():
            if ev in line:
                sunrise_children.append(
                    SunriseChild(gate=child_gate, reason=reason, ts=ts, payload=kv)
                )
                break

    rows = sorted(
        latest.values(),
        key=lambda r: (_GATE_ORDER_INDEX.get(r.gate, len(GATE_ORDER)), r.ts),
    )
    return rows, sunrise_children


def decisive_block(rows: list[GateRow], sunrise_children: list[SunriseChild]) -> Optional[str]:
    blocked = [r for r in rows if r.verdict == "BLOCKED"]
    if not blocked:
        return None

    terminal = [r for r in blocked if r.terminal]
    pool = terminal or blocked

    def is_parent_of_present_child(r: GateRow) -> bool:
        return any(
            (other.parent == r.gate) or other.gate.startswith(r.gate + ".")
            for other in pool
            if other is not r
        )

    leaves = [r for r in pool if not is_parent_of_present_child(r)]
    chosen = leaves[0] if leaves else pool[0]

    # If the chosen gate is the bare sunrise parent, name the child if known.
    if chosen.gate == "sunrise" and sunrise_children:
        child = sunrise_children[-1]
        return f"{child.gate}  ({child.reason})  [via sunrise]"
    return chosen.gate


def render(ticker: str, day: str, rows: list[GateRow], sunrise_children: list[SunriseChild]) -> str:
    out: list[str] = []
    out.append("")
    out.append(f"Ticker: {ticker.upper()}    Date: {day}")
    out.append("=" * 72)

    if not rows and not sunrise_children:
        out.append("")
        out.append("No ledger data found for this ticker/date.")
        out.append("")
        out.append("Possible reasons:")
        out.append("  * Bot not restarted on the ledger code yet")
        out.append("    (phase.b.decision lines come only from the new code).")
        out.append("  * Wrong date, ticker, or log glob (try --glob 'logs/run.log*').")
        out.append("")
        return "\n".join(out)

    sunrise_closed = any(
        r.gate == "sunrise" and r.verdict == "BLOCKED" for r in rows
    )
    children_by_gate: dict[str, list[SunriseChild]] = {}
    for c in sunrise_children:
        children_by_gate.setdefault(c.gate, []).append(c)

    seen: set[str] = set()
    for gid, label, _gtype in GATE_ORDER:
        row = next((r for r in rows if r.gate == gid), None)
        is_child = gid.startswith("sunrise.") and gid != "sunrise"

        if row is None and is_child:
            events = children_by_gate.get(gid)
            if sunrise_closed and events:
                c = events[-1]
                mark = "BLOCKED" if gid != "sunrise.time_window" or (
                    "closed" in c.reason or "not yet" in c.reason
                ) else "PASS"
                out.append(f"  [{c.ts}] {label:<40} -> {mark}")
                seen.add(gid)
            continue

        if row is None:
            continue

        extra = ""
        if row.payload:
            extra = "  " + " ".join(f"{k}={v}" for k, v in row.payload.items())
        term = "  (terminal)" if row.terminal else ""
        out.append(f"  [{row.ts}] {label:<40} -> {row.verdict}{term}{extra}")
        seen.add(gid)

    for r in rows:
        if r.gate not in seen:
            out.append(f"  [{r.ts}] {r.gate:<40} -> {r.verdict}")

    out.append("-" * 72)
    verdict = decisive_block(rows, sunrise_children)
    if verdict is None:
        out.append(
            "VERDICT: no blocking gate recorded "
            "(ticker may not have been a candidate, or it traded)."
        )
    else:
        out.append(f"VERDICT: NOT TRADED - first decisive block: {verdict}")
    out.append("")
    return "\n".join(out)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Ticker Gate Ledger report")
    parser.add_argument("ticker", help="Market ticker or series prefix, e.g. KXLOWTBOS")
    parser.add_argument(
        "date",
        nargs="?",
        default=datetime.date.today().isoformat(),
        help="Market day YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--glob", default="logs/run.log*", help="Log glob (default: logs/run.log*)"
    )
    args = parser.parse_args(argv)

    rows, children = collect(args.ticker, args.date, args.glob)
    print(render(args.ticker, args.date, rows, children))
    return 0


if __name__ == "__main__":
    sys.exit(main())
