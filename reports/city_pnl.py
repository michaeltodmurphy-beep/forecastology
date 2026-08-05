"""reports/city_pnl.py — Per-city (or bucket / count) realized P&L report.

Usage
-----
    python -m reports.city_pnl [options]

Options
-------
    --days N           Lookback window in calendar days (default 60)
    --family {LOW,HIGH,ALL}
                       Filter by temperature family (default LOW)
    --bucket           Break down by entry_price_bucket instead of city
    --count            Break down by stop_loss_count_at_entry
    --csv              Output CSV instead of aligned plain-text table
    --db-url URL       SQLAlchemy database URL (overrides MYSQL_DATABASE_URL env var)

Examples
--------
    python -m reports.city_pnl --days 60 --family LOW
    python -m reports.city_pnl --days 90 --family LOW --bucket
    python -m reports.city_pnl --days 60 --family LOW --count
    python -m reports.city_pnl --days 60 --family LOW --csv
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import datetime
from collections import defaultdict
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def breakeven_win_rate(avg_entry: float, avg_loss_exit: float) -> Optional[float]:
    """W* = (avg_entry − avg_loss_exit) / (100 − avg_loss_exit).

    Returns None when the denominator is zero or negative.
    """
    denom = 100.0 - avg_loss_exit
    if denom <= 0:
        return None
    return (avg_entry - avg_loss_exit) / denom


def ev_per_contract(
    win_rate: float,
    avg_entry: float,
    avg_loss_exit: float,
) -> float:
    """Expected value per contract in cents."""
    return win_rate * (100.0 - avg_entry) + (1.0 - win_rate) * (avg_loss_exit - avg_entry)


def verdict(
    trades: int,
    observed_win_rate: float,
    w_star: Optional[float],
    min_trades: int = 25,
) -> str:
    if trades < min_trades:
        return "INSUFFICIENT"
    if w_star is None:
        return "INSUFFICIENT"
    return "KEEP" if observed_win_rate >= w_star else "CUT?"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_outcomes(
    db_url: str,
    lookback_days: int,
    family_filter: Optional[str],
):
    """Return a list of dicts for resolved TradeOutcome rows."""
    from sqlalchemy import create_engine, text

    engine = create_engine(db_url, pool_pre_ping=True)
    since = (datetime.date.today() - datetime.timedelta(days=lookback_days)).isoformat()

    query = """
        SELECT
            city,
            family,
            entry_price_bucket,
            stop_loss_count_at_entry,
            entry_price_avg,
            exit_price_avg,
            outcome,
            realized_pnl_cents
        FROM trade_outcomes
        WHERE outcome IN ('SETTLED_WIN', 'SETTLED_LOSS', 'STOPPED', 'CLOSED_OUT')
          AND created_at >= :since
    """
    params: dict = {"since": since}
    if family_filter and family_filter.upper() != "ALL":
        query += " AND family = :family"
        params["family"] = family_filter.upper()

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _win_exit_price(row: dict) -> float:
    """Return the effective exit price for P&L math.

    Settled wins settle at 100¢; settled losses at 0¢.
    Stopped/closed-out use the actual exit price or 0 if missing.
    """
    outcome = (row.get("outcome") or "").upper()
    if outcome == "SETTLED_WIN":
        return 100.0
    if outcome == "SETTLED_LOSS":
        return 0.0
    return float(row.get("exit_price_avg") or 0)


def _is_win(row: dict) -> bool:
    outcome = (row.get("outcome") or "").upper()
    if outcome == "SETTLED_WIN":
        return True
    if outcome == "SETTLED_LOSS":
        return False
    exit_p = float(row.get("exit_price_avg") or 0)
    entry_p = float(row.get("entry_price_avg") or 0)
    return exit_p > entry_p


def aggregate(rows: list[dict], group_key: str) -> list[dict]:
    """Group rows by *group_key* and compute stats per group."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = str(row.get(group_key) or "unknown")
        buckets[key].append(row)

    results = []
    for group, group_rows in sorted(buckets.items()):
        wins = [r for r in group_rows if _is_win(r)]
        losses = [r for r in group_rows if not _is_win(r)]
        n = len(group_rows)
        win_pct = len(wins) / n if n else 0.0

        avg_entry = (
            sum(float(r.get("entry_price_avg") or 0) for r in group_rows) / n
            if n else 0.0
        )
        loss_exits = [_win_exit_price(r) for r in losses]
        avg_loss_exit = sum(loss_exits) / len(loss_exits) if loss_exits else 0.0

        w_star = breakeven_win_rate(avg_entry, avg_loss_exit)
        ev = ev_per_contract(win_pct, avg_entry, avg_loss_exit)
        v = verdict(n, win_pct, w_star)

        results.append({
            "group": group,
            "trades": n,
            "wins": len(wins),
            "win_pct": win_pct,
            "w_star": w_star,
            "avg_entry": avg_entry,
            "avg_loss_exit": avg_loss_exit,
            "ev_per_contract": ev,
            "verdict": v,
        })

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

_HEADERS = [
    ("group", 22),
    ("trades", 7),
    ("wins", 5),
    ("win%", 7),
    ("W*", 7),
    ("avg_entry", 10),
    ("avg_loss_exit", 14),
    ("EV/contract", 11),
    ("verdict", 11),
]


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "   N/A"
    return f"{v * 100:6.1f}%"


def _fmt_cents(v: float) -> str:
    return f"{v:8.1f}¢"


def print_table(results: list[dict], label: str) -> None:
    header_line = "  ".join(h.ljust(w) for h, w in _HEADERS)
    sep = "-" * len(header_line)
    print(f"\n{label}")
    print(sep)
    print(header_line)
    print(sep)
    for r in results:
        group = str(r["group"])[:22].ljust(22)
        trades = str(r["trades"]).rjust(7)
        wins = str(r["wins"]).rjust(5)
        win_pct = _fmt_pct(r["win_pct"]).rjust(7)
        w_star = _fmt_pct(r["w_star"]).rjust(7)
        avg_entry = _fmt_cents(r["avg_entry"]).rjust(10)
        avg_loss_exit = _fmt_cents(r["avg_loss_exit"]).rjust(14)
        ev = _fmt_cents(r["ev_per_contract"]).rjust(11)
        verdict_col = r["verdict"].ljust(11)
        print(f"  {group}  {trades}  {wins}  {win_pct}  {w_star}  {avg_entry}  {avg_loss_exit}  {ev}  {verdict_col}")
    print(sep)


def print_csv(results: list[dict], label: str) -> None:
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=[
            "group", "trades", "wins", "win_pct", "w_star",
            "avg_entry_cents", "avg_loss_exit_cents", "ev_per_contract_cents", "verdict",
        ],
    )
    writer.writeheader()
    for r in results:
        writer.writerow({
            "group": r["group"],
            "trades": r["trades"],
            "wins": r["wins"],
            "win_pct": f"{r['win_pct'] * 100:.2f}%" if r["win_pct"] is not None else "",
            "w_star": f"{r['w_star'] * 100:.2f}%" if r["w_star"] is not None else "",
            "avg_entry_cents": f"{r['avg_entry']:.1f}",
            "avg_loss_exit_cents": f"{r['avg_loss_exit']:.1f}",
            "ev_per_contract_cents": f"{r['ev_per_contract']:.1f}",
            "verdict": r["verdict"],
        })


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Per-city / bucket realized P&L report from trade_outcomes table."
    )
    parser.add_argument("--days", type=int, default=60, help="Lookback window in days (default 60)")
    parser.add_argument(
        "--family",
        choices=["LOW", "HIGH", "ALL"],
        default="LOW",
        help="Temperature family filter (default LOW)",
    )
    parser.add_argument(
        "--bucket",
        action="store_true",
        help="Break down by entry_price_bucket instead of city",
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help="Break down by stop_loss_count_at_entry",
    )
    parser.add_argument("--csv", action="store_true", help="Output CSV instead of aligned table")
    parser.add_argument(
        "--db-url",
        default=None,
        help="SQLAlchemy database URL (overrides MYSQL_DATABASE_URL env var)",
    )
    args = parser.parse_args()

    db_url = args.db_url or os.getenv("MYSQL_DATABASE_URL") or os.getenv("MYSQL_URL")
    if not db_url:
        print("ERROR: No database URL. Set MYSQL_DATABASE_URL or pass --db-url.", file=sys.stderr)
        sys.exit(1)

    try:
        rows = _load_outcomes(db_url, args.days, args.family)
    except Exception as exc:
        print(f"ERROR: Failed to load outcomes: {exc}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("No resolved trade outcomes found for the given filters.")
        return

    if not args.bucket and not args.count:
        # Default: group by city
        results = aggregate(rows, "city")
        label = f"Per-City P&L — {args.family} trades, last {args.days} days ({len(rows)} resolved outcomes)"
        if args.csv:
            print_csv(results, label)
        else:
            print_table(results, label)

    if args.bucket:
        results = aggregate(rows, "entry_price_bucket")
        label = f"Per-Bucket P&L — {args.family} trades, last {args.days} days"
        if args.csv:
            print_csv(results, label)
        else:
            print_table(results, label)

    if args.count:
        results = aggregate(rows, "stop_loss_count_at_entry")
        label = f"Per-Stop-Loss-Count P&L — {args.family} trades, last {args.days} days"
        if args.csv:
            print_csv(results, label)
        else:
            print_table(results, label)


if __name__ == "__main__":
    main()
