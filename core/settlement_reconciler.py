# core/settlement_reconciler.py
"""Background settlement reconciler.

Periodically checks for KXLOW*/KXHIGH* positions whose markets should have
settled (date_prefix in the past) and writes / updates TradeOutcome rows.

Lifecycle of a TradeOutcome row:
  1. Created at entry time (outcome=OPEN) by ``_execute_entry``.
  2. Updated to STOPPED/CLOSED_OUT immediately after a STOP_LOSS/SELL fill.
  3. For positions held to settlement this reconciler queries the Kalshi REST
     API and sets outcome to SETTLED_WIN or SETTLED_LOSS.

The reconciler never blocks or interferes with the WS reader / stop-loss hot
path — it runs in a background asyncio task that sleeps between runs.
"""
from __future__ import annotations

import asyncio
import datetime
import re
from typing import Optional, TYPE_CHECKING

import httpx
import structlog

from sqlalchemy import select, update, func as sa_func

from app.models import ExecutedTrade, TradeAction, TradeOutcome, TradeOutcomeStatus
from core.trade_outcome_utils import (
    entry_price_bucket,
    parse_bracket_temp,
    detect_family,
)
from core.local_time_gate import SERIES_CITY, get_series_prefix
from core.state_machine import parse_series_and_date, _parse_date_prefix

if TYPE_CHECKING:
    from app.config import AppConfig
    from app.database import DatabaseManager

logger = structlog.get_logger(__name__)

_MONTH_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


async def _fetch_kalshi_market_result(
    market_ticker: str,
    rest_base_url: str,
    api_key: str,
    private_key_path: str,
) -> Optional[str]:
    """Query Kalshi REST API for the settlement result of *market_ticker*.

    Returns "yes", "no", or None (unresolved / unavailable).
    """
    try:
        from app.signing import load_private_key, build_auth_headers
        private_key = load_private_key(private_key_path)
        path = f"/trade-api/v2/markets/{market_ticker}"
        headers = build_auth_headers(private_key, api_key, method="GET", path=path)
        url = rest_base_url.rstrip("/") + path
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            market = data.get("market") or data
            return market.get("result") or None
    except Exception as exc:
        logger.warning(
            "reconciler.kalshi_fetch_failed",
            market_ticker=market_ticker,
            error=str(exc),
        )
        return None


def _classify_exit_outcome(exit_action: str) -> TradeOutcomeStatus:
    """Map an ExecutedTrade action to an exit outcome status."""
    if exit_action in (TradeAction.STOP_LOSS, "STOP_LOSS"):
        return TradeOutcomeStatus.STOPPED
    return TradeOutcomeStatus.CLOSED_OUT


def _compute_pnl(entry_price: int, exit_price: int, qty: int) -> int:
    """Compute realized P&L in cents (may be negative)."""
    return (exit_price - entry_price) * qty


async def run_reconciliation_cycle(
    db: "DatabaseManager",
    config: "AppConfig",
) -> None:
    """Run one full reconciliation cycle.

    1. Gather all market_tickers in executed_trades that have BUY fills.
    2. For each ticker: classify outcome if not already final.
    3. Upsert the TradeOutcome row.
    """
    logger.info("reconciler.started")
    today = datetime.date.today()

    try:
        async with await db.get_session() as session:
            # All distinct market_tickers that have at least one BUY row
            rows = await session.execute(
                select(ExecutedTrade.market_ticker)
                .where(ExecutedTrade.action == TradeAction.BUY)
                .distinct()
            )
            buy_tickers = [r[0] for r in rows.fetchall()]
    except Exception as exc:
        logger.error("reconciler.db_read_failed", error=str(exc))
        return

    processed = 0
    for market_ticker in buy_tickers:
        try:
            await _reconcile_one_market(market_ticker, db, config, today)
            processed += 1
        except Exception as exc:
            logger.warning(
                "reconciler.market_error",
                market_ticker=market_ticker,
                error=str(exc),
            )

    logger.info("reconciler.complete", processed=processed, total=len(buy_tickers))


async def _reconcile_one_market(
    market_ticker: str,
    db: "DatabaseManager",
    config: "AppConfig",
    today: datetime.date,
) -> None:
    """Reconcile the TradeOutcome for one market_ticker."""
    parsed = parse_series_and_date(market_ticker)
    if parsed is None:
        return
    series_ticker, date_prefix = parsed
    market_date = _parse_date_prefix(date_prefix)

    async with await db.get_session() as session:
        # Check if we already have a final outcome row
        existing_q = await session.execute(
            select(TradeOutcome).where(
                TradeOutcome.market_ticker == market_ticker,
                TradeOutcome.date_prefix == date_prefix,
            )
        )
        existing: Optional[TradeOutcome] = existing_q.scalar_one_or_none()

        if existing is not None and existing.outcome in (
            TradeOutcomeStatus.SETTLED_WIN,
            TradeOutcomeStatus.SETTLED_LOSS,
            TradeOutcomeStatus.STOPPED,
            TradeOutcomeStatus.CLOSED_OUT,
        ):
            # Already final — nothing to do
            return

        # Fetch all trades for this market
        trades_q = await session.execute(
            select(ExecutedTrade).where(
                ExecutedTrade.market_ticker == market_ticker
            ).order_by(ExecutedTrade.executed_at)
        )
        trades = trades_q.scalars().all()

    buy_trades = [t for t in trades if t.action == TradeAction.BUY]
    exit_trades = [
        t for t in trades
        if t.action in (TradeAction.STOP_LOSS, TradeAction.SELL)
    ]

    if not buy_trades:
        return

    # Compute entry averages
    total_buy_qty = sum(t.quantity for t in buy_trades)
    total_buy_cost = sum(t.price * t.quantity for t in buy_trades)
    entry_price_avg = total_buy_cost // total_buy_qty if total_buy_qty else 0

    # Compute exit averages
    total_exit_qty = sum(t.quantity for t in exit_trades)
    total_exit_cost = sum(t.price * t.quantity for t in exit_trades) if exit_trades else 0
    exit_price_avg = total_exit_cost // total_exit_qty if total_exit_qty else None

    net_qty = total_buy_qty - total_exit_qty

    prefix = get_series_prefix(market_ticker)
    city = SERIES_CITY.get(prefix)
    family = detect_family(market_ticker)
    bracket_temp = parse_bracket_temp(market_ticker)

    # ── Determine outcome ────────────────────────────────────────────────────
    # Realized P&L already locked in by earlier SELL/STOP_LOSS fills.  When only
    # part of a position is exited and the remainder settles, we must add this
    # leg back; otherwise an early exit (typically a partial stop-loss loss) is
    # silently dropped from the aggregate P&L.
    outcome: Optional[TradeOutcomeStatus] = None
    realized_pnl: Optional[int] = None

    exited_qty = min(total_buy_qty, total_exit_qty)
    if exited_qty > 0 and exit_price_avg is not None:
        partial_exit_pnl = _compute_pnl(entry_price_avg, exit_price_avg, exited_qty)
    else:
        partial_exit_pnl = 0

    if exit_trades and net_qty <= 0:
        # Fully exited via SELL/STOP_LOSS
        # Use the last exit action to classify
        last_exit_action = exit_trades[-1].action
        outcome = _classify_exit_outcome(last_exit_action)
        exit_qty_used = exited_qty
        realized_pnl = _compute_pnl(entry_price_avg, exit_price_avg, exit_qty_used)

    elif market_date is not None and market_date < today:
        # Position should have settled — query Kalshi
        result = await _fetch_kalshi_market_result(
            market_ticker,
            config.rest_base_url,
            config.kalshi_api_key,
            config.kalshi_private_key_path,
        )
        if result == "yes":
            outcome = TradeOutcomeStatus.SETTLED_WIN
        elif result == "no":
            outcome = TradeOutcomeStatus.SETTLED_LOSS
        else:
            logger.info(
                "reconciler.market_unresolved",
                market_ticker=market_ticker,
                date_prefix=date_prefix,
                kalshi_result=result,
            )
            # Leave as OPEN or keep existing
            outcome = existing.outcome if existing else TradeOutcomeStatus.OPEN

        if result in ("yes", "no"):
            settle_price = 100 if result == "yes" else 0
            remaining_qty = max(min(total_buy_qty, net_qty), 0)
            remaining_pnl = (
                _compute_pnl(entry_price_avg, settle_price, remaining_qty)
                if remaining_qty > 0 else 0
            )
            # Blend the already-realized early-exit leg (partial_exit_pnl) with
            # the settlement leg so an early stop-loss is not dropped.
            realized_pnl = partial_exit_pnl + remaining_pnl
            total_exit_qty = exited_qty + remaining_qty
            exit_price_avg = settle_price

    else:
        # Market still open/today — leave OPEN
        outcome = existing.outcome if existing else TradeOutcomeStatus.OPEN

    # ── Upsert TradeOutcome row ──────────────────────────────────────────────
    async with await db.get_session() as session:
        upsert_q = await session.execute(
            select(TradeOutcome).where(
                TradeOutcome.market_ticker == market_ticker,
                TradeOutcome.date_prefix == date_prefix,
            )
        )
        row: Optional[TradeOutcome] = upsert_q.scalar_one_or_none()

        if row is None:
            row = TradeOutcome(
                series_ticker=series_ticker,
                date_prefix=date_prefix,
                market_ticker=market_ticker,
                city=city,
                family=family,
                entry_price_avg=entry_price_avg,
                entry_qty=total_buy_qty,
                exit_price_avg=exit_price_avg,
                exit_qty=total_exit_qty if exit_trades or outcome in (
                    TradeOutcomeStatus.SETTLED_WIN, TradeOutcomeStatus.SETTLED_LOSS
                ) else None,
                outcome=outcome,
                realized_pnl_cents=realized_pnl,
                bracket_temp=bracket_temp,
                entry_price_bucket=entry_price_bucket(entry_price_avg) if entry_price_avg else None,
            )
            session.add(row)
        else:
            # Only update mutable fields; never overwrite a final outcome with OPEN
            if outcome not in (None, TradeOutcomeStatus.OPEN) or row.outcome == TradeOutcomeStatus.OPEN:
                row.outcome = outcome
            row.entry_price_avg = entry_price_avg
            row.entry_qty = total_buy_qty
            if exit_price_avg is not None:
                row.exit_price_avg = exit_price_avg
            if total_exit_qty > 0:
                row.exit_qty = total_exit_qty
            if realized_pnl is not None:
                row.realized_pnl_cents = realized_pnl
            if row.city is None:
                row.city = city
            if row.family is None:
                row.family = family
            if row.bracket_temp is None:
                row.bracket_temp = bracket_temp

        try:
            await session.commit()
            logger.info(
                "reconciler.outcome_written",
                market_ticker=market_ticker,
                outcome=str(outcome),
                realized_pnl_cents=realized_pnl,
            )
        except Exception as exc:
            await session.rollback()
            logger.warning(
                "reconciler.upsert_failed",
                market_ticker=market_ticker,
                error=str(exc),
            )
