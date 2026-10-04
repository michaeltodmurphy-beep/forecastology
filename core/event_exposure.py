# core/event_exposure.py
"""Per-event (series + date, i.e. city/day) aggregate exposure cap.

Brackets within one Kalshi temperature event are mutually exclusive, so
holding several of them for the same city/day stacks exposure without adding
edge.  The per-ticker caps cannot see this, and the per-cycle duplicate-entry
set in the state machine resets every watchlist sweep.  This module computes
the event's existing exposure from the DB-backed ``positions`` table (plus any
caller-supplied in-memory / in-flight quantities) and refuses a buy that would
push the event over the configured cap.  Being DB-backed, the check persists
across cycles and processes.
"""
import datetime
from typing import Optional

import structlog
from sqlalchemy import select

from app.models import Position as PositionModel
from core.local_time_gate import SERIES_TIMEZONE, ZoneInfo

logger = structlog.get_logger(__name__)

# (quantity, price_cents) keyed by market ticker.
Holdings = dict[str, tuple[int, int]]


def event_key(market_ticker: str) -> Optional[str]:
    """Return the event key ``SERIES-DATE`` for a market ticker, e.g.
    ``KXLOWTSEA-26JUN21-B50.5`` -> ``KXLOWTSEA-26JUN21``."""
    parts = (market_ticker or "").upper().split("-")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        return None
    return f"{parts[0]}-{parts[1]}"


def resolve_event_caps(config) -> tuple[Optional[int], Optional[int]]:
    """Return ``(max_contracts, max_cost_cents)``; ``None`` means disabled."""
    raw_contracts = int(getattr(config, "event_max_contracts", 0) or 0)
    raw_cost = int(getattr(config, "event_max_cost_cents", 0) or 0)
    return (
        raw_contracts if raw_contracts > 0 else None,
        raw_cost if raw_cost > 0 else None,
    )


def _event_is_expired(key: str) -> bool:
    # Position has no settlement/expiry fields; closed rows are deleted or zeroed.
    # Compare in the event city's timezone so an Eastern rollover cannot drop
    # still-active West-coast holdings. Unknown series use Eastern time.
    try:
        series, date_prefix = key.split("-")
        if len(date_prefix) != 7:
            return False
        market_date = datetime.datetime.strptime("20" + date_prefix, "%Y%b%d").date()
        timezone = SERIES_TIMEZONE.get(series, "America/New_York")
        today = datetime.datetime.now(ZoneInfo(timezone)).date()
    except (ValueError, IndexError):
        return False
    return market_date < today


async def load_event_holdings(db, market_ticker: str, default_price_cents: int) -> Holdings:
    """Return DB-recorded open positions (qty > 0) sharing *market_ticker*'s event.

    Stale DB rows are excluded; today's positions remain active. Caller-known
    holdings and live orders are not filtered by this calendar-date heuristic.
    Raises on DB errors so callers can apply their configured failure policy.
    """
    key = event_key(market_ticker)
    holdings: Holdings = {}
    if key is None or _event_is_expired(key):
        return holdings
    async with await db.get_session() as session:
        result = await session.execute(
            select(PositionModel).where(PositionModel.quantity > 0)
        )
        rows = result.scalars().all()
    for pos in rows:
        ticker = getattr(pos, "market_ticker", "") or ""
        if event_key(ticker) != key:
            continue
        qty = max(int(getattr(pos, "quantity", 0) or 0), 0)
        if qty <= 0:
            continue
        price = int(getattr(pos, "avg_entry_price", 0) or 0)
        holdings[ticker] = (qty, price if price > 0 else default_price_cents)
    return holdings


def _merge(base: Holdings, extra: Optional[Holdings]) -> Holdings:
    """Merge per-ticker holdings, keeping the larger quantity for each ticker."""
    out = dict(base)
    for ticker, (qty, price) in (extra or {}).items():
        qty = max(int(qty or 0), 0)
        if qty <= 0:
            continue
        if ticker not in out or qty > out[ticker][0]:
            out[ticker] = (qty, int(price or 0))
    return out


async def event_exposure_allows_buy(
    config,
    db,
    market_ticker: str,
    proposed_qty: int,
    proposed_price_cents: int,
    extra_holdings: Optional[Holdings] = None,
    inflight: Optional[Holdings] = None,
    source: str = "",
    target_existing_qty: Optional[int] = None,
) -> bool:
    """Return True if buying *proposed_qty* @ *proposed_price_cents* keeps the
    event within its aggregate cap.  Logs a structured event and returns False
    when the cap would be exceeded. Unverifiable exposure blocks only when
    ``event_exposure_fail_closed`` is enabled.
    Proposed cost reserves the submission ceiling when spread crossing is
    enabled (the default), while held positions retain their actual cost basis.

    ``extra_holdings`` are other known positions (e.g. in-memory state) merged
    with the DB per ticker (max qty wins).  ``inflight`` are unfilled/working
    orders, added on top.  ``target_existing_qty``, when given, is the
    caller's already-verified current quantity for *market_ticker* itself
    (e.g. from the live exchange) and replaces the DB/in-memory figure for
    that one ticker; sibling brackets are always taken from DB + memory.
    """
    max_contracts, max_cost = resolve_event_caps(config)
    if max_contracts is None and max_cost is None:
        return True
    key = event_key(market_ticker)
    if key is None:
        return True
    ceiling = max(int(getattr(config, "spread_monitor_price", 100) or 100), 1)
    try:
        holdings = await load_event_holdings(db, market_ticker, ceiling)
    except Exception as e:  # noqa: BLE001 - apply policy to any lookup error
        fail_closed = getattr(config, "event_exposure_fail_closed", False)
        logger.critical(
            "entry.event_exposure_unverifiable",
            ticker=market_ticker,
            event_ticker=key,
            source=source,
            error=str(e),
            action="event_exposure_cap_blocked" if fail_closed else "event_exposure_cap_bypassed",
        )
        return not fail_closed
    holdings = _merge(
        holdings,
        {
            t: v for t, v in (extra_holdings or {}).items()
            if event_key(t) == key
        },
    )
    if target_existing_qty is not None:
        target = (market_ticker or "").upper()
        for t in [t for t in holdings if t.upper() == target]:
            prior_price = holdings.pop(t)[1]
            if target_existing_qty > 0:
                holdings[t] = (int(target_existing_qty), prior_price)
        if target_existing_qty > 0 and not any(t.upper() == target for t in holdings):
            holdings[market_ticker] = (int(target_existing_qty), max(int(proposed_price_cents or 0), 0) or ceiling)
    existing_qty = sum(q for q, _ in holdings.values())
    existing_cost = sum(q * p for q, p in holdings.values())
    for ticker, (qty, price) in (inflight or {}).items():
        if event_key(ticker) != key:
            continue
        qty = max(int(qty or 0), 0)
        existing_qty += qty
        existing_cost += qty * (int(price or 0) or ceiling)

    add_qty = max(int(proposed_qty or 0), 0)
    proposed_limit = (
        ceiling if getattr(config, "entry_cross_spread_to_ceiling", True)
        else min(max(int(proposed_price_cents or 0), 0), ceiling)
    )
    add_cost = add_qty * proposed_limit
    total_qty = existing_qty + add_qty
    total_cost = existing_cost + add_cost
    over_contracts = max_contracts is not None and total_qty > max_contracts
    over_cost = max_cost is not None and total_cost > max_cost
    if over_contracts or over_cost:
        logger.warning(
            "entry.event_exposure_cap_blocked",
            ticker=market_ticker,
            event_ticker=key,
            source=source,
            existing_qty=existing_qty,
            existing_cost_cents=existing_cost,
            proposed_qty=add_qty,
            proposed_cost_cents=add_cost,
            total_qty=total_qty,
            total_cost_cents=total_cost,
            max_contracts=max_contracts,
            max_cost_cents=max_cost,
            event_tickers=sorted(holdings.keys()),
            reason="contracts" if over_contracts else "cost",
            action="event_exposure_cap_blocked",
        )
        return False
    return True
