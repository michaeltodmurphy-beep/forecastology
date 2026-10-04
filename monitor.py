"""
forecastology-monitor

Monitors open positions for hedge and reconciliation conditions.
Reads positions from DB, checks current prices via REST.

Responsibilities:
  - Price reconciliation and last_price updates in DB
  - Cleanup of expired/settled positions
  - Optional hedge bracket execution when price drops below hedge_trigger

Role: READ-ONLY / NON-PRIMARY EXECUTOR
  This process must never submit stop-loss sells or primary exit orders.
  Stop-loss execution is owned exclusively by the WebSocket-driven
  StopLossWatcher in run.py.  Hedge buys are the only order-submission
  permitted here, and only when explicitly triggered by the hedge_trigger
  price condition.

Runs every ~30 seconds via systemd timer.
"""

import asyncio
import datetime
import httpx
import structlog
from typing import Optional, Union

from app.config import AppConfig
from app.signing import load_private_key, build_auth_headers
from app.database import DatabaseManager
from app.models import Position as PositionModel, ExecutedTrade, TradeAction, TradeStatus
from core.event_exposure import Holdings, event_exposure_allows_buy
from core.types import OrderRequest, OrderSide, ensure_app_client_order_id
from data.ticker_cache import TickerCache
from execution.base import BaseExecutor, ExecutionResult
from execution.factory import create_executor
from sqlalchemy import select, delete, update

logger = structlog.get_logger(__name__)

MONTH_ORD = {m: i+1 for i, m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}


def _parse_ticker_date(ticker: str) -> Optional[datetime.date]:
    """Parse ticker like KXLOWTSEA-26JUN21-B50.5 and return date."""
    parts = ticker.split('-')
    if len(parts) < 2:
        return None
    date_str = parts[1]
    try:
        year = 2000 + int(date_str[:2])
        month = MONTH_ORD.get(date_str[2:5], 0)
        day = int(date_str[5:])
        return datetime.date(year, month, day)
    except (ValueError, IndexError):
        return None


def _get_event_ticker(ticker: str) -> str:
    """Get event ticker from a market ticker."""
    parts = ticker.split('-')
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return ticker


async def _get_market_price_rest(
    ticker: str,
    private_key,
    api_key: str,
    base_url: str,
    client: httpx.AsyncClient,
) -> Optional[dict]:
    """Fetch market data via REST."""
    path = f"/trade-api/v2/markets/{ticker}"
    url = f"{base_url}{path}"
    headers = build_auth_headers(private_key, api_key, "GET", path)
    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code == 200:
            mkt = resp.json().get("market", {})
            result = {}
            lp = mkt.get("last_price_dollars")
            result["last_price"] = round(float(lp) * 100) if lp and float(lp) > 0 else None
            ya = mkt.get("yes_ask")
            result["yes_ask"] = round(float(ya) * 100) if ya and float(ya) > 0 else None
            yb = mkt.get("yes_bid")
            result["yes_bid"] = round(float(yb) * 100) if yb and float(yb) > 0 else None
            na = mkt.get("no_ask")
            result["no_ask"] = round(float(na) * 100) if na and float(na) > 0 else None
            return result
    except Exception:
        pass
    return None


async def _find_hedge_bracket(
    event_ticker: str,
    config: AppConfig,
    client: httpx.AsyncClient,
) -> Optional[str]:
    """
    Find the highest-priced bracket in the same event to use as hedge.
    Only returns a ticker if its best_ask is above stop_loss price.
    """
    private_key = load_private_key(config.kalshi_private_key_path)
    path = "/trade-api/v2/markets"
    url = f"{config.rest_base_url}{path}"
    headers = build_auth_headers(private_key, config.kalshi_api_key, "GET", path)
    try:
        resp = await client.get(url, headers=headers,
                                params={"event_ticker": event_ticker, "limit": 100})
        if resp.status_code in (200, 201):
            markets = resp.json().get("markets", [])
            best_ticker = None
            best_ask = 0
            for m in markets:
                ticker = m.get("ticker", "")
                if not ticker:
                    continue
                ya = m.get("yes_ask")
                if not ya or float(ya) <= 0:
                    continue
                ask = round(float(ya) * 100)
                if ask > 35 and ask > best_ask:
                    best_ask = ask
                    best_ticker = ticker
            return best_ticker
    except Exception:
        pass
    return None


async def _buy_hedge(
    ticker: str,
    price_cents: int,
    qty: int,
    config: AppConfig,
    client: Optional[httpx.AsyncClient] = None,
    existing_position_qty: int = 0,
    executor: Optional[BaseExecutor] = None,
    db: Optional[DatabaseManager] = None,
    extra_holdings: Optional[Holdings] = None,
) -> Union[ExecutionResult, bool]:
    """Buy a hedge bracket through the shared executor.

    Routes through ``create_executor`` (the same factory the scanner uses) so
    the hedge inherits the executor's V2 ``/portfolio/events/orders`` payload,
    its ``TRADING_MODE`` (PAPER never touches the exchange) and ``DRY_RUN``
    guards, its per-market position cap, and the max-buy-price ceiling
    (``SPREAD_MONITOR_PRICE``).  ``client`` is unused (kept for call-site
    compatibility); this function never issues raw HTTP itself.

    Returns ``False`` when blocked before reaching the executor (hedge cap,
    price ceiling, per-event exposure cap); otherwise returns the executor's
    ``ExecutionResult`` so the caller can reconcile the ACTUAL fill quantity.
    """
    # Hard cap: never submit a hedge order that exceeds the per-step maximum.
    hedge_max_factor = max(int(config.hedge_max_factor), 1)
    max_allowed_qty = config.initial_contract_count * (2 ** (hedge_max_factor - 1))
    total_position_qty = max(int(existing_position_qty or 0), 0) + max(int(qty or 0), 0)
    if qty > max_allowed_qty or total_position_qty > max_allowed_qty:
        logger.critical(
            "hedge.cap_blocked",
            ticker=ticker,
            existing_position_qty=max(int(existing_position_qty or 0), 0),
            proposed_qty=qty,
            total_position_qty=total_position_qty,
            max_allowed_qty=max_allowed_qty,
            initial_contract_count=config.initial_contract_count,
            hedge_factor=hedge_max_factor,
            action="monitor_buy_hedge_blocked",
        )
        return False
    max_price = config.spread_monitor_price
    if price_cents > max_price:
        logger.warning(
            "monitor.hedge_blocked_above_ceiling",
            ticker=ticker,
            price=price_cents,
            max_price=max_price,
            action="monitor_buy_hedge_ceiling_blocked",
        )
        return False
    known_holdings = dict(extra_holdings or {})
    existing_qty = max(int(existing_position_qty or 0), 0)
    if existing_qty > known_holdings.get(ticker, (0, 0))[0]:
        known_holdings[ticker] = (existing_qty, max_price)
    if db is not None and not await event_exposure_allows_buy(
        config, db, ticker, qty, price_cents, source="monitor_hedge",
        extra_holdings=known_holdings,
    ):
        return False

    owns_executor = executor is None
    if owns_executor:
        executor = create_executor(
            trading_mode=config.trading_mode,
            ticker_cache=TickerCache(),
            rest_base_url=config.rest_base_url,
            api_key=config.kalshi_api_key,
            private_key_path=config.kalshi_private_key_path,
            dry_run=config.dry_run,
            max_buy_qty=max_allowed_qty,
            entry_cross_spread_to_ceiling=getattr(config, "entry_cross_spread_to_ceiling", True),
            position_cap_fail_closed=getattr(config, "position_cap_fail_closed", False),
        )
    order = OrderRequest(
        market_ticker=ticker,
        side=OrderSide.BUY_YES,
        price=price_cents,
        quantity=qty,
        client_order_id=ensure_app_client_order_id(),
        is_hedge=True,
        known_position_qty=max(int(existing_position_qty or 0), 0),
    )
    try:
        result = await executor.buy_yes(order, max_price=max_price)
    except Exception as e:
        logger.error("monitor.hedge_error", ticker=ticker, error=str(e))
        return False
    finally:
        if owns_executor and hasattr(executor, "close"):
            try:
                await executor.close()
            except Exception:
                pass
    logger.info(
        "monitor.hedge_result",
        ticker=ticker,
        price=price_cents,
        requested_qty=qty,
        fill_qty=result.fill_quantity,
        fill_price=result.fill_price,
        status=result.status,
        success=result.success,
    )
    return result


async def run_monitor_cycle(config: AppConfig, db: DatabaseManager):
    """
    One monitor cycle:
    1. Load open positions from DB
    2. For each position, fetch current price via REST
    3. If price <= hedge_trigger: buy opposite bracket
    4. Clean up old/expired positions

    Stop-loss execution is owned exclusively by the WebSocket-driven
    StopLossWatcher in run.py and is NOT executed here.

    Role enforcement: this function must never submit primary exit/stop-loss
    orders.  It is a read-mostly reconciliation process.  Only hedge buys
    are permitted.
    """
    today = datetime.date.today()

    async with await db.get_session() as session:
        result = await session.execute(
            select(PositionModel).where(PositionModel.quantity > 0)
        )
        positions = result.scalars().all()

    if not positions:
        logger.debug("monitor.no_positions")
        return

    logger.info("monitor.positions_to_check", count=len(positions))

    private_key = load_private_key(config.kalshi_private_key_path)

    async with httpx.AsyncClient(timeout=15.0) as client:
        standalone_qty = {}
        standalone_prices = {}
        parent_hedge_qty = {}
        legacy_targets = {}
        for pos in positions:
            standalone_qty[pos.market_ticker] = max(int(pos.quantity or 0), 0)
            standalone_prices[pos.market_ticker] = (
                max(int(pos.avg_entry_price or 0), 0) or config.spread_monitor_price
            )
            qty = max(int(pos.hedge_quantity or 0), 0)
            target = pos.hedge_market_ticker
            if qty > 0 and not target:
                # Older partial fills did not persist their hedge target.
                target = await _find_hedge_bracket(_get_event_ticker(pos.market_ticker), config, client)
                legacy_targets[pos.market_ticker] = target
            if qty > 0 and target:
                parent_hedge_qty[target] = parent_hedge_qty.get(target, 0) + qty
        known_qty = {
            ticker: max(standalone_qty.get(ticker, 0), parent_hedge_qty.get(ticker, 0))
            for ticker in standalone_qty.keys() | parent_hedge_qty.keys()
        }
        known_cost = {
            ticker: qty * (
                standalone_prices[ticker]
                if standalone_qty.get(ticker, 0) >= parent_hedge_qty.get(ticker, 0)
                else config.spread_monitor_price
            )
            for ticker, qty in known_qty.items()
        }
        for pos in positions:
            ticker = pos.market_ticker

            # Fetch current price via REST (authoritative source).
            price_data = await _get_market_price_rest(
                ticker, private_key,
                config.kalshi_api_key, config.rest_base_url, client
            )
            if not price_data:
                continue

            current_price = price_data.get("last_price") or price_data.get("yes_ask") or 0
            yes_ask = price_data.get("yes_ask") or 0
            yes_bid = price_data.get("yes_bid") or 0

            logger.debug("monitor.price_check", ticker=ticker,
                         price=current_price, entry=pos.avg_entry_price)

            # Update DB last_price
            async with await db.get_session() as session:
                await session.execute(
                    update(PositionModel)
                    .where(PositionModel.market_ticker == ticker)
                    .values(last_price=current_price)
                )
                await session.commit()

            # Check if expired/settled (both ask and bid are 0)
            if yes_ask == 0 and yes_bid == 0:
                ticker_date = _parse_ticker_date(ticker)
                if ticker_date and ticker_date < today:
                    logger.info("monitor.position_expired", ticker=ticker)
                    async with await db.get_session() as session:
                        await session.execute(
                            delete(PositionModel).where(PositionModel.market_ticker == ticker)
                        )
                        await session.commit()
                    continue

            # Stop-loss execution is owned by the websocket-driven watcher in run.py.
            # Keep monitor focused on reconciliation and hedge fallback logic.
            # The target is retained on partial fills; only the actual quantity
            # determines completion, and retries stay on that same target.
            already_hedged_qty = max(int(pos.hedge_quantity or 0), 0)
            needed_hedge_qty = max(int(pos.quantity or 0), 0) - already_hedged_qty
            if (current_price <= config.hedge_trigger_price and current_price > 0
                    and needed_hedge_qty > 0):
                event_ticker = _get_event_ticker(ticker)

                hedge_ticker = pos.hedge_market_ticker or legacy_targets.get(ticker)
                if not hedge_ticker:
                    hedge_ticker = await _find_hedge_bracket(event_ticker, config, client)

                if hedge_ticker is None:
                    logger.warning("monitor.hedge_no_bracket_found", ticker=ticker)
                    continue

                # Get hedge bracket price
                hedge_data = await _get_market_price_rest(
                    hedge_ticker,
                    private_key,
                    config.kalshi_api_key, config.rest_base_url, client
                )
                if not hedge_data:
                    continue

                hedge_price = hedge_data.get("yes_ask") or hedge_data.get("last_price") or 0
                if hedge_price <= 0:
                    continue

                logger.info("monitor.hedge_attempt", ticker=ticker,
                           hedge_ticker=hedge_ticker, hedge_price=hedge_price)

                existing_hedge_qty = known_qty.get(hedge_ticker, 0)

                result = await _buy_hedge(
                    hedge_ticker,
                    hedge_price,
                    needed_hedge_qty,
                    config,
                    client,
                    existing_position_qty=existing_hedge_qty,
                    db=db,
                    extra_holdings={
                        target: (qty, (known_cost[target] + qty - 1) // qty)
                        for target, qty in known_qty.items() if qty > 0
                    },
                )

                if not isinstance(result, ExecutionResult):
                    continue  # blocked before submission; retried next cycle

                # Trust the reported fill quantity even on a non-success status so
                # contracts that did fill are never forgotten (and re-bought).
                filled_qty = max(int(result.fill_quantity or 0), 0)
                if filled_qty <= 0:
                    # Nothing filled (IOC expired / rejected / DRY_RUN): do NOT
                    # mark hedged, so the next monitor cycle retries.
                    logger.warning("monitor.hedge_not_filled", ticker=ticker,
                                   hedge_ticker=hedge_ticker, status=result.status,
                                   requested_qty=needed_hedge_qty)
                    continue

                fill_price = int(result.fill_price or 0) or hedge_price
                fully_filled = filled_qty >= needed_hedge_qty
                new_hedged_qty = already_hedged_qty + filled_qty
                position_values = {
                    "hedge_quantity": new_hedged_qty,
                    "hedge_market_ticker": hedge_ticker,
                }
                async with await db.get_session() as session:
                    session.add(ExecutedTrade(
                        market_ticker=hedge_ticker,
                        action=TradeAction.HEDGE,
                        side="yes",
                        price=fill_price,
                        quantity=filled_qty,
                        total_cost_cents=result.total_cost_cents,
                        trade_mode=config.trading_mode,
                        status=TradeStatus.FILLED if fully_filled else TradeStatus.PARTIAL,
                        kalshi_order_id=result.order_id or None,
                        notes=result.notes,
                    ))
                    await session.execute(
                        update(PositionModel)
                        .where(PositionModel.market_ticker == ticker)
                        .values(**position_values)
                    )
                    await session.commit()
                known_qty[hedge_ticker] = existing_hedge_qty + filled_qty
                known_cost[hedge_ticker] = (
                    known_cost.get(hedge_ticker, 0) + max(int(result.total_cost_cents or 0), 0)
                )
                pos.hedge_quantity = new_hedged_qty
                pos.hedge_market_ticker = hedge_ticker
                if fully_filled:
                    logger.info("monitor.hedge_executed", ticker=ticker,
                                hedge_ticker=hedge_ticker, qty=new_hedged_qty)
                else:
                    logger.warning("monitor.hedge_partial_fill", ticker=ticker,
                                   hedge_ticker=hedge_ticker, filled_qty=filled_qty,
                                   requested_qty=needed_hedge_qty,
                                   hedged_qty=new_hedged_qty,
                                   position_qty=pos.quantity)

    logger.info("monitor.cycle_complete", checked=len(positions))


def main():
    """Entry point for systemd timer."""
    config = AppConfig.from_env()

    logger.info("monitor.start", mode=config.trading_mode)
    if config.trading_mode == "LIVE":
        if "demo" in config.rest_base_url.lower() or "demo" in config.ws_url.lower():
            raise RuntimeError("LIVE mode must use Kalshi PRODUCTION URLs.")
        logger.warning("monitor.live_mode", message="REAL MONEY TRADING ENABLED")

    db = DatabaseManager(config.mysql_database_url)

    async def _run():
        await db.initialize()
        try:
            await run_monitor_cycle(config, db)
        finally:
            await db.dispose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
