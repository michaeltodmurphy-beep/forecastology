"""
forecastology-monitor

Monitors open positions for hedge and stop-loss conditions.
Reads positions from DB, checks current prices via shared state file or REST.

Runs every ~30 seconds via systemd timer.
"""

import asyncio
import json
import uuid
import datetime
import httpx
import structlog
from typing import Optional

from app.config import AppConfig
from app.signing import load_private_key, build_auth_headers
from app.database import DatabaseManager
from app.models import Position as PositionModel, ExecutedTrade, TradeAction, TradeStatus
from sqlalchemy import select, delete, update

logger = structlog.get_logger(__name__)

SHARED_STATE_FILE = "/dev/shm/forecastology_state.json"
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


def _read_state() -> dict:
    """Read the shared state file."""
    try:
        with open(SHARED_STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return {"prices": {}, "orderbooks": {}, "all_markets": []}


async def _get_market_price_rest(
    ticker: str,
    private_key,
    api_key: str,
    base_url: str,
    client: httpx.AsyncClient,
) -> Optional[dict]:
    """Fetch market data via REST as fallback."""
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


async def _get_current_price(ticker: str) -> Optional[dict]:
    """Get current price from shared state first, then REST."""
    state = _read_state()
    ob = state.get("orderbooks", {}).get(ticker)
    if ob:
        return {
            "last_price": ob.get("best_ask"),
            "yes_ask": ob.get("best_ask"),
            "yes_bid": ob.get("best_bid"),
        }
    price = state.get("prices", {}).get(ticker)
    if price:
        return {"last_price": price, "yes_ask": price, "yes_bid": None}
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


async def _sell_position(
    ticker: str,
    qty: int,
    price_cents: int,
    config: AppConfig,
    client: httpx.AsyncClient,
) -> bool:
    """Sell a position at 1¢ for stop-loss."""
    private_key = load_private_key(config.kalshi_private_key_path)
    order_id = str(uuid.uuid4())
    payload = {
        "ticker": ticker,
        "side": "ask",
        "type": "limit",
        "price": f"{price_cents / 100:.4f}",
        "count": f"{qty}.00",
        "client_order_id": order_id,
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }
    path = "/trade-api/v2/portfolio/orders"
    url = f"{config.rest_base_url}{path}"
    headers = build_auth_headers(private_key, config.kalshi_api_key, "POST", path)
    headers["Content-Type"] = "application/json"
    try:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code in (200, 201):
            logger.info("monitor.sold", ticker=ticker, price=price_cents, qty=qty)
            return True
        else:
            logger.warning("monitor.sell_rejected", ticker=ticker,
                           status=resp.status_code)
            return False
    except Exception as e:
        logger.error("monitor.sell_error", ticker=ticker, error=str(e))
        return False


async def _buy_hedge(
    ticker: str,
    price_cents: int,
    qty: int,
    config: AppConfig,
    client: httpx.AsyncClient,
) -> bool:
    """Buy a hedge bracket at the given price with max_price for fill guarantee."""
    private_key = load_private_key(config.kalshi_private_key_path)
    max_price = 90
    order_id = str(uuid.uuid4())
    price_str = f"{price_cents / 100:.4f}"
    max_price_str = f"{max_price / 100:.4f}"
    payload = {
        "ticker": ticker,
        "side": "bid",
        "type": "limit",
        "price": max_price_str if max_price > price_cents else price_str,
        "count": f"{qty}.00",
        "client_order_id": order_id,
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }
    path = "/trade-api/v2/portfolio/orders"
    url = f"{config.rest_base_url}{path}"
    headers = build_auth_headers(private_key, config.kalshi_api_key, "POST", path)
    headers["Content-Type"] = "application/json"
    try:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code in (200, 201):
            logger.info("monitor.hedge_filled", ticker=ticker, price=price_cents, qty=qty)
            return True
        else:
            logger.warning("monitor.hedge_rejected", ticker=ticker,
                           status=resp.status_code)
            return False
    except Exception as e:
        logger.error("monitor.hedge_error", ticker=ticker, error=str(e))
        return False


async def run_monitor_cycle(config: AppConfig, db: DatabaseManager):
    """
    One monitor cycle:
    1. Load open positions from DB
    2. For each position, get current price (shared state -> REST)
    3. If price <= stop_loss (35): sell at 1¢
    4. If price <= hedge_trigger (48): buy opposite bracket
    5. Clean up old/expired positions
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

    async with httpx.AsyncClient(timeout=15.0) as client:
        for pos in positions:
            ticker = pos.market_ticker

            # Get current price (shared state first, REST fallback)
            price_data = await _get_current_price(ticker)
            if not price_data or (price_data.get("yes_ask") is None and price_data.get("last_price") is None):
                # REST fallback
                private_key = load_private_key(config.kalshi_private_key_path)
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

            # STOP LOSS: price <= stop_loss (35¢)
            if current_price <= config.stop_loss_price and current_price > 0:
                logger.info("monitor.stop_loss_triggered", ticker=ticker,
                           price=current_price, stop_loss=config.stop_loss_price)

                success = await _sell_position(
                    ticker, pos.quantity, 1, config, client
                )

                if success:
                    async with await db.get_session() as session:
                        session.add(ExecutedTrade(
                            market_ticker=ticker,
                            action=TradeAction.STOP_LOSS,
                            side="yes",
                            price=1,
                            quantity=pos.quantity,
                            total_cost_cents=1 * pos.quantity,
                            trade_mode=config.trading_mode,
                            status=TradeStatus.FILLED,
                        ))
                        await session.execute(
                            delete(PositionModel).where(PositionModel.market_ticker == ticker)
                        )
                        await session.commit()
                    logger.info("monitor.stop_loss_executed", ticker=ticker)

            # HEDGE TRIGGER: price <= hedge_trigger (48¢) — only if not already hedged
            elif (current_price <= config.hedge_trigger_price and current_price > 0
                  and not pos.hedge_market_ticker):
                event_ticker = _get_event_ticker(ticker)

                hedge_ticker = await _find_hedge_bracket(
                    event_ticker, config, client
                )

                if hedge_ticker is None:
                    logger.warning("monitor.hedge_no_bracket_found", ticker=ticker)
                    continue

                # Get hedge bracket price
                hedge_data = await _get_market_price_rest(
                    hedge_ticker,
                    load_private_key(config.kalshi_private_key_path),
                    config.kalshi_api_key, config.rest_base_url, client
                )
                if not hedge_data:
                    continue

                hedge_price = hedge_data.get("yes_ask") or hedge_data.get("last_price") or 0
                if hedge_price <= 0:
                    continue

                logger.info("monitor.hedge_attempt", ticker=ticker,
                           hedge_ticker=hedge_ticker, hedge_price=hedge_price)

                success = await _buy_hedge(
                    hedge_ticker, hedge_price, pos.quantity, config, client
                )

                if success:
                    async with await db.get_session() as session:
                        session.add(ExecutedTrade(
                            market_ticker=hedge_ticker,
                            action=TradeAction.HEDGE,
                            side="yes",
                            price=hedge_price,
                            quantity=pos.quantity,
                            total_cost_cents=hedge_price * pos.quantity,
                            trade_mode=config.trading_mode,
                            status=TradeStatus.FILLED,
                        ))
                        await session.execute(
                            update(PositionModel)
                            .where(PositionModel.market_ticker == ticker)
                            .values(hedge_market_ticker=hedge_ticker,
                                    hedge_quantity=pos.quantity)
                        )
                        await session.commit()
                    logger.info("monitor.hedge_executed", ticker=ticker,
                               hedge_ticker=hedge_ticker)

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
