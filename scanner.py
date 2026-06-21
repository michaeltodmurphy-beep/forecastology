"""
forecastology-scanner

A lightweight, stateless scanner that reads market data from the shared
state file (written by run.py WS daemon) and buys when conditions are met.

NO in-memory bracket tracking. NO WebSocket connection. NO loops through all markets.
Just: read state -> check prices -> buy if conditions met.

Designed to run as a systemd timer every ~2 seconds.
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
from app.models import ExecutedTrade, TradeAction, TradeStatus, Position as PositionModel
from sqlalchemy import select, delete, update

logger = structlog.get_logger(__name__)

SHARED_STATE_FILE = "/dev/shm/forecastology_state.json"


def _read_state() -> dict:
    """Read the shared state file written by the WS daemon."""
    try:
        with open(SHARED_STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return {"prices": {}, "orderbooks": {}, "all_markets": []}


async def buy_market(
    config: AppConfig,
    ticker: str,
    price_cents: int,
    client: httpx.AsyncClient,
) -> bool:
    """
    Place a buy order for a market at the given price.
    Uses spread_monitor_price as max_price to ensure fill.
    Returns True if filled, False otherwise.
    """
    private_key = load_private_key(config.kalshi_private_key_path)
    max_price = config.spread_monitor_price  # e.g. 90

    order_id = str(uuid.uuid4())
    price_str = f"{price_cents / 100:.4f}"
    max_price_str = f"{max_price / 100:.4f}"

    payload = {
        "ticker": ticker,
        "side": "bid",
        "type": "limit",
        "price": max_price_str if max_price > price_cents else price_str,
        "count": f"{config.initial_contract_count}.00",
        "client_order_id": order_id,
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }

    path = "/trade-api/v2/portfolio/orders"
    url = f"{config.rest_base_url}{path}"
    headers = build_auth_headers(private_key, config.kalshi_api_key, "POST", path)
    headers["Content-Type"] = "application/json"

    logger.info("scanner.buy_attempt", ticker=ticker, price=price_cents,
                max_price=max_price, qty=config.initial_contract_count)

    try:
        resp = await client.post(url, json=payload, headers=headers)
        data = resp.json()

        if resp.status_code in (200, 201):
            fill = data.get("fill", {})
            fill_price = fill.get("price", price_cents)
            fill_qty = fill.get("count", config.initial_contract_count)
            logger.info("scanner.buy_filled", ticker=ticker,
                        price=fill_price, qty=fill_qty)
            return True
        else:
            logger.warning("scanner.buy_rejected", ticker=ticker,
                           status=resp.status_code, response=data)
            return False
    except Exception as e:
        logger.error("scanner.buy_error", ticker=ticker, error=str(e))
        return False


async def run_scan_cycle(config: AppConfig, db: DatabaseManager):
    """
    One scan cycle:
    1. Read state from shared file (written by WS daemon)
    2. For each market with price data, check buy conditions
    3. If price >= buy_trigger and spread <= min_spread, buy
    4. Log successful buys to DB as positions

    Runs every ~2 seconds via systemd timer.
    """
    state = _read_state()
    all_markets = state.get("all_markets", [])
    prices = state.get("prices", {})
    orderbooks = state.get("orderbooks", {})

    if not all_markets:
        logger.debug("scanner.no_markets")
        return

    logger.debug("scanner.state_read", markets=len(all_markets),
                 prices=len(prices), orderbooks=len(orderbooks))

    # Load held tickers from DB to avoid re-buying
    held_tickers: set[str] = set()
    async with await db.get_session() as session:
        result = await session.execute(
            select(PositionModel.market_ticker).where(PositionModel.quantity > 0)
        )
        held_tickers = {row[0] for row in result.fetchall()}

    buy_trigger = config.buy_trigger_price      # 85
    min_spread = config.minimum_spread            # 7

    # Track which tickers we attempt to buy (max 3 per cycle)
    buy_attempts = 0
    max_buy_attempts = 3

    # Check all markets that have price data
    markets_to_check = [t for t in all_markets if t in orderbooks and t not in held_tickers]

    for ticker in markets_to_check:
        ob = orderbooks[ticker]
        ask = ob.get("best_ask")
        bid = ob.get("best_bid")
        spread = ob.get("spread")

        if ask is None or bid is None or spread is None:
            continue

        # Condition: ask >= buy_trigger AND spread <= min_spread
        if ask >= buy_trigger and spread <= min_spread:
            logger.info("scanner.buy_signal", ticker=ticker,
                        ask=ask, bid=bid, spread=spread)

            async with httpx.AsyncClient(timeout=15.0) as client:
                success = await buy_market(config, ticker, ask, client)

                if success:
                    buy_attempts += 1
                    # Log to DB as a position
                    async with await db.get_session() as session:
                        # Extract event info from ticker
                        parts = ticker.split('-')
                        event_ticker = f"{parts[0]}-{parts[1]}" if len(parts) >= 2 else ""
                        series_ticker = parts[0] if parts else ""

                        # Save position
                        pos = PositionModel(
                            market_ticker=ticker,
                            event_ticker=event_ticker,
                            series_ticker=series_ticker,
                            side="yes",
                            quantity=config.initial_contract_count,
                            avg_entry_price=ask,
                            last_price=ask,
                        )
                        session.add(pos)

                        # Log executed trade
                        trade = ExecutedTrade(
                            market_ticker=ticker,
                            action=TradeAction.BUY,
                            side="yes",
                            price=ask,
                            quantity=config.initial_contract_count,
                            total_cost_cents=ask * config.initial_contract_count,
                            trade_mode=config.trading_mode,
                            status=TradeStatus.FILLED,
                        )
                        session.add(trade)
                        await session.commit()

                if buy_attempts >= max_buy_attempts:
                    logger.info("scanner.max_buy_attempts_reached", count=max_buy_attempts)
                    break

    logger.info("scanner.cycle_complete", checked=len(markets_to_check),
                 held=len(held_tickers), buys=buy_attempts)


def main():
    """Entry point for systemd timer. Runs one scan cycle and exits."""
    config = AppConfig.from_env()

    logger.info("scanner.start", mode=config.trading_mode)
    if config.trading_mode == "LIVE":
        if "demo" in config.rest_base_url.lower() or "demo" in config.ws_url.lower():
            raise RuntimeError("LIVE mode must use Kalshi PRODUCTION URLs.")
        logger.warning("scanner.live_mode", message="REAL MONEY TRADING ENABLED")

    db = DatabaseManager(config.mysql_database_url)

    async def _run():
        await db.initialize()
        try:
            await run_scan_cycle(config, db)
        finally:
            await db.dispose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
