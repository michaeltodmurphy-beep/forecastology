"""
forecastology-scanner

Standalone buy scanner that fetches current market data via the Kalshi REST
API and places buy orders when conditions are met.

IMPORTANT: This script gates execution against the run.py daemon lockfile.
If run.py is already running, the scanner exits immediately without placing
any orders.  When run.py is active ALL buy decisions are handled by its
integrated TemperatureStrategy / WebSocket state machine, and a parallel
scanner would cause split-brain order execution.

This script is intended for use only in legacy deployments where run.py is
NOT running as an always-on daemon.  When both services are deployed, disable
the scanner systemd timer and rely on run.py exclusively.
"""

import asyncio
import fcntl
import os
import uuid
import httpx
import structlog
from typing import Optional

from app.config import AppConfig
from app.signing import load_private_key, build_auth_headers
from app.database import DatabaseManager
from app.models import ExecutedTrade, TradeAction, TradeStatus, Position as PositionModel
from core.constants import SERIES_LIST, get_eastern_today_date_prefix
from core.types import ensure_app_client_order_id
from sqlalchemy import select

logger = structlog.get_logger(__name__)

DAEMON_LOCKFILE = os.getenv("FORECASTOLOGY_LOCKFILE", "/tmp/forecastology.lock")


def _daemon_is_running() -> bool:
    """Return True if the run.py daemon currently holds the process lockfile.

    Uses a non-blocking exclusive flock attempt.  If the lock is already held
    by another process the acquisition fails, meaning run.py is active.
    """
    try:
        handle = open(DAEMON_LOCKFILE, "r")
    except FileNotFoundError:
        return False
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Successfully acquired → no daemon running; release immediately.
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        return False
    except OSError:
        # Lock is held by another process → daemon is running.
        handle.close()
        return True


async def _fetch_markets_via_rest(
    config: AppConfig,
    client: httpx.AsyncClient,
) -> tuple[list, dict]:
    """
    Fetch today's temperature markets and their prices via the Kalshi REST API.

    Returns:
        all_markets: list of market tickers available today
        orderbooks:  dict[ticker -> {"best_ask": int, "best_bid": int, "spread": int|None}]
    """
    today_prefix = get_eastern_today_date_prefix()
    all_markets: list = []
    orderbooks: dict = {}

    private_key = load_private_key(config.kalshi_private_key_path)
    path = "/trade-api/v2/markets"
    url = f"{config.rest_base_url}{path}"

    event_tickers = [f"{s}-{today_prefix}" for s in SERIES_LIST]

    async def _fetch_event(event_ticker: str) -> list:
        headers = build_auth_headers(private_key, config.kalshi_api_key, "GET", path)
        try:
            resp = await client.get(url, headers=headers,
                                    params={"event_ticker": event_ticker, "limit": 100})
            if resp.status_code in (200, 201):
                return resp.json().get("markets", [])
        except Exception as e:
            logger.warning("scanner.fetch_event_error", event_ticker=event_ticker, error=str(e))
        return []

    results = await asyncio.gather(*[_fetch_event(et) for et in event_tickers])

    for mkts in results:
        for m in mkts:
            ticker = m.get("ticker", "")
            if not ticker:
                continue
            all_markets.append(ticker)
            ya = m.get("yes_ask")
            yb = m.get("yes_bid")
            if ya and float(ya) > 0:
                ask = round(float(ya) * 100)
                bid = round(float(yb) * 100) if yb and float(yb) > 0 else 0
                spread = ask - bid if bid > 0 else None
                orderbooks[ticker] = {"best_ask": ask, "best_bid": bid, "spread": spread}

    logger.debug("scanner.markets_fetched", count=len(all_markets),
                 with_prices=len(orderbooks))
    return all_markets, orderbooks


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

    order_id = ensure_app_client_order_id(str(uuid.uuid4()))
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
    1. Fetch today's markets and prices via REST API
    2. For each market with price data, check buy conditions
    3. If price >= buy_trigger and spread <= min_spread, buy
    4. Log successful buys to DB as positions

    Runs every ~2 seconds via systemd timer (only when run.py daemon is not active).
    """
    markets_to_check: list = []
    held_tickers: set[str] = set()
    buy_attempts = 0

    async with httpx.AsyncClient(timeout=15.0) as client:
        all_markets, orderbooks = await _fetch_markets_via_rest(config, client)

        if not all_markets:
            logger.debug("scanner.no_markets")
            return

        logger.debug("scanner.markets_loaded", markets=len(all_markets),
                     with_prices=len(orderbooks))

        # Load held tickers from DB to avoid re-buying
        async with await db.get_session() as session:
            result = await session.execute(
                select(PositionModel.market_ticker).where(PositionModel.quantity > 0)
            )
            held_tickers = {row[0] for row in result.fetchall()}

        buy_trigger = config.buy_trigger_price
        min_spread = config.minimum_spread

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
    """Entry point for systemd timer. Runs one scan cycle and exits.

    Exits immediately (without placing any orders) if the run.py daemon is
    already running, to prevent split-brain order execution.
    """
    # Critical #1 guard: do not compete with the always-on run.py daemon.
    if _daemon_is_running():
        logger.info(
            "scanner.daemon_active_skip",
            message="run.py daemon is running — scanner will not execute to avoid split-brain",
            lockfile=DAEMON_LOCKFILE,
        )
        return

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
