"""
forecastology-scanner

A lightweight, stateless scanner that reads Kalshi WebSocket ticker data
and buys when conditions are met.

NO in-memory bracket tracking. NO loops through all markets.
Just: ticker arrives -> check price/spread -> buy if conditions met.

Designed to run as a systemd timer every ~2 seconds.
"""

import asyncio
import json
import uuid
import datetime
import httpx
import structlog
import websockets
from typing import Optional

from app.config import AppConfig
from app.signing import load_private_key, build_auth_headers, build_ws_headers
from app.database import DatabaseManager
from app.models import ExecutedTrade, TradeAction, TradeStatus

logger = structlog.get_logger(__name__)

# Cache for today's ticker date prefix (e.g. "26JUN21") to avoid re-parsing
_TODAY_PREFIX: Optional[str] = None


def _get_today_prefix() -> str:
    """Return today's date prefix in Kalshi format (e.g. '26JUN21')."""
    global _TODAY_PREFIX
    if _TODAY_PREFIX is None:
        months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=-4)
        _TODAY_PREFIX = f"{now.strftime('%y')}{months[now.month-1]}{now.strftime('%d')}"
    return _TODAY_PREFIX


def _is_today_market(ticker: str) -> bool:
    """Quick check if a ticker is for today's date."""
    parts = ticker.split('-')
    return len(parts) >= 2 and parts[1] == _get_today_prefix()


def _is_temp_market(ticker: str) -> bool:
    """Check if ticker is a KXHIGH/KXLOW temperature market."""
    t = ticker.upper()
    return "KXHIGH" in t or "KXLOW" in t


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

    # Build payload
    price_str = f"{price_cents / 100:.4f}"
    max_price_str = f"{max_price / 100:.4f}"
    order_id = str(uuid.uuid4())

    payload = {
        "ticker": ticker,
        "side": "bid",
        "type": "limit",
        "price": price_str,
        "count": f"{config.initial_contract_count}.00",
        "client_order_id": order_id,
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }

    # Use max_price if higher than our target
    if max_price > price_cents:
        payload["price"] = max_price_str

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
    1. Fetch today's temp markets via REST (fast, one call per event)
    2. For each market, check price from REST
    3. If price >= buy_trigger and spread <= min_spread, buy

    This runs every ~2 seconds via systemd timer.
    """
    private_key = load_private_key(config.kalshi_private_key_path)
    today_prefix = _get_today_prefix()

    # Series list (40 series: 20 cities x high/low)
    series_list = [
        "KXHIGHTATL", "KXLOWTATL",
        "KXHIGHAUS", "KXLOWTAUS",
        "KXHIGHTBOS", "KXLOWTBOS",
        "KXHIGHCHI", "KXLOWTCHI",
        "KXHIGHTDAL", "KXLOWTDAL",
        "KXHIGHDEN", "KXLOWTDEN",
        "KXHIGHTHOU", "KXLOWTHOU",
        "KXHIGHTLV", "KXLOWTLV",
        "KXHIGHLAX", "KXLOWTLAX",
        "KXHIGHMIA", "KXLOWTMIA",
        "KXHIGHTMIN", "KXLOWTMIN",
        "KXHIGHTNOLA", "KXLOWTNOLA",
        "KXHIGHNY", "KXLOWTNYC",
        "KXHIGHTOKC", "KXLOWTOKC",
        "KXHIGHPHIL", "KXLOWTPHIL",
        "KXHIGHTPHX", "KXLOWTPHX",
        "KXHIGHTSATX", "KXLOWTSATX",
        "KXHIGHTSFO", "KXLOWTSFO",
        "KXHIGHTSEA", "KXLOWTSEA",
        "KXHIGHTDC", "KXLOWTDC",
    ]

    event_tickers = [f"{s}-{today_prefix}" for s in series_list]
    markets_path = "/trade-api/v2/markets"
    markets_url = f"{config.rest_base_url}{markets_path}"

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Fetch markets for all events in parallel
        async def fetch_event(event_ticker: str) -> list[dict]:
            headers = build_auth_headers(private_key, config.kalshi_api_key, "GET", markets_path)
            try:
                resp = await client.get(
                    markets_url, headers=headers,
                    params={"event_ticker": event_ticker, "limit": 100}
                )
                if resp.status_code in (200, 201):
                    return resp.json().get("markets", [])
            except Exception:
                pass
            return []

        results = await asyncio.gather(
            *[fetch_event(et) for et in event_tickers],
            return_exceptions=True
        )

        all_markets: list[dict] = []
        for r in results:
            if isinstance(r, list):
                all_markets.extend(r)

        logger.info("scanner.fetched_markets", count=len(all_markets))

        # Load held tickers from DB to avoid re-buying
        held_tickers: set[str] = set()
        async with await db.get_session() as session:
            from sqlalchemy import select
            from app.models import Position as PositionModel
            result = await session.execute(
                select(PositionModel.market_ticker).where(PositionModel.quantity > 0)
            )
            held_tickers = {row[0] for row in result.fetchall()}

        # Check each market's price
        buy_trigger = config.buy_trigger_price      # 82
        max_price = config.spread_monitor_price      # 90
        min_spread = config.minimum_spread            # 7

        for m in all_markets:
            ticker = m.get("ticker", "")
            if not ticker or ticker in held_tickers:
                continue

            # Get current price from market data
            yes_ask_raw = m.get("yes_ask")
            yes_bid_raw = m.get("yes_bid")

            if not yes_ask_raw or not yes_bid_raw:
                continue

            yes_ask = round(float(yes_ask_raw) * 100)
            yes_bid = round(float(yes_bid_raw) * 100)
            spread = yes_ask - yes_bid

            # Check buy conditions: price >= buy_trigger, spread <= min_spread
            if yes_ask >= buy_trigger and spread <= min_spread:
                logger.info("scanner.buy_signal", ticker=ticker,
                            ask=yes_ask, bid=yes_bid, spread=spread)

                success = await buy_market(config, ticker, yes_ask, client)

                if success:
                    # Log to DB
                    async with await db.get_session() as session:
                        trade = ExecutedTrade(
                            market_ticker=ticker,
                            action=TradeAction.BUY,
                            side="yes",
                            price=yes_ask,
                            quantity=config.initial_contract_count,
                            total_cost_cents=yes_ask * config.initial_contract_count,
                            trade_mode=config.trading_mode,
                            status=TradeStatus.FILLED,
                        )
                        session.add(trade)
                        await session.commit()

        logger.info("scanner.cycle_complete", checked=len(all_markets),
                     held=len(held_tickers))


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
