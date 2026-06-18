# execution/live.py
import json
import uuid
import httpx
import structlog
from app.signing import load_private_key, build_auth_headers
from execution.base import BaseExecutor, ExecutionResult
from core.types import OrderRequest, OrderSide
from core.constants import (
    REST_PORTFOLIO_BALANCE, REST_PORTFOLIO_ORDERS,
    REST_PORTFOLIO_POSITIONS,
)

logger = structlog.get_logger(__name__)


class LiveTradeExecutor(BaseExecutor):
    """
    Routes real orders to the Kalshi Production REST API.
    NEVER connects to demo/sandbox URLs.
    """

    def __init__(self, base_url: str, api_key: str, private_key_path: str):
        self.base_url = base_url
        self.api_key = api_key
        self._private_key = load_private_key(private_key_path)
        self._client = httpx.AsyncClient(timeout=30.0)

    def _headers(self, method: str, path: str) -> dict:
        return build_auth_headers(self._private_key, self.api_key, method, path)

    async def buy_yes(self, order: OrderRequest) -> ExecutionResult:
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload()
        logger.info("live.buy_yes_payload", ticker=order.market_ticker,
                     payload=json.dumps(payload), price=order.price)
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"
        
        # Log raw request for debugging
        logger.info("live.buy_yes_raw", ticker=order.market_ticker,
                    url=url, payload=json.dumps(payload),
                    auth_header=json.dumps(headers.get("Kalshi-Auth",""))[:200])

        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            data = resp.json()

            if resp.status_code in (200, 201):
                fill = data.get("fill", {})
                order_id = data.get("order_id", "")
                logger.info("live.buy_yes_filled",
                            ticker=order.market_ticker, price=order.price, qty=order.quantity)
                return ExecutionResult(
                    success=True,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=fill.get("price", order.price),
                    fill_quantity=fill.get("count", order.quantity),
                    total_cost_cents=fill.get("price", order.price) * fill.get("count", order.quantity),
                    order_id=order_id,
                    status="FILLED",
                    notes=json.dumps(data),
                )
            else:
                logger.error("live.buy_yes_rejected", ticker=order.market_ticker,
                             status=resp.status_code, response=data)
                return ExecutionResult(
                    success=False, market_ticker=order.market_ticker,
                    side="yes", price=order.price, quantity=order.quantity,
                    fill_price=0, fill_quantity=0, total_cost_cents=0,
                    status="REJECTED", notes=json.dumps(data),
                )
        except Exception as e:
            logger.error("live.buy_yes_error", error=str(e))
            return ExecutionResult(
                success=False, market_ticker=order.market_ticker,
                side="yes", price=order.price, quantity=order.quantity,
                fill_price=0, fill_quantity=0, total_cost_cents=0,
                status="REJECTED", notes=str(e),
            )

    async def sell_yes(self, order: OrderRequest) -> ExecutionResult:
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload()
        # For selling YES contracts, use action="sell" with side="yes"
        payload["action"] = "sell"
        # Ensure count is present (Kalshi validates this strictly)
        if "count" not in payload or not payload.get("count"):
            payload["count"] = order.quantity
        # Add the count_fp as string pennies as required by Kalshi sell API
        payload["count_fp"] = f"{payload.get('count', order.quantity)}.00"
        headers = self._headers("POST", path)

        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            data = resp.json()

            if resp.status_code in (200, 201):
                fill = data.get("fill", {})
                order_id = data.get("order_id", "")
                logger.info("live.sell_yes_filled",
                            ticker=order.market_ticker, price=order.price, qty=order.quantity)
                return ExecutionResult(
                    success=True,
                    market_ticker=order.market_ticker,
                    side="no",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=fill.get("price", order.price),
                    fill_quantity=fill.get("count", order.quantity),
                    total_cost_cents=-(fill.get("price", order.price) * fill.get("count", order.quantity)),
                    order_id=order_id,
                    status="FILLED",
                    notes=json.dumps(data),
                )
            else:
                logger.error("live.sell_yes_rejected", ticker=order.market_ticker,
                             status=resp.status_code, response=data)
                return ExecutionResult(
                    success=False, market_ticker=order.market_ticker,
                    side="no", price=order.price, quantity=order.quantity,
                    fill_price=0, fill_quantity=0, total_cost_cents=0,
                    status="REJECTED", notes=json.dumps(data),
                )
        except Exception as e:
            logger.error("live.sell_yes_error", error=str(e))
            return ExecutionResult(
                success=False, market_ticker=order.market_ticker,
                side="no", price=order.price, quantity=order.quantity,
                fill_price=0, fill_quantity=0, total_cost_cents=0,
                status="REJECTED", notes=str(e),
            )

    async def get_balance(self) -> int:
        path = REST_PORTFOLIO_BALANCE
        url = f"{self.base_url}{path}"
        headers = self._headers("GET", path)
        resp = await self._client.get(url, headers=headers)
        data = resp.json()
        return int(float(data.get("balance", 0)) * 100)

    async def get_active_markets(self, series_prefix: str = "") -> list[dict]:
        # All 20 cities and their Kalshi ticker codes
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
        all_markets = []
        import datetime
        now = datetime.datetime.utcnow()
        months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
        today_prefix = f"{str(now.year)[-2:]}{months[now.month-1]}{now.day}"

        markets_path = "/trade-api/v2/markets"
        markets_url = f"{self.base_url}{markets_path}"

        async def _fetch_all_series_markets(series: str):
            """Fetch ALL markets for a series via pagination, filter to today+ only."""
            collected = []
            cursor = None
            while True:
                headers = self._headers("GET", markets_path)
                params = {"series_ticker": series, "limit": 100}
                if cursor:
                    params["cursor"] = cursor
                try:
                    resp = await self._client.get(markets_url, headers=headers, params=params)
                    if resp.status_code not in (200, 201):
                        break
                    mkts = resp.json().get("markets", [])
                    if not mkts:
                        break
                    for m in mkts:
                        ticker = m.get("ticker", "")
                        if not ticker:
                            continue
                        try:
                            parts = ticker.split("-")
                            if len(parts) >= 2:
                                ticker_date = parts[1]
                                if ticker_date >= today_prefix:
                                    collected.append(m)
                        except Exception:
                            collected.append(m)
                    cursor = resp.json().get("cursor")
                    if not cursor:
                        break
                except Exception:
                    break
            return collected

        # Fetch all 40 series in parallel
        import asyncio
        results = await asyncio.gather(
            *[_fetch_all_series_markets(s) for s in series_list],
            return_exceptions=True
        )
        for mkts in results:
            if isinstance(mkts, list):
                all_markets.extend(mkts)

        logger.info("live.found_temp_markets", count=len(all_markets))
        return all_markets

    async def get_positions(self) -> dict[str, dict]:
        path = REST_PORTFOLIO_POSITIONS
        url = f"{self.base_url}{path}"
        headers = self._headers("GET", path)
        resp = await self._client.get(url, headers=headers)
        data = resp.json()
        positions = {}
        for pos in data.get("market_positions", []):
            ticker = pos.get("ticker", "")
            if ticker:
                count = pos.get("position_fp", "0")
                try:
                    pos["count"] = int(float(count))
                except (ValueError, TypeError):
                    pos["count"] = 0
                pos["market_ticker"] = ticker
                positions[ticker] = pos
        return positions
    async def close(self):
        await self._client.aclose()
