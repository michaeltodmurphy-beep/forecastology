# execution/live.py
import json
import uuid
import httpx
import structlog
from typing import Optional
from app.signing import load_private_key, build_auth_headers
from execution.base import BaseExecutor, ExecutionResult
from core.types import OrderRequest, OrderSide
from core.constants import (
    REST_PORTFOLIO_BALANCE, REST_PORTFOLIO_ORDERS,
    REST_PORTFOLIO_POSITIONS, SERIES_LIST,
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

    async def buy_yes(self, order: OrderRequest, max_price: Optional[int] = None) -> ExecutionResult:
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload(max_price)
        logger.info("live.buy_yes_payload", ticker=order.market_ticker,
                     payload=json.dumps(payload), price=order.price)
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"

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
        payload = order.to_kalshi_payload()  # side="offer" for selling
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"

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
        from core.constants import get_eastern_today_date_prefix
        all_markets = []
        today_prefix = get_eastern_today_date_prefix(days_offset=0)

        markets_path = "/trade-api/v2/markets"
        markets_url = f"{self.base_url}{markets_path}"

        event_tickers = [f"{s}-{today_prefix}" for s in SERIES_LIST]

        async def _fetch_event_markets(event_ticker: str):
            """Fetch markets for one event (no pagination needed, <100 per event)."""
            headers = self._headers("GET", markets_path)
            try:
                resp = await self._client.get(
                    markets_url, headers=headers,
                    params={"event_ticker": event_ticker, "limit": 100}
                )
                if resp.status_code in (200, 201):
                    return resp.json().get("markets", [])
            except Exception:
                pass
            return []

        # Fetch all events in parallel
        import asyncio
        results = await asyncio.gather(
            *[_fetch_event_markets(et) for et in event_tickers],
            return_exceptions=True
        )
        for mkts in results:
            if isinstance(mkts, list):
                all_markets.extend(mkts)

        logger.info("live.found_temp_markets", count=len(all_markets),
                     event_count=len(event_tickers))
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
                # Parse position quantity — can be an int or string float
                count = pos.get("position_fp", "0")
                try:
                    pos["count"] = int(float(count))
                except (ValueError, TypeError):
                    pos["count"] = 0
                # Normalize average fill cost: Kalshi may return it as
                # "average_fill_cost_dollars" (e.g. "0.8400") or in cents
                cost_str = pos.get("average_fill_cost_dollars", "")
                if cost_str:
                    try:
                        pos["average_fill_cost_cents"] = round(float(cost_str) * 100)
                    except (ValueError, TypeError):
                        pos["average_fill_cost_cents"] = 0
                else:
                    pos["average_fill_cost_cents"] = 0
                # Extract current market price from last_price field (in dollars)
                last_price_str = pos.get("last_price", "")
                if last_price_str:
                    try:
                        pos["last_price_cents"] = round(float(last_price_str) * 100)
                    except (ValueError, TypeError):
                        pos["last_price_cents"] = 0
                else:
                    pos["last_price_cents"] = 0
                pos["market_ticker"] = ticker
                positions[ticker] = pos
        return positions
    async def close(self):
        await self._client.aclose()
