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


def _to_cents_int(value) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return 0


def _to_quantity_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_dollars_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_fill(data: dict) -> tuple[int, int]:
    """Return (fill_count, avg_fill_price_cents) from a Kalshi order-create response.

    Kalshi returns the order object with fill_count_fp and *_fill_cost_dollars.
    The average fill price is total fill cost / fill count (fees excluded).
    NOTE: yes_price_dollars on an order is the LIMIT price, not the fill price,
    so it must NOT be used as the cost basis.
    """
    order = data.get("order") if isinstance(data.get("order"), dict) else data

    # Fill count: prefer fill_count_fp; fall back to legacy keys.
    fill_obj = data.get("fill") or {}
    fill_count_raw = _to_quantity_float(
        order.get("fill_count_fp")
        or order.get("fill_count")
        or order.get("filled_count")
        or fill_obj.get("count_fp")
        or fill_obj.get("count")
    )
    if fill_count_raw <= 0:
        return 0, 0

    # Total fill cost in dollars (taker + maker), fees excluded.
    taker_cost = _to_dollars_float(order.get("taker_fill_cost_dollars"))
    maker_cost = _to_dollars_float(order.get("maker_fill_cost_dollars"))
    total_cost_dollars = taker_cost + maker_cost

    if total_cost_dollars > 0:
        avg_price_cents = round((total_cost_dollars / fill_count_raw) * 100)
    else:
        # Fallback chain when fill-cost is absent (older/partial responses):
        #  1) an explicit fill avg price, if present
        #  2) yes_price_dollars (LIMIT price) as a last resort — log a warning
        explicit = (fill_obj.get("price") or fill_obj.get("avg_price")
                    or order.get("avg_price"))
        if explicit:
            avg_price_cents = _to_cents_int(explicit)
        else:
            yp = order.get("yes_price_dollars")
            avg_price_cents = round(_to_dollars_float(yp) * 100) if yp else 0
            if avg_price_cents > 0:
                logger.warning("live.fill_price_fallback_to_limit",
                               note="used yes_price_dollars (limit) as fill price")
    fill_count = max(int(round(fill_count_raw)), 0)
    return fill_count, avg_price_cents


def _avg_fill_price_cents_from_fills(fills: list, ticker: str) -> int:
    """Compute volume-weighted average BUY fill price in cents for a ticker."""
    total_count = 0.0
    weighted_dollars = 0.0
    for f in fills:
        if f.get("ticker") != ticker and f.get("market_ticker") != ticker:
            continue
        if (f.get("action") or "").lower() != "buy":
            continue
        cnt = _to_dollars_float(f.get("count_fp") or f.get("count"))
        price = _to_dollars_float(f.get("yes_price_dollars"))
        if cnt > 0 and price > 0:
            total_count += cnt
            weighted_dollars += price * cnt
    if total_count > 0:
        return round((weighted_dollars / total_count) * 100)
    return 0


class LiveTradeExecutor(BaseExecutor):
    """
    Routes real orders to the Kalshi Production REST API.
    NEVER connects to demo/sandbox URLs.
    """

    def __init__(self, base_url: str, api_key: str, private_key_path: str, dry_run: bool = False, max_buy_qty: Optional[int] = None):
        self.base_url = base_url
        self.api_key = api_key
        self.dry_run = dry_run
        self.max_buy_qty = max_buy_qty
        self._private_key = load_private_key(private_key_path)
        self._client = httpx.AsyncClient(timeout=30.0)

    def _headers(self, method: str, path: str) -> dict:
        return build_auth_headers(self._private_key, self.api_key, method, path)

    async def _current_position_qty(self, ticker: str) -> int:
        try:
            positions = await self.get_positions()
        except Exception as e:
            logger.warning("live.position_cap_lookup_failed", ticker=ticker, error=str(e))
            return 0
        raw = (positions.get(ticker) or {}).get("count", 0)
        try:
            return max(int(float(raw or 0)), 0)
        except (TypeError, ValueError):
            return 0

    async def buy_yes(self, order: OrderRequest, max_price: Optional[int] = None) -> ExecutionResult:
        if self.max_buy_qty is not None and order.quantity > self.max_buy_qty:
            logger.critical(
                "hedge.cap_blocked",
                ticker=order.market_ticker,
                proposed_qty=order.quantity,
                max_allowed_qty=self.max_buy_qty,
                action="executor_hard_cap_blocked_submission",
            )
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="REJECTED",
                notes=f"hard_cap_blocked: qty={order.quantity} exceeds max_buy_qty={self.max_buy_qty}",
            )
        if self.max_buy_qty is not None:
            existing_position_qty = await self._current_position_qty(order.market_ticker)
            total_position_qty = existing_position_qty + max(int(order.quantity or 0), 0)
            if total_position_qty > self.max_buy_qty:
                logger.critical(
                    "hedge.cap_blocked",
                    ticker=order.market_ticker,
                    existing_position_qty=existing_position_qty,
                    proposed_qty=order.quantity,
                    total_position_qty=total_position_qty,
                    max_allowed_qty=self.max_buy_qty,
                    action="executor_position_cap_blocked_total",
                )
                return ExecutionResult(
                    success=False,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=0,
                    fill_quantity=0,
                    total_cost_cents=0,
                    status="REJECTED",
                    notes=(
                        f"position_cap_blocked: existing={existing_position_qty} + "
                        f"proposed={order.quantity} exceeds max_buy_qty={self.max_buy_qty}"
                    ),
                )
        if self.dry_run:
            logger.warning(
                "live.dry_run_skip_order",
                ticker=order.market_ticker,
                side="buy_yes",
                price=order.price,
                quantity=order.quantity,
                max_price=max_price,
            )
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="DRY_RUN",
                notes="dry_run",
            )
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload(
            max_price,
            time_in_force="immediate_or_cancel",
        )
        logger.info("live.buy_yes_payload", ticker=order.market_ticker,
                     payload=json.dumps(payload), price=order.price)
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"

        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            data = resp.json()

            if resp.status_code in (200, 201):
                fill_quantity, fill_price = _extract_fill(data)
                if fill_quantity <= 0:
                    logger.warning("live.buy_yes_no_fill", ticker=order.market_ticker, response=data)
                    return ExecutionResult(
                        success=False,
                        market_ticker=order.market_ticker,
                        side="yes",
                        price=order.price,
                        quantity=order.quantity,
                        fill_price=0,
                        fill_quantity=0,
                        total_cost_cents=0,
                        status="NO_FILL",
                        notes=json.dumps(data),
                    )
                order_id = data.get("order_id", "")
                logger.info("live.buy_yes_filled",
                            ticker=order.market_ticker, fill_price=fill_price, fill_count=fill_quantity)
                return ExecutionResult(
                    success=True,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=fill_price,
                    fill_quantity=fill_quantity,
                    total_cost_cents=fill_price * fill_quantity,
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
        if self.dry_run:
            logger.warning(
                "live.dry_run_skip_order",
                ticker=order.market_ticker,
                side="sell_yes",
                price=order.price,
                quantity=order.quantity,
            )
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="DRY_RUN",
                notes="dry_run",
            )
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload(
            time_in_force="immediate_or_cancel",
            reduce_only=True,
        )  # side="ask" for selling YES
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"

        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            data = resp.json()

            if resp.status_code in (200, 201):
                fill_quantity, fill_price = _extract_fill(data)
                if fill_quantity <= 0:
                    logger.warning("live.sell_yes_no_fill", ticker=order.market_ticker, response=data)
                    return ExecutionResult(
                        success=False,
                        market_ticker=order.market_ticker,
                        side="yes",
                        price=order.price,
                        quantity=order.quantity,
                        fill_price=0,
                        fill_quantity=0,
                        total_cost_cents=0,
                        status="NO_FILL",
                        notes=json.dumps(data),
                    )
                order_id = data.get("order_id", "")
                logger.info("live.sell_yes_filled",
                            ticker=order.market_ticker, fill_price=fill_price, fill_count=fill_quantity)
                return ExecutionResult(
                    success=True,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=fill_price,
                    fill_quantity=fill_quantity,
                    total_cost_cents=-(fill_price * fill_quantity),
                    order_id=order_id,
                    status="FILLED",
                    notes=json.dumps(data),
                )
            else:
                logger.error("live.sell_yes_rejected", ticker=order.market_ticker,
                             status=resp.status_code, response=data)
                return ExecutionResult(
                    success=False, market_ticker=order.market_ticker,
                    side="yes", price=order.price, quantity=order.quantity,
                    fill_price=0, fill_quantity=0, total_cost_cents=0,
                    status="REJECTED", notes=json.dumps(data),
                )
        except Exception as e:
            logger.error("live.sell_yes_error", error=str(e))
            return ExecutionResult(
                success=False, market_ticker=order.market_ticker,
                side="yes", price=order.price, quantity=order.quantity,
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

    async def get_fills(self, ticker: Optional[str] = None, limit: int = 200, max_pages: int = 5) -> list[dict]:
        """Fetch fills from /trade-api/v2/portfolio/fills.

        If `ticker` is provided, only that market's fills are returned, paginating
        via cursor up to `max_pages` pages (a single bracket rarely exceeds one page).
        NO caching: always fetches fresh so cost basis is never stale.
        """
        path = "/trade-api/v2/portfolio/fills"
        out: list[dict] = []
        cursor = ""
        for _ in range(max_pages):
            headers = self._headers("GET", path)
            params: dict = {"limit": limit}
            if ticker:
                params["ticker"] = ticker
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await self._client.get(f"{self.base_url}{path}", headers=headers, params=params)
            except Exception as e:
                logger.warning("live.get_fills_error", ticker=ticker, error=str(e))
                break
            if resp.status_code not in (200, 201):
                break
            body = resp.json()
            page = body.get("fills", [])
            out.extend(page)
            cursor = body.get("cursor", "")
            if not cursor or not page:
                break
        return out

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
                cost_cents = 0
                cost_source = "none"

                cost_str = pos.get("average_fill_cost_dollars", "")
                if cost_str:
                    try:
                        parsed_dollars = round(float(cost_str) * 100)
                        if parsed_dollars > 0:
                            cost_cents = parsed_dollars
                            cost_source = "average_fill_cost_dollars"
                    except (ValueError, TypeError):
                        pass

                if cost_cents <= 0:
                    average_fill_cost = _to_cents_int(pos.get("average_fill_cost"))
                    if average_fill_cost > 0:
                        cost_cents = average_fill_cost
                        cost_source = "average_fill_cost"

                if cost_cents <= 0 and pos["count"] > 0:
                    for total_field in ("market_exposure", "total_traded"):
                        total_cents = _to_cents_int(pos.get(total_field))
                        if total_cents > 0:
                            cost_cents = round(total_cents / pos["count"])
                            cost_source = total_field
                            break

                if cost_cents <= 0 and pos["count"] > 0:
                    ticker_fills = await self.get_fills(ticker=ticker)
                    fills_cost = _avg_fill_price_cents_from_fills(ticker_fills, ticker)
                    if fills_cost > 0:
                        cost_cents = fills_cost
                        cost_source = "fills_history"

                pos["average_fill_cost_cents"] = cost_cents if cost_cents > 0 else 0
                logger.debug(
                    "live.position_cost_basis",
                    ticker=ticker,
                    source=cost_source,
                    cents=pos["average_fill_cost_cents"],
                )
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

    async def place_limit_sell(self, order: OrderRequest) -> ExecutionResult:
        """Place a resting GTC SELL_YES limit order on the exchange.

        Unlike sell_yes (which uses immediate_or_cancel for stop-loss exits),
        this uses good_till_canceled so the order rests on the book until
        filled or explicitly cancelled.
        """
        if self.dry_run:
            logger.warning(
                "live.dry_run_skip_order",
                ticker=order.market_ticker,
                side="place_limit_sell",
                price=order.price,
                quantity=order.quantity,
            )
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="DRY_RUN",
                notes="dry_run",
            )
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload(
            time_in_force="good_till_canceled",
        )
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"
        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            data = resp.json()
            if resp.status_code in (200, 201):
                order_data = data.get("order") if isinstance(data.get("order"), dict) else data
                order_id = order_data.get("order_id") or data.get("order_id") or ""
                logger.info(
                    "live.place_limit_sell_placed",
                    ticker=order.market_ticker,
                    price=order.price,
                    qty=order.quantity,
                    order_id=order_id,
                )
                return ExecutionResult(
                    success=True,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=0,
                    fill_quantity=0,
                    total_cost_cents=0,
                    order_id=order_id,
                    status="RESTING",
                    notes=json.dumps(data),
                )
            else:
                logger.error(
                    "live.place_limit_sell_rejected",
                    ticker=order.market_ticker,
                    status=resp.status_code,
                    response=data,
                )
                return ExecutionResult(
                    success=False,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=0,
                    fill_quantity=0,
                    total_cost_cents=0,
                    status="REJECTED",
                    notes=json.dumps(data),
                )
        except Exception as e:
            logger.error("live.place_limit_sell_error", error=str(e))
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="REJECTED",
                notes=str(e),
            )

    async def cancel_order(self, order_id: str, market_ticker: str = "") -> bool:
        """Cancel a resting order by exchange order ID.

        Returns True if the order was cancelled or is already absent (404).
        Returns False on unexpected error.
        """
        if not order_id:
            return False
        path = f"{REST_PORTFOLIO_ORDERS}/{order_id}"
        url = f"{self.base_url}{path}"
        headers = self._headers("DELETE", path)
        try:
            resp = await self._client.delete(url, headers=headers)
            if resp.status_code in (200, 201, 204):
                logger.info(
                    "live.cancel_order_ok",
                    order_id=order_id,
                    ticker=market_ticker or None,
                )
                return True
            if resp.status_code == 404:
                # Already filled or cancelled — treat as success.
                logger.info(
                    "live.cancel_order_not_found",
                    order_id=order_id,
                    ticker=market_ticker or None,
                    note="already_filled_or_cancelled",
                )
                return True
            logger.error(
                "live.cancel_order_failed",
                order_id=order_id,
                ticker=market_ticker or None,
                status=resp.status_code,
            )
            return False
        except Exception as e:
            logger.error("live.cancel_order_error", order_id=order_id, error=str(e))
            return False

    async def list_open_sell_orders(self, ticker: str) -> list[dict]:
        """Return live resting SELL orders for *ticker* from the exchange.

        Uses the portfolio orders endpoint.  Only sell/ask orders whose status
        is still open are returned.  On any error this logs and returns an empty
        list so callers fail open (existing tracked-order behaviour unchanged).
        """
        out: list[dict] = []
        if not ticker:
            return out
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        try:
            headers = self._headers("GET", path)
            resp = await self._client.get(
                url, headers=headers, params={"ticker": ticker, "limit": 200}
            )
            if resp.status_code not in (200, 201):
                logger.warning(
                    "live.list_open_sell_orders_failed",
                    ticker=ticker,
                    status=resp.status_code,
                )
                return out
            data = resp.json()
            orders = data.get("orders") if isinstance(data.get("orders"), list) else []
            for o in orders:
                if not isinstance(o, dict):
                    continue
                if (o.get("ticker") or o.get("market_ticker")) != ticker:
                    continue
                side = (o.get("side") or o.get("action") or "").lower()
                if side not in ("sell", "ask"):
                    continue
                status = (o.get("status") or "").lower()
                if status in ("filled", "cancelled", "canceled", "expired", "settled", "not_found"):
                    continue
                out.append(o)
        except Exception as e:
            logger.warning("live.list_open_sell_orders_error", ticker=ticker, error=str(e))
            return []
        return out

    async def cancel_open_sell_orders(self, ticker: str, client_prefix: str = "") -> int:
        """Cancel any live resting SELL orders for *ticker* on the exchange.

        Only cancels orders whose client_order_id starts with *client_prefix*
        (when provided) so a user's manual order is never touched.  Returns the
        number of orders cancelled.  On error this logs and returns the count
        already cancelled.
        """
        cancelled = 0
        if not ticker:
            return 0
        try:
            orders = await self.list_open_sell_orders(ticker)
        except Exception:
            orders = []
        for o in orders:
            cid = o.get("client_order_id") or ""
            if client_prefix and not cid.startswith(client_prefix):
                continue
            order_id = o.get("order_id") or ""
            if not order_id:
                continue
            ok = await self.cancel_order(order_id, market_ticker=ticker)
            if ok:
                cancelled += 1
        return cancelled

    async def get_order_status(self, order_id: str) -> Optional[str]:
        """Return the exchange-reported status string for an order, or None."""
        if not order_id:
            return None
        path = f"{REST_PORTFOLIO_ORDERS}/{order_id}"
        url = f"{self.base_url}{path}"
        headers = self._headers("GET", path)
        try:
            resp = await self._client.get(url, headers=headers)
            if resp.status_code == 404:
                return "not_found"
            if resp.status_code in (200, 201):
                data = resp.json()
                order_data = data.get("order") if isinstance(data.get("order"), dict) else data
                return str(order_data.get("status") or "unknown").lower()
            return None
        except Exception as e:
            logger.error("live.get_order_status_error", order_id=order_id, error=str(e))
            return None

    async def place_limit_buy(self, order: OrderRequest) -> ExecutionResult:
        """Place a resting GTC BUY_YES limit order on the exchange.

        Unlike buy_yes (which uses immediate_or_cancel), this uses
        good_till_canceled so the order rests on the book until filled or
        explicitly cancelled.  Used by the partial-fill chaser.
        """
        if self.dry_run:
            logger.warning(
                "live.dry_run_skip_order",
                ticker=order.market_ticker,
                side="place_limit_buy",
                price=order.price,
                quantity=order.quantity,
            )
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="DRY_RUN",
                notes="dry_run",
            )
        path = REST_PORTFOLIO_ORDERS
        url = f"{self.base_url}{path}"
        payload = order.to_kalshi_payload(time_in_force="good_till_canceled")
        headers = self._headers("POST", path)
        headers["Content-Type"] = "application/json"
        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            data = resp.json()
            if resp.status_code in (200, 201):
                order_data = data.get("order") if isinstance(data.get("order"), dict) else data
                order_id = order_data.get("order_id") or data.get("order_id") or ""
                fill_qty = int(float(order_data.get("filled_count") or order_data.get("fill_count_fp") or 0))
                fill_price = _to_cents_int(order_data.get("avg_price") or 0)
                logger.info(
                    "live.place_limit_buy_placed",
                    ticker=order.market_ticker,
                    price=order.price,
                    qty=order.quantity,
                    order_id=order_id,
                )
                return ExecutionResult(
                    success=True,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=fill_price,
                    fill_quantity=fill_qty,
                    total_cost_cents=fill_price * fill_qty,
                    order_id=order_id,
                    status="RESTING",
                    notes=json.dumps(data),
                )
            else:
                logger.error(
                    "live.place_limit_buy_rejected",
                    ticker=order.market_ticker,
                    status=resp.status_code,
                    response=data,
                )
                return ExecutionResult(
                    success=False,
                    market_ticker=order.market_ticker,
                    side="yes",
                    price=order.price,
                    quantity=order.quantity,
                    fill_price=0,
                    fill_quantity=0,
                    total_cost_cents=0,
                    status="REJECTED",
                    notes=json.dumps(data),
                )
        except Exception as e:
            logger.error("live.place_limit_buy_error", error=str(e))
            return ExecutionResult(
                success=False,
                market_ticker=order.market_ticker,
                side="yes",
                price=order.price,
                quantity=order.quantity,
                fill_price=0,
                fill_quantity=0,
                total_cost_cents=0,
                status="REJECTED",
                notes=str(e),
            )

    async def get_order_fill_info(self, order_id: str) -> dict:
        """Return fill details for an order.

        Returns a dict with keys:
          status     — str ('resting', 'filled', 'cancelled', 'not_found', 'unknown')
          fill_qty   — int, cumulative filled quantity
          fill_price — int, average fill price in cents
        """
        if not order_id:
            return {"status": "not_found", "fill_qty": 0, "fill_price": 0}
        path = f"{REST_PORTFOLIO_ORDERS}/{order_id}"
        url = f"{self.base_url}{path}"
        headers = self._headers("GET", path)
        try:
            resp = await self._client.get(url, headers=headers)
            if resp.status_code == 404:
                return {"status": "not_found", "fill_qty": 0, "fill_price": 0}
            if resp.status_code in (200, 201):
                data = resp.json()
                order_data = data.get("order") if isinstance(data.get("order"), dict) else data
                status_raw = str(order_data.get("status") or "unknown").lower()
                fill_qty = int(float(order_data.get("filled_count") or order_data.get("fill_count_fp") or 0))
                fill_price = _to_cents_int(order_data.get("avg_price") or 0)
                return {"status": status_raw, "fill_qty": fill_qty, "fill_price": fill_price}
            return {"status": "unknown", "fill_qty": 0, "fill_price": 0}
        except Exception as e:
            logger.error("live.get_order_fill_info_error", order_id=order_id, error=str(e))
            return {"status": "unknown", "fill_qty": 0, "fill_price": 0}
