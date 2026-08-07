# execution/paper.py
import time
import structlog
from typing import Optional
from execution.base import BaseExecutor, ExecutionResult
from core.types import OrderRequest, OrderSide
from core.constants import SERIES_LIST
from data.ticker_cache import TickerCache

logger = structlog.get_logger(__name__)


class PaperTradeExecutor(BaseExecutor):
    """
    Simulates trade execution internally.
    Prices come from the cached order book (lowest ask for buys, highest bid for sells).
    All fills are logged to the database.
    """

    def __init__(
        self,
        ticker_cache: TickerCache,
        rest_base_url: str = "",
        api_key: str = "",
        private_key_path: str = "",
        initial_balance_cents: int = 100_000_00,
        max_buy_qty: Optional[int] = None,
    ):
        self.ticker_cache = ticker_cache
        self.rest_base_url = rest_base_url
        self.api_key = api_key
        self.private_key_path = private_key_path
        self.balance_cents = initial_balance_cents
        self.positions: dict[str, dict] = {}
        self.max_buy_qty = max_buy_qty

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
            existing_position_qty = 0
            existing = self.positions.get(order.market_ticker)
            if existing is not None:
                try:
                    existing_position_qty = max(int(existing.get("quantity", 0) or 0), 0)
                except (TypeError, ValueError):
                    existing_position_qty = 0
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
        # Simulate: fill at the lowest available ask from cache
        ob = self.ticker_cache.get_orderbook(order.market_ticker)
        fill_price = order.price  # default to requested price

        if ob and ob.yes_asks:
            ask_price = ob.yes_asks[0].price
            # Fill at ask if it is at or below our limit; otherwise use limit price
            if ask_price <= order.price:
                fill_price = ask_price
            else:
                fill_price = order.price

        total_cost = fill_price * order.quantity

        if self.balance_cents < total_cost:
            return ExecutionResult(
                success=False, market_ticker=order.market_ticker,
                side="yes", price=order.price, quantity=order.quantity,
                fill_price=fill_price, fill_quantity=0,
                total_cost_cents=0, status="REJECTED",
                notes=f"Insufficient balance: need {total_cost}, have {self.balance_cents}"
            )

        self.balance_cents -= total_cost

        # Update positions
        if order.market_ticker in self.positions:
            pos = self.positions[order.market_ticker]
            old_cost = pos["avg_entry_price"] * pos["quantity"]
            new_cost = fill_price * order.quantity
            new_qty = pos["quantity"] + order.quantity
            pos["quantity"] = new_qty
            pos["avg_entry_price"] = (old_cost + new_cost) // new_qty
        else:
            self.positions[order.market_ticker] = {
                "market_ticker": order.market_ticker,
                "side": "yes",
                "quantity": order.quantity,
                "avg_entry_price": fill_price,
            }

        logger.info("paper.buy_yes",
                    ticker=order.market_ticker, price=fill_price, qty=order.quantity,
                    new_balance=self.balance_cents)

        return ExecutionResult(
            success=True, market_ticker=order.market_ticker,
            side="yes", price=order.price, quantity=order.quantity,
            fill_price=fill_price, fill_quantity=order.quantity,
            total_cost_cents=total_cost, order_id=f"paper_{int(time.time()*1000)}",
            status="FILLED", notes="Paper trade simulated"
        )

    async def sell_yes(self, order: OrderRequest) -> ExecutionResult:
        # Simulate: sell at the highest bid
        ob = self.ticker_cache.get_orderbook(order.market_ticker)
        fill_price = order.price

        if ob and ob.yes_bids:
            fill_price = max(order.price, ob.yes_bids[0].price)

        total_proceeds = fill_price * order.quantity

        # Reduce positions
        if order.market_ticker in self.positions:
            pos = self.positions[order.market_ticker]
            if pos["quantity"] < order.quantity:
                return ExecutionResult(
                    success=False, market_ticker=order.market_ticker,
                    side="yes", price=order.price, quantity=order.quantity,
                    fill_price=fill_price, fill_quantity=0,
                    total_cost_cents=0, status="REJECTED",
                    notes=f"Insufficient position: have {pos['quantity']}, need {order.quantity}"
                )
            pos["quantity"] -= order.quantity
            if pos["quantity"] == 0:
                del self.positions[order.market_ticker]
        else:
            return ExecutionResult(
                success=False, market_ticker=order.market_ticker,
                side="yes", price=order.price, quantity=order.quantity,
                fill_price=fill_price, fill_quantity=0,
                total_cost_cents=0, status="REJECTED",
                notes="No position to sell"
            )

        self.balance_cents += total_proceeds

        logger.info("paper.sell_yes",
                    ticker=order.market_ticker, price=fill_price, qty=order.quantity,
                    new_balance=self.balance_cents)

        return ExecutionResult(
            success=True, market_ticker=order.market_ticker,
            side="yes", price=order.price, quantity=order.quantity,
            fill_price=fill_price, fill_quantity=order.quantity,
            total_cost_cents=-total_proceeds, order_id=f"paper_{int(time.time()*1000)}",
            status="FILLED", notes="Paper trade simulated"
        )

    async def get_balance(self) -> int:
        return self.balance_cents

    async def get_active_markets(self, series_prefix: str = "") -> list[dict]:
        import httpx
        from app.signing import load_private_key, build_auth_headers
        from core.constants import get_eastern_today_date_prefix

        private_key = load_private_key(self.private_key_path)

        today_prefix = get_eastern_today_date_prefix(days_offset=0)
        event_tickers = [f"{series}-{today_prefix}" for series in SERIES_LIST]

        markets_path = "/trade-api/v2/markets"
        markets_url = f"{self.rest_base_url}{markets_path}"

        all_markets = []
        async with httpx.AsyncClient() as client:
            for event_ticker in event_tickers:
                m_headers = build_auth_headers(private_key, self.api_key, "GET", markets_path)
                try:
                    resp = await client.get(
                        markets_url,
                        headers=m_headers,
                        params={"event_ticker": event_ticker, "limit": 100}
                    )
                except Exception as e:
                    logger.warning("paper.api_error", event_ticker=event_ticker, error=str(e))
                    continue
                if resp.status_code == 200:
                    mkts = resp.json().get("markets", [])
                    all_markets.extend(mkts)
                else:
                    logger.warning("paper.api_error", event_ticker=event_ticker, status=resp.status_code)

            logger.info("paper.found_temp_markets", count=len(all_markets))

        return all_markets

    async def get_positions(self) -> dict[str, dict]:
        result = {}
        for ticker, pos in self.positions.items():
            qty = pos.get("quantity", 0)
            entry_price = pos.get("avg_entry_price", 0)  # cents
            # Use cached last price if available
            last_price = self.ticker_cache.get_last_price(ticker)
            if last_price is None:
                last_price = 0
            result[ticker] = {
                "market_ticker": ticker,
                "side": pos.get("side", "yes"),
                "count": qty,
                "average_fill_cost_cents": entry_price,
                "last_price_cents": last_price,
                "position_fp": str(qty),
                "average_fill_cost_dollars": f"{entry_price/100:.4f}",
            }
        return result

    async def place_limit_sell(self, order) -> "ExecutionResult":
        """Paper mode: no-op — does not place real resting orders."""
        from execution.base import ExecutionResult
        import structlog as _sl
        _sl.get_logger(__name__).info(
            "paper.place_limit_sell_skipped",
            ticker=order.market_ticker,
            price=order.price,
            qty=order.quantity,
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
            status="PAPER_NOOP",
            notes="paper_mode",
        )

    async def cancel_order(self, order_id: str, market_ticker: str = "") -> bool:
        """Paper mode: no-op cancel — always returns True."""
        return True

    async def get_order_status(self, order_id: str) -> "Optional[str]":
        """Paper mode: resting orders don't exist, always not_found."""
        return "not_found"
