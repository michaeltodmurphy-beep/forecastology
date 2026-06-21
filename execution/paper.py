# execution/paper.py
import time
import structlog
from typing import Optional
from execution.base import BaseExecutor, ExecutionResult
from core.types import OrderRequest, OrderSide
from data.ticker_cache import TickerCache

logger = structlog.get_logger(__name__)


class PaperTradeExecutor(BaseExecutor):
    """
    Simulates trade execution internally.
    Prices come from the cached order book (lowest ask for buys, highest bid for sells).
    All fills are logged to the database.
    """

    def __init__(self, ticker_cache: TickerCache, initial_balance_cents: int = 100_000_00):
        self.ticker_cache = ticker_cache
        self.balance_cents = initial_balance_cents
        self.positions: dict[str, dict] = {}

    async def buy_yes(self, order: OrderRequest, max_price: Optional[int] = None) -> ExecutionResult:
        # Simulate: fill at the lowest available ask from cache
        ob = self.ticker_cache.get_orderbook(order.market_ticker)
        fill_price = order.price  # default to requested price

        if ob and ob.yes_asks:
            # Fill at the lowest ask, but not above our limit price
            fill_price = min(order.price, ob.yes_asks[0].price)

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
        from app.config import AppConfig
        from app.signing import load_private_key, build_auth_headers
        from core.constants import get_eastern_today_date_prefix
        
        config = AppConfig.from_env()
        private_key = load_private_key(config.kalshi_private_key_path)
        
        # All 40 series (20 cities × low/high) with their exact Kalshi series tickers
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
        
        # Build event tickers for today and tomorrow in US Eastern time.
        today_prefix = get_eastern_today_date_prefix(days_offset=0)
        tomorrow_prefix = get_eastern_today_date_prefix(days_offset=1)
        event_tickers = [f"{series}-{today_prefix}" for series in series_list] + \
                         [f"{series}-{tomorrow_prefix}" for series in series_list]
        
        markets_path = "/trade-api/v2/markets"
        markets_url = f"{config.rest_base_url}{markets_path}"
        
        all_markets = []
        async with httpx.AsyncClient() as client:
            for event_ticker in event_tickers:
                m_headers = build_auth_headers(private_key, config.kalshi_api_key, "GET", markets_path)
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
            last_price = self.cache.get_last_price(ticker)
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
