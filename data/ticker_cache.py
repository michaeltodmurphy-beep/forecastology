# data/ticker_cache.py
from typing import Optional
from core.types import OrderBook, OrderBookLevel
import structlog

logger = structlog.get_logger(__name__)


class TickerCache:
    """
    In-memory cache for latest market data.
    Updated in real-time by WebSocket manager handlers.
    """

    def __init__(self):
        # market_ticker -> last_price (cents)
        self.last_prices: dict[str, int] = {}
        # market_ticker -> OrderBook
        self.orderbooks: dict[str, OrderBook] = {}
        # market_ticker -> full market info (from lifecycle or REST)
        self.market_metadata: dict[str, dict] = {}

    def update_last_price(self, ticker: str, price: int):
        self.last_prices[ticker] = price

    def update_orderbook_snapshot(self, ticker: str, snapshot: dict):
        """Process an orderbook_snapshot message."""
        yes_bids = []
        yes_asks = []
        
        # Kalshi format: yes_dollars_fp = [[price_str, qty_str], ...]
        # "yes" side in dollars_fp represents yes bids (buy orders for YES)
        yes_fp = snapshot.get("yes_dollars_fp", [])
        for entry in yes_fp:
            price_dollars = float(entry[0])
            qty = float(entry[1])
            price_cents = int(price_dollars * 100)
            if qty > 0:
                yes_bids.append(OrderBookLevel(price=price_cents, quantity=int(qty), order_count=0))
        
        # "no" side in dollars_fp represents no bids
        # A no bid at price X = a yes ask at price (100 - X)
        no_fp = snapshot.get("no_dollars_fp", [])
        for entry in no_fp:
            price_dollars = float(entry[0])
            qty = float(entry[1])
            yes_ask_price = 100 - int(price_dollars * 100)
            if qty > 0:
                yes_asks.append(OrderBookLevel(price=yes_ask_price, quantity=int(qty), order_count=0))
        
        yes_bids.sort(key=lambda x: x.price, reverse=True)  # highest first
        yes_asks.sort(key=lambda x: x.price)  # lowest first
        
        self.orderbooks[ticker] = OrderBook(yes_bids=yes_bids, yes_asks=yes_asks)

    def update_orderbook_delta(self, ticker: str, delta: dict):
        """Apply incremental delta to an existing orderbook."""
        ob = self.orderbooks.get(ticker)
        if not ob:
            return  # silently ignore until snapshot arrives

        # Delta format: { "price_dollars": "0.6800", "delta_fp": "-5.00", "side": "yes" }
        price_dollars = delta.get("price_dollars")
        delta_fp = delta.get("delta_fp")
        side = delta.get("side")
        
        if price_dollars is None or delta_fp is None or side is None:
            return
            
        price_cents = int(float(price_dollars) * 100)
        delta_qty = int(float(delta_fp))  # can be negative
        
        if side == "yes":
            # Yes bid change
            old_level = next((l for l in ob.yes_bids if l.price == price_cents), None)
            old_qty = old_level.quantity if old_level else 0
            new_qty = old_qty + delta_qty
            
            ob.yes_bids = [l for l in ob.yes_bids if l.price != price_cents]
            if new_qty > 0:
                ob.yes_bids.append(OrderBookLevel(price=price_cents, quantity=new_qty, order_count=0))
            ob.yes_bids.sort(key=lambda x: x.price, reverse=True)
            
        elif side == "no":
            # No bid change -> affects yes asks
            yes_ask_price = 100 - price_cents
            old_level = next((l for l in ob.yes_asks if l.price == yes_ask_price), None)
            old_qty = old_level.quantity if old_level else 0
            new_qty = old_qty + delta_qty
            
            ob.yes_asks = [l for l in ob.yes_asks if l.price != yes_ask_price]
            if new_qty > 0:
                ob.yes_asks.append(OrderBookLevel(price=yes_ask_price, quantity=new_qty, order_count=0))
            ob.yes_asks.sort(key=lambda x: x.price)

    def get_last_price(self, ticker: str) -> Optional[int]:
        return self.last_prices.get(ticker)

    def get_orderbook(self, ticker: str) -> Optional[OrderBook]:
        return self.orderbooks.get(ticker)
