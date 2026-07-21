# core/types.py
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import uuid


class Phase(Enum):
    MONITORING = "MONITORING"   # Phase A: scanning all markets
    WATCHING = "WATCHING"       # Phase A: price >= monitor_start
    ENTERING = "ENTERING"       # Phase B: price >= buy_trigger, checking spreads
    HOLDING = "HOLDING"         # Phase C: position held, monitoring for hedge/stop
    HEDGED = "HEDGED"          # Phase C: hedge placed
    CLOSED = "CLOSED"          # Position fully closed


class OrderSide(Enum):
    BUY_YES = "buy"
    SELL_YES = "sell"


APP_CLIENT_ORDER_PREFIX = "APP_"


def ensure_app_client_order_id(client_order_id: Optional[str] = None) -> str:
    if not client_order_id:
        return f"{APP_CLIENT_ORDER_PREFIX}{uuid.uuid4().hex}"
    if client_order_id.startswith(APP_CLIENT_ORDER_PREFIX):
        return client_order_id
    return f"{APP_CLIENT_ORDER_PREFIX}{client_order_id}"


@dataclass
class OrderBookLevel:
    price: int       # cents
    quantity: int    # contracts
    order_count: int


@dataclass
class OrderBook:
    yes_bids: list[OrderBookLevel] = field(default_factory=list)
    yes_asks: list[OrderBookLevel] = field(default_factory=list)
    # NO bids are tracked so the YES ask can be derived as (100 - best_no_bid).
    # On Kalshi, YES ask = 100 - highest NO bid (complementary contract pricing).
    no_bids: list[OrderBookLevel] = field(default_factory=list)

    @property
    def best_bid(self) -> Optional[int]:
        return self.yes_bids[0].price if self.yes_bids else None

    @property
    def best_ask(self) -> Optional[int]:
        """Return the YES ask price in cents.

        Prefers explicit YES ask levels (populated from ticker channel when
        available).  Falls back to deriving the ask from NO bids:
        YES ask = 100 - best_NO_bid, which is the minimum price at which
        someone will sell YES (equivalently, the best price to buy NO minus
        their implied cost).
        """
        if self.yes_asks:
            return self.yes_asks[0].price
        if self.no_bids:
            return 100 - self.no_bids[0].price
        return None

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


@dataclass
class MarketBracket:
    """Represents a single temperature bracket market (e.g., Miami High 96-97)."""
    market_ticker: str
    event_ticker: str
    series_ticker: str
    bracket_label: str       # e.g., "96-97"
    phase: Phase = Phase.MONITORING
    last_price: Optional[int] = None
    last_checked_price: Optional[int] = None
    crossed_buy: bool = False
    falling_knife_guard: bool = False
    pending_entry: bool = False  # crossed 85 but spread was too wide, still waiting
    orderbook: Optional[OrderBook] = None
    position_quantity: int = 0
    avg_entry: int = 0
    hedge_market: Optional[str] = None
    hedge_quantity: int = 0


@dataclass
class OrderRequest:
    market_ticker: str
    side: OrderSide
    price: int
    quantity: int
    client_order_id: Optional[str] = None
    is_hedge: bool = False

    def to_kalshi_payload(
        self,
        max_price: Optional[int] = None,
        time_in_force: Optional[str] = None,
        reduce_only: bool = False,
    ) -> dict:
        # New V2 /portfolio/events/orders format
        # side: "bid" for buying YES, "ask" for selling YES
        # price: string dollars (e.g. "0.8900"), count: string (e.g. "1.00")
        kalshi_side = "bid" if self.side == OrderSide.BUY_YES else "ask"
        self.client_order_id = ensure_app_client_order_id(self.client_order_id)
        
        # For buys: use max_price if given (allows crossing the spread to get filled)
        # For sells: use the actual price
        price_str = f"{self.price / 100:.4f}"
        if kalshi_side == "bid" and max_price is not None and max_price > self.price:
            price_str = f"{max_price / 100:.4f}"
        
        return {
            "ticker": self.market_ticker,
            "side": kalshi_side,
            "type": "limit",
            "price": price_str,
            "count": f"{self.quantity}.00",
            "client_order_id": self.client_order_id,
            "time_in_force": time_in_force or "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": False,
            "cancel_order_on_pause": False,
            "reduce_only": reduce_only,
        }
