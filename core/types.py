# core/types.py
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


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


@dataclass
class OrderBookLevel:
    price: int       # cents
    quantity: int    # contracts
    order_count: int


@dataclass
class OrderBook:
    yes_bids: list[OrderBookLevel] = field(default_factory=list)
    yes_asks: list[OrderBookLevel] = field(default_factory=list)
    # Note: In Kalshi, no asks are implicit from yes bids (100 - price).

    @property
    def best_bid(self) -> Optional[int]:
        return self.yes_bids[0].price if self.yes_bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return self.yes_asks[0].price if self.yes_asks else None

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
    pending_entry: bool = False  # crossed 85 but spread was too wide, still waiting
    orderbook: Optional[OrderBook] = None
    position_quantity: int = 0
    avg_entry: Optional[int] = None
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

    def to_kalshi_payload(self) -> dict:
        # Always side="yes" — we trade YES contracts.
        # action="buy" to buy YES, action="sell" to sell YES (close position).
        action = "sell" if self.side == OrderSide.SELL_YES else "buy"
        return {
            "ticker": self.market_ticker,
            "type": "limit",
            "action": action,
            "side": "yes",
            "yes_price": self.price,
            "count": self.quantity,
            "client_order_id": self.client_order_id or "",
        }
