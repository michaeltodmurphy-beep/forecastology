# execution/base.py
from abc import ABC, abstractmethod
from typing import Optional
from core.types import OrderRequest


class ExecutionResult:
    """Result of an executed order."""
    def __init__(
        self,
        success: bool,
        market_ticker: str,
        side: str,
        price: int,
        quantity: int,
        fill_price: int,
        fill_quantity: int,
        total_cost_cents: int,
        order_id: str = "",
        status: str = "FILLED",
        notes: str = "",
    ):
        self.success = success
        self.market_ticker = market_ticker
        self.side = side
        self.price = price
        self.quantity = quantity
        self.fill_price = fill_price
        self.fill_quantity = fill_quantity
        self.total_cost_cents = total_cost_cents
        self.order_id = order_id
        self.status = status
        self.notes = notes


class BaseExecutor(ABC):
    """Abstract base for trade execution."""

    @abstractmethod
    async def buy_yes(self, order: OrderRequest, max_price: Optional[int] = None) -> ExecutionResult:
        ...

    @abstractmethod
    async def sell_yes(self, order: OrderRequest) -> ExecutionResult:
        ...

    @abstractmethod
    async def get_balance(self) -> int:
        """Return cash balance in cents."""
        ...

    @abstractmethod
    async def get_active_markets(self, series_prefix: str = "") -> list[dict]:
        """Fetch currently active markets, optionally filtered by series prefix."""
        ...

    @abstractmethod
    async def get_positions(self) -> dict[str, dict]:
        """Return current positions keyed by market ticker."""
        ...

    async def place_limit_sell(self, order: "OrderRequest") -> "ExecutionResult":
        """Place a resting (GTC) limit SELL_YES order.

        Default implementation delegates to sell_yes with no-op semantics so
        that subclasses which do not override it still satisfy the interface
        without raising.  Live executor overrides this for a real GTC order.
        """
        from execution.base import ExecutionResult
        return ExecutionResult(
            success=False,
            market_ticker=order.market_ticker,
            side="yes",
            price=order.price,
            quantity=order.quantity,
            fill_price=0,
            fill_quantity=0,
            total_cost_cents=0,
            status="NOT_SUPPORTED",
            notes="place_limit_sell not implemented",
        )

    async def cancel_order(self, order_id: str, market_ticker: str = "") -> bool:
        """Cancel a resting order by exchange order ID.

        Returns True if the order was cancelled (or already absent), False on
        unexpected error.  Default is a no-op returning False; subclasses that
        support order management must override.
        """
        return False

    async def get_order_status(self, order_id: str) -> Optional[str]:
        """Return the exchange-reported status string for an order, or None if
        the order cannot be found or the call fails."""
        return None
