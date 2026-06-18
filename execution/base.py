# execution/base.py
from abc import ABC, abstractmethod
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
    async def buy_yes(self, order: OrderRequest) -> ExecutionResult:
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
