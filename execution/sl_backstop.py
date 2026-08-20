# execution/sl_backstop.py
"""Resting "disaster" limit-sell backstop for open YES positions.

When the bot holds a YES position, SlBackstopManager places a resting GTC
SELL_YES limit order on the exchange a fixed offset below the normal
stop-loss trigger price.  If the market gaps down through the stop before
the reactive path fires, the resting order fills exchange-side with zero
round-trip latency.

Lifecycle:
  place  → called after entry fill and after startup position restore.
  cancel → called (awaited) before any other exit path submits its own sell,
           and on any normal position close.  Returns True when it is safe
           to proceed with the sell (order cancelled or already gone).
  check_fill → called during position reconciliation to detect if the
               backstop order was filled while the reactive path was not
               watching.  Returns True if a fill is detected.

Thread-safety: methods are coroutine-safe; each call is fully awaited by
the state machine before proceeding.
"""

import asyncio
import time
import uuid
import structlog
from typing import Optional

from core.types import OrderRequest, OrderSide, ensure_app_client_order_id
from execution.base import BaseExecutor, ExecutionResult

logger = structlog.get_logger(__name__)

# Prefix used when constructing client_order_ids for backstop orders so they
# can be identified at startup reconciliation.
_BACKSTOP_COI_PREFIX = "APP_BSP_"


def backstop_client_order_id(ticker: str) -> str:
    """Return a unique client_order_id for a ticker's backstop order.

    A UUID4 suffix is appended so that each new placement has a guaranteed-
    unique ID — Kalshi rejects re-use of a client_order_id even after the
    original order has been cancelled.  The ``APP_BSP_`` prefix is preserved
    so startup reconciliation can identify backstop orders by prefix match
    via ``is_backstop_client_order_id``.
    """
    # Sanitize ticker for use in ID (replace non-alphanum with _)
    safe = ticker.replace("-", "_").replace(".", "_")
    unique_suffix = uuid.uuid4().hex[:12]
    return f"{_BACKSTOP_COI_PREFIX}{safe}_{unique_suffix}"


def is_backstop_client_order_id(client_order_id: str) -> bool:
    """Return True if the given client_order_id belongs to a backstop order."""
    return bool(client_order_id) and client_order_id.startswith(_BACKSTOP_COI_PREFIX)


class SlBackstopManager:
    """Manages resting backstop sell orders for held YES positions.

    Instantiate once per TemperatureStrategy and pass the executor and
    trading_mode.  When ``sl_backstop_enabled=False`` every method is a
    no-op so the feature adds zero overhead when disabled.
    """

    def __init__(
        self,
        executor: BaseExecutor,
        *,
        sl_backstop_enabled: bool,
        sl_backstop_offset: int,       # cents offset below SL ask price
        stop_loss_price_ask: int,      # cents, reactive SL trigger
        trading_mode: str = "PAPER",   # "LIVE" or "PAPER"
    ):
        self._executor = executor
        self._enabled = sl_backstop_enabled
        self._offset = max(int(sl_backstop_offset or 0), 0)
        self._sl_price_ask = int(stop_loss_price_ask)
        self._trading_mode = (trading_mode or "PAPER").upper()

        # Per-ticker in-memory store: ticker → exchange order_id
        self._order_ids: dict[str, str] = {}
        # Lock to serialise concurrent place/cancel calls for the same ticker
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._enabled

    def get_order_id(self, ticker: str) -> Optional[str]:
        """Return the in-memory backstop order ID for *ticker*, or None."""
        return self._order_ids.get(ticker)

    def set_order_id(self, ticker: str, order_id: Optional[str]) -> None:
        """Persist an adopted order ID (used during startup reconciliation)."""
        if order_id:
            self._order_ids[ticker] = order_id
        else:
            self._order_ids.pop(ticker, None)

    def backstop_price(self) -> int:
        """Return the resting price in cents: SL ask price minus offset, floor 1."""
        return max(self._sl_price_ask - self._offset, 1)

    # ------------------------------------------------------------------
    # Place
    # ------------------------------------------------------------------

    async def place(
        self,
        ticker: str,
        qty: int,
        *,
        existing_order_id: Optional[str] = None,
    ) -> Optional[str]:
        """Place a resting GTC SELL_YES backstop for *ticker* at *qty* contracts.

        If *existing_order_id* is provided the old order is cancelled first
        (replace-on-quantity-change semantics).

        Returns the new exchange order ID on success, None on failure.
        Paper mode is a no-op and returns None.
        """
        if not self._enabled:
            return None
        # PAPER mode: never place real orders.
        if self._trading_mode == "PAPER":
            logger.info(
                "sl.backstop_paper_skip",
                ticker=ticker,
                qty=qty,
                action="place_skipped_paper_mode",
            )
            return None
        if qty <= 0:
            return None

        async with self._lock:
            # Cancel existing order first if there is one.
            current_id = existing_order_id or self._order_ids.get(ticker)
            if current_id:
                await self._cancel_one(ticker, current_id, reason="replace")
                self._order_ids.pop(ticker, None)

            price = self.backstop_price()
            order = OrderRequest(
                market_ticker=ticker,
                side=OrderSide.SELL_YES,
                price=price,
                quantity=qty,
                # Use a deterministic client_order_id so startup reconciliation
                # can identify this order on restart.
                client_order_id=backstop_client_order_id(ticker),
            )

            try:
                result: ExecutionResult = await self._executor.place_limit_sell(order)
            except Exception as exc:
                logger.error(
                    "sl.backstop_place_error",
                    ticker=ticker,
                    price=price,
                    qty=qty,
                    error=str(exc),
                )
                return None

            if result.success and result.order_id:
                self._order_ids[ticker] = result.order_id
                logger.info(
                    "sl.backstop_placed",
                    ticker=ticker,
                    order_id=result.order_id,
                    price=price,
                    qty=qty,
                )
                return result.order_id
            else:
                logger.warning(
                    "sl.backstop_place_failed",
                    ticker=ticker,
                    price=price,
                    qty=qty,
                    status=result.status,
                    notes=result.notes,
                )
                return None

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    async def cancel(self, ticker: str, *, reason: str = "exit") -> bool:
        """Cancel the backstop order for *ticker* and await confirmation.

        Returns True when it is safe to proceed with an exit sell (the order
        is cancelled, was already absent, or was never placed).  Returns
        False only on an unexpected error where we cannot confirm cancellation.

        This method MUST be awaited and confirmed True before placing a
        reactive exit sell to prevent overselling.
        """
        if not self._enabled:
            return True
        async with self._lock:
            order_id = self._order_ids.get(ticker)
            if not order_id:
                # No backstop active — safe to proceed.
                return True
            ok = await self._cancel_one(ticker, order_id, reason=reason)
            if ok:
                self._order_ids.pop(ticker, None)
            return ok

    async def cancel_orphan(self, ticker: str, order_id: str) -> None:
        """Cancel a backstop order for a ticker no longer held (startup cleanup)."""
        logger.info(
            "sl.backstop_orphan_canceled",
            ticker=ticker,
            order_id=order_id,
        )
        await self._cancel_one(ticker, order_id, reason="orphan")
        self._order_ids.pop(ticker, None)

    # ------------------------------------------------------------------
    # Fill detection
    # ------------------------------------------------------------------

    async def check_fill(self, ticker: str) -> bool:
        """Return True if the backstop order has been filled (or is gone).

        A return value of True means the reactive SL path should treat this
        as an already-executed stop-loss and NOT submit a second sell.
        """
        if not self._enabled:
            return False
        order_id = self._order_ids.get(ticker)
        if not order_id:
            return False
        try:
            status = await self._executor.get_order_status(order_id)
        except Exception as exc:
            logger.warning(
                "sl.backstop_check_fill_error",
                ticker=ticker,
                order_id=order_id,
                error=str(exc),
            )
            return False
        if status in ("filled", "executed", "settled"):
            logger.info(
                "sl.backstop_filled",
                ticker=ticker,
                order_id=order_id,
                status=status,
            )
            self._order_ids.pop(ticker, None)
            return True
        if status in ("not_found", "cancelled", "canceled", "expired"):
            # Gone without our knowledge — clear the record.
            logger.info(
                "sl.backstop_gone",
                ticker=ticker,
                order_id=order_id,
                status=status,
            )
            self._order_ids.pop(ticker, None)
        return False

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _cancel_one(self, ticker: str, order_id: str, *, reason: str) -> bool:
        try:
            ok = await self._executor.cancel_order(order_id, market_ticker=ticker)
        except Exception as exc:
            logger.error(
                "sl.backstop_cancel_error",
                ticker=ticker,
                order_id=order_id,
                reason=reason,
                error=str(exc),
            )
            return False
        if ok:
            logger.info(
                "sl.backstop_canceled",
                ticker=ticker,
                order_id=order_id,
                reason=reason,
            )
        else:
            logger.warning(
                "sl.backstop_cancel_failed",
                ticker=ticker,
                order_id=order_id,
                reason=reason,
            )
        return ok
