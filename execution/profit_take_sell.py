# execution/profit_take_sell.py
"""Resting "take-profit" limit-sell for open YES positions.

When the bot holds a YES position, ProfitTakeSellManager places a resting GTC
SELL_YES limit order on the exchange at a fixed high price (default 99¢).  If
the market trades up to that price, the order fills exchange-side with zero
round-trip latency, locking in the profit automatically.

This is intentionally the mirror image of the SL "disaster" backstop
(execution/sl_backstop.py): the backstop rests *below* the stop-loss trigger
as a gap-down hedge, while this order rests *at the top of the book* as a
take-profit exit.  Both share the same lifecycle discipline (place on entry /
adopt at startup, cancel-before-any-reactive-sell to prevent overselling).

Lifecycle:
  place  → called after entry fill and after startup position restore.
  cancel → called (awaited) before any other exit path submits its own sell,
           and on any normal position close.  Returns True when it is safe
           to proceed with the sell (order cancelled or already gone).
  check_fill → called during position reconciliation to detect if the
               take-profit order was filled while the reactive path was not
               watching.  Returns True if a fill is detected.

Thread-safety: methods are coroutine-safe; each call is fully awaited by
the state machine before proceeding.
"""

import asyncio
import uuid
import structlog
from typing import Optional

from core.types import OrderRequest, OrderSide
from execution.base import BaseExecutor, ExecutionResult

logger = structlog.get_logger(__name__)

# Prefix used when constructing client_order_ids for profit-take orders so they
# can be identified at startup reconciliation.
_PROFIT_TAKE_COI_PREFIX = "APP_PTS_"


def profit_take_client_order_id(ticker: str) -> str:
    """Return a unique client_order_id for a ticker's profit-take order.

    A UUID4 suffix is appended so that each new placement has a guaranteed-
    unique ID — Kalshi rejects re-use of a client_order_id even after the
    original order has been cancelled.  The ``APP_PTS_`` prefix is preserved
    so startup reconciliation can identify profit-take orders by prefix match
    via ``is_profit_take_client_order_id``.
    """
    # Sanitize ticker for use in ID (replace non-alphanum with _)
    safe = ticker.replace("-", "_").replace(".", "_")
    unique_suffix = uuid.uuid4().hex[:12]
    return f"{_PROFIT_TAKE_COI_PREFIX}{safe}_{unique_suffix}"


def is_profit_take_client_order_id(client_order_id: str) -> bool:
    """Return True if the given client_order_id belongs to a profit-take order."""
    return bool(client_order_id) and client_order_id.startswith(_PROFIT_TAKE_COI_PREFIX)


class ProfitTakeSellManager:
    """Manages resting take-profit sell orders for held YES positions.

    Instantiate once per TemperatureStrategy and pass the executor and
    trading_mode.  When ``profit_take_sell_enabled=False`` every method is a
    no-op so the feature adds zero overhead when disabled.
    """

    def __init__(
        self,
        executor: BaseExecutor,
        *,
        profit_take_sell_enabled: bool,
        profit_take_sell_price: int,   # cents, resting SELL limit (default 99)
        trading_mode: str = "PAPER",   # "LIVE" or "PAPER"
    ):
        self._executor = executor
        self._enabled = profit_take_sell_enabled
        price = int(profit_take_sell_price or 99)
        # Sanity floor at 2¢ so a bad config value can't place a near-free sell,
        # but honour any user-configured take-profit target (default 99¢).
        self._price = max(price, 2)
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

    @property
    def price(self) -> int:
        """Return the resting take-profit sell price in cents."""
        return self._price

    def get_order_id(self, ticker: str) -> Optional[str]:
        """Return the in-memory profit-take order ID for *ticker*, or None."""
        return self._order_ids.get(ticker)

    def set_order_id(self, ticker: str, order_id: Optional[str]) -> None:
        """Persist an adopted order ID (used during startup reconciliation)."""
        if order_id:
            self._order_ids[ticker] = order_id
        else:
            self._order_ids.pop(ticker, None)

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
        """Place a resting GTC SELL_YES take-profit for *ticker* at *qty* contracts.

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
                "profit_take.paper_skip",
                ticker=ticker,
                qty=qty,
                action="place_skipped_paper_mode",
            )
            return None
        if qty <= 0:
            return None

        async with self._lock:
            # Cancel existing order first if there is one.  We must NOT place a
            # new resting sell while an old one may still be live on the book —
            # doing so would stack multiple pending GTC sells for the same
            # position.  Only proceed to (re)place if the old order is confirmed
            # cancelled; otherwise keep tracking the old order id and abort.
            current_id = existing_order_id or self._order_ids.get(ticker)
            if current_id:
                replaced = await self._cancel_one(ticker, current_id, reason="replace")
                if not replaced:
                    logger.warning(
                        "profit_take.replace_cancel_failed_skip_place",
                        ticker=ticker,
                        order_id=current_id,
                        reason="old_order_may_still_be_live",
                    )
                    return None
                self._order_ids.pop(ticker, None)

            price = self._price
            order = OrderRequest(
                market_ticker=ticker,
                side=OrderSide.SELL_YES,
                price=price,
                quantity=qty,
                # Use a deterministic client_order_id so startup reconciliation
                # can identify this order on restart.
                client_order_id=profit_take_client_order_id(ticker),
            )

            try:
                result: ExecutionResult = await self._executor.place_limit_sell(order)
            except Exception as exc:
                logger.error(
                    "profit_take.place_error",
                    ticker=ticker,
                    price=price,
                    qty=qty,
                    error=str(exc),
                )
                return None

            if result.success and result.order_id:
                self._order_ids[ticker] = result.order_id
                logger.info(
                    "profit_take.placed",
                    ticker=ticker,
                    order_id=result.order_id,
                    price=price,
                    qty=qty,
                )
                return result.order_id
            else:
                logger.warning(
                    "profit_take.place_failed",
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
        """Cancel the profit-take order for *ticker* and await confirmation.

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
                # No profit-take active — safe to proceed.
                return True
            ok = await self._cancel_one(ticker, order_id, reason=reason)
            if ok:
                self._order_ids.pop(ticker, None)
            return ok

    async def cancel_orphan(self, ticker: str, order_id: str) -> None:
        """Cancel a profit-take order for a ticker no longer held (startup cleanup)."""
        logger.info(
            "profit_take.orphan_canceled",
            ticker=ticker,
            order_id=order_id,
        )
        await self._cancel_one(ticker, order_id, reason="orphan")
        self._order_ids.pop(ticker, None)

    # ------------------------------------------------------------------
    # Fill detection
    # ------------------------------------------------------------------

    async def check_fill(self, ticker: str) -> bool:
        """Return True if the profit-take order has been filled (or is gone).

        A return value of True means the reactive exit path should treat this
        as an already-executed take-profit and NOT submit a second sell.
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
                "profit_take.check_fill_error",
                ticker=ticker,
                order_id=order_id,
                error=str(exc),
            )
            return False
        if status in ("filled", "executed", "settled"):
            logger.info(
                "profit_take.filled",
                ticker=ticker,
                order_id=order_id,
                status=status,
            )
            self._order_ids.pop(ticker, None)
            return True
        if status in ("not_found", "cancelled", "canceled", "expired"):
            # Gone without our knowledge — clear the record.
            logger.info(
                "profit_take.gone",
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
                "profit_take.cancel_error",
                ticker=ticker,
                order_id=order_id,
                reason=reason,
                error=str(exc),
            )
            return False
        if ok:
            logger.info(
                "profit_take.canceled",
                ticker=ticker,
                order_id=order_id,
                reason=reason,
            )
        else:
            logger.warning(
                "profit_take.cancel_failed",
                ticker=ticker,
                order_id=order_id,
                reason=reason,
            )
        return ok
