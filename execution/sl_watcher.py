import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Literal, Optional

import structlog

logger = structlog.get_logger(__name__)

PositionSide = Literal["yes", "no"]
ExitHandler = Callable[[str, PositionSide, int, int], Awaitable[bool]]


@dataclass
class WatchedPosition:
    sl_price: int
    side: PositionSide
    quantity: int
    exit_in_progress: bool = False


class StopLossWatcher:
    def __init__(self, exit_handler: ExitHandler):
        self._exit_handler = exit_handler
        self._positions: dict[str, WatchedPosition] = {}
        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()

    async def register_position(
        self,
        ticker: str,
        side: PositionSide,
        quantity: int,
        sl_price: int,
    ) -> None:
        async with self._lock:
            self._positions[ticker] = WatchedPosition(
                sl_price=sl_price,
                side=side,
                quantity=quantity,
            )
        logger.info(
            "sl.position_registered",
            ticker=ticker,
            side=side,
            quantity=quantity,
            sl_price=sl_price,
        )

    async def unregister_position(self, ticker: str) -> None:
        async with self._lock:
            removed = self._positions.pop(ticker, None)
        if removed is not None:
            logger.info("sl.position_unregistered", ticker=ticker)

    async def on_market_update(self, ticker: str, best_ask: Optional[int]) -> bool:
        if best_ask is None:
            return False

        async with self._lock:
            position = self._positions.get(ticker)
            if (
                position is None
                or position.exit_in_progress
                or best_ask > position.sl_price
            ):
                return False

            position.exit_in_progress = True
            side = position.side
            quantity = position.quantity
            sl_price = position.sl_price

        logger.warning(
            "sl.trigger_condition_met",
            ticker=ticker,
            side=side,
            quantity=quantity,
            best_ask=best_ask,
            sl_price=sl_price,
        )
        logger.info(
            "sl.exit_order_submitted",
            ticker=ticker,
            side=side,
            quantity=quantity,
            best_ask=best_ask,
        )

        try:
            success = await self._exit_handler(ticker, side, quantity, best_ask)
        except Exception as exc:
            logger.error("sl.exit_order_failed", ticker=ticker, error=str(exc))
            success = False

        if success:
            logger.info("sl.exit_order_succeeded", ticker=ticker, quantity=quantity)
            await self.unregister_position(ticker)
            return True

        async with self._lock:
            current = self._positions.get(ticker)
            if current is not None:
                current.exit_in_progress = False

        logger.warning("sl.exit_order_failed", ticker=ticker, quantity=quantity)
        return False

    async def run(self) -> None:
        logger.info("sl.watcher_started")
        try:
            await self._shutdown.wait()
        except asyncio.CancelledError:
            logger.info("sl.watcher_cancelled")
            raise
        finally:
            logger.info("sl.watcher_stopped")

    async def stop(self) -> None:
        logger.info("sl.watcher_stopping")
        self._shutdown.set()
