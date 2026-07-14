import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Literal, Optional

import structlog

from execution.errors import PermanentExecutionError, TransientExecutionError

logger = structlog.get_logger(__name__)

PositionSide = Literal["yes", "no"]
StopLossWatcherState = Literal["IDLE", "TRIGGERED", "SUBMITTING", "RETRYING", "TERMINAL"]
ExitHandlerResult = Literal["terminal", "in_flight", "retry"]
ExitHandler = Callable[[str, PositionSide, int, int], Awaitable[bool | ExitHandlerResult]]


@dataclass
class WatchedPosition:
    sl_price: int
    side: PositionSide
    quantity: int
    exit_in_progress: bool = False
    state: StopLossWatcherState = "IDLE"
    trigger_price: Optional[int] = None
    last_best_ask: Optional[int] = None


class StopLossWatcher:
    def __init__(self, exit_handler: ExitHandler, *, poll_interval_ms: int = 250):
        self._exit_handler = exit_handler
        self._positions: dict[str, WatchedPosition] = {}
        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        self._poll_interval_s = max(int(poll_interval_ms or 0), 1) / 1000.0
        self._worker_tasks: dict[str, asyncio.Task] = {}

    @staticmethod
    def _action_key(ticker: str) -> str:
        return f"{ticker}:STOP_LOSS"

    @staticmethod
    def _normalize_result(result: bool | ExitHandlerResult | None) -> ExitHandlerResult:
        if result is True:
            return "terminal"
        if result == "terminal":
            return "terminal"
        if result == "in_flight":
            return "in_flight"
        return "retry"

    async def register_position(
        self,
        ticker: str,
        side: PositionSide,
        quantity: int,
        sl_price: int,
    ) -> None:
        async with self._lock:
            existing = self._positions.get(ticker)
            if existing is None:
                self._positions[ticker] = WatchedPosition(
                    sl_price=sl_price,
                    side=side,
                    quantity=quantity,
                )
            else:
                existing.sl_price = sl_price
                existing.side = side
                existing.quantity = quantity
                if existing.state == "TERMINAL":
                    existing.state = "IDLE"
                    existing.exit_in_progress = False
                if not existing.exit_in_progress and existing.last_best_ask is not None:
                    if existing.last_best_ask <= existing.sl_price:
                        existing.state = "TRIGGERED"
                        existing.trigger_price = existing.last_best_ask
        logger.info(
            "sl.position_registered",
            ticker=ticker,
            side=side,
            quantity=quantity,
            sl_price=sl_price,
        )

    async def rearm_position(self, ticker: str, *, trigger_price: Optional[int] = None) -> bool:
        async with self._lock:
            position = self._positions.get(ticker)
            if position is None:
                return False
            if position.exit_in_progress:
                return False
            position.state = "TRIGGERED"
            if trigger_price is not None:
                position.trigger_price = trigger_price
                position.last_best_ask = trigger_price
            elif position.last_best_ask is not None:
                position.trigger_price = position.last_best_ask
            else:
                position.trigger_price = position.sl_price
        logger.info(
            "sl.position_rearmed",
            ticker=ticker,
            action_key=self._action_key(ticker),
            trigger_price=trigger_price,
        )
        return True

    async def unregister_position(self, ticker: str) -> None:
        async with self._lock:
            removed = self._positions.pop(ticker, None)
            worker_task = self._worker_tasks.pop(ticker, None)
        if (
            worker_task is not None
            and worker_task is not asyncio.current_task()
            and not worker_task.done()
        ):
            worker_task.cancel()
        if removed is not None:
            logger.info("sl.position_unregistered", ticker=ticker)

    async def on_market_update(self, ticker: str, best_ask: Optional[int]) -> bool:
        if best_ask is None:
            return False

        suppressed_state: Optional[StopLossWatcherState] = None
        async with self._lock:
            position = self._positions.get(ticker)
            if position is None:
                return False

            position.last_best_ask = best_ask
            if best_ask > position.sl_price:
                if position.state in {"TRIGGERED", "SUBMITTING", "RETRYING"} and not position.exit_in_progress:
                    position.state = "IDLE"
                    position.trigger_price = None
                return False
            if position.state in {"TRIGGERED", "SUBMITTING", "RETRYING"} or position.exit_in_progress:
                suppressed_state = position.state
            else:
                position.state = "TRIGGERED"
                position.trigger_price = best_ask
                return True

        logger.info(
            "sl.trigger_suppressed_in_flight",
            ticker=ticker,
            action_key=self._action_key(ticker),
            best_ask=best_ask,
            state=suppressed_state or "SUBMITTING",
        )
        return False

    async def _set_position_state(
        self,
        ticker: str,
        *,
        state: StopLossWatcherState,
        exit_in_progress: bool,
    ) -> Optional[WatchedPosition]:
        async with self._lock:
            current = self._positions.get(ticker)
            if current is None:
                return None
            current.state = state
            current.exit_in_progress = exit_in_progress
            return current

    async def _process_position(self, ticker: str, side: PositionSide, quantity: int, trigger_price: int) -> None:
        try:
            result = await self._exit_handler(ticker, side, quantity, trigger_price)
            normalized = self._normalize_result(result)
        except PermanentExecutionError as exc:
            logger.error(
                "sl.exit_order_permanent_failure",
                ticker=ticker,
                side=side,
                quantity=quantity,
                error_class=exc.error_class,
                error=str(exc),
            )
            await self._set_position_state(ticker, state="RETRYING", exit_in_progress=False)
            logger.warning(
                "sl.exit_order_failed",
                ticker=ticker,
                quantity=quantity,
                error_class=exc.error_class,
            )
            return
        except TransientExecutionError as exc:
            logger.warning(
                "sl.exit_order_transient_failure",
                ticker=ticker,
                side=side,
                quantity=quantity,
                error_class=exc.error_class,
                error=str(exc),
            )
            normalized = "retry"
        except Exception as exc:
            logger.error(
                "sl.exit_order_unexpected_error",
                ticker=ticker,
                side=side,
                quantity=quantity,
                error_class="unknown",
                error=str(exc),
            )
            normalized = "retry"

        if normalized == "terminal":
            await self._set_position_state(ticker, state="TERMINAL", exit_in_progress=False)
            logger.info("sl.exit_order_succeeded", ticker=ticker, quantity=quantity)
            await self.unregister_position(ticker)
            return

        next_state: StopLossWatcherState = "SUBMITTING" if normalized == "in_flight" else "RETRYING"
        await self._set_position_state(ticker, state=next_state, exit_in_progress=False)
        if normalized == "retry":
            logger.warning("sl.exit_order_failed", ticker=ticker, quantity=quantity)

    async def _run_cycle_once(self) -> None:
        scheduled: list[tuple[str, PositionSide, int, int]] = []
        async with self._lock:
            for ticker, position in self._positions.items():
                existing_task = self._worker_tasks.get(ticker)
                if existing_task is not None and not existing_task.done():
                    continue
                if position.exit_in_progress or position.state not in {"TRIGGERED", "SUBMITTING", "RETRYING"}:
                    continue
                position.exit_in_progress = True
                scheduled.append(
                    (
                        ticker,
                        position.side,
                        position.quantity,
                        position.last_best_ask or position.trigger_price or position.sl_price,
                    )
                )

        for ticker, side, quantity, trigger_price in scheduled:
            task = asyncio.create_task(self._process_position(ticker, side, quantity, trigger_price))
            self._worker_tasks[ticker] = task

            def _cleanup(done_task: asyncio.Task, *, task_ticker: str = ticker) -> None:
                current = self._worker_tasks.get(task_ticker)
                if current is done_task:
                    self._worker_tasks.pop(task_ticker, None)

            task.add_done_callback(_cleanup)

    async def run(self) -> None:
        logger.info("sl.watcher_started")
        try:
            while not self._shutdown.is_set():
                await self._run_cycle_once()
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=self._poll_interval_s)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            logger.info("sl.watcher_cancelled")
            raise
        finally:
            logger.info("sl.watcher_stopped")

    async def stop(self) -> None:
        logger.info("sl.watcher_stopping")
        self._shutdown.set()
        for task in list(self._worker_tasks.values()):
            if not task.done():
                task.cancel()
