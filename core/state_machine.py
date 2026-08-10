# core/state_machine.py
import asyncio
import datetime
from dataclasses import dataclass
import re
import time
import structlog
from typing import Literal, Optional
from core.types import (
    Phase, MarketBracket, OrderRequest, OrderSide, OrderBook, OrderBookLevel,
    APP_CLIENT_ORDER_PREFIX,
)
from core.constants import WEATHER_CATEGORY, get_eastern_today_date_prefix
from core.local_time_gate import is_entry_allowed, get_series_station_code, get_series_timezone
from core.sunrise_gate import SunriseEntryGate
from data.ticker_cache import TickerCache
from data.websocket_manager import WebSocketManager
from execution.base import BaseExecutor, ExecutionResult
from execution.errors import TransientExecutionError, PermanentExecutionError
from execution.sl_watcher import StopLossWatcher
from execution.sl_backstop import SlBackstopManager
from app.database import DatabaseManager
from app.config import AppConfig
from app.config import _parse_intraday_schedule as _parse_intraday_schedule_cfg
from app.signing import load_private_key
from app.models import (
    StreamedTicker, StreamedTrade, ExecutedTrade, TradeAction, TradeStatus,
    Position as PositionModel, PortfolioSnapshot, StopLossLedger,
    OrderAction, OrderActionStatus,
    TradeOutcome, TradeOutcomeStatus,
)
from sqlalchemy import select, delete, update

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)
SERIES_DATE_RE = re.compile(r"^(.+?)-(\d{2}[A-Z]{3}\d{2})-(?:T\d+|B\d+\.?\d*)$")
StopLossCycleState = Literal["TRIGGERED", "SUBMITTING", "RETRYING", "TERMINAL"]
_ET_ZONE = ZoneInfo("America/New_York")


def parse_series_and_date(market_ticker: str) -> Optional[tuple[str, str]]:
    match = SERIES_DATE_RE.match(market_ticker)
    if not match:
        return None
    return match.group(1), match.group(2)


_MONTH_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_date_prefix(date_prefix: str) -> Optional[datetime.date]:
    """Parse a ticker date prefix like '26JUN25' into a datetime.date, or None on failure."""
    try:
        year = 2000 + int(date_prefix[:2])
        month = _MONTH_NUM.get(date_prefix[2:5])
        day = int(date_prefix[5:])
        if month is None:
            return None
        return datetime.date(year, month, day)
    except (ValueError, IndexError):
        return None


def get_buy_trigger_price(config: "AppConfig", market_ticker: str) -> Optional[int]:
    ticker_upper = market_ticker.upper()
    if "KXLOW" in ticker_upper:
        return int(config.buy_trigger_price_low)
    if "KXHIGH" in ticker_upper:
        return int(config.buy_trigger_price_high)
    return None


def hedge_policy(
    initial_qty: int,
    hedge_max_factor: int,
    stop_loss_count: int,
) -> tuple[int, bool, int]:
    """
    Compute hedge entry policy for a given stop_loss_count.

    ``hedge_max_factor`` is the **total number of allowed buy levels** (counting
    from 0).  Buying is allowed while ``stop_loss_count < hedge_max_factor``.

    - count=0              → initial_qty                  (initial buy)
    - count=1              → initial_qty * 2              (first recovery)
    - count=factor-1       → initial_qty * 2^(factor-1)  (last allowed buy)
    - count >= factor      → not allowed

    Example: initial=3, factor=3 → allowed counts 0,1,2 → sizes 3,6,12; max=12.

    Returns:
        (next_qty, is_allowed, max_allowed_qty)
    """
    factor = max(int(hedge_max_factor), 1)
    max_allowed_qty = initial_qty * (2 ** (factor - 1))
    is_allowed = stop_loss_count < factor
    next_qty = initial_qty * (2 ** stop_loss_count) if is_allowed else 0
    return next_qty, is_allowed, max_allowed_qty


def _parse_hhmm_et(value: str) -> datetime.time:
    """Parse 'HH:MM' into a :class:`datetime.time` object."""
    h, m = value.strip().split(":")
    return datetime.time(int(h), int(m))


def is_low_entry_halted_et(config: "AppConfig", now_utc: Optional[datetime.datetime] = None) -> tuple[bool, dict]:
    """Return (halted, log_ctx) for the Low-ticker 22:00 ET entry gate.

    Returns ``(True, ctx)`` if new KXLOW entries should be blocked (it is at
    or after the configured halt time in Eastern time), ``(False, {})`` otherwise.
    Always returns ``(False, {})`` when the feature is disabled.
    """
    if not config.low_ticker_entry_halt_enabled:
        return False, {}
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_et = now_utc.astimezone(_ET_ZONE)
    try:
        halt_time = _parse_hhmm_et(config.low_ticker_entry_halt_time_et)
    except (ValueError, AttributeError):
        halt_time = datetime.time(22, 0)
    halted = now_et.time() >= halt_time
    ctx = {
        "now_et": now_et.strftime("%H:%M:%S"),
        "halt_time_et": config.low_ticker_entry_halt_time_et,
    }
    return halted, ctx


def is_low_10pm_ask_eligible(config: "AppConfig", yes_ask: Optional[int]) -> bool:
    """Return True when the Low 10 PM rule should apply for the current ask."""
    if yes_ask is None:
        return False
    threshold = int(getattr(config, "low_ticker_10pm_max_ask", 93))
    return yes_ask < threshold


def is_past_low_pm_close_time_local(
    config: "AppConfig",
    ticker: str,
    now_utc: Optional[datetime.datetime] = None,
) -> tuple[bool, Optional[datetime.date], dict]:
    """Return whether ticker's local wall-clock is at/after LOW_PM_CLOSE_TIME."""
    tz_name = get_series_timezone(ticker)
    if not tz_name:
        return False, None, {}
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_local = now_utc.astimezone(ZoneInfo(tz_name))
    try:
        closeout_time = _parse_hhmm_et(getattr(config, "low_pm_close_time", "22:00"))
    except (ValueError, AttributeError):
        closeout_time = datetime.time(22, 0)
    past = now_local.time() >= closeout_time
    return past, now_local.date(), {
        "timezone": tz_name,
        "local_time": now_local.strftime("%H:%M:%S"),
        "close_time_local": closeout_time.strftime("%H:%M"),
    }


def is_low_pm_close_ask_eligible(config: "AppConfig", yes_ask: Optional[int]) -> bool:
    if yes_ask is None:
        return False
    threshold = int(getattr(config, "low_pm_close_amount", 93))
    return yes_ask < threshold


def is_low_pm_close_override_ticker(config: "AppConfig", ticker: str) -> bool:
    overrides = getattr(config, "pm_tickers_close", set()) or set()
    ticker_upper = ticker.upper()
    series_prefix = ticker_upper.split("-", 1)[0]
    for raw in overrides:
        prefix = str(raw).strip().upper()
        if not prefix:
            continue
        if series_prefix == prefix or ticker_upper.startswith(prefix):
            return True
    return False


@dataclass
class StopLossCycle:
    action_key: str
    state: StopLossCycleState
    trigger_source: str
    trigger_ts_ms: int


class TemperatureStrategy:
    """
    Core state machine for daily high/low temperature market brackets.

    Phase A: Market Monitoring
    Phase B: Trade Entry (with spread check)
    Phase C: Position Management (Stop Loss)
    """

    def __init__(
        self,
        config: AppConfig,
        cache: TickerCache,
        ws_manager: WebSocketManager,
        executor: BaseExecutor,
        db: DatabaseManager,
        stop_loss_watcher: Optional[StopLossWatcher] = None,
    ):
        self.config = config
        self.cache = cache
        self.ws = ws_manager
        self.executor = executor
        self.db = db
        self.stop_loss_watcher = stop_loss_watcher

        # Resting "disaster" limit-sell backstop manager (opt-in via config).
        self.sl_backstop: Optional[SlBackstopManager] = None
        if getattr(config, "sl_backstop_enabled", False):
            self.sl_backstop = SlBackstopManager(
                executor=executor,
                sl_backstop_enabled=True,
                sl_backstop_offset=int(getattr(config, "sl_backstop_offset", 5)),
                stop_loss_price_ask=int(config.stop_loss_price_ask),
                trading_mode=config.trading_mode,
            )

        # Cached loaded date flags (set of date strings already loaded)
        # State: market_ticker -> MarketBracket
        self.brackets: dict[str, MarketBracket] = {}

        # Active positions we hold
        self.active_positions: dict[str, MarketBracket] = {}

        # Watchlist: markets whose price >= monitor_start
        self.watchlist: dict[str, MarketBracket] = {}

        # Cached private key to avoid repeated file reads
        self._private_key = load_private_key(config.kalshi_private_key_path)

        # Running flag
        self._running = False

        # Readiness gate: set to True after _restore_positions() completes
        # successfully.  Risk-critical execution paths must not fire until
        # this is True to prevent acting on stale/incomplete in-memory state.
        self._reconciliation_complete = False
        self._sl_exit_tasks: dict[str, asyncio.Task] = {}
        self._sl_cycles: dict[str, StopLossCycle] = {}
        # Per-ticker app-owned quantity ledger used to prevent exits from touching
        # external/manual holdings when MANAGE_EXTERNAL_POSITIONS=false.
        self._app_owned_qty: dict[str, int] = {}
        # Per-cycle duplicate-entry guard: tracks (series_ticker, date_prefix, count)
        # tuples already entered in the current _evaluate_watchlist cycle.  Prevents
        # multiple brackets in the same series/day from all entering at the same count.
        self._entry_step_seen: set[tuple[str, str, int]] = set()
        # Per-station NWS entry-gate cache:
        # (station, ticker_type) -> (computed_monotonic_ts, has_data, gate_open)
        self._nws_gate_cache: dict[tuple[str, str | None], tuple[float, bool, bool]] = {}
        self._nws_gate_cache_refresh_seconds = 30
        self._sunrise_entry_gate = SunriseEntryGate(config)

        # Bounded queue for non-blocking trade-log persistence (Change B).
        # Trade logging is non-critical; drops are acceptable when the queue
        # is saturated so that DB writes never block the WS reader / SL path.
        _TRADE_LOG_QUEUE_MAX = 500
        self._trade_log_queue: asyncio.Queue = asyncio.Queue(maxsize=_TRADE_LOG_QUEUE_MAX)
        self._trade_log_writer_task: Optional[asyncio.Task] = None
        self._trade_log_drop_count: int = 0
        self._trade_log_drop_last_warned: float = 0.0

        # Dedicated held-position SL evaluation loop (fast, independent of entry scanning).
        self._held_positions_task: Optional[asyncio.Task] = None
        # Throttle the REST get_positions() call inside _evaluate_held_positions so the
        # fast loop (~250 ms) does not flood the API.  Position data is refreshed at most
        # every held_position_price_refresh_seconds (default 10 s); the cheap in-memory
        # SL checks still run every cycle.
        self._positions_api_cache: dict = {}
        self._positions_api_last_fetch: float = 0.0
        # Low-ticker PM close idempotency: track the local date already evaluated
        # for each ticker so it is not re-evaluated within the same local day.
        self._low_ticker_pm_close_eval_dates: dict[str, datetime.date] = {}

        # Intraday checkpoint exit state ------------------------------------------
        # Idempotency: (ticker, "HH:MM") → local date already evaluated.
        self._intraday_checkpoint_eval_dates: dict[tuple[str, str], datetime.date] = {}
        # Confirmation read: (ticker, "HH:MM") → monotonic time of first below-threshold read.
        # Once set, a second read ≥60 s later that is still below threshold triggers exit.
        self._intraday_checkpoint_pending: dict[tuple[str, str], float] = {}
        # HWM arm state: (ticker, local_date) → True when armed.
        self._hwm_armed: dict[tuple[str, datetime.date], bool] = {}
        # HWM confirmation read: ticker → monotonic time of first below-exit-price read.
        self._hwm_pending: dict[str, float] = {}

        # Timestamp-based throttle for portfolio snapshot logging.
        # Using wall-clock time (not a counter) so the interval is not coupled
        # to the strategy-loop cadence and no "counter-modulo" pattern exists
        # that could be mistaken for a periodic sell trigger.
        self._last_snapshot_ts: float = 0.0
        _SNAPSHOT_INTERVAL_S: float = 60.0  # log once per minute
        self._snapshot_interval_s: float = _SNAPSHOT_INTERVAL_S

        # Long-lived HTTP client for REST fetches; created in start(), closed in stop().
        self._http_client: Optional["httpx.AsyncClient"] = None

    @staticmethod
    def _first_non_none(*values):
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _to_cents(raw) -> Optional[int]:
        if raw is None or raw == "":
            return None
        try:
            return round(float(raw) * 100)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_quantity_float(raw) -> float:
        if raw is None or raw == "":
            return 0.0
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _fill_qty(cls, fill: dict) -> float:
        return max(
            cls._to_quantity_float(
                cls._first_non_none(
                    fill.get("count_fp"),
                    fill.get("count"),
                    fill.get("fill_count_fp"),
                    fill.get("fill_count"),
                    fill.get("filled_count"),
                    fill.get("quantity_fp"),
                    fill.get("quantity"),
                )
            ),
            0.0,
        )

    async def _app_owned_qty_from_fills(
        self,
        ticker: str,
        *,
        total_position_qty: int,
        fallback_app_owned_qty: int,
        source: str,
    ) -> int:
        total_qty = max(int(total_position_qty or 0), 0)
        fallback_qty = max(min(int(fallback_app_owned_qty or 0), total_qty), 0)
        if self.config.manage_external_positions or not hasattr(self.executor, "get_fills"):
            return total_qty if self.config.manage_external_positions else fallback_qty

        try:
            fills = await self.executor.get_fills(ticker=ticker)
        except Exception as e:
            logger.warning("reconcile.app_fill_lookup_failed", ticker=ticker, source=source, error=str(e))
            return fallback_qty

        app_net_qty = 0.0
        matched_any = False
        for fill in fills or []:
            fill_ticker = self._first_non_none(fill.get("ticker"), fill.get("market_ticker"), "")
            if fill_ticker != ticker:
                continue
            client_order_id = str(
                self._first_non_none(
                    fill.get("client_order_id"),
                    fill.get("order_client_id"),
                    fill.get("clientOrderId"),
                    "",
                )
            )
            if not client_order_id.startswith(APP_CLIENT_ORDER_PREFIX):
                continue
            qty = self._fill_qty(fill)
            if qty <= 0:
                continue
            matched_any = True
            action = str(self._first_non_none(fill.get("action"), fill.get("side"), "")).lower()
            if action in {"sell", "ask", "sell_yes", "sell_no"}:
                app_net_qty -= qty
            else:
                app_net_qty += qty

        if not matched_any:
            return fallback_qty

        app_owned_qty = max(min(int(round(max(app_net_qty, 0.0))), total_qty), 0)
        logger.info(
            "reconcile.app_fill_matched",
            ticker=ticker,
            source=source,
            total_position_qty=total_qty,
            app_fill_net_qty=app_net_qty,
            app_owned_qty=app_owned_qty,
            external_qty=max(total_qty - app_owned_qty, 0),
        )
        return app_owned_qty

    @staticmethod
    def _market_is_settled(rest_data: Optional[dict]) -> bool:
        if not rest_data:
            return False
        status = str(rest_data.get("status") or "").lower()
        result = str(rest_data.get("result") or "").lower()
        settlement_ts = rest_data.get("settlement_ts")
        is_settled = rest_data.get("is_settled")
        if isinstance(is_settled, str):
            is_settled = is_settled.lower() == "true"
        return bool(
            is_settled
            or settlement_ts
            or status in {"settled", "finalized", "resolved"}
            or result in {"yes", "no"}
        )

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _set_ownership(
        self,
        ticker: str,
        *,
        total_position_qty: int,
        app_owned_qty: int,
        source: str,
        action: str,
    ) -> tuple[int, int]:
        total_qty = max(int(total_position_qty or 0), 0)
        app_qty = max(min(int(app_owned_qty or 0), total_qty), 0)
        external_qty = max(total_qty - app_qty, 0)
        self._app_owned_qty[ticker] = app_qty
        ownership = "app_owned" if app_qty > 0 else "external_manual"
        logger.info(
            "ownership.classified",
            ticker=ticker,
            ownership=ownership,
            total_position_qty=total_qty,
            app_owned_qty=app_qty,
            external_qty=external_qty,
            source=source,
            action=action,
        )
        return app_qty, external_qty

    def _managed_exit_quantity(self, ticker: str, total_position_qty: int) -> tuple[int, int, int]:
        total_qty = max(int(total_position_qty or 0), 0)
        app_qty = self._app_owned_qty.get(ticker)
        if app_qty is None:
            # Default to app-owned for in-memory positions created by app paths.
            app_qty = total_qty
        app_qty = max(min(int(app_qty or 0), total_qty), 0)
        external_qty = max(total_qty - app_qty, 0)
        managed_qty = total_qty if self.config.manage_external_positions else app_qty
        return managed_qty, app_qty, external_qty

    async def _rollback_stop_loss_count_if_counted(self, bracket: MarketBracket) -> None:
        if getattr(bracket, "_stop_loss_counted", False):
            await self._decrement_stop_loss_count_for_market(bracket.market_ticker)
            bracket._stop_loss_counted = False

    async def _confirmed_remaining_stop_loss_qty(
        self,
        bracket: MarketBracket,
        *,
        filled_qty: int = 0,
        action_key: Optional[str] = None,
    ) -> dict[str, int | bool]:
        ticker = bracket.market_ticker
        prior_total_qty = max(int(bracket.position_quantity or 0), 0)
        prior_app_qty = self._app_owned_qty.get(ticker, prior_total_qty)
        try:
            positions = await self.executor.get_positions()
        except Exception as e:
            logger.warning(
                "phase.c.stop_loss_verify_failed",
                ticker=ticker,
                action_key=action_key,
                error=str(e),
            )
            managed_qty, app_owned_qty, external_qty = self._managed_exit_quantity(
                ticker,
                prior_total_qty,
            )
            return {
                "confirmed": False,
                "total_qty": prior_total_qty,
                "managed_qty": managed_qty,
                "app_owned_qty": app_owned_qty,
                "external_qty": external_qty,
            }

        live_position = positions.get(ticker)
        live_total_qty = 0
        if live_position is not None:
            try:
                live_total_qty = max(int(float(live_position.get("count", 0) or 0)), 0)
            except (TypeError, ValueError):
                live_total_qty = 0

        if self.config.manage_external_positions:
            live_app_owned_qty = live_total_qty
        else:
            fallback_app_qty = max(int(prior_app_qty or 0) - max(int(filled_qty or 0), 0), 0)
            fallback_app_qty = min(fallback_app_qty, live_total_qty)
            live_app_owned_qty = await self._app_owned_qty_from_fills(
                ticker,
                total_position_qty=live_total_qty,
                fallback_app_owned_qty=fallback_app_qty,
                source="stop_loss_reconciliation",
            )

        live_external_qty = max(live_total_qty - live_app_owned_qty, 0)
        live_managed_qty = live_total_qty if self.config.manage_external_positions else live_app_owned_qty
        return {
            "confirmed": True,
            "total_qty": live_total_qty,
            "managed_qty": live_managed_qty,
            "app_owned_qty": live_app_owned_qty,
            "external_qty": live_external_qty,
        }

    async def _handle_stop_loss_exhaustion(
        self,
        bracket: MarketBracket,
        *,
        action_key: str,
        trigger_source: str,
        trigger_ts_ms: int,
        attempts: int,
        last_price: Optional[int],
    ) -> dict[str, int | bool]:
        remaining = await self._confirmed_remaining_stop_loss_qty(
            bracket,
            action_key=action_key,
        )
        bracket.position_quantity = int(remaining["total_qty"])
        self._set_ownership(
            bracket.market_ticker,
            total_position_qty=bracket.position_quantity,
            app_owned_qty=int(remaining["app_owned_qty"]),
            source="stop_loss_exhausted",
            action="position_reconciled",
        )
        if int(remaining["managed_qty"]) > 0:
            logger.critical(
                "sl.exit_exhausted_unprotected",
                ticker=bracket.market_ticker,
                action_key=action_key,
                qty=int(remaining["managed_qty"]),
                last_price=last_price,
                stop_loss_price=self.config.stop_loss_price,
                attempts=attempts,
                trigger_source=trigger_source,
                elapsed_ms=self._now_ms() - trigger_ts_ms,
                position_confirmed=bool(remaining["confirmed"]),
            )
            await self._register_stop_loss_watcher(bracket)
            if self.stop_loss_watcher is not None:
                await self.stop_loss_watcher.rearm_position(
                    bracket.market_ticker,
                    trigger_price=last_price,
                )
            self._set_sl_cycle_state(bracket.market_ticker, "RETRYING")
        return remaining

    def _compute_fast_sl_exit_price(self, reference_price: int, attempt: int) -> int:
        offset = max(int(self.config.sl_exit_aggressive_offset_ticks or 0), 0)
        max_slippage = max(int(self.config.sl_exit_max_slippage or 0), 0)
        floor_price = max(1, reference_price - max_slippage)
        ladder_step = max(offset, 1)
        price = reference_price - offset - ((max(attempt, 1) - 1) * ladder_step)
        return max(1, min(99, max(price, floor_price)))

    def _set_sl_cycle_state(self, ticker: str, state: StopLossCycleState) -> None:
        cycle = self._sl_cycles.get(ticker)
        if cycle is not None:
            cycle.state = state

    async def _dispatch_stop_loss_exit(
        self,
        bracket: MarketBracket,
        *,
        trigger_price: int,
        trigger_source: str,
    ) -> None:
        ticker = bracket.market_ticker
        action_key = f"{ticker}:STOP_LOSS"
        current_cycle = self._sl_cycles.get(ticker)
        existing_task = self._sl_exit_tasks.get(ticker)
        if existing_task is not None and not existing_task.done():
            logger.info(
                "sl.trigger_suppressed_in_flight",
                ticker=ticker,
                action_key=action_key,
                state=current_cycle.state if current_cycle is not None else "SUBMITTING",
                trigger_source=trigger_source,
            )
            return

        trigger_ts_ms = self._now_ms()
        self._sl_cycles[ticker] = StopLossCycle(
            action_key=action_key,
            state="TRIGGERED",
            trigger_source=trigger_source,
            trigger_ts_ms=trigger_ts_ms,
        )
        logger.info(
            "sl.trigger_detected",
            ticker=ticker,
            action_key=action_key,
            trigger_source=trigger_source,
            trigger_ts_ms=trigger_ts_ms,
        )
        sl_exit_mode = (self.config.sl_exit_mode or "PANIC_FLATTEN").upper()
        if sl_exit_mode == "PANIC_FLATTEN":
            coro = self._run_panic_flatten_exit(
                bracket=bracket,
                trigger_price=trigger_price,
                trigger_source=trigger_source,
                trigger_ts_ms=trigger_ts_ms,
            )
        else:
            coro = self._run_fast_sl_exit(
                bracket=bracket,
                trigger_price=trigger_price,
                trigger_source=trigger_source,
                trigger_ts_ms=trigger_ts_ms,
            )
        task = asyncio.create_task(coro)
        self._sl_exit_tasks[ticker] = task

        def _cleanup(_task: asyncio.Task) -> None:
            current = self._sl_exit_tasks.get(ticker)
            if current is _task:
                self._sl_exit_tasks.pop(ticker, None)
            self._sl_cycles.pop(ticker, None)

        task.add_done_callback(_cleanup)

    async def _run_fast_sl_exit(
        self,
        bracket: MarketBracket,
        *,
        trigger_price: int,
        trigger_source: str,
        trigger_ts_ms: int,
    ) -> None:
        ticker = bracket.market_ticker
        action_key = f"{ticker}:STOP_LOSS"
        max_attempts = max(int(self.config.sl_exit_max_attempts or 1), 1)
        retry_sleep_s = max(int(self.config.sl_exit_retry_interval_ms or 0), 0) / 1000.0
        for attempt in range(1, max_attempts + 1):
            current = self.active_positions.get(ticker)
            if current is None or current.position_quantity <= 0:
                remaining = await self._confirmed_remaining_stop_loss_qty(
                    bracket,
                    action_key=action_key,
                )
                if int(remaining["managed_qty"]) <= 0:
                    self._set_sl_cycle_state(ticker, "TERMINAL")
                    logger.info(
                        "sl.position_gone",
                        ticker=ticker,
                        action_key=action_key,
                        attempt=attempt,
                        elapsed_ms=self._now_ms() - trigger_ts_ms,
                        reason="position_missing",
                    )
                    return
                bracket.position_quantity = int(remaining["total_qty"])
                self._set_ownership(
                    ticker,
                    total_position_qty=bracket.position_quantity,
                    app_owned_qty=int(remaining["app_owned_qty"]),
                    source="stop_loss_missing_reconciled",
                    action="position_reconciled",
                )
                current = bracket
            self._set_sl_cycle_state(ticker, "SUBMITTING")
            reference_price = current.last_price if current.last_price is not None else trigger_price
            price = self._compute_fast_sl_exit_price(reference_price, attempt)
            market_gone = await self._execute_stop_loss(
                current,
                override_price=price,
                bypass_cooldown=True,
                trigger_ts_ms=trigger_ts_ms,
                attempt=attempt,
            )
            if market_gone:
                self._set_sl_cycle_state(ticker, "TERMINAL")
                await self._decrement_stop_loss_count_for_market(current.market_ticker)
                current._stop_loss_counted = False
                return
            if ticker not in self.active_positions:
                self._set_sl_cycle_state(ticker, "TERMINAL")
                logger.info(
                    "sl.position_gone",
                    ticker=ticker,
                    action_key=action_key,
                    attempt=attempt,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                    reason="position_cleared",
                )
                return
            if attempt < max_attempts:
                self._set_sl_cycle_state(ticker, "RETRYING")
            if attempt < max_attempts and retry_sleep_s > 0:
                await asyncio.sleep(retry_sleep_s)
        remaining = await self._handle_stop_loss_exhaustion(
            bracket,
            action_key=action_key,
            trigger_source=trigger_source,
            trigger_ts_ms=trigger_ts_ms,
            attempts=max_attempts,
            last_price=bracket.last_price if bracket.last_price is not None else trigger_price,
        )
        if int(remaining["managed_qty"]) <= 0:
            self._set_sl_cycle_state(ticker, "TERMINAL")
            await self._decrement_stop_loss_count_for_market(bracket.market_ticker)
            bracket._stop_loss_counted = False
            if int(remaining["total_qty"]) > 0:
                await self._remove_active_position(ticker, bracket)
            logger.info(
                "sl.position_gone",
                ticker=ticker,
                action_key=action_key,
                attempt=max_attempts,
                elapsed_ms=self._now_ms() - trigger_ts_ms,
                reason="position_cleared_after_retry_exhaustion",
            )
            return
        logger.warning(
            "sl.exit_retry_exhausted",
            ticker=ticker,
            action_key=action_key,
            trigger_source=trigger_source,
            attempts=max_attempts,
            elapsed_ms=self._now_ms() - trigger_ts_ms,
            reason="max_attempts_exhausted",
        )
        await self._rollback_stop_loss_count_if_counted(bracket)

    async def _run_panic_flatten_exit(
        self,
        bracket: MarketBracket,
        *,
        trigger_price: int,
        trigger_source: str,
        trigger_ts_ms: int,
    ) -> None:
        """Panic-flatten exit: immediately sell at floor price to guarantee fill speed.

        On trigger, submits a sell at ``sl_panic_sell_price`` (default 1¢) so
        Kalshi matches at the best available bid rather than chasing the ladder.
        Retries rapidly up to ``sl_panic_max_retries`` with ``sl_panic_retry_ms``
        interval if the position is not fully cleared.

        Before each submit attempt, the latest cached YES ask is re-checked.
        If the quote is missing or stale, the submit proceeds in *degraded mode*
        (logged as ``sl.panic_revalidation_degraded``) rather than aborting —
        failing to exit is worse than a marginal false positive.  A hard abort
        only occurs when a fresh quote confirms the ask has genuinely recovered
        above the stop threshold (``sl.panic_revalidation_aborted``,
        ``reason="ask_above_stop"``).
        """
        ticker = bracket.market_ticker
        action_key = f"{ticker}:STOP_LOSS"
        panic_price = max(1, int(self.config.sl_panic_sell_price or 1))
        max_retries = max(int(self.config.sl_panic_max_retries or 1), 1)
        retry_sleep_s = max(int(self.config.sl_panic_retry_ms or 0), 0) / 1000.0
        stop_loss_ask_cents = int(self.config.stop_loss_price)
        max_quote_age_ms = int(self.config.sl_panic_max_quote_age_ms or 30000)

        logger.warning(
            "sl.panic_triggered",
            ticker=ticker,
            action_key=action_key,
            trigger_source=trigger_source,
            trigger_price=trigger_price,
            stop_loss_price_ask=stop_loss_ask_cents,
            panic_price=panic_price,
            qty=bracket.position_quantity,
            trigger_ts_ms=trigger_ts_ms,
        )

        for attempt in range(1, max_retries + 1):
            current = self.active_positions.get(ticker)
            if current is None or current.position_quantity <= 0:
                remaining = await self._confirmed_remaining_stop_loss_qty(
                    bracket,
                    action_key=action_key,
                )
                if int(remaining["managed_qty"]) <= 0:
                    self._set_sl_cycle_state(ticker, "TERMINAL")
                    logger.info(
                        "sl.position_gone",
                        ticker=ticker,
                        action_key=action_key,
                        attempt=attempt,
                        elapsed_ms=self._now_ms() - trigger_ts_ms,
                        reason="position_missing",
                    )
                    return
                bracket.position_quantity = int(remaining["total_qty"])
                self._set_ownership(
                    ticker,
                    total_position_qty=bracket.position_quantity,
                    app_owned_qty=int(remaining["app_owned_qty"]),
                    source="panic_stop_loss_missing_reconciled",
                    action="position_reconciled",
                )
                current = bracket

            # ------------------------------------------------------------------
            # Pre-submit revalidation: re-check ASK condition against the latest
            # cached quote immediately before placing the panic order.
            #
            # If the quote is missing or stale, proceed in *degraded mode*
            # rather than aborting — the initial trigger was already validated
            # and failing to exit is worse than a marginal false positive.
            # Degraded mode is logged explicitly so it is visible in production.
            # A hard abort only occurs when a fresh quote confirms the ask has
            # genuinely recovered above the stop threshold.
            # ------------------------------------------------------------------
            now_ms_rv = self._now_ms()
            quote_rv = self.cache.get_quote(ticker)
            quote_ts_rv = self.cache.get_quote_ts(ticker)

            _degraded_mode = False

            if quote_rv is None:
                logger.warning(
                    "sl.panic_revalidation_degraded",
                    ticker=ticker,
                    action_key=action_key,
                    attempt=attempt,
                    reason="no_cached_quote",
                    stop_loss_price_ask=stop_loss_ask_cents,
                    elapsed_ms=now_ms_rv - trigger_ts_ms,
                )
                _degraded_mode = True
            else:
                best_bid_rv, best_ask_rv = quote_rv

                # Freshness check (skip if max_quote_age_ms=0 or no timestamp)
                if max_quote_age_ms > 0 and quote_ts_rv is not None:
                    age_ms_rv = (now_ms_rv / 1000.0 - quote_ts_rv) * 1000.0
                    if age_ms_rv > max_quote_age_ms:
                        logger.warning(
                            "sl.panic_revalidation_degraded",
                            ticker=ticker,
                            action_key=action_key,
                            attempt=attempt,
                            reason="stale_quote",
                            quote_age_ms=int(age_ms_rv),
                            max_quote_age_ms=max_quote_age_ms,
                            best_bid_yes=best_bid_rv,
                            best_ask_yes=best_ask_rv,
                            stop_loss_price_ask=stop_loss_ask_cents,
                            units="cents",
                            elapsed_ms=now_ms_rv - trigger_ts_ms,
                        )
                        _degraded_mode = True

                if not _degraded_mode:
                    ask_trigger_rv = (
                        best_ask_rv is not None
                        and best_ask_rv <= stop_loss_ask_cents
                    )
                    trigger_met_rv = ask_trigger_rv
                    logger.info(
                        "sl.panic_revalidation",
                        ticker=ticker,
                        action_key=action_key,
                        attempt=attempt,
                        best_bid_yes=best_bid_rv,
                        best_ask_yes=best_ask_rv,
                        stop_loss_price_ask=stop_loss_ask_cents,
                        units="cents",
                        trigger_met=trigger_met_rv,
                        elapsed_ms=now_ms_rv - trigger_ts_ms,
                    )

                    if not trigger_met_rv:
                        self._set_sl_cycle_state(ticker, "TERMINAL")
                        logger.warning(
                            "sl.panic_revalidation_aborted",
                            ticker=ticker,
                            action_key=action_key,
                            attempt=attempt,
                            reason="prices_above_stops",
                            best_bid_yes=best_bid_rv,
                            best_ask_yes=best_ask_rv,
                            stop_loss_price_ask=stop_loss_ask_cents,
                            units="cents",
                            elapsed_ms=now_ms_rv - trigger_ts_ms,
                        )
                        logger.warning(
                            "sl.exit_failed",
                            ticker=ticker,
                            action_key=action_key,
                            attempt=attempt,
                            elapsed_ms=now_ms_rv - trigger_ts_ms,
                            reason="prices_above_stops",
                        )
                        await self._rollback_stop_loss_count_if_counted(current)
                        return
            # ------------------------------------------------------------------

            if attempt == 1:
                self._set_sl_cycle_state(ticker, "SUBMITTING")
                logger.warning(
                    "sl.panic_submit",
                    ticker=ticker,
                    action_key=action_key,
                    panic_price=panic_price,
                    qty=current.position_quantity,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                )
            else:
                self._set_sl_cycle_state(ticker, "RETRYING")
                logger.warning(
                    "sl.panic_retry",
                    ticker=ticker,
                    action_key=action_key,
                    retry_index=attempt - 1,
                    panic_price=panic_price,
                    qty=current.position_quantity,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                )

            try:
                market_gone = await self._execute_stop_loss(
                    current,
                    override_price=panic_price,
                    bypass_cooldown=True,
                    trigger_ts_ms=trigger_ts_ms,
                    attempt=attempt,
                )
            except Exception as exc:
                logger.error(
                    "sl.panic_submit_error",
                    ticker=ticker,
                    action_key=action_key,
                    attempt=attempt,
                    error=str(exc),
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                    reason="submit_error",
                )
                if attempt < max_retries and retry_sleep_s > 0:
                    await asyncio.sleep(retry_sleep_s)
                continue

            if market_gone:
                self._set_sl_cycle_state(ticker, "TERMINAL")
                logger.info(
                    "sl.panic_filled",
                    ticker=ticker,
                    action_key=action_key,
                    attempt=attempt,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                    reason="market_gone",
                )
                await self._decrement_stop_loss_count_for_market(current.market_ticker)
                current._stop_loss_counted = False
                return

            if ticker not in self.active_positions:
                self._set_sl_cycle_state(ticker, "TERMINAL")
                logger.info(
                    "sl.position_gone",
                    ticker=ticker,
                    action_key=action_key,
                    attempt=attempt,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                    reason="position_cleared",
                )
                return

            if attempt < max_retries and retry_sleep_s > 0:
                await asyncio.sleep(retry_sleep_s)

        remaining = await self._handle_stop_loss_exhaustion(
            bracket,
            action_key=action_key,
            trigger_source=trigger_source,
            trigger_ts_ms=trigger_ts_ms,
            attempts=max_retries,
            last_price=bracket.last_price if bracket.last_price is not None else trigger_price,
        )
        if int(remaining["managed_qty"]) <= 0:
            self._set_sl_cycle_state(ticker, "TERMINAL")
            await self._decrement_stop_loss_count_for_market(bracket.market_ticker)
            bracket._stop_loss_counted = False
            if int(remaining["total_qty"]) > 0:
                await self._remove_active_position(ticker, bracket)
            logger.info(
                "sl.position_gone",
                ticker=ticker,
                action_key=action_key,
                attempt=max_retries,
                elapsed_ms=self._now_ms() - trigger_ts_ms,
                reason="position_cleared_after_retry_exhaustion",
            )
            return
        logger.warning(
            "sl.exit_retry_exhausted",
            ticker=ticker,
            action_key=action_key,
            trigger_source=trigger_source,
            panic_price=panic_price,
            attempts=max_retries,
            elapsed_ms=self._now_ms() - trigger_ts_ms,
            reason="max_retries_exhausted",
        )
        await self._rollback_stop_loss_count_if_counted(bracket)

    async def _remove_active_position(self, ticker: str, bracket: MarketBracket):
        bracket.phase = Phase.CLOSED
        self.active_positions.pop(ticker, None)
        self.brackets.pop(ticker, None)
        self._app_owned_qty.pop(ticker, None)
        await self._unregister_stop_loss_watcher(ticker)
        # Cancel any resting backstop so it does not linger as an orphan order.
        await self._cancel_sl_backstop(ticker, reason="position_closed")
        async with await self.db.get_session() as session:
            await session.execute(
                delete(PositionModel).where(PositionModel.market_ticker == ticker)
            )
            await session.commit()

    async def _register_stop_loss_watcher(self, bracket: MarketBracket) -> None:
        if self.stop_loss_watcher is None or bracket.position_quantity <= 0:
            return
        managed_qty, app_owned_qty, external_qty = self._managed_exit_quantity(
            bracket.market_ticker,
            bracket.position_quantity,
        )
        if managed_qty <= 0:
            logger.info(
                "exit.skipped_no_app_qty",
                ticker=bracket.market_ticker,
                total_position_qty=bracket.position_quantity,
                app_owned_qty=app_owned_qty,
                external_qty=external_qty,
                action="watcher_not_registered",
            )
            await self._unregister_stop_loss_watcher(bracket.market_ticker)
            return
        await self.stop_loss_watcher.register_position(
            bracket.market_ticker,
            side="yes",
            quantity=managed_qty,
            sl_price=self.config.stop_loss_price,
        )
        # Place (or replace) the resting backstop order for the app-owned qty.
        await self._place_sl_backstop(bracket, qty=app_owned_qty)

    async def _unregister_stop_loss_watcher(self, ticker: str) -> None:
        if self.stop_loss_watcher is None:
            return
        await self.stop_loss_watcher.unregister_position(ticker)

    async def _place_sl_backstop(
        self, bracket: "MarketBracket", *, qty: Optional[int] = None
    ) -> None:
        """Place (or replace) the resting backstop sell for *bracket*.

        Respects app-owned quantity semantics — only app_owned_qty contracts
        are protected.  No-op when SL_BACKSTOP_ENABLED=false or PAPER mode.
        """
        if self.sl_backstop is None or not self.sl_backstop.enabled:
            return
        ticker = bracket.market_ticker
        if qty is None:
            _, app_owned_qty, _ = self._managed_exit_quantity(
                ticker, bracket.position_quantity
            )
            qty = app_owned_qty
        if qty <= 0:
            return
        order_id = await self.sl_backstop.place(ticker, qty)
        # Persist the order ID to the DB so restarts can find it.
        if order_id is not None:
            await self._persist_backstop_order_id(ticker, order_id)

    async def _cancel_sl_backstop(self, ticker: str, *, reason: str = "exit") -> bool:
        """Cancel the backstop order for *ticker* and await confirmation.

        Must be awaited and return True before an exit sell is placed to
        prevent overselling.  Returns True if no backstop is active.
        """
        if self.sl_backstop is None or not self.sl_backstop.enabled:
            return True
        ok = await self.sl_backstop.cancel(ticker, reason=reason)
        if ok:
            await self._persist_backstop_order_id(ticker, None)
        return ok

    async def _persist_backstop_order_id(
        self, ticker: str, order_id: Optional[str]
    ) -> None:
        """Update Position.sl_backstop_order_id in the DB (best-effort)."""
        try:
            async with await self.db.get_session() as session:
                await session.execute(
                    update(PositionModel)
                    .where(PositionModel.market_ticker == ticker)
                    .values(sl_backstop_order_id=order_id)
                )
                await session.commit()
        except Exception as exc:
            logger.warning(
                "sl.backstop_persist_order_id_failed",
                ticker=ticker,
                order_id=order_id,
                error=str(exc),
            )

    @staticmethod
    def _avg_buy_fill_price_cents_from_fills(fills: list, ticker: str) -> int:
        total_count = 0.0
        weighted_dollars = 0.0
        for fill in fills or []:
            if fill.get("ticker") != ticker and fill.get("market_ticker") != ticker:
                continue
            if (fill.get("action") or "").lower() != "buy":
                continue
            count = fill.get("count_fp") or fill.get("count") or 0
            price = fill.get("yes_price_dollars") or 0
            try:
                count_f = float(count)
                price_f = float(price)
            except (TypeError, ValueError):
                continue
            if count_f > 0 and price_f > 0:
                total_count += count_f
                weighted_dollars += price_f * count_f
        if total_count > 0:
            return round((weighted_dollars / total_count) * 100)
        return 0

    async def _resolve_entry_cost_basis(self, ticker: str) -> tuple[int, Optional[str]]:
        try:
            positions = await self.executor.get_positions()
            pos_data = positions.get(ticker, {}) if isinstance(positions, dict) else {}
            avg_from_positions = int(pos_data.get("average_fill_cost_cents") or 0)
            if avg_from_positions > 0:
                return avg_from_positions, "positions"
        except Exception as e:
            logger.warning("phase.c.entry_backfill_positions_failed",
                           ticker=ticker, error=str(e))

        if hasattr(self.executor, "get_fills"):
            try:
                fills = await self.executor.get_fills(ticker=ticker)
                avg_fn = getattr(self.executor, "_avg_fill_price_cents_from_fills", None)
                if callable(avg_fn):
                    avg_from_fills = int(avg_fn(fills, ticker) or 0)
                else:
                    avg_from_fills = self._avg_buy_fill_price_cents_from_fills(fills, ticker)
                if avg_from_fills > 0:
                    return avg_from_fills, "fills"
            except Exception as e:
                logger.warning("phase.c.entry_backfill_fills_failed",
                               ticker=ticker, error=str(e))

        return 0, None

    async def _seed_rest_price_for_ticker(self, ticker: str) -> bool:
        """Fetch the current price for *ticker* via REST and populate TickerCache.

        Called on startup for every restored/adopted position so the stop-loss
        watcher has a live price from the moment the position is active, even
        before the first WebSocket tick arrives for that ticker.

        Returns True if a price was successfully seeded, False otherwise.
        """
        if self.cache.get_quote(ticker) is not None or self.cache.get_last_price(ticker) is not None:
            # Already have a price — nothing to do.
            return True
        try:
            rest_data = await self._fetch_market_data_via_rest(ticker)
            if rest_data:
                yes_bid = rest_data.get("yes_bid")
                yes_ask = rest_data.get("yes_ask")
                last_price = rest_data.get("price")
                if yes_bid is not None and yes_ask is not None:
                    self.cache.update_quote(ticker, yes_bid, yes_ask)
                    logger.info(
                        "strategy.restored_position_price_seeded",
                        ticker=ticker,
                        yes_bid=yes_bid,
                        yes_ask=yes_ask,
                        source="rest_startup",
                    )
                    return True
                if last_price is not None and last_price > 0:
                    self.cache.update_last_price(ticker, last_price)
                    logger.info(
                        "strategy.restored_position_price_seeded",
                        ticker=ticker,
                        last_price=last_price,
                        source="rest_last_price_startup",
                    )
                    return True
        except Exception as e:
            logger.warning(
                "strategy.restored_position_price_seed_failed",
                ticker=ticker,
                error=str(e),
            )
        logger.warning(
            "strategy.restored_position_no_price",
            ticker=ticker,
            message="No REST price available at startup; stop-loss will wait for first WS tick",
        )
        return False

    async def start(self):
        """Register WebSocket handlers and start the strategy loop."""
        self._running = True

        # Create long-lived HTTP client for REST fetches (avoids per-call TLS handshakes).
        import httpx
        self._http_client = httpx.AsyncClient(timeout=5.0)

        # Register handlers for WebSocket message types
        self.ws.on_message("ticker", self._handle_ticker)
        self.ws.on_message("trade", self._handle_trade)
        self.ws.on_message("orderbook_snapshot", self._handle_orderbook_snapshot)
        self.ws.on_message("orderbook_delta", self._handle_orderbook_delta)
        self.ws.on_message("market_lifecycle_v2", self._handle_lifecycle)

        # One-time REST discovery at startup to get the full list of existing markets.
        # After this, all updates (new markets, price changes) come via WebSocket.
        active_markets = await self.executor.get_active_markets()
        for m in active_markets:
            ticker = m.get("ticker", "")
            if ticker and ("KXHIGH" in ticker.upper() or "KXLOW" in ticker.upper()):
                if ticker not in self.brackets:
                    self.brackets[ticker] = MarketBracket(
                        market_ticker=ticker,
                        event_ticker=m.get("event_ticker", ""),
                        series_ticker=m.get("series_ticker", ""),
                        bracket_label=m.get("title", ""),
                        phase=Phase.MONITORING,
                        falling_knife_guard=False,
                    )

        tickers = list(self.brackets.keys())
        logger.info("strategy.discovered_markets", count=len(tickers))

        # Subscribe to ALL markets via WebSocket — no ticker filter so we get
        # price data for every market. New temperature brackets are auto-detected
        # as they appear in the data or via lifecycle events.
        await self.ws.subscribe("orderbook_snapshot")
        await self.ws.subscribe("orderbook_delta")
        await self.ws.subscribe("market_lifecycle_v2")
        await self.ws.subscribe("ticker")
        await self.ws.subscribe("trade")

        # Restore positions BEFORE starting the strategy loop, so we don't
        # attempt to re-buy markets we already hold.
        logger.info("strategy.reconciliation_starting")
        await self._restore_positions()
        logger.info(
            "strategy.reconciliation_complete",
            restored_positions=len(self.active_positions),
        )

        # Start the strategy evaluation loop
        asyncio.create_task(self._strategy_loop())

        # Start the dedicated held-position SL evaluation loop (fast, independent of
        # entry scanning — decoupled so a slow watchlist cycle never delays SL checks).
        self._held_positions_task = asyncio.create_task(self._held_positions_loop())

        # Start the background trade-log writer (Change B)
        self._trade_log_writer_task = asyncio.create_task(self._trade_log_writer())

        logger.info("strategy.started",
                     monitor_start=self.config.monitor_start_price,
                     buy_trigger_low=self.config.buy_trigger_price_low,
                     buy_trigger_high=self.config.buy_trigger_price_high,
                     minimum_spread=self.config.minimum_spread,
                     spread_monitor=self.config.spread_monitor_price,
                     stop_loss=self.config.stop_loss_price,
                     mode=self.config.trading_mode,
                     low_trades=self.config.low_trades,
                     high_trades=self.config.high_trades,
                     manage_external_positions=self.config.manage_external_positions,
                     enable_local_settle_gate=self.config.enable_local_settle_gate,
                     default_entry_start_local=self.config.default_entry_start_local,
                     phoenix_entry_start_local=self.config.phoenix_entry_start_local,
                     restored_positions=len(self.active_positions))
        if self.config.entry_gate_mode == "SUNRISE":
            logger.info(
                "strategy.sunrise_gate_config",
                entry_gate_mode=self.config.entry_gate_mode,
                sunrise_strategy_time=self.config.sunrise_strategy_time,
                sunrise_entry_window_minutes=self.config.sunrise_entry_window_minutes,
                sunrise_require_temp_rising=self.config.sunrise_require_temp_rising,
                sunrise_source=self.config.sunrise_source,
                sunrise_require_am_low=self.config.sunrise_require_am_low,
                nws_low_deadline_hour=self.config.nws_low_deadline_hour,
                sunrise_temp_rise_required=self.config.sunrise_temp_rise_required,
                sunrise_temp_baseline_minutes=self.config.sunrise_temp_baseline_minutes,
            )

        hedge_max = int(self.config.hedge_max_factor)
        initial_qty = self.config.initial_contract_count
        _, _, max_allowed_qty = hedge_policy(initial_qty, hedge_max, 0)
        logger.info(
            "strategy.hedge_cap_active",
            hedge_max_factor=hedge_max,
            initial_contract_count=initial_qty,
            max_allowed_qty=max_allowed_qty,
            message=(
                f"Martingale cap: initial={initial_qty}, factor={hedge_max}, "
                f"max_qty={max_allowed_qty}; buying blocked when stop_loss_count >= {hedge_max}"
            ),
        )

        # Start DB cleanup task (runs hourly)
        asyncio.create_task(self._db_cleanup_loop())

        # Start Low-ticker PM close loop (per-ticker local time)
        asyncio.create_task(self._low_ticker_closeout_loop())

        # Start settlement reconciler loop
        asyncio.create_task(self._settlement_reconciler_loop())

        # Start intraday checkpoint + HWM exit loop
        asyncio.create_task(self._intraday_exit_loop())

    async def _restore_positions(self):
        """
        On startup, re-populate active_positions from the database
        so that position management continues across restarts.  Also mark
        restored brackets as crossed_buy so the strategy does not attempt
        to re-enter them.

        Sets ``_reconciliation_complete = True`` on success so that
        risk-critical execution paths (the readiness gate) know they can
        safely act on in-memory state.
        """
        self._reconciliation_complete = False
        try:
            await self._restore_positions_inner()
        except Exception as exc:
            logger.error(
                "strategy.reconciliation_failed",
                error=str(exc),
                restored_so_far=len(self.active_positions),
            )
            raise
        else:
            self._reconciliation_complete = True

    async def _restore_positions_inner(self):
        """Internal implementation of position restore; called by _restore_positions."""
        async with await self.db.get_session() as session:
            # Only restore positions from the last 3 days (old settled positions
            # cause noise on every restart as they get immediately cleaned up).
            three_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=3)
            result = await session.execute(
                select(PositionModel).where(
                    PositionModel.quantity > 0,
                    PositionModel.position_ts >= three_days_ago
                )
            )
            db_positions = result.scalars().all()
        db_by_ticker = {pos.market_ticker: pos for pos in db_positions}

        # Use Eastern today as the reference for stale-position checks, consistent
        # with how tickers are dated (get_eastern_today_date_prefix).  Fail open
        # (do not skip any positions) if the date cannot be parsed.
        today_eastern = _parse_date_prefix(get_eastern_today_date_prefix())
        api_positions: dict[str, dict] = {}

        # In LIVE mode, also fetch positions directly from Kalshi API
        if self.config.trading_mode == "LIVE":
            try:
                api_positions = await self.executor.get_positions()
                for ticker, pos_data in api_positions.items():
                    # Skip empty/zero-quantity positions
                    qty = int(float(pos_data.get("count", 0)))
                    if qty <= 0:
                        continue
                    # Skip positions whose market date is before today — they have
                    # already settled overnight and no longer exist on the exchange.
                    parsed = parse_series_and_date(ticker)
                    if parsed is not None:
                        _, date_prefix = parsed
                        market_date = _parse_date_prefix(date_prefix)
                        if today_eastern is not None and market_date is not None and market_date < today_eastern:
                            logger.info("strategy.skipped_stale_position",
                                        ticker=ticker, date_prefix=date_prefix)
                            continue
                    bracket = self.brackets.get(ticker)
                    if bracket is None:
                        bracket = MarketBracket(
                            market_ticker=ticker,
                            event_ticker="",
                            series_ticker="",
                            bracket_label="",
                            phase=Phase.HOLDING,
                            falling_knife_guard=False,
                        )
                        self.brackets[ticker] = bracket
                    bracket.phase = Phase.HOLDING
                    bracket.crossed_buy = True
                    bracket.position_quantity = qty
                    db_pos = db_by_ticker.get(ticker)
                    db_qty = max(int((db_pos.quantity if db_pos else 0) or 0), 0)
                    if self.config.manage_external_positions:
                        app_owned_qty = qty
                    else:
                        app_owned_qty = await self._app_owned_qty_from_fills(
                            ticker,
                            total_position_qty=qty,
                            fallback_app_owned_qty=min(db_qty, qty),
                            source="startup_live_positions",
                        )
                    app_owned_qty, external_qty = self._set_ownership(
                        ticker,
                        total_position_qty=qty,
                        app_owned_qty=app_owned_qty,
                        source="startup_live_positions",
                        action="position_restored",
                    )
                    entry = pos_data.get("average_fill_cost_cents", 0) or 0
                    entry_source = "api"
                    if entry <= 0:
                        db_entry = (db_pos.avg_entry_price or 0) if db_pos else 0
                        if db_entry > 0:
                            entry = db_entry
                            entry_source = "db"
                        else:
                            entry_source = "none"
                    if entry > 0:
                        bracket.avg_entry = entry
                        bracket.last_price = entry
                    elif not bracket.avg_entry or bracket.avg_entry <= 0:
                        bracket.avg_entry = 0
                    self.active_positions[ticker] = bracket
                    # Adopt any persisted backstop order so _register_stop_loss_watcher
                    # can cancel+replace it rather than creating a duplicate.
                    if self.sl_backstop is not None:
                        db_pos_rec = db_by_ticker.get(ticker)
                        existing_bsp_id = (
                            getattr(db_pos_rec, "sl_backstop_order_id", None) or None
                        )
                        if existing_bsp_id:
                            self.sl_backstop.set_order_id(ticker, existing_bsp_id)
                            logger.info(
                                "sl.backstop_orphan_adopted",
                                ticker=ticker,
                                order_id=existing_bsp_id,
                                source="startup_live_positions",
                            )
                    await self._register_stop_loss_watcher(bracket)
                    logger.info("strategy.restored_live_position", ticker=ticker,
                                qty=qty, entry=bracket.avg_entry, entry_source=entry_source,
                                app_owned_qty=app_owned_qty, external_qty=external_qty)
                    await self._seed_rest_price_for_ticker(ticker)
            except Exception as e:
                logger.error("strategy.restore_positions_error", error=str(e))

        for pos in db_positions:
            ticker = pos.market_ticker
            # Skip positions whose market date is before today — they have
            # already settled overnight and no longer exist on the exchange.
            parsed = parse_series_and_date(ticker)
            if parsed is not None:
                _, date_prefix = parsed
                market_date = _parse_date_prefix(date_prefix)
                if today_eastern is not None and market_date is not None and market_date < today_eastern:
                    logger.info("strategy.skipped_stale_position",
                                ticker=ticker, date_prefix=date_prefix)
                    continue
            bracket = self.brackets.get(ticker)
            if bracket is None:
                bracket = MarketBracket(
                    market_ticker=ticker,
                    event_ticker=pos.event_ticker or "",
                    series_ticker=pos.series_ticker or "",
                    bracket_label="",
                    phase=Phase.HOLDING,
                    falling_knife_guard=False,
                )
                self.brackets[ticker] = bracket

            bracket.phase = Phase.HOLDING
            bracket.crossed_buy = True
            api_qty_raw = (api_positions.get(ticker) or {}).get("count", 0) if self.config.trading_mode == "LIVE" else 0
            try:
                api_qty = int(float(api_qty_raw or 0))
            except (TypeError, ValueError):
                api_qty = 0
            total_qty = max(api_qty, int(pos.quantity or 0))
            bracket.position_quantity = total_qty
            bracket.avg_entry = pos.avg_entry_price or 0
            bracket.last_price = pos.last_price
            bracket.hedge_market = pos.hedge_market_ticker
            bracket.hedge_quantity = pos.hedge_quantity
            if self.config.manage_external_positions:
                app_owned_qty = total_qty
            else:
                app_owned_qty = await self._app_owned_qty_from_fills(
                    ticker,
                    total_position_qty=total_qty,
                    fallback_app_owned_qty=min(int(pos.quantity or 0), total_qty),
                    source="startup_db_positions",
                )
            app_owned_qty, external_qty = self._set_ownership(
                ticker,
                total_position_qty=total_qty,
                app_owned_qty=app_owned_qty,
                source="startup_db_positions",
                action="position_restored",
            )

            self.active_positions[ticker] = bracket
            # Adopt any persisted backstop order so _register_stop_loss_watcher
            # can cancel+replace it rather than creating a duplicate.
            if self.sl_backstop is not None:
                existing_bsp_id = getattr(pos, "sl_backstop_order_id", None) or None
                if existing_bsp_id:
                    self.sl_backstop.set_order_id(ticker, existing_bsp_id)
                    logger.info(
                        "sl.backstop_orphan_adopted",
                        ticker=ticker,
                        order_id=existing_bsp_id,
                        source="startup_db_positions",
                    )
            await self._register_stop_loss_watcher(bracket)
            logger.info("strategy.restored_position", ticker=ticker,
                        qty=total_qty, entry=bracket.avg_entry,
                        hedge_market=bracket.hedge_market,
                        app_owned_qty=app_owned_qty, external_qty=external_qty)
            await self._seed_rest_price_for_ticker(ticker)

    async def _ensure_bracket(self, market_ticker: str, event_ticker: str = "", series_ticker: str = "", bracket_label: str = ""):
        """Create a new MarketBracket if the ticker is a temperature market and unknown."""
        if market_ticker in self.brackets:
            return
        today_prefix = get_eastern_today_date_prefix(days_offset=0)
        if today_prefix not in market_ticker:
            return
        # Only track KXHIGH/KXLOW temperature markets
        if not ("KXHIGH" in market_ticker.upper() or "KXLOW" in market_ticker.upper()):
            return
        self.brackets[market_ticker] = MarketBracket(
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
            bracket_label=bracket_label,
            phase=Phase.MONITORING,
            falling_knife_guard=False,
        )
        logger.debug("strategy.new_bracket_discovered", ticker=market_ticker, label=bracket_label)

    async def _handle_ticker(self, msg: dict):
        """Process ticker updates from WebSocket."""
        ticker_data = msg.get("msg", msg)
        market_ticker = ticker_data.get("market_ticker") or ticker_data.get("ticker")
        if not market_ticker:
            return

        last_price_raw = ticker_data.get("last_price")
        # Prefer *_dollars variants (authoritative); fall back to bare fields
        yes_bid_raw = self._first_non_none(
            ticker_data.get("yes_bid_dollars"),
            ticker_data.get("yes_bid"),
        )
        yes_ask_raw = self._first_non_none(
            ticker_data.get("yes_ask_dollars"),
            ticker_data.get("yes_ask"),
        )

        # Convert dollars to cents
        last_price = self._to_cents(last_price_raw)
        yes_bid = self._to_cents(yes_bid_raw)
        yes_ask = self._to_cents(yes_ask_raw)

        if last_price is not None:
            self.cache.update_last_price(market_ticker, last_price)

        # Cache YES bid/ask from ticker channel — this is the authoritative price source
        if yes_bid is not None and yes_ask is not None:
            self.cache.update_quote(market_ticker, yes_bid, yes_ask)

        # RISK FIRST: feed the stop-loss watcher before any discovery/bookkeeping
        if self.stop_loss_watcher is not None and (yes_ask is not None or yes_bid is not None):
            await self.stop_loss_watcher.on_market_update(
                market_ticker,
                best_ask=yes_ask,
                best_bid=yes_bid,
            )

        # Auto-discover new temperature markets (non-critical path — fire-and-forget
        # so a slow REST/DB round-trip never delays SL evaluation on the next tick)
        asyncio.create_task(self._ensure_bracket(market_ticker))

        # Update brackets in state
        if market_ticker in self.brackets:
            bracket = self.brackets[market_ticker]
            bracket.last_price = last_price

    async def _handle_trade(self, msg: dict):
        """Process trade updates - log to database."""
        trade_data = msg.get("msg", msg)
        market_ticker = trade_data.get("market_ticker")
        price = trade_data.get("price")
        quantity = trade_data.get("quantity")
        side = trade_data.get("side")
        trade_ts = trade_data.get("ts")

        if not market_ticker or price is None:
            return

        # Update last price synchronously — cheap, on the hot path.
        self.cache.update_last_price(market_ticker, price)

        # Enqueue the trade record for background persistence.
        # If the queue is full we drop the row (non-critical bookkeeping) so
        # that the WS reader is never blocked by DB writes.
        record = {
            "market_ticker": market_ticker,
            "price": price,
            "quantity": quantity or 0,
            "side": side,
            "trade_ts": trade_ts,
        }
        try:
            self._trade_log_queue.put_nowait(record)
        except asyncio.QueueFull:
            self._trade_log_drop_count += 1
            now = time.monotonic()
            if now - self._trade_log_drop_last_warned >= 60.0:
                self._trade_log_drop_last_warned = now
                logger.warning(
                    "trade_log.queue_full_dropping",
                    dropped_since_last_warn=self._trade_log_drop_count,
                )
                self._trade_log_drop_count = 0

    async def _trade_log_writer(self) -> None:
        """Background task: drains ``_trade_log_queue`` and persists records to DB.

        Trade logging is non-critical bookkeeping.  This task runs independently
        of the WS reader so that DB commits never block SL trigger handling.
        """
        logger.info("trade_log_writer.started")
        try:
            while True:
                record = await self._trade_log_queue.get()
                try:
                    trade_ts = record["trade_ts"]
                    async with await self.db.get_session() as session:
                        st = StreamedTrade(
                            market_ticker=record["market_ticker"],
                            price=record["price"],
                            quantity=record["quantity"],
                            side=record["side"],
                            trade_ts=(
                                datetime.datetime.fromtimestamp(trade_ts / 1000)
                                if trade_ts else datetime.datetime.utcnow()
                            ),
                        )
                        session.add(st)
                        await session.commit()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("trade_log_writer.write_failed", error=str(exc))
                finally:
                    self._trade_log_queue.task_done()
        except asyncio.CancelledError:
            logger.info("trade_log_writer.cancelled")
            raise
        finally:
            logger.info("trade_log_writer.stopped")

    async def _handle_orderbook_snapshot(self, msg: dict):
        """Process orderbook snapshot - initialize cache baseline price."""
        data = msg.get("msg", msg)
        market_ticker = data.get("market_ticker")
        if not market_ticker:
            return
        
        self.cache.update_orderbook_snapshot(market_ticker, data)
        
        ob = self.cache.get_orderbook(market_ticker)
        if ob and ob.best_ask is not None:
            price = ob.best_ask
            self.cache.update_last_price(market_ticker, price)
            
            # Record the initial snapshot price
            if market_ticker in self.brackets:
                bracket = self.brackets[market_ticker]
                bracket.last_price = price

            # RISK FIRST: Feed orderbook-derived prices into the SL watcher before
            # any discovery/bookkeeping. Forward both best_ask and best_bid so the
            # watcher can log bid prices for context.
            if self.stop_loss_watcher is not None and price > 0:
                best_bid = ob.best_bid if ob.best_bid is not None else None
                await self.stop_loss_watcher.on_market_update(
                    market_ticker, best_ask=price, best_bid=best_bid
                )

        # Auto-discover new temperature markets (non-critical path)
        asyncio.create_task(
            self._ensure_bracket(market_ticker, bracket_label=data.get("title", ""))
        )

    async def _handle_orderbook_delta(self, msg: dict):
        """Process orderbook delta - update cached price."""
        data = msg.get("msg", msg)
        market_ticker = data.get("market_ticker")
        if not market_ticker:
            return
        
        self.cache.update_orderbook_delta(market_ticker, data)
        
        ob = self.cache.get_orderbook(market_ticker)
        if not ob:
            # Auto-discover new temperature markets (non-critical path)
            asyncio.create_task(self._ensure_bracket(market_ticker))
            return
            
        current_price = ob.best_ask
        if current_price is not None:
            self.cache.update_last_price(market_ticker, current_price)
            
            if market_ticker in self.brackets:
                self.brackets[market_ticker].last_price = current_price

            # RISK FIRST: Feed orderbook-derived prices into the SL watcher before
            # any discovery/bookkeeping. Forward both best_ask and best_bid so the
            # watcher can log bid prices for context.
            if self.stop_loss_watcher is not None and current_price > 0:
                best_bid = ob.best_bid if ob.best_bid is not None else None
                await self.stop_loss_watcher.on_market_update(
                    market_ticker, best_ask=current_price, best_bid=best_bid
                )

        # Auto-discover new temperature markets (non-critical path)
        asyncio.create_task(self._ensure_bracket(market_ticker))

    async def _handle_lifecycle(self, msg: dict):
        """Handle market lifecycle events (new markets, status changes)."""
        data = msg.get("msg", msg)
        event_type = data.get("type", "")

        if event_type == "created":
            market_ticker = data.get("market_ticker", "")
            event_ticker = data.get("event_ticker", "")
            series_ticker = data.get("series_ticker", "")
            today_prefix = get_eastern_today_date_prefix(days_offset=0)

            if market_ticker and today_prefix not in market_ticker:
                return

            # Before adding the bracket, check if this is a NEW event
            # that we don't have brackets for yet. If so, fetch ALL of them.
            if event_ticker and series_ticker:
                known_events = {b.event_ticker for b in self.brackets.values() if b.event_ticker}
                if event_ticker not in known_events:
                    import httpx
                    from app.signing import load_private_key, build_auth_headers
                    private_key = load_private_key(self.config.kalshi_private_key_path)
                    headers = build_auth_headers(private_key, self.config.kalshi_api_key, "GET", "/trade-api/v2/markets")
                    url = f"{self.config.rest_base_url}/trade-api/v2/markets"
                    try:
                        client = self._http_client
                        if client is None or client.is_closed:
                            import httpx as _httpx
                            client = _httpx.AsyncClient(timeout=5.0)
                        resp = await client.get(url, headers=headers, params={"event_ticker": event_ticker, "limit": 100})
                        if resp.status_code in (200, 201):
                            all_markets = resp.json().get("markets", [])
                            count = 0
                            for m in all_markets:
                                t = m.get("ticker", "")
                                if t:
                                    existed = t in self.brackets
                                    await self._ensure_bracket(
                                        t,
                                        event_ticker=event_ticker,
                                        series_ticker=series_ticker,
                                        bracket_label=m.get("title", ""),
                                    )
                                    if not existed and t in self.brackets:
                                        count += 1
                            logger.info("strategy.new_event_brackets",
                                        event_ticker=event_ticker, count=count)
                    except Exception as e:
                        logger.error("strategy.new_event_brackets_error",
                                      event_ticker=event_ticker, error=str(e))

            if market_ticker:
                await self._ensure_bracket(
                    market_ticker,
                    event_ticker=event_ticker,
                    series_ticker=series_ticker,
                    bracket_label=data.get("title", ""),
                )

    async def _strategy_loop(self):
        """
        Main strategy evaluation loop runs every ~1 second.
        Evaluates all brackets and transitions phases.
        Held-position SL management runs in the separate _held_positions_loop.
        """
        while self._running:
            if not self._reconciliation_complete:
                now_gate = asyncio.get_event_loop().time()
                last_gate_log = getattr(self, "_last_gate_log", 0)
                if now_gate - last_gate_log >= 10:
                    self._last_gate_log = now_gate
                    logger.warning(
                        "strategy.readiness_gate_blocking",
                        msg="strategy loop blocked until reconciliation completes",
                    )
                await asyncio.sleep(1)
                continue
            try:
                await asyncio.wait_for(self._evaluate_watchlist(), timeout=30.0)
                await asyncio.wait_for(self._log_snapshot(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.error("strategy.loop_timeout", msg="A strategy step timed out and was skipped")
            except Exception as e:
                logger.error("strategy.loop_error", error=str(e), exc_info=True)
            await asyncio.sleep(1)

    async def _held_positions_loop(self):
        """
        Dedicated fast loop for held-position SL management.

        Runs independently of the entry-scanning _strategy_loop so that a slow
        watchlist cycle never delays stop-loss evaluation.  Cadence is controlled
        by held_positions_loop_interval_ms (default 250 ms).

        REST get_positions() calls inside _evaluate_held_positions are throttled
        to at most once per held_position_price_refresh_seconds (default 10 s) so
        the higher cycle frequency does not multiply API call volume.
        """
        interval_s = max(0.05, self.config.held_positions_loop_interval_ms / 1000.0)
        while self._running:
            if not self._reconciliation_complete:
                now_gate = asyncio.get_event_loop().time()
                last_gate_log = getattr(self, "_last_held_pos_gate_log", 0)
                if now_gate - last_gate_log >= 10:
                    self._last_held_pos_gate_log = now_gate
                    logger.warning(
                        "held_positions.readiness_gate_blocking",
                        msg="held-position loop blocked until reconciliation completes",
                    )
                await asyncio.sleep(interval_s)
                continue
            try:
                await asyncio.wait_for(self._evaluate_held_positions(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error(
                    "held_positions.loop_timeout",
                    msg="_evaluate_held_positions timed out and was skipped",
                )
            except Exception as e:
                logger.error("held_positions.loop_error", error=str(e), exc_info=True)
            await asyncio.sleep(interval_s)

    async def _fetch_live_prices(self, tickers: list[str]) -> dict[str, OrderBook]:
        """
        Get live prices from WebSocket cache.
        Uses orderbook cache (if available) and ticker last_price cache.
        Does NOT make REST calls — that would be too slow/rate-limited.
        """
        results = {}
        if not tickers:
            return results

        for t in tickers:
            ob = self.cache.get_orderbook(t)
            if ob and ob.best_ask is not None and ob.best_bid is not None:
                results[t] = ob
            else:
                # Check ticker cache for a last_price we can use
                lp = self.cache.get_last_price(t)
                if lp and lp > 0:
                    from core.types import OrderBookLevel
                    level = OrderBookLevel(price=lp, quantity=1, order_count=0)
                    results[t] = OrderBook(yes_bids=[level], yes_asks=[level])

        return results

    async def _evaluate_watchlist(self):
        """
        Simple entry check: every cycle, loop all brackets.
        Uses WebSocket ticker quote for prices (primary, instant).
        Falls back to REST for brackets that have no cached quote data.
        Max 5 REST calls per cycle to avoid rate limits.
        """
        # Reset per-cycle duplicate-entry guard each time we start a new sweep.
        self._entry_step_seen = set()
        rest_calls_this_cycle = 0
        max_rest_per_cycle = 5

        for ticker, bracket in list(self.brackets.items()):
            price = None
            spread = None
            rest_data = None
            yes_bid = None
            yes_ask = None

            # Primary source: ticker channel quote (yes_ask as price, yes_ask - yes_bid as spread)
            quote = self.cache.get_quote(ticker)
            if quote is not None:
                yes_bid_q, yes_ask_q = quote
                yes_bid = yes_bid_q
                yes_ask = yes_ask_q
                price = yes_ask_q
                spread = yes_ask_q - yes_bid_q

            should_evaluate_entry = not bracket.crossed_buy and bracket.phase == Phase.MONITORING

            # Fallback: REST endpoint (entry-evaluation brackets only)
            if price is None and should_evaluate_entry and rest_calls_this_cycle < max_rest_per_cycle:
                rest_data = await self._fetch_market_data_via_rest(ticker)
                rest_calls_this_cycle += 1
                if rest_data:
                    yes_bid = rest_data.get("yes_bid")
                    yes_ask = rest_data.get("yes_ask")
                    if "yes_ask" in rest_data and "yes_bid" in rest_data:
                        price = rest_data["yes_ask"]
                        spread = rest_data["yes_ask"] - rest_data["yes_bid"]
                    elif "yes_ask" in rest_data:
                        price = rest_data["yes_ask"]
                    elif "price" in rest_data:
                        price = rest_data["price"]
                    if spread is None and rest_data and "spread" in rest_data:
                        spread = rest_data["spread"]

            if price is None:
                continue

            bracket.last_price = price

            buy_trigger = get_buy_trigger_price(self.config, ticker)

            if price > self.config.spread_monitor_price:
                bracket.falling_knife_guard = True
            elif buy_trigger is not None and price < buy_trigger:
                bracket.falling_knife_guard = False

            if not should_evaluate_entry:
                continue

            if buy_trigger is None:
                logger.info("phase.b.entry_blocked_unknown_family", ticker=ticker)
                continue

            # Skip if we don't have both price (yes_ask) and spread
            if spread is None:
                continue

            if (
                yes_bid is not None
                and yes_ask is not None
                and yes_ask >= 99
                and yes_bid <= self.config.eval_price_floor
            ):
                continue

            # Skip near-dead brackets early (quietly) — they will never reach buy_trigger.
            if price <= self.config.eval_price_floor:
                continue

            if price < buy_trigger:
                logger.debug("phase.b.below_trigger", ticker=ticker, price=price,
                             buy_trigger=buy_trigger)
                continue

            if price > self.config.spread_monitor_price:
                # Price above the maximum we're willing to enter; log and skip
                logger.info("phase.b.missed_entry", ticker=ticker,
                            price=price, max_price=self.config.spread_monitor_price)
                continue

            if bracket.falling_knife_guard:
                logger.info("phase.b.falling_knife_blocked", ticker=ticker, price=price)
                continue

            if spread <= self.config.minimum_spread:
                # --- No-trade ticker gate ---
                if self.config.no_trade_tickers:
                    ticker_upper = ticker.upper()
                    if any(ticker_upper == nt or ticker_upper.startswith(nt + "-")
                           for nt in self.config.no_trade_tickers):
                        logger.info("phase.b.entry_blocked_by_config",
                                    ticker=ticker, reason="NO_TRADE_TICKERS")
                        continue
                # ----------------------------

                # --- Trade-direction toggle gate ---
                ticker_upper = ticker.upper()
                is_high = "KXHIGH" in ticker_upper
                is_low = "KXLOW" in ticker_upper
                if is_high and not self.config.high_trades:
                    logger.info("phase.b.entry_blocked_by_config",
                                ticker=ticker, reason="HIGH_TRADES=no")
                    continue
                if is_low and not self.config.low_trades:
                    logger.info("phase.b.entry_blocked_by_config",
                                ticker=ticker, reason="LOW_TRADES=no")
                    continue
                # -----------------------------------

                # --- Low-ticker Eastern-time entry halt gate (22:00 ET) ---
                if is_low:
                    _halted, _halt_ctx = is_low_entry_halted_et(self.config)
                    if _halted and is_low_10pm_ask_eligible(self.config, yes_ask):
                        logger.info(
                            "entry.blocked_low_after_2200_et",
                            ticker=ticker,
                            yes_ask=yes_ask,
                            low_10pm_max_ask=self.config.low_ticker_10pm_max_ask,
                            **_halt_ctx,
                        )
                        continue
                # -----------------------------------------------------------

                # --- City-local-time settle gate (Low tickers only) ---
                # KXHIGH* tickers are never subject to this gate.
                # KXLOW* tickers must wait until their city's local resume time
                # (01:00 local standard; 00:00 for Phoenix) before new entries
                # are allowed, providing the RESUME half of the STOP/RESUME rule.
                _market_date = None
                parsed = parse_series_and_date(ticker)
                if parsed is not None:
                    _, _date_prefix = parsed
                    _market_date = _parse_date_prefix(_date_prefix)
                if is_low:
                    gate_ok, gate_ctx = is_entry_allowed(ticker, self.config, market_date=_market_date)
                    if not gate_ok:
                        logger.info("entry.blocked_local_settle_gate", **gate_ctx)
                        continue
                # -------------------------------------------------------

                use_nws_window_for_low = True
                if is_low and self.config.entry_gate_mode == "SUNRISE":
                    sunrise_decision = self._sunrise_entry_gate.evaluate(
                        ticker=ticker,
                        now_utc=datetime.datetime.now(datetime.timezone.utc),
                    )
                    if not sunrise_decision.allowed:
                        continue
                    use_nws_window_for_low = sunrise_decision.use_nws_window_fallback

                # --- NWS temperature-window gate ---
                _station = get_series_station_code(ticker)
                apply_nws_temp_gate = (
                    _station is not None
                    and (
                        not is_low
                        or self.config.entry_gate_mode == "NWS_WINDOW"
                        or use_nws_window_for_low
                    )
                )
                if apply_nws_temp_gate:
                    try:
                        from nws.gate import has_forecast, is_trading_gate_open
                        now_utc = datetime.datetime.now(datetime.timezone.utc)
                        cache_now = time.monotonic()
                        _ticker_type = "HIGH" if is_high else ("LOW" if is_low else None)
                        _cache_key = (_station, _ticker_type)
                        cache_entry = self._nws_gate_cache.get(_cache_key)
                        if (
                            cache_entry is None
                            or cache_now - cache_entry[0] >= self._nws_gate_cache_refresh_seconds
                        ):
                            _has_data = await asyncio.to_thread(
                                has_forecast, _station, now_utc, _market_date
                            )
                            _gate_open = True
                            if _has_data:
                                _gate_open = await asyncio.to_thread(
                                    is_trading_gate_open,
                                    _station,
                                    now_utc,
                                    _market_date,
                                    _ticker_type,
                                )
                            self._nws_gate_cache[_cache_key] = (cache_now, _has_data, _gate_open)
                        else:
                            _, _has_data, _gate_open = cache_entry
                        if not _has_data:
                            logger.info(
                                "entry.blocked_nws_temp_gate_no_data",
                                ticker=ticker,
                                station=_station,
                            )
                            continue
                        if not _gate_open:
                            logger.info(
                                "entry.blocked_nws_temp_gate",
                                ticker=ticker,
                                station=_station,
                                now_utc=now_utc.isoformat(),
                            )
                            continue
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "entry.blocked_nws_temp_gate_error",
                            ticker=ticker,
                            station=_station,
                            exc_info=True,
                        )
                        continue
                # -----------------------------------

                bracket.crossed_buy = True
                count = await self._get_stop_loss_count_for_market(ticker)
                hedge_max = int(self.config.hedge_max_factor)
                next_qty, is_allowed, max_allowed_qty = hedge_policy(
                    self.config.initial_contract_count, hedge_max, count
                )
                live_total_qty = max(int(bracket.position_quantity or 0), 0)
                try:
                    live_positions = await self.executor.get_positions()
                    live_raw = (live_positions.get(ticker) or {}).get("count", 0)
                    live_total_qty = max(live_total_qty, max(int(float(live_raw or 0)), 0))
                except Exception:
                    pass
                fallback_app_qty = min(
                    max(int(self._app_owned_qty.get(ticker, bracket.position_quantity or 0) or 0), 0),
                    live_total_qty,
                )
                known_app_qty = await self._app_owned_qty_from_fills(
                    ticker,
                    total_position_qty=live_total_qty,
                    fallback_app_owned_qty=fallback_app_qty,
                    source="watchlist_entry_guard",
                )
                if count == 0 and known_app_qty > 0 and known_app_qty >= next_qty:
                    logger.info(
                        "phase.b.entry_blocked_existing_position",
                        ticker=ticker,
                        app_owned_qty=known_app_qty,
                        proposed_qty=next_qty,
                        max_allowed_qty=max_allowed_qty,
                        action="already_holding_app_owned_qty",
                    )
                    continue

                if not is_allowed:
                    logger.info(
                        "hedge.cap_blocked",
                        ticker=ticker,
                        series_ticker=bracket.series_ticker,
                        hedge_step=count,
                        hedge_factor=hedge_max,
                        initial_qty=self.config.initial_contract_count,
                        max_allowed_qty=max_allowed_qty,
                        action="entry_blocked_at_cap",
                    )
                    logger.info("phase.b.recovery_cap_reached",
                                series_ticker=bracket.series_ticker,
                                count=count,
                                max_doublings=hedge_max)
                    continue

                if known_app_qty + next_qty > max_allowed_qty:
                    logger.critical(
                        "hedge.cap_blocked",
                        ticker=ticker,
                        series_ticker=bracket.series_ticker,
                        hedge_step=count,
                        hedge_factor=hedge_max,
                        initial_qty=self.config.initial_contract_count,
                        existing_position_qty=known_app_qty,
                        proposed_qty=next_qty,
                        total_position_qty=known_app_qty + next_qty,
                        max_allowed_qty=max_allowed_qty,
                        action="recovery_position_cap_blocked",
                    )
                    continue

                bracket.crossed_buy = True
                spread_note = "crossed" if spread == 0 else "tight" if spread <= 3 else "normal"
                logger.info("phase.b.buying", ticker=ticker,
                            label=bracket.bracket_label, price=price, spread=spread,
                            spread_note=spread_note)

                # Duplicate-entry guard: at most one entry per (series, date, count) per cycle.
                parsed_key = parse_series_and_date(ticker)
                if parsed_key is not None:
                    step_key = (*parsed_key, count)
                    if step_key in self._entry_step_seen:
                        logger.warning(
                            "hedge.duplicate_blocked",
                            ticker=ticker,
                            series_ticker=bracket.series_ticker,
                            hedge_step=count,
                            hedge_factor=hedge_max,
                            initial_qty=self.config.initial_contract_count,
                            proposed_qty=next_qty,
                            max_allowed_qty=max_allowed_qty,
                            action="duplicate_entry_suppressed",
                        )
                        continue
                    self._entry_step_seen.add(step_key)

                if count > 0:
                    logger.info(
                        "hedge.step_advanced",
                        ticker=ticker,
                        series_ticker=bracket.series_ticker,
                        hedge_step=count,
                        hedge_factor=hedge_max,
                        initial_qty=self.config.initial_contract_count,
                        proposed_qty=next_qty,
                        max_allowed_qty=max_allowed_qty,
                        action="recovery_entry",
                    )
                    logger.info("phase.b.recovery_sized_entry",
                                series_ticker=bracket.series_ticker,
                                count=count,
                                multiplier=2 ** count,
                                quantity=next_qty)
                    await self._execute_entry(bracket, quantity=next_qty)
                else:
                    await self._execute_entry(bracket)
            else:
                logger.info("phase.b.spread_too_wide", ticker=ticker,
                            price=price, spread=spread)

    async def _execute_entry(self, bracket: MarketBracket, ob: Optional[OrderBook] = None, quantity: Optional[int] = None):
        """
        Execute the initial buy order.
        Buy INITIAL_CONTRACT_COUNT at the lowest ask (fetched live from Kalshi API).
        """
        # ── NWS gate enforcement — final order-submission boundary ──────────
        # This check runs unconditionally at the narrowest execution point so
        # that no upstream path (watchlist logic, caches, or future callers)
        # can bypass it.  Applies only to weather entry orders; exits, SL, and
        # panic-flatten paths never call _execute_entry.
        _gate_station = get_series_station_code(bracket.market_ticker)
        _ticker_upper = bracket.market_ticker.upper()
        _is_high = "KXHIGH" in _ticker_upper
        _is_low = "KXLOW" in _ticker_upper
        _use_nws_window_for_low_final = True
        if _is_low and self.config.entry_gate_mode == "SUNRISE":
            _sunrise_decision = self._sunrise_entry_gate.evaluate(
                ticker=bracket.market_ticker,
                now_utc=datetime.datetime.now(datetime.timezone.utc),
            )
            if not _sunrise_decision.allowed:
                bracket.phase = Phase.MONITORING
                bracket.crossed_buy = False
                return
            _use_nws_window_for_low_final = _sunrise_decision.use_nws_window_fallback
        _apply_nws_gate_final = (
            _gate_station is not None
            and (
                not _is_low
                or self.config.entry_gate_mode == "NWS_WINDOW"
                or _use_nws_window_for_low_final
            )
        )
        if _apply_nws_gate_final:
            _gate_now_utc = datetime.datetime.now(datetime.timezone.utc)
            _gate_market_date = None
            _gate_parsed = parse_series_and_date(bracket.market_ticker)
            if _gate_parsed is not None:
                _, _gate_date_prefix = _gate_parsed
                _gate_market_date = _parse_date_prefix(_gate_date_prefix)
            try:
                import nws.gate as _nws_gate_mod
                _gate_ticker_type = "HIGH" if _is_high else ("LOW" if _is_low else None)
                _gate_open = await asyncio.to_thread(
                    _nws_gate_mod.is_trading_gate_open,
                    _gate_station, _gate_now_utc, _gate_market_date,
                    _gate_ticker_type,
                )
                if _gate_open:
                    logger.info(
                        "entry.allowed_nws_gate_final",
                        ticker=bracket.market_ticker,
                        station_code=_gate_station,
                        is_high=_is_high,
                        is_low=_is_low,
                        now_utc=_gate_now_utc.isoformat(),
                        gate_open=True,
                        decision_reason="gate_open",
                        market_date=_gate_market_date.isoformat() if _gate_market_date else None,
                    )
                else:
                    logger.info(
                        "entry.blocked_nws_gate_final",
                        ticker=bracket.market_ticker,
                        station_code=_gate_station,
                        is_high=_is_high,
                        is_low=_is_low,
                        now_utc=_gate_now_utc.isoformat(),
                        gate_open=False,
                        decision_reason="gate_closed",
                        market_date=_gate_market_date.isoformat() if _gate_market_date else None,
                    )
                    bracket.phase = Phase.MONITORING
                    bracket.crossed_buy = False
                    return
            except Exception as _gate_exc:  # noqa: BLE001
                logger.warning(
                    "entry.blocked_nws_gate_final",
                    ticker=bracket.market_ticker,
                    station_code=_gate_station,
                    is_high=_is_high,
                    is_low=_is_low,
                    now_utc=_gate_now_utc.isoformat(),
                    gate_open=False,
                    decision_reason="gate_error",
                    error_class=type(_gate_exc).__name__,
                    error_message=str(_gate_exc),
                    market_date=_gate_market_date.isoformat() if _gate_market_date else None,
                )
                bracket.phase = Phase.MONITORING
                bracket.crossed_buy = False
                return
        # ────────────────────────────────────────────────────────────────────

        if ob is None:
            prices = await self._fetch_live_prices([bracket.market_ticker])
            ob = prices.get(bracket.market_ticker)
        price = get_buy_trigger_price(self.config, bracket.market_ticker)
        if price is None:
            bracket.phase = Phase.MONITORING
            bracket.crossed_buy = False
            logger.info("entry.blocked_unknown_family", ticker=bracket.market_ticker)
            return
        if ob and ob.yes_asks:
            price = ob.yes_asks[0].price

        proposed_qty = quantity or self.config.initial_contract_count

        # Hard pre-submit cap guard — last line of defence in case upstream logic
        # ever regresses.  Compute the absolute maximum permitted quantity and
        # refuse to place an order that would exceed it.
        hedge_max = int(self.config.hedge_max_factor)
        _, _, max_allowed_qty = hedge_policy(
            self.config.initial_contract_count, hedge_max, 0
        )
        if proposed_qty > max_allowed_qty:
            logger.critical(
                "hedge.cap_blocked",
                ticker=bracket.market_ticker,
                initial_contract_count=self.config.initial_contract_count,
                proposed_qty=proposed_qty,
                max_allowed_qty=max_allowed_qty,
                hedge_factor=hedge_max,
                action="hard_cap_guard_blocked_submission",
            )
            bracket.phase = Phase.MONITORING
            return

        existing_position_qty = max(int(bracket.position_quantity or 0), 0)
        try:
            positions = await self.executor.get_positions()
            existing_raw = (positions.get(bracket.market_ticker) or {}).get("count", 0)
            existing_position_qty = max(existing_position_qty, int(float(existing_raw or 0)))
        except Exception as e:
            logger.warning(
                "phase.b.entry_position_lookup_failed",
                ticker=bracket.market_ticker,
                error=str(e),
            )
        total_position_qty = existing_position_qty + proposed_qty
        if total_position_qty > max_allowed_qty:
            logger.critical(
                "hedge.cap_blocked",
                ticker=bracket.market_ticker,
                initial_contract_count=self.config.initial_contract_count,
                existing_position_qty=existing_position_qty,
                proposed_qty=proposed_qty,
                total_position_qty=total_position_qty,
                max_allowed_qty=max_allowed_qty,
                hedge_factor=hedge_max,
                action="hard_cap_guard_position_total_blocked_submission",
            )
            bracket.phase = Phase.MONITORING
            return

        import uuid
        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.BUY_YES,
            price=price,
            quantity=proposed_qty,
        )

        # Use spread_monitor_price as max price to ensure quick fill
        max_price = self.config.spread_monitor_price
        result = await self.executor.buy_yes(order, max_price=max_price)

        # Log to database
        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=bracket.market_ticker,
                action=TradeAction.BUY,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.commit()

        if result.success:
            reconciled_fill_price = result.fill_price
            if result.fill_quantity > 0 and result.fill_price <= 0:
                backfilled_cents, source = await self._resolve_entry_cost_basis(bracket.market_ticker)
                if backfilled_cents > 0:
                    reconciled_fill_price = backfilled_cents
                    logger.info("phase.b.entry_cost_reconciled",
                                ticker=bracket.market_ticker,
                                source=source,
                                cents=backfilled_cents)

            bracket.phase = Phase.HOLDING
            new_position_qty = max(existing_position_qty + max(int(result.fill_quantity or 0), 0), 0)
            try:
                positions = await self.executor.get_positions()
                existing_raw = (positions.get(bracket.market_ticker) or {}).get("count", 0)
                new_position_qty = max(new_position_qty, max(int(float(existing_raw or 0)), 0))
            except Exception:
                pass
            bracket.position_quantity = new_position_qty
            bracket.avg_entry = reconciled_fill_price
            prior_app_owned_qty = max(int(self._app_owned_qty.get(bracket.market_ticker, existing_position_qty) or 0), 0)
            fallback_app_owned_qty = min(
                new_position_qty,
                prior_app_owned_qty + max(int(result.fill_quantity or 0), 0),
            )
            app_owned_qty = await self._app_owned_qty_from_fills(
                bracket.market_ticker,
                total_position_qty=new_position_qty,
                fallback_app_owned_qty=fallback_app_owned_qty,
                source="entry_fill",
            )
            self._set_ownership(
                bracket.market_ticker,
                total_position_qty=bracket.position_quantity,
                app_owned_qty=app_owned_qty,
                source="entry_fill",
                action="position_updated",
            )
            self.active_positions[bracket.market_ticker] = bracket
            await self._register_stop_loss_watcher(bracket)
            if bracket.entry_time is None:
                bracket.entry_time = datetime.datetime.now(datetime.timezone.utc)
            logger.info("phase.b.entry_filled", ticker=bracket.market_ticker,
                        price=result.fill_price, qty=result.fill_quantity,
                        cost=result.total_cost_cents,
                        active_count=len(self.active_positions),
                        active_keys=list(self.active_positions.keys()))

            # Update positions table (upsert)
            async with await self.db.get_session() as session:
                existing = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = existing.scalar_one_or_none()
                if pos:
                    # Keep DB position aligned to reconciled cumulative quantity.
                    pos.quantity = bracket.position_quantity
                    pos.avg_entry_price = reconciled_fill_price
                    pos.last_price = reconciled_fill_price
                    if self.sl_backstop is not None:
                        pos.sl_backstop_order_id = self.sl_backstop.get_order_id(bracket.market_ticker)
                else:
                    # Insert new position
                    pos = PositionModel(
                        market_ticker=bracket.market_ticker,
                        event_ticker=bracket.event_ticker,
                        series_ticker=bracket.series_ticker,
                        side="yes",
                        quantity=bracket.position_quantity,
                        avg_entry_price=reconciled_fill_price,
                        last_price=reconciled_fill_price,
                        sl_backstop_order_id=(
                            self.sl_backstop.get_order_id(bracket.market_ticker)
                            if self.sl_backstop is not None else None
                        ),
                    )
                    session.add(pos)

                # Clear any stale STOP_LOSS OrderAction from a previous position
                # cycle on this ticker.  If a SUCCEEDED action is left in the DB
                # from an earlier SL and the bot re-enters the same market, the
                # idempotency guard in _execute_stop_loss would find it, treat the
                # new SL as "already done" (market_gone=True), and then decrement
                # the stop_loss_count — keeping the counter stuck at 0 and causing
                # endless base-size re-buys.  Deleting it here ensures each new
                # position starts with a fresh STOP_LOSS action record.
                sl_action_key = f"{bracket.market_ticker}:STOP_LOSS"
                await session.execute(
                    delete(OrderAction).where(OrderAction.action_key == sl_action_key)
                )

                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error("phase.b.entry_db_error", ticker=bracket.market_ticker, error=str(e))

            # ── Non-blocking TradeOutcome creation (best-effort) ─────────────
            asyncio.create_task(
                self._create_entry_outcome(
                    bracket=bracket,
                    fill_price=reconciled_fill_price,
                    fill_qty=result.fill_quantity,
                    ob=ob,
                )
            )
        else:
            bracket.phase = Phase.MONITORING
            logger.warning("phase.b.entry_failed", ticker=bracket.market_ticker,
                           notes=result.notes)

    async def _get_stop_loss_count_for_market(self, market_ticker: str) -> int:
        parsed = parse_series_and_date(market_ticker)
        if not parsed:
            return 0

        series_ticker, date_prefix = parsed
        async with await self.db.get_session() as session:
            result = await session.execute(
                select(StopLossLedger).where(
                    StopLossLedger.series_ticker == series_ticker,
                    StopLossLedger.date_prefix == date_prefix,
                )
            )
            row = result.scalar_one_or_none()
        return row.stop_loss_count if row else 0

    async def _create_entry_outcome(
        self,
        bracket,
        fill_price: int,
        fill_qty: int,
        ob: Optional[OrderBook],
    ) -> None:
        """Create an initial OPEN TradeOutcome row for a new entry.

        Best-effort: wrapped in a top-level try/except so any failure is logged
        and swallowed without affecting the position lifecycle.
        """
        from core.trade_outcome_utils import (
            entry_price_bucket as _bucket,
            parse_bracket_temp,
            detect_family,
        )
        from core.local_time_gate import SERIES_CITY, get_series_prefix
        try:
            parsed = parse_series_and_date(bracket.market_ticker)
            if parsed is None:
                return
            series_ticker, date_prefix = parsed
            prefix = get_series_prefix(bracket.market_ticker)
            city = SERIES_CITY.get(prefix)
            family = detect_family(bracket.market_ticker)
            bracket_temp_val = parse_bracket_temp(bracket.market_ticker)
            price_bucket = _bucket(fill_price) if fill_price else None

            # Spread at entry
            entry_spread: Optional[int] = None
            entry_ask: Optional[int] = fill_price
            if ob is not None and ob.yes_bids and ob.yes_asks:
                try:
                    entry_spread = ob.yes_asks[0].price - ob.yes_bids[0].price
                except Exception:
                    pass

            # StopLossLedger count at entry
            sl_count_at_entry: Optional[int] = None
            try:
                sl_count_at_entry = await self._get_stop_loss_count_for_market(bracket.market_ticker)
            except Exception:
                pass

            # NWS forecast context (best-effort)
            forecast_low_temp: Optional[float] = None
            minutes_to_forecast_low: Optional[int] = None
            forecast_vs_bracket_delta: Optional[float] = None
            try:
                station = get_series_station_code(bracket.market_ticker)
                if station is not None:
                    import nws.gate as _nws_gate
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    fcast = await asyncio.to_thread(
                        _nws_gate.get_station_forecast_context,
                        station,
                        now_utc,
                    )
                    if fcast is not None:
                        forecast_low_temp = fcast.get("forecast_low_temp")
                        minutes_to_forecast_low = fcast.get("minutes_to_forecast_low")
                        if forecast_low_temp is not None and bracket_temp_val is not None:
                            forecast_vs_bracket_delta = forecast_low_temp - bracket_temp_val
            except Exception as exc:
                logger.warning(
                    "entry.outcome_forecast_context_failed",
                    ticker=bracket.market_ticker,
                    error=str(exc),
                )

            async with await self.db.get_session() as session:
                # Upsert — if a row already exists (e.g. recovery re-entry) update it
                existing_q = await session.execute(
                    select(TradeOutcome).where(
                        TradeOutcome.market_ticker == bracket.market_ticker,
                        TradeOutcome.date_prefix == date_prefix,
                    )
                )
                existing = existing_q.scalar_one_or_none()
                if existing is None:
                    row = TradeOutcome(
                        series_ticker=series_ticker,
                        date_prefix=date_prefix,
                        market_ticker=bracket.market_ticker,
                        city=city,
                        family=family,
                        entry_price_avg=fill_price,
                        entry_qty=fill_qty,
                        outcome=TradeOutcomeStatus.OPEN,
                        stop_loss_count_at_entry=sl_count_at_entry,
                        entry_ask_cents=entry_ask,
                        entry_spread_cents=entry_spread,
                        entry_price_bucket=price_bucket,
                        bracket_temp=bracket_temp_val,
                        forecast_low_temp=forecast_low_temp,
                        minutes_to_forecast_low=minutes_to_forecast_low,
                        forecast_vs_bracket_delta=forecast_vs_bracket_delta,
                    )
                    session.add(row)
                else:
                    # Recovery buy: update entry averages
                    if existing.entry_qty and existing.entry_price_avg:
                        old_cost = existing.entry_price_avg * existing.entry_qty
                        new_cost = fill_price * fill_qty
                        new_qty = existing.entry_qty + fill_qty
                        existing.entry_price_avg = (old_cost + new_cost) // new_qty if new_qty else fill_price
                        existing.entry_qty = new_qty
                    existing.outcome = TradeOutcomeStatus.OPEN
                try:
                    await session.commit()
                    logger.info(
                        "entry.outcome_created",
                        ticker=bracket.market_ticker,
                        fill_price=fill_price,
                        sl_count_at_entry=sl_count_at_entry,
                        price_bucket=price_bucket,
                    )
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "entry.outcome_db_failed",
                        ticker=bracket.market_ticker,
                        error=str(exc),
                    )
        except Exception as exc:
            logger.warning(
                "entry.outcome_context_failed",
                ticker=bracket.market_ticker,
                error=str(exc),
            )

    async def _update_exit_outcome(
        self,
        market_ticker: str,
        exit_price: int,
        exit_qty: int,
        outcome: TradeOutcomeStatus,
    ) -> None:
        """Update an existing TradeOutcome row with exit data.

        Best-effort: never raises.  Called from a non-blocking asyncio.create_task.
        """
        try:
            parsed = parse_series_and_date(market_ticker)
            if parsed is None:
                return
            _, date_prefix = parsed

            async with await self.db.get_session() as session:
                q = await session.execute(
                    select(TradeOutcome).where(
                        TradeOutcome.market_ticker == market_ticker,
                        TradeOutcome.date_prefix == date_prefix,
                    )
                )
                row: Optional[TradeOutcome] = q.scalar_one_or_none()
                if row is None:
                    logger.warning(
                        "exit.outcome_row_not_found",
                        market_ticker=market_ticker,
                        date_prefix=date_prefix,
                    )
                    return

                # Compute realized P&L if we have entry data
                pnl: Optional[int] = None
                if row.entry_price_avg is not None and exit_price is not None and exit_qty:
                    entry_qty_used = min(row.entry_qty or exit_qty, exit_qty)
                    pnl = (exit_price - row.entry_price_avg) * entry_qty_used

                row.exit_price_avg = exit_price
                row.exit_qty = exit_qty
                row.outcome = outcome
                row.realized_pnl_cents = pnl

                try:
                    await session.commit()
                    logger.info(
                        "exit.outcome_updated",
                        market_ticker=market_ticker,
                        outcome=str(outcome),
                        realized_pnl_cents=pnl,
                    )
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "exit.outcome_update_db_failed",
                        market_ticker=market_ticker,
                        error=str(exc),
                    )
        except Exception as exc:
            logger.warning(
                "exit.outcome_update_failed",
                market_ticker=market_ticker,
                error=str(exc),
            )

    async def _increment_stop_loss_count_for_market(self, market_ticker: str) -> None:
        parsed = parse_series_and_date(market_ticker)
        if not parsed:
            return

        series_ticker, date_prefix = parsed
        # Clamp the stored count to hedge_max_factor so a corrupt/stale ledger
        # row can never produce an oversized order on the next recovery entry.
        max_count = max(int(self.config.hedge_max_factor), 1)
        async with await self.db.get_session() as session:
            result = await session.execute(
                select(StopLossLedger).where(
                    StopLossLedger.series_ticker == series_ticker,
                    StopLossLedger.date_prefix == date_prefix,
                )
            )
            row = result.scalar_one_or_none()
            if row is None:
                row = StopLossLedger(
                    series_ticker=series_ticker,
                    date_prefix=date_prefix,
                    stop_loss_count=min(1, max_count),
                    updated_at=datetime.datetime.utcnow(),
                )
                session.add(row)
            else:
                new_count = row.stop_loss_count + 1
                if new_count > max_count:
                    logger.warning(
                        "hedge.stop_loss_count_clamped",
                        series_ticker=series_ticker,
                        date_prefix=date_prefix,
                        raw_count=new_count,
                        clamped_to=max_count,
                        hedge_max_factor=max_count,
                    )
                    new_count = max_count
                row.stop_loss_count = new_count
                row.updated_at = datetime.datetime.utcnow()
            await session.commit()

    async def _decrement_stop_loss_count_for_market(self, market_ticker: str) -> None:
        parsed = parse_series_and_date(market_ticker)
        if not parsed:
            return

        series_ticker, date_prefix = parsed
        async with await self.db.get_session() as session:
            result = await session.execute(
                select(StopLossLedger).where(
                    StopLossLedger.series_ticker == series_ticker,
                    StopLossLedger.date_prefix == date_prefix,
                )
            )
            row = result.scalar_one_or_none()
            if row is not None and row.stop_loss_count > 0:
                row.stop_loss_count -= 1
                row.updated_at = datetime.datetime.utcnow()
                await session.commit()

    async def _evaluate_held_positions(self):
        """Phase C: manage held positions with live sellable-price stop-losses.

        The REST get_positions() call is throttled to at most once per
        held_position_price_refresh_seconds (default 10 s) so that the fast
        _held_positions_loop (~250 ms cadence) does not flood the Kalshi API.
        Cached position data is used between refreshes; in-memory SL price
        checks run every cycle regardless.
        """
        now_fetch = asyncio.get_event_loop().time()
        refresh_interval = max(int(self.config.held_position_price_refresh_seconds or 1), 1)
        if now_fetch - self._positions_api_last_fetch >= refresh_interval:
            try:
                self._positions_api_cache = await self.executor.get_positions()
                self._positions_api_last_fetch = now_fetch
            except Exception as e:
                logger.error("phase.c.get_positions_failed", error=str(e))
        api_positions = self._positions_api_cache
        await self._adopt_untracked_exchange_fills(api_positions)
        if not self.active_positions:
            return

        active_tickers = list(self.active_positions.keys())
        absent_tickers = [
            ticker for ticker in active_tickers
            if ticker not in api_positions or api_positions.get(ticker) is None
        ]
        total_active = len(active_tickers)
        mass_absence = (
            total_active >= 2
            and len(absent_tickers) / total_active > 0.5
        )
        if mass_absence:
            now_mass = asyncio.get_event_loop().time()
            last_mass_log = getattr(self, "_last_mass_absence_log", 0)
            if now_mass - last_mass_log >= 60:
                self._last_mass_absence_log = now_mass
                logger.warning(
                    "phase.c.positions_api_mass_absence",
                    count_absent=len(absent_tickers),
                    count_total=total_active,
                )

        for ticker, bracket in list(self.active_positions.items()):
            pos_data = api_positions.get(ticker)
            position_absent = ticker not in api_positions or pos_data is None
            now_ts = asyncio.get_event_loop().time()
            last_seen = getattr(bracket, "_last_seen_in_api", 0)
            if position_absent:
                if last_seen == 0:
                    bracket._last_seen_in_api = now_ts
                    last_seen = now_ts
                seconds_absent = now_ts - last_seen
                grace = 30
                if seconds_absent < grace:
                    logger.debug("phase.c.position_missing_within_grace", ticker=ticker,
                                 seconds_absent=int(seconds_absent))
                elif not mass_absence:
                    last_absent_log = getattr(bracket, "_last_position_absent_log", 0)
                    if now_ts - last_absent_log >= 60:
                        bracket._last_position_absent_log = now_ts
                        logger.warning(
                            "phase.c.position_not_in_api_after_grace",
                            ticker=ticker,
                            qty=bracket.position_quantity,
                            phase=bracket.phase.name,
                            action="retained_pending_settlement_confirmation",
                        )
            else:
                bracket._last_seen_in_api = now_ts
                bracket._last_position_absent_log = 0

                api_count = pos_data.get("count", 1)
                if api_count == 0:
                    logger.info("phase.c.position_settled", ticker=ticker,
                                qty=bracket.position_quantity, api_count=api_count)
                    await self._remove_active_position(ticker, bracket)
                    continue
                try:
                    bracket.position_quantity = int(float(api_count))
                except (TypeError, ValueError):
                    bracket.position_quantity = max(int(bracket.position_quantity or 0), 0)
                app_known = self._app_owned_qty.get(ticker, bracket.position_quantity)
                app_known = await self._app_owned_qty_from_fills(
                    ticker,
                    total_position_qty=bracket.position_quantity,
                    fallback_app_owned_qty=app_known,
                    source="periodic_reconciliation",
                )
                self._set_ownership(
                    ticker,
                    total_position_qty=bracket.position_quantity,
                    app_owned_qty=app_known,
                    source="periodic_reconciliation",
                    action="position_reconciled",
                )

            if not bracket.avg_entry or bracket.avg_entry <= 0:
                heal_source: Optional[str] = None
                avg_cents = int((pos_data or {}).get("average_fill_cost_cents") or 0)
                if avg_cents > 0:
                    bracket.avg_entry = avg_cents
                    heal_source = "positions_inline"
                else:
                    now_heal = asyncio.get_event_loop().time()
                    last_heal = getattr(bracket, '_last_entry_heal_attempt', 0)
                    if now_heal - last_heal >= 60:
                        bracket._last_entry_heal_attempt = now_heal
                        backfilled_cents, src = await self._resolve_entry_cost_basis(ticker)
                        if backfilled_cents > 0:
                            bracket.avg_entry = backfilled_cents
                            heal_source = src

                if heal_source is not None and bracket.avg_entry > 0:
                    logger.info("phase.c.entry_self_healed",
                                ticker=ticker,
                                source=heal_source,
                                cents=bracket.avg_entry)
                    try:
                        async with await self.db.get_session() as session:
                            existing_pos = await session.execute(
                                select(PositionModel).where(
                                    PositionModel.market_ticker == ticker
                                )
                            )
                            pos_row = existing_pos.scalar_one_or_none()
                            if pos_row:
                                pos_row.avg_entry_price = bracket.avg_entry
                                await session.commit()
                    except Exception:
                        pass

            current_price = None
            yes_bid = None
            yes_ask = None
            rest_data = None
            zero_bid_collapse = False
            blind_below_stop = False
            # Track where the price ultimately came from so the watcher guard
            # can decide whether to defer to the WebSocket-driven StopLossWatcher
            # or to act directly (when the watcher has no live tick).
            price_source = "none"
            last_known_price = self.cache.get_last_price(ticker)
            if last_known_price is None:
                last_known_price = bracket.last_price
            quote = self.cache.get_quote(ticker)
            quote_ts = self.cache.get_quote_ts(ticker)
            quote_is_fresh = False
            if quote is not None and quote_ts is not None:
                quote_is_fresh = (
                    time.time() - quote_ts
                    < max(int(self.config.held_position_price_refresh_seconds or 0), 1)
                )
            if quote is not None and quote_is_fresh:
                yes_bid, yes_ask = quote
                if yes_bid > 0:
                    current_price = yes_bid
                    price_source = "websocket"
                elif yes_bid == 0:
                    zero_bid_collapse = True
                    price_source = "websocket"

            if current_price is None:
                now_fetch = asyncio.get_event_loop().time()
                last_fetch = getattr(bracket, "_last_rest_price_fetch", 0)
                if now_fetch - last_fetch >= self.config.held_position_price_refresh_seconds:
                    bracket._last_rest_price_fetch = now_fetch
                    rest_data = await self._fetch_market_data_via_rest(ticker)
                    if rest_data:
                        yes_ask = rest_data.get("yes_ask")
                        rest_yes_bid = rest_data.get("yes_bid")
                        if rest_yes_bid is not None and rest_yes_bid > 0:
                            current_price = rest_yes_bid
                            zero_bid_collapse = False
                            price_source = "fallback_quote"
                        elif rest_yes_bid == 0:
                            zero_bid_collapse = True
                            price_source = "fallback_quote"
                        elif rest_data.get("price") is not None:
                            current_price = rest_data["price"]
                            price_source = "fallback_quote"

            if position_absent and not mass_absence and self._market_is_settled(rest_data):
                logger.info(
                    "phase.c.position_settled",
                    ticker=ticker,
                    qty=bracket.position_quantity,
                    source="market_status",
                    market_status=rest_data.get("status") if rest_data else None,
                )
                await self._remove_active_position(ticker, bracket)
                continue

            if current_price is None:
                last_price = self.cache.get_last_price(ticker)
                if (
                    last_price is not None
                    and yes_ask is not None
                    and last_price <= yes_ask
                ):
                    current_price = last_price
                    price_source = "last_price"

            if current_price is None and not zero_bid_collapse:
                bracket._consecutive_no_price_cycles = getattr(
                    bracket, "_consecutive_no_price_cycles", 0
                ) + 1
                if not getattr(bracket, "_no_price_since", 0):
                    bracket._no_price_since = asyncio.get_event_loop().time()
                now_warn = asyncio.get_event_loop().time()
                last_warn = getattr(bracket, "_last_no_price_log", 0)
                if now_warn - last_warn >= 60:
                    bracket._last_no_price_log = now_warn
                    logger.warning(
                        "phase.c.no_live_price",
                        ticker=ticker,
                        price_source="none",
                        reason="no_websocket_or_rest_price",
                    )
                if (
                    bracket.position_quantity > 0
                    and bracket._consecutive_no_price_cycles > self.config.max_no_price_cycles
                ):
                    last_alert = getattr(bracket, "_last_unprotected_log", 0)
                    if now_warn - last_alert >= 60:
                        bracket._last_unprotected_log = now_warn
                        logger.warning(
                            "phase.c.held_position_unprotected",
                            ticker=ticker,
                            qty=bracket.position_quantity,
                            blind_cycles=bracket._consecutive_no_price_cycles,
                            seconds_blind=int(now_warn - bracket._no_price_since),
                            last_known_price=last_known_price,
                        )
                    # When we've gone blind for too long and the last known price
                    # was already below the stop threshold, the safe action is to
                    # attempt protection. If the last known price was still healthy,
                    # alert loudly but do not force an exit on data loss alone.
                    if (
                        last_known_price is not None
                        and last_known_price < self.config.stop_loss_price
                    ):
                        current_price = last_known_price
                        blind_below_stop = True
                        price_source = "blind_last_known"

                    # ── Escalation path for persistently-blind positions ──────
                    # When blind cycles exceed the configurable threshold, emit a
                    # CRITICAL alert and (if configured) attempt a protective
                    # panic-flatten of the app-owned quantity.
                    unprotected_max = max(
                        int(getattr(self.config, "sl_unprotected_max_blind_cycles", 30) or 30), 1
                    )
                    if bracket._consecutive_no_price_cycles >= unprotected_max:
                        last_escalation = getattr(bracket, "_last_unprotected_escalation", 0)
                        if not getattr(bracket, "_unprotected_escalated", False) or now_warn - last_escalation >= 300:
                            bracket._last_unprotected_escalation = now_warn
                            bracket._unprotected_escalated = True
                            managed_qty_esc, app_owned_qty_esc, external_qty_esc = self._managed_exit_quantity(
                                ticker,
                                bracket.position_quantity,
                            )
                            logger.critical(
                                "phase.c.unprotected_escalation",
                                ticker=ticker,
                                qty=bracket.position_quantity,
                                app_owned_qty=app_owned_qty_esc,
                                external_qty=external_qty_esc,
                                blind_cycles=bracket._consecutive_no_price_cycles,
                                seconds_blind=int(now_warn - getattr(bracket, "_no_price_since", now_warn)),
                                last_known_price=last_known_price,
                                threshold=unprotected_max,
                                flatten_enabled=bool(getattr(self.config, "sl_flatten_unprotected_on_blind", False)),
                                message=(
                                    f"Position {ticker} has been blind for "
                                    f"{bracket._consecutive_no_price_cycles} cycles "
                                    f"(>= threshold {unprotected_max}): stop-loss CANNOT FIRE. "
                                    "Manual intervention may be required."
                                ),
                            )
                            flatten_on_blind = bool(getattr(self.config, "sl_flatten_unprotected_on_blind", False))
                            if flatten_on_blind and current_price is None:
                                if app_owned_qty_esc <= 0:
                                    logger.critical(
                                        "phase.c.unprotected_flatten_skipped_no_app_qty",
                                        ticker=ticker,
                                        app_owned_qty=app_owned_qty_esc,
                                        external_qty=external_qty_esc,
                                        message="SL_FLATTEN_UNPROTECTED_ON_BLIND=true but app_owned_qty=0; no sell attempted",
                                    )
                                else:
                                    logger.critical(
                                        "phase.c.unprotected_panic_flatten_attempt",
                                        ticker=ticker,
                                        app_owned_qty=app_owned_qty_esc,
                                        message="Attempting protective panic-flatten of app-owned qty due to persistent price blindness",
                                    )
                                    # Use last_known_price as override; _execute_stop_loss
                                    # will use PANIC_FLATTEN logic regardless.
                                    override_px = last_known_price if last_known_price and last_known_price > 0 else self.config.stop_loss_price
                                    await self._execute_stop_loss(
                                        bracket,
                                        override_price=override_px,
                                        bypass_cooldown=True,
                                    )

                if current_price is None:
                    continue

            bracket._consecutive_no_price_cycles = 0
            bracket._no_price_since = 0

            bracket.last_price = current_price

            # Determine stop-loss exit mode for this evaluation cycle.
            sl_exit_mode_upper = (self.config.sl_exit_mode or "PANIC_FLATTEN").upper()
            is_panic_flatten = sl_exit_mode_upper == "PANIC_FLATTEN"

            # --- Apply zero_bid_collapse side-effects (both modes) ----------
            if zero_bid_collapse:
                if bracket.last_price != 0:
                    bracket.last_price = 0
                current_price = 0
                # Discard ask=0 as invalid; keep a valid positive ask for PANIC check
                yes_ask = yes_ask if yes_ask and yes_ask > 0 else None

            stop_loss_ask = int(self.config.stop_loss_price)
            ask_trigger = yes_ask is not None and yes_ask <= stop_loss_ask

            # --- Compute stop_loss_reason (mode-specific) -------------------
            stop_loss_reason = None
            if is_panic_flatten:
                if ask_trigger:
                    stop_loss_reason = "ask_at_or_below_stop"
            else:
                if zero_bid_collapse:
                    stop_loss_reason = "zero_bid_collapse"
                elif blind_below_stop:
                    stop_loss_reason = "blind_last_known_below_stop"
                elif ask_trigger:
                    stop_loss_reason = "ask_at_or_below_stop"
                elif current_price < stop_loss_ask:
                    stop_loss_reason = "price_below_stop"

            logger.debug(
                "phase.c.price_check",
                ticker=ticker,
                side="yes",
                stop_loss_ask=stop_loss_ask,
                price_source=price_source,
                price=current_price,
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                sl_exit_mode=sl_exit_mode_upper,
                trigger_met=stop_loss_reason is not None,
            )

            if stop_loss_reason is not None:
                # Defer to the WebSocket-driven StopLossWatcher only when the
                # price came directly from the live WS quote cache — the watcher
                # holds the same tick and will fire immediately.
                # For zero_bid_collapse the ask is usually above stop_loss so
                # the watcher will NOT fire; Phase C must act.
                # For REST-fallback, last_price, or blind prices the watcher has
                # no live tick and cannot act; Phase C is the only safety net.
                # For PANIC_FLATTEN with websocket source: always defer to watcher
                # because the watcher uses ask/bid stop-loss thresholds directly.
                if self.stop_loss_watcher is not None and price_source == "websocket" and not zero_bid_collapse:
                    continue
                if bracket.position_quantity <= 0:
                    bracket.phase = Phase.CLOSED
                    self.active_positions.pop(ticker, None)
                    self.brackets.pop(ticker, None)
                    logger.info("phase.c.stop_loss_zero_qty", ticker=ticker)
                    continue
                managed_qty, app_owned_qty, external_qty = self._managed_exit_quantity(
                    ticker,
                    bracket.position_quantity,
                )
                if managed_qty <= 0:
                    logger.info(
                        "exit.skipped_no_app_qty",
                        ticker=ticker,
                        total_position_qty=bracket.position_quantity,
                        app_owned_qty=app_owned_qty,
                        external_qty=external_qty,
                        action="stop_loss_not_executed",
                    )
                    continue
                bracket._sl_held_for_spread = False
                bracket._last_sl_held_log = 0
                bracket._sl_held_for_spread_since = 0
                # For PANIC_FLATTEN, log with ask-centric details for auditability.
                if is_panic_flatten:
                    logger.warning(
                        "sl.panic_trigger_evaluated",
                        ticker=ticker,
                        side="yes",
                        best_bid_yes=yes_bid,
                        best_ask_yes=yes_ask,
                        stop_loss_price_ask=stop_loss_ask,
                        units="cents",
                        source=price_source,
                        trigger_met=True,
                    )
                logger.warning("phase.c.stop_loss_triggered", ticker=ticker,
                               last_price=current_price,
                               stop_loss_ask=stop_loss_ask,
                               reason=stop_loss_reason)
                if not getattr(bracket, "_stop_loss_counted", False):
                    await self._increment_stop_loss_count_for_market(bracket.market_ticker)
                    bracket._stop_loss_counted = True
                if self.config.enable_fast_sl_exit:
                    if is_panic_flatten and ask_trigger and yes_ask is not None:
                        trigger_px = yes_ask
                    else:
                        trigger_px = current_price
                    await self._dispatch_stop_loss_exit(
                        bracket,
                        trigger_price=trigger_px if trigger_px is not None else current_price,
                        trigger_source="phase_c",
                    )
                else:
                    market_gone = await self._execute_stop_loss(bracket)
                    if market_gone:
                        # The sell returned market_not_found: the market settled.
                        # Undo the ledger increment since no real stop-loss occurred.
                        await self._decrement_stop_loss_count_for_market(bracket.market_ticker)
                        bracket._stop_loss_counted = False
            else:
                # Price has recovered above the stop threshold — clear the spread
                # guard so a future re-trigger logs fresh.
                if getattr(bracket, "_sl_held_for_spread", False):
                    bracket._sl_held_for_spread = False
                    bracket._last_sl_held_log = 0
                    bracket._sl_held_for_spread_since = 0
                if getattr(bracket, "_stop_loss_counted", False):
                    task = self._sl_exit_tasks.get(ticker)
                    in_flight = task is not None and not task.done()
                    if not in_flight:
                        action_key = f"{ticker}:STOP_LOSS"
                        async with await self.db.get_session() as session:
                            action = await session.execute(
                                select(OrderAction).where(OrderAction.action_key == action_key)
                            )
                            action_row = action.scalar_one_or_none()
                        if action_row is None or action_row.status != OrderActionStatus.SUCCEEDED:
                            await self._rollback_stop_loss_count_if_counted(bracket)

    async def _adopt_untracked_exchange_fills(self, api_positions: dict[str, dict]) -> None:
        for ticker, bracket in list(self.brackets.items()):
            if bracket.phase == Phase.HOLDING:
                continue
            pos_data = api_positions.get(ticker) or {}
            qty = int(float(pos_data.get("count", 0) or 0))
            if qty <= 0:
                continue
            adopted_app_qty = qty
            if not self.config.manage_external_positions:
                adopted_app_qty = await self._app_owned_qty_from_fills(
                    ticker,
                    total_position_qty=qty,
                    fallback_app_owned_qty=0,
                    source="untracked_exchange_position",
                )
                self._set_ownership(
                    ticker,
                    total_position_qty=qty,
                    app_owned_qty=adopted_app_qty,
                    source="untracked_exchange_position",
                    action="ignored_external_manual" if adopted_app_qty <= 0 else "app_fill_reclassified",
                )
                if adopted_app_qty <= 0:
                    continue

            prior_phase = bracket.phase.name
            entry = int(pos_data.get("average_fill_cost_cents", 0) or 0)
            if entry <= 0:
                backfilled_cents, _source = await self._resolve_entry_cost_basis(ticker)
                if backfilled_cents > 0:
                    entry = backfilled_cents

            bracket.phase = Phase.HOLDING
            bracket.crossed_buy = True
            bracket.position_quantity = qty
            self._set_ownership(
                ticker,
                total_position_qty=qty,
                app_owned_qty=adopted_app_qty,
                source="untracked_exchange_position",
                action="adopted_legacy_mode" if self.config.manage_external_positions else "adopted_app_fill_position",
            )
            if entry > 0:
                bracket.avg_entry = entry
                bracket.last_price = entry
            self.active_positions[ticker] = bracket
            await self._register_stop_loss_watcher(bracket)

            async with await self.db.get_session() as session:
                existing = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == ticker)
                )
                pos = existing.scalar_one_or_none()
                if pos:
                    pos.quantity = qty
                    if entry > 0:
                        pos.avg_entry_price = entry
                        pos.last_price = entry
                    if self.sl_backstop is not None:
                        pos.sl_backstop_order_id = self.sl_backstop.get_order_id(ticker)
                else:
                    pos = PositionModel(
                        market_ticker=ticker,
                        event_ticker=bracket.event_ticker,
                        series_ticker=bracket.series_ticker,
                        side="yes",
                        quantity=qty,
                        avg_entry_price=entry,
                        last_price=entry,
                        sl_backstop_order_id=(
                            self.sl_backstop.get_order_id(ticker)
                            if self.sl_backstop is not None else None
                        ),
                    )
                    session.add(pos)
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error("phase.b.untracked_fill_db_error", ticker=ticker, error=str(e))

            now_ts = asyncio.get_event_loop().time()
            last_log = getattr(bracket, "_last_untracked_fill_adopted_log", 0)
            if now_ts - last_log >= 60:
                bracket._last_untracked_fill_adopted_log = now_ts
                logger.warning(
                    "phase.b.untracked_fill_adopted",
                    ticker=ticker,
                    qty=qty,
                    cost_basis_cents=entry,
                    prior_phase=prior_phase,
                )

    async def _execute_stop_loss(
        self,
        bracket: MarketBracket,
        *,
        override_price: Optional[int] = None,
        bypass_cooldown: bool = False,
        trigger_ts_ms: Optional[int] = None,
        attempt: int = 1,
    ):
        """Execute a stop-loss: sell position at market (1¢) to guarantee fill.

        Checks a persistent idempotency record (``OrderAction``) before placing
        the order so that retries and reconnect bursts cannot submit duplicate
        stop-loss sells for the same position.

        Returns True if the sell was rejected with market_not_found (the market
        has settled and is gone — position is cleaned up quietly as confirmed
        settlement).  Returns False in all other cases.
        """
        now = asyncio.get_event_loop().time()
        last_attempt = getattr(bracket, '_last_stop_loss_attempt', 0)
        if not bypass_cooldown and now - last_attempt < self.config.sl_execute_cooldown_seconds:
            return False
        bracket._last_stop_loss_attempt = now

        action_key = f"{bracket.market_ticker}:STOP_LOSS"

        # --- Idempotency check -------------------------------------------------
        # If a SUCCEEDED record already exists for this action_key the position
        # was already exited (possibly in a previous process lifetime).  Clean
        # up in-memory state and return True (treated as market_gone / settled).
        async with await self.db.get_session() as session:
            existing = await session.execute(
                select(OrderAction).where(OrderAction.action_key == action_key)
            )
            action_row = existing.scalar_one_or_none()

        if action_row is not None and action_row.status == OrderActionStatus.SUCCEEDED:
            logger.info(
                "phase.c.stop_loss_duplicate_suppressed",
                ticker=bracket.market_ticker,
                action_key=action_key,
            )
            async with await self.db.get_session() as session:
                await session.execute(
                    delete(PositionModel).where(
                        PositionModel.market_ticker == bracket.market_ticker
                    )
                )
                await session.commit()
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            self._app_owned_qty.pop(bracket.market_ticker, None)
            await self._unregister_stop_loss_watcher(bracket.market_ticker)
            return True

        # Create or transition the idempotency record directly to SUBMITTED.
        # Inserting as SUBMITTED immediately (rather than PENDING then updating)
        # saves one DB round-trip on the common new-record path.
        # An existing FAILED record is retried; an existing SUBMITTED record
        # means another attempt is in-flight — skip this cycle.
        if action_row is None:
            async with await self.db.get_session() as session:
                action_row = OrderAction(
                    action_key=action_key,
                    action_type="STOP_LOSS",
                    market_ticker=bracket.market_ticker,
                    status=OrderActionStatus.SUBMITTED,
                )
                session.add(action_row)
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    # A concurrent process beat us to it — re-query and check.
                    async with await self.db.get_session() as session2:
                        existing2 = await session2.execute(
                            select(OrderAction).where(
                                OrderAction.action_key == action_key
                            )
                        )
                        action_row = existing2.scalar_one_or_none()
                    if action_row is not None and action_row.status in (
                        OrderActionStatus.SUBMITTED, OrderActionStatus.SUCCEEDED
                    ):
                        logger.info(
                            "phase.c.stop_loss_concurrent_skip",
                            ticker=bracket.market_ticker,
                            action_key=action_key,
                            status=action_row.status,
                        )
                        return False
        elif action_row.status == OrderActionStatus.SUBMITTED:
            logger.info(
                "phase.c.stop_loss_in_flight_skip",
                ticker=bracket.market_ticker,
                action_key=action_key,
            )
            logger.info(
                "sl.trigger_suppressed_in_flight",
                ticker=bracket.market_ticker,
                action_key=action_key,
                state="SUBMITTING",
            )
            return False
        else:
            # FAILED or PENDING — transition to SUBMITTED before calling the API.
            async with await self.db.get_session() as session:
                await session.execute(
                    update(OrderAction)
                    .where(OrderAction.action_key == action_key)
                    .values(status=OrderActionStatus.SUBMITTED)
                )
                await session.commit()
        # action_row is now SUBMITTED — proceed to API call.

        price = override_price if override_price is not None else 1
        managed_qty, app_owned_qty, external_qty = self._managed_exit_quantity(
            bracket.market_ticker,
            bracket.position_quantity,
        )
        if managed_qty <= 0:
            logger.info(
                "exit.skipped_no_app_qty",
                ticker=bracket.market_ticker,
                total_position_qty=bracket.position_quantity,
                app_owned_qty=app_owned_qty,
                external_qty=external_qty,
                action="stop_loss_not_submitted",
            )
            async with await self.db.get_session() as session:
                await session.execute(
                    update(OrderAction)
                    .where(OrderAction.action_key == action_key)
                    .values(status=OrderActionStatus.PENDING, notes="skipped_no_app_qty")
                )
                await session.commit()
            return False
        if managed_qty < bracket.position_quantity and not self.config.manage_external_positions:
            logger.info(
                "exit.capped_to_app_owned",
                ticker=bracket.market_ticker,
                total_position_qty=bracket.position_quantity,
                app_owned_qty=app_owned_qty,
                external_qty=external_qty,
                requested_qty=bracket.position_quantity,
                capped_qty=managed_qty,
                action="stop_loss_submit",
            )

        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.SELL_YES,
            price=price,
            quantity=managed_qty,
        )

        # Cancel the resting backstop BEFORE submitting the reactive sell to
        # prevent overselling.  This must be awaited and confirmed before the
        # sell order is placed.
        await self._cancel_sl_backstop(bracket.market_ticker, reason="sl_exit")

        submit_start_ms = self._now_ms()
        if trigger_ts_ms is not None:
            logger.info(
                "sl.exit_submit_start",
                ticker=bracket.market_ticker,
                action_key=action_key,
                attempt=attempt,
                price=price,
                elapsed_ms=submit_start_ms - trigger_ts_ms,
            )
        try:
            result = await self.executor.sell_yes(order)
        except Exception:
            # Reset to FAILED so the next retry is not blocked by the SUBMITTED
            # in-flight guard.  Best-effort: a DB error here is non-fatal.
            try:
                async with await self.db.get_session() as session:
                    await session.execute(
                        update(OrderAction)
                        .where(OrderAction.action_key == action_key)
                        .values(status=OrderActionStatus.FAILED)
                    )
                    await session.commit()
            except Exception:
                pass
            raise
        if trigger_ts_ms is not None:
            logger.info(
                "sl.exit_submitted",
                ticker=bracket.market_ticker,
                action_key=action_key,
                attempt=attempt,
                order_id=result.order_id or None,
                client_order_id=order.client_order_id,
                elapsed_ms=submit_start_ms - trigger_ts_ms,
            )

        # Detect market_not_found (HTTP 404): the market has already settled and
        # no longer exists on the exchange.  Treat this as confirmed settlement —
        # clean up the position quietly without logging a false stop-loss.
        if not result.success and "market_not_found" in (result.notes or ""):
            logger.info(
                "phase.c.position_settled_market_gone",
                ticker=bracket.market_ticker,
                qty=bracket.position_quantity,
            )
            async with await self.db.get_session() as session:
                await session.execute(
                    delete(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                # Also mark action as SUCCEEDED so it is not retried.
                await session.execute(
                    update(OrderAction)
                    .where(OrderAction.action_key == action_key)
                    .values(status=OrderActionStatus.SUCCEEDED,
                            notes="market_not_found: settled")
                )
                await session.commit()
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            self._app_owned_qty.pop(bracket.market_ticker, None)
            await self._unregister_stop_loss_watcher(bracket.market_ticker)
            if trigger_ts_ms is not None:
                logger.info(
                    "sl.position_gone",
                    ticker=bracket.market_ticker,
                    action_key=action_key,
                    attempt=attempt,
                    fill_qty=0,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                    reason="market_not_found",
                )
            return True

        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=bracket.market_ticker,
                action=TradeAction.STOP_LOSS,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.commit()

        remaining = await self._confirmed_remaining_stop_loss_qty(
            bracket,
            filled_qty=result.fill_quantity,
            action_key=action_key,
        )
        live_count = int(remaining["total_qty"])
        remaining_managed_qty = int(remaining["managed_qty"])

        if bool(remaining["confirmed"]) and remaining_managed_qty == 0:
            # Remove from positions table and mark action SUCCEEDED.
            async with await self.db.get_session() as session:
                await session.execute(
                    delete(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                await session.execute(
                    update(OrderAction)
                    .where(OrderAction.action_key == action_key)
                    .values(status=OrderActionStatus.SUCCEEDED)
                )
                await session.commit()
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            self._app_owned_qty.pop(bracket.market_ticker, None)
            await self._unregister_stop_loss_watcher(bracket.market_ticker)
            logger.info("phase.c.stop_loss_executed", ticker=bracket.market_ticker,
                        action_key=action_key,
                        price=result.fill_price, proceeds=-result.total_cost_cents,
                        remaining_total_qty=live_count,
                        remaining_app_owned_qty=remaining_managed_qty)
            # Non-blocking TradeOutcome update (best-effort)
            asyncio.create_task(
                self._update_exit_outcome(
                    market_ticker=bracket.market_ticker,
                    exit_price=result.fill_price,
                    exit_qty=result.fill_quantity,
                    outcome=TradeOutcomeStatus.STOPPED,
                )
            )
            if trigger_ts_ms is not None:
                logger.info(
                    "sl.exit_fill_observed",
                    ticker=bracket.market_ticker,
                    action_key=action_key,
                    attempt=attempt,
                    fill_qty=result.fill_quantity,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                )
        else:
            # Partial or unfilled — reset action to PENDING so the next cycle retries.
            async with await self.db.get_session() as session:
                await session.execute(
                    update(OrderAction)
                    .where(OrderAction.action_key == action_key)
                    .values(status=OrderActionStatus.PENDING)
                )
                await session.execute(
                    update(PositionModel)
                    .where(PositionModel.market_ticker == bracket.market_ticker)
                    .values(quantity=live_count)
                )
                await session.commit()
            prev_qty = bracket.position_quantity
            bracket.position_quantity = live_count
            self._set_ownership(
                bracket.market_ticker,
                total_position_qty=live_count,
                app_owned_qty=int(remaining["app_owned_qty"]),
                source="stop_loss_result",
                action="position_reconciled",
            )
            if result.fill_quantity > 0 or live_count < prev_qty:
                bracket._consecutive_unfilled_sl = 0
            else:
                bracket._consecutive_unfilled_sl = getattr(bracket, "_consecutive_unfilled_sl", 0) + 1
            logger.warning(
                "phase.c.stop_loss_partial_or_unfilled",
                ticker=bracket.market_ticker,
                action_key=action_key,
                attempted_qty=order.quantity,
                filled_qty=result.fill_quantity,
                remaining_count=live_count,
                remaining_app_owned_qty=remaining_managed_qty,
                notes=result.notes,
                last_price=bracket.last_price,
            )
            if bracket._consecutive_unfilled_sl >= self.config.stop_loss_max_unfilled_attempts:
                logger.critical(
                    "sl.exit_exhausted_unprotected",
                    ticker=bracket.market_ticker,
                    action_key=action_key,
                    qty=remaining_managed_qty,
                    last_price=bracket.last_price,
                    stop_loss_price=self.config.stop_loss_price,
                    attempts=bracket._consecutive_unfilled_sl,
                    elapsed_ms=self._now_ms() - trigger_ts_ms if trigger_ts_ms is not None else None,
                    reason="unfilled_or_partial",
                )
            await self._register_stop_loss_watcher(bracket)
            if self.stop_loss_watcher is not None and remaining_managed_qty > 0:
                await self.stop_loss_watcher.rearm_position(
                    bracket.market_ticker,
                    trigger_price=bracket.last_price,
                )
            if trigger_ts_ms is not None:
                logger.warning(
                    "sl.exit_failed",
                    ticker=bracket.market_ticker,
                    action_key=action_key,
                    attempt=attempt,
                    remaining_count=live_count,
                    filled_qty=result.fill_quantity,
                    elapsed_ms=self._now_ms() - trigger_ts_ms,
                    reason="unfilled_or_partial",
                )
        return False

    async def _execute_stop_loss_from_watcher(
        self,
        ticker: str,
        side: str,
        quantity: int,
        trigger_price: int,
    ) -> bool:
        # Readiness gate: do not execute risk actions until startup reconciliation
        # has fully completed.  Firing before state is restored could act on
        # incomplete in-memory position data.
        if not self._reconciliation_complete:
            logger.warning(
                "phase.c.stop_loss_readiness_gate",
                ticker=ticker,
                msg="blocked: reconciliation not yet complete",
            )
            return False

        bracket = self.active_positions.get(ticker)
        if bracket is None:
            await self._unregister_stop_loss_watcher(ticker)
            logger.info("phase.c.stop_loss_position_missing", ticker=ticker)
            return True

        bracket.position_quantity = quantity
        app_known = self._app_owned_qty.get(ticker, quantity)
        self._set_ownership(
            ticker,
            total_position_qty=quantity,
            app_owned_qty=app_known,
            source="watcher_trigger",
            action="position_reconciled",
        )
        logger.warning(
            "phase.c.stop_loss_triggered",
            ticker=ticker,
            side=side,
            last_price=trigger_price,
            stop_loss=self.config.stop_loss_price,
            source="websocket_watcher",
        )
        if not getattr(bracket, "_stop_loss_counted", False):
            await self._increment_stop_loss_count_for_market(bracket.market_ticker)
            bracket._stop_loss_counted = True

        if self.config.enable_fast_sl_exit:
            await self._dispatch_stop_loss_exit(
                bracket,
                trigger_price=trigger_price,
                trigger_source="websocket_watcher",
            )
            return False
        market_gone = await self._execute_stop_loss(bracket)
        if market_gone:
            await self._decrement_stop_loss_count_for_market(bracket.market_ticker)
            bracket._stop_loss_counted = False
            return True

        if ticker not in self.active_positions:
            return True

        await self._register_stop_loss_watcher(bracket)
        return False

    async def _fetch_market_data_via_rest(self, ticker: str) -> Optional[dict]:
        """
        Fetch current market data via Kalshi REST /markets/{ticker} endpoint.
        Returns dict with price, yes_ask, yes_bid, spread in cents, or None.
        """
        import httpx
        from app.signing import build_auth_headers
        markets_path = f"/trade-api/v2/markets/{ticker}"
        markets_url = f"{self.config.rest_base_url}{markets_path}"
        try:
            rest_headers = build_auth_headers(self._private_key, self.config.kalshi_api_key, "GET", markets_path)
            client = self._http_client
            if client is None or client.is_closed:
                client = httpx.AsyncClient(timeout=5.0)
            resp = await client.get(markets_url, headers=rest_headers)
            if resp.status_code == 200:
                mkt = resp.json().get("market", {})
                result = {}

                ya = self._first_non_none(mkt.get("yes_ask_dollars"), mkt.get("yes_ask"))
                ya_cents = self._to_cents(ya)
                if ya_cents is not None:
                    result["yes_ask"] = ya_cents

                yb = self._first_non_none(mkt.get("yes_bid_dollars"), mkt.get("yes_bid"))
                yb_cents = self._to_cents(yb)
                if yb_cents is not None:
                    result["yes_bid"] = yb_cents

                lp = self._first_non_none(
                    mkt.get("last_price_dollars"),
                    mkt.get("last_price"),
                )
                lp_cents = self._to_cents(lp)
                if lp_cents is not None:
                    result["price"] = lp_cents
                elif "yes_ask" in result:
                    result["price"] = result["yes_ask"]

                if "yes_ask" in result and "yes_bid" in result:
                    result["spread"] = result["yes_ask"] - result["yes_bid"]

                status = self._first_non_none(mkt.get("status"), mkt.get("market_status"))
                if status is not None:
                    result["status"] = status
                if mkt.get("result") is not None:
                    result["result"] = mkt.get("result")
                if mkt.get("settlement_ts") is not None:
                    result["settlement_ts"] = mkt.get("settlement_ts")
                if mkt.get("is_settled") is not None:
                    result["is_settled"] = mkt.get("is_settled")

                return result if result else None
        except Exception as e:
            logger.warning("rest.fetch_failed", ticker=ticker, error=str(e))
        return None

    async def _log_snapshot(self):
        """Log portfolio snapshot at most once per ``_snapshot_interval_s`` (default 60 s).

        Uses a wall-clock timestamp throttle rather than a strategy-loop counter
        so the interval is independent of loop cadence.  This method contains
        zero sell / exit / order-submission logic — it is logging-only.
        """
        now_ts = asyncio.get_event_loop().time()
        if now_ts - self._last_snapshot_ts < self._snapshot_interval_s:
            return
        self._last_snapshot_ts = now_ts

        balance = await self.executor.get_balance()
        total_risk = sum(
            b.position_quantity * (100 - (b.last_price or 0))
            for b in self.active_positions.values()
        )

        async with await self.db.get_session() as session:
            ps = PortfolioSnapshot(
                cash_balance_cents=balance,
                total_positions=len(self.active_positions),
                total_risk_cents=total_risk,
            )
            session.add(ps)
            await session.commit()

        # Sync in-memory prices to DB positions
        async with await self.db.get_session() as session:
            for ticker, bracket in self.active_positions.items():
                if bracket.last_price is not None:
                    result = await session.execute(
                        select(PositionModel).where(PositionModel.market_ticker == ticker)
                    )
                    pos = result.scalar_one_or_none()
                    if pos:
                        pos.last_price = bracket.last_price
            await session.commit()

        logger.info("strategy.snapshot", balance=balance,
                    positions=len(self.active_positions), risk=total_risk,
                    active_tickers=list(self.active_positions.keys()),
                    total_brackets=len(self.brackets),
                    bracket_count_by_series={
                        'KXHIGHT': len([t for t in self.brackets if 'HIGHT' in t or 'HIGH' in t]),
                        'KXLOWT': len([t for t in self.brackets if 'LOWT' in t or 'LOW' in t]),
                    },
                    phase_details={t: (b.phase.name, b.crossed_buy) for t, b in self.brackets.items() if b.phase != Phase.MONITORING or b.crossed_buy})

    async def _db_cleanup_loop(self):
        """Delete old trades every hour to prevent disk bloat."""
        while self._running:
            try:
                async with await self.db.get_session() as session:
                    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
                    
                    # Delete trades older than 24 hours
                    await session.execute(
                        delete(StreamedTrade).where(StreamedTrade.trade_ts < cutoff)
                    )
                    
                    await session.commit()
                    logger.info("db.cleanup", hours_retained=24)
            except Exception as e:
                logger.error("db.cleanup_error", error=str(e))
            
            await asyncio.sleep(3600)  # run every hour

    async def _run_low_ticker_closeout(self, now_utc: Optional[datetime.datetime] = None) -> None:
        """Flatten all open KXLOW* positions.

        Routes each exit through the existing ``_execute_stop_loss`` machinery
        so the ownership/idempotency guards are respected.  KXHIGH* positions
        are never touched.  ``manage_external_positions=false`` is honoured by
        the exit path — external/manual holdings are not sold.
        """
        low_tickers = [
            ticker for ticker in list(self.active_positions.keys())
            if "KXLOW" in ticker.upper()
        ]
        if not low_tickers:
            logger.info("lowticker.daily_closeout_complete", closed=0,
                        reason="no_open_low_positions")
            return

        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)

        eligible_low_tickers = []
        for ticker in low_tickers:
            past_close_time_local, local_date, local_ctx = is_past_low_pm_close_time_local(
                self.config,
                ticker,
                now_utc=now_utc,
            )
            if not past_close_time_local or local_date is None:
                continue
            if self._low_ticker_pm_close_eval_dates.get(ticker) == local_date:
                continue
            self._low_ticker_pm_close_eval_dates[ticker] = local_date

            if is_low_pm_close_override_ticker(self.config, ticker):
                logger.info(
                    "lowticker.daily_closeout_ticker_override_match",
                    ticker=ticker,
                    local_date=local_date.isoformat(),
                    **local_ctx,
                )
                eligible_low_tickers.append(ticker)
                continue

            yes_ask = None
            quote = self.cache.get_quote(ticker)
            if quote is not None:
                _, yes_ask = quote
            if yes_ask is None:
                rest_data = await self._fetch_market_data_via_rest(ticker)
                if rest_data:
                    yes_ask = rest_data.get("yes_ask")

            if is_low_pm_close_ask_eligible(self.config, yes_ask):
                eligible_low_tickers.append(ticker)
            else:
                logger.info(
                    "lowticker.daily_closeout_ticker_skipped_threshold",
                    ticker=ticker,
                    yes_ask=yes_ask,
                    low_pm_close_amount=self.config.low_pm_close_amount,
                    local_date=local_date.isoformat(),
                    **local_ctx,
                )

        if not eligible_low_tickers:
            logger.info(
                "lowticker.daily_closeout_complete",
                closed=0,
                reason="no_open_low_positions_eligible_for_pm_close",
                low_pm_close_amount=self.config.low_pm_close_amount,
            )
            return

        logger.info("lowticker.daily_closeout_start", tickers=eligible_low_tickers,
                    count=len(eligible_low_tickers))
        closed = 0
        for ticker in eligible_low_tickers:
            bracket = self.active_positions.get(ticker)
            if bracket is None:
                continue
            try:
                await self._execute_stop_loss(bracket, bypass_cooldown=True)
                closed += 1
                logger.info("lowticker.daily_closeout_ticker_done", ticker=ticker)
            except Exception as e:
                logger.error("lowticker.daily_closeout_ticker_error",
                             ticker=ticker, error=str(e), exc_info=True)
        logger.info("lowticker.daily_closeout_complete", closed=closed,
                    total=len(eligible_low_tickers))

    async def _low_ticker_closeout_loop(self) -> None:
        """Scheduled loop that evaluates low-ticker PM close per ticker-local time."""
        if not self.config.low_ticker_daily_closeout_enabled:
            logger.info("lowticker.closeout_disabled")
            return

        _late_start_handled = False
        while self._running:
            await asyncio.sleep(30)
            if not self._reconciliation_complete:
                continue
            try:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                if not _late_start_handled:
                    _late_start_handled = True
                    if not self.config.low_ticker_closeout_on_late_start:
                        for ticker in list(self.active_positions.keys()):
                            if "KXLOW" not in ticker.upper():
                                continue
                            past, local_date, local_ctx = is_past_low_pm_close_time_local(
                                self.config,
                                ticker,
                                now_utc=now_utc,
                            )
                            if past and local_date is not None:
                                self._low_ticker_pm_close_eval_dates[ticker] = local_date
                                logger.info(
                                    "lowticker.closeout_late_start_skipped",
                                    ticker=ticker,
                                    local_date=local_date.isoformat(),
                                    **local_ctx,
                                )
                        continue
                await self._run_low_ticker_closeout(now_utc=now_utc)
            except Exception as e:
                logger.error("lowticker.closeout_loop_error", error=str(e), exc_info=True)

    async def _settlement_reconciler_loop(self) -> None:
        """Periodic background loop that reconciles TradeOutcome rows.

        Runs at ``reconciler_interval_minutes`` cadence (default 60 min).
        Config-gated by ``enable_settlement_reconciler`` (default true).
        Never blocks or raises outside its own try/except.
        """
        if not self.config.enable_settlement_reconciler:
            logger.info("reconciler.disabled")
            return

        from core.settlement_reconciler import run_reconciliation_cycle

        interval_s = max(int(self.config.reconciler_interval_minutes), 1) * 60
        while self._running:
            await asyncio.sleep(interval_s)
            if not self._reconciliation_complete:
                continue
            try:
                await run_reconciliation_cycle(self.db, self.config)
            except Exception as exc:
                logger.error("reconciler.loop_error", error=str(exc), exc_info=True)

    async def _intraday_exit_loop(self) -> None:
        """Background loop evaluating intraday checkpoint and HWM exits for KXLOW* positions.

        Mirrors ``_low_ticker_closeout_loop``: sleeps 30 s per iteration, gated
        on ``_reconciliation_complete``, respects ``self._running``.  Both
        features are fully inert (zero side-effects) when disabled.
        """
        intraday_enabled = getattr(self.config, "intraday_exit_enabled", True)
        hwm_enabled = getattr(self.config, "hwm_exit_enabled", False)
        if not intraday_enabled and not hwm_enabled:
            logger.info("intraday.exit_loop_disabled")
            return

        while self._running:
            await asyncio.sleep(30)
            if not self._reconciliation_complete:
                continue
            try:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                await self._run_intraday_exits(now_utc=now_utc)
            except Exception as exc:
                logger.error("intraday.exit_loop_error", error=str(exc), exc_info=True)

    async def _run_intraday_exits(
        self, now_utc: Optional[datetime.datetime] = None
    ) -> None:
        """Evaluate intraday checkpoint exits and HWM deterioration exits.

        Only KXLOW* positions in ``active_positions`` are examined.  KXHIGH*
        positions are never touched.
        """
        intraday_enabled = getattr(self.config, "intraday_exit_enabled", True)
        hwm_enabled = getattr(self.config, "hwm_exit_enabled", False)
        if not intraday_enabled and not hwm_enabled:
            return

        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)

        schedule = _parse_intraday_schedule_cfg(
            getattr(self.config, "intraday_exit_schedule", None)
        )
        grace_minutes = int(getattr(self.config, "intraday_exit_entry_grace_minutes", 90))
        hwm_arm_price = int(getattr(self.config, "hwm_arm_price", 93))
        hwm_exit_price = int(getattr(self.config, "hwm_exit_price", 88))
        # Seconds between first below-threshold read and confirmation read.
        confirm_seconds = 60.0

        now_mono = asyncio.get_event_loop().time()

        low_tickers = [
            t for t in list(self.active_positions.keys()) if "KXLOW" in t.upper()
        ]

        for ticker in low_tickers:
            bracket = self.active_positions.get(ticker)
            if bracket is None:
                continue

            tz_name = get_series_timezone(ticker)
            if not tz_name:
                continue

            now_local = now_utc.astimezone(ZoneInfo(tz_name))
            local_date = now_local.date()
            local_time = now_local.time()
            local_time_str = now_local.strftime("%H:%M:%S")

            # ── Grace-period check ──────────────────────────────────────────
            in_grace = False
            if bracket.entry_time is not None:
                age_s = (now_utc - bracket.entry_time).total_seconds()
                if age_s < grace_minutes * 60:
                    in_grace = True
            # Restored positions with no known entry_time → treat as past grace

            # ── Ask / bid from cache or REST ────────────────────────────────
            yes_ask: Optional[int] = None
            yes_bid: Optional[int] = None
            quote = self.cache.get_quote(ticker)
            if quote is not None:
                yes_bid, yes_ask = quote
            if yes_ask is None:
                try:
                    rest_data = await self._fetch_market_data_via_rest(ticker)
                    if rest_data:
                        yes_ask = rest_data.get("yes_ask")
                        if yes_bid is None:
                            yes_bid = rest_data.get("yes_bid")
                except Exception:
                    pass

            # ── Feature 1: Intraday checkpoints ─────────────────────────────
            exited_this_cycle = False
            if intraday_enabled:
                exited_this_cycle = False
                for chk_time_str, chk_threshold in schedule:
                    if exited_this_cycle:
                        break
                    chk_key = (ticker, chk_time_str)

                    # Idempotency: already evaluated for this date
                    if self._intraday_checkpoint_eval_dates.get(chk_key) == local_date:
                        continue

                    # Only fire at/after checkpoint local time
                    try:
                        chk_h, chk_m = chk_time_str.split(":")
                        chk_time = datetime.time(int(chk_h), int(chk_m))
                    except (ValueError, TypeError):
                        continue
                    if local_time < chk_time:
                        continue

                    below_threshold = yes_ask is not None and yes_ask < chk_threshold

                    logger.info(
                        "intraday.checkpoint_evaluated",
                        ticker=ticker,
                        checkpoint=chk_time_str,
                        local_time=local_time_str,
                        yes_ask=yes_ask,
                        threshold=chk_threshold,
                        below_threshold=below_threshold,
                    )

                    if not below_threshold:
                        # Mark evaluated; clear any stale pending for this key
                        self._intraday_checkpoint_eval_dates[chk_key] = local_date
                        self._intraday_checkpoint_pending.pop(chk_key, None)
                        continue

                    # Ask is below threshold
                    if in_grace:
                        logger.info(
                            "intraday.exit_skipped_grace_period",
                            ticker=ticker,
                            checkpoint=chk_time_str,
                            yes_ask=yes_ask,
                            threshold=chk_threshold,
                        )
                        self._intraday_checkpoint_eval_dates[chk_key] = local_date
                        continue

                    if yes_ask is None:
                        # No ask yet — skip; retry on next cycle
                        continue

                    pending_since = self._intraday_checkpoint_pending.get(chk_key)
                    if pending_since is None:
                        # First read below threshold
                        self._intraday_checkpoint_pending[chk_key] = now_mono
                        logger.info(
                            "intraday.exit_pending_confirmation",
                            ticker=ticker,
                            checkpoint=chk_time_str,
                            yes_ask=yes_ask,
                            threshold=chk_threshold,
                        )
                        continue

                    elapsed = now_mono - pending_since
                    if elapsed < confirm_seconds:
                        # Still within confirmation window — wait
                        continue

                    # Second read confirmed below threshold
                    logger.info(
                        "intraday.exit_confirmed",
                        ticker=ticker,
                        checkpoint=chk_time_str,
                        yes_ask=yes_ask,
                        threshold=chk_threshold,
                        confirmation_elapsed_s=round(elapsed, 1),
                    )
                    self._intraday_checkpoint_eval_dates[chk_key] = local_date
                    self._intraday_checkpoint_pending.pop(chk_key, None)

                    await self._intraday_execute_exit(
                        bracket,
                        yes_bid=yes_bid,
                        log_prefix="intraday",
                        checkpoint=chk_time_str,
                    )
                    exited_this_cycle = True

            # ── Feature 2: HWM deterioration exit ───────────────────────────
            if hwm_enabled and not exited_this_cycle:
                is_noon_or_later = local_time >= datetime.time(12, 0)
                hwm_key = (ticker, local_date)

                # Arm when ask reaches/exceeds hwm_arm_price after local noon
                if is_noon_or_later and yes_ask is not None and yes_ask >= hwm_arm_price:
                    if not self._hwm_armed.get(hwm_key):
                        self._hwm_armed[hwm_key] = True
                        logger.info(
                            "hwm.armed",
                            ticker=ticker,
                            yes_ask=yes_ask,
                            hwm_arm_price=hwm_arm_price,
                            local_date=local_date.isoformat(),
                        )

                if self._hwm_armed.get(hwm_key):
                    below_exit = yes_ask is not None and yes_ask <= hwm_exit_price

                    if below_exit and not in_grace:
                        pending_since = self._hwm_pending.get(ticker)
                        if pending_since is None:
                            self._hwm_pending[ticker] = now_mono
                            logger.info(
                                "hwm.exit_pending_confirmation",
                                ticker=ticker,
                                yes_ask=yes_ask,
                                hwm_exit_price=hwm_exit_price,
                            )
                        else:
                            elapsed = now_mono - pending_since
                            if elapsed >= confirm_seconds:
                                logger.info(
                                    "hwm.exit_confirmed",
                                    ticker=ticker,
                                    yes_ask=yes_ask,
                                    hwm_exit_price=hwm_exit_price,
                                    confirmation_elapsed_s=round(elapsed, 1),
                                )
                                self._hwm_pending.pop(ticker, None)
                                # Disarm to prevent re-triggering after exit
                                self._hwm_armed[hwm_key] = False
                                await self._intraday_execute_exit(
                                    bracket,
                                    yes_bid=yes_bid,
                                    log_prefix="hwm",
                                )
                    else:
                        # Ask recovered above exit price — clear pending confirmation
                        if ticker in self._hwm_pending:
                            logger.info(
                                "hwm.exit_flicker_ignored",
                                ticker=ticker,
                                yes_ask=yes_ask,
                                hwm_exit_price=hwm_exit_price,
                            )
                            self._hwm_pending.pop(ticker, None)

    async def _intraday_execute_exit(
        self,
        bracket: "MarketBracket",
        *,
        yes_bid: Optional[int],
        log_prefix: str,
        **log_kwargs,
    ) -> None:
        """Exit a KXLOW* position via limit-at-bid first, panic-fallback second.

        This is a *scheduled capital-preservation* exit, not a reactive
        stop-loss.  The StopLossLedger martingale count is NOT incremented
        here (unlike the watcher-triggered path).  If the exit falls back to
        ``_execute_stop_loss``, whatever that path does stands.

        Steps
        -----
        1. Cancel the resting backstop order (same cancel-before-sell rule).
        2. Submit a GTC SELL_YES limit at ``yes_bid`` via ``place_limit_sell``.
        3. Wait ~30 s; if still not fully filled, cancel and fall back to
           ``_execute_stop_loss(bracket, bypass_cooldown=True)``.
        4. If the executor does not support ``place_limit_sell``, fall back
           immediately to ``_execute_stop_loss``.
        """
        ticker = bracket.market_ticker
        managed_qty, app_owned_qty, external_qty = self._managed_exit_quantity(
            ticker, bracket.position_quantity
        )
        if managed_qty <= 0:
            logger.info(
                "exit.skipped_no_app_qty",
                ticker=ticker,
                total_position_qty=bracket.position_quantity,
                app_owned_qty=app_owned_qty,
                external_qty=external_qty,
                action=f"{log_prefix}_exit_not_executed",
            )
            return

        # Cancel resting backstop before submitting sell (prevent overselling)
        await self._cancel_sl_backstop(ticker, reason=f"{log_prefix}_exit")

        bid_price = yes_bid if yes_bid and yes_bid > 0 else None
        limit_succeeded = False

        if bid_price is not None:
            order = OrderRequest(
                market_ticker=ticker,
                side=OrderSide.SELL_YES,
                price=bid_price,
                quantity=managed_qty,
            )
            logger.info(
                f"{log_prefix}.exit_submitted",
                ticker=ticker,
                price=bid_price,
                qty=managed_qty,
                **log_kwargs,
            )
            try:
                result = await self.executor.place_limit_sell(order)
                placed_order_id = result.order_id if result.order_id else None
                if result.status != "NOT_SUPPORTED" and placed_order_id:
                    # Wait ~30 s for the resting limit to fill
                    await asyncio.sleep(30)
                    fill_status = await self.executor.get_order_status(placed_order_id)
                    filled = (fill_status or "").lower() in ("filled", "settled")
                    if filled:
                        limit_succeeded = True
                        logger.info(
                            f"{log_prefix}.exit_filled",
                            ticker=ticker,
                            price=bid_price,
                            qty=managed_qty,
                            order_id=placed_order_id,
                            **log_kwargs,
                        )
                        await self._record_and_remove_intraday_exit(bracket, result)
                    else:
                        await self.executor.cancel_order(placed_order_id, ticker)
                        logger.info(
                            f"{log_prefix}.exit_fallback_panic",
                            ticker=ticker,
                            reason="limit_not_filled",
                            **log_kwargs,
                        )
            except Exception as exc:
                logger.warning(
                    f"{log_prefix}.exit_limit_error",
                    ticker=ticker,
                    error=str(exc),
                )

        if not limit_succeeded:
            if bid_price is None:
                logger.info(
                    f"{log_prefix}.exit_fallback_panic",
                    ticker=ticker,
                    reason="no_bid",
                    **log_kwargs,
                )
            await self._execute_stop_loss(bracket, bypass_cooldown=True)

    async def _record_and_remove_intraday_exit(
        self, bracket: "MarketBracket", result: "ExecutionResult"
    ) -> None:
        """Record a successful limit-at-bid intraday exit and clean up position state."""
        ticker = bracket.market_ticker
        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=ticker,
                action=TradeAction.STOP_LOSS,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.execute(
                delete(PositionModel).where(PositionModel.market_ticker == ticker)
            )
            await session.commit()

        bracket.phase = Phase.CLOSED
        self.active_positions.pop(ticker, None)
        self.brackets.pop(ticker, None)
        self._app_owned_qty.pop(ticker, None)
        await self._unregister_stop_loss_watcher(ticker)

        asyncio.create_task(
            self._update_exit_outcome(
                market_ticker=ticker,
                exit_price=result.fill_price,
                exit_qty=result.fill_quantity,
                outcome=TradeOutcomeStatus.STOPPED,
            )
        )

    async def stop(self):
        self._running = False
        for task in list(self._sl_exit_tasks.values()):
            if not task.done():
                task.cancel()
        self._sl_exit_tasks.clear()
        self._sl_cycles.clear()
        if self._held_positions_task is not None and not self._held_positions_task.done():
            self._held_positions_task.cancel()
            try:
                await self._held_positions_task
            except asyncio.CancelledError:
                pass
            self._held_positions_task = None
        if self._trade_log_writer_task is not None and not self._trade_log_writer_task.done():
            self._trade_log_writer_task.cancel()
            try:
                await self._trade_log_writer_task
            except asyncio.CancelledError:
                pass
            self._trade_log_writer_task = None
        if self._http_client is not None and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None
        logger.info("strategy.stopped")
