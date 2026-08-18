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
from core.log_dedupe import DedupeLogger
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








def _is_warm_series(config: "AppConfig", market_ticker: str) -> bool:
    """Return True if *market_ticker* belongs to a configured warm-trade series.

    Warm series are identified by their uppercase series prefix (text before the
    first '-'), matched against ``config.warm_trade_tickers``.  They bypass the
    sunrise/NWS entry gate but still enforce the AM-low deadline and local
    settle gate.
    """
    warm = getattr(config, "warm_trade_tickers", None) or set()
    if not warm:
        return False
    prefix = market_ticker.upper().split("-")[0]
    return prefix in warm


def get_buy_trigger_price(config: "AppConfig", market_ticker: str) -> Optional[int]:
    ticker_upper = market_ticker.upper()
    if "KXLOW" in ticker_upper:
        warm_trigger = int(getattr(config, "buy_trigger_price_low_warm", 0) or 0)
        if warm_trigger and _is_warm_series(config, market_ticker):
            return warm_trigger
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
        self._log_dedupe = DedupeLogger(summary_interval_seconds=300)

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

        # Partial-fill chaser tasks: one per ticker, keyed by market_ticker.
        # Each task runs _partial_fill_chase_loop for the remaining quantity.
        self._chase_tasks: dict[str, asyncio.Task] = {}

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
    def _ticker_market_day(ticker: str) -> str:
        parsed = parse_series_and_date(ticker)
        if parsed is not None:
            return parsed[1]
        return datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    def _log_deduped_info(
        self,
        event: str,
        dedupe_key: str,
        **fields,
    ) -> None:
        self._log_dedupe.log(
            logger,
            "info",
            event,
            dedupe_key,
            day=self._ticker_market_day(dedupe_key),
            **fields,
        )

    @staticmethod
    def _reset_falling_knife_state(bracket: MarketBracket) -> None:
        bracket.falling_knife_guard = False
        bracket.pending_knife_tick = False
        bracket.last_above_ceiling_at = None

    def _update_falling_knife_guard(
        self,
        bracket: MarketBracket,
        ticker: str,
        price: int,
        buy_trigger: Optional[int],
        now_utc: datetime.datetime,
    ) -> None:
        if buy_trigger is not None and price < buy_trigger:
            self._reset_falling_knife_state(bracket)
            return

        if price > self.config.spread_monitor_price:
            bracket.last_above_ceiling_at = now_utc
            if bracket.pending_knife_tick:
                bracket.falling_knife_guard = True
            else:
                bracket.pending_knife_tick = True
            return

        bracket.pending_knife_tick = False

        decay_minutes = int(getattr(self.config, "falling_knife_decay_minutes", 10))
        if (
            bracket.falling_knife_guard
            and decay_minutes > 0
            and bracket.last_above_ceiling_at is not None
            and now_utc - bracket.last_above_ceiling_at > datetime.timedelta(minutes=decay_minutes)
        ):
            minutes_elapsed = (
                now_utc - bracket.last_above_ceiling_at
            ).total_seconds() / 60.0
            self._reset_falling_knife_state(bracket)
            logger.info(
                "phase.b.falling_knife_decayed",
                ticker=ticker,
                minutes_elapsed=minutes_elapsed,
            )

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

        # Cancel any resting chaser before dispatching the SL task.
        chase_task = self._chase_tasks.get(ticker)
        if chase_task is not None and not chase_task.done():
            chase_task.cancel()
            self._chase_tasks.pop(ticker, None)

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
                     max_spread=self.config.minimum_spread,
                     spread_monitor=self.config.spread_monitor_price,
                     falling_knife_decay_minutes=self.config.falling_knife_decay_minutes,
                     stop_loss=self.config.stop_loss_price,
                     mode=self.config.trading_mode,
                     low_trades=self.config.low_trades,
                     high_trades=self.config.high_trades,
                     manage_external_positions=self.config.manage_external_positions,
                     enable_local_settle_gate=self.config.enable_local_settle_gate,
                     default_entry_start_local=self.config.default_entry_start_local,
                     phoenix_entry_start_local=self.config.phoenix_entry_start_local,
                     partial_fill_chase=getattr(self.config, "partial_fill_chase", False),
                     chase_interval_seconds=getattr(self.config, "chase_interval_seconds", 60),
                     chase_max_minutes=getattr(self.config, "chase_max_minutes", 30),
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
                sunrise_obs_max_age_minutes=self.config.sunrise_obs_max_age_minutes,
                sunrise_obs_max_age_overrides=dict(sorted(self.config.sunrise_obs_max_age_overrides.items())),
                sunrise_obs_source=self.config.sunrise_obs_source,
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
            should_evaluate_entry = not bracket.crossed_buy and bracket.phase == Phase.MONITORING
            if should_evaluate_entry and self.config.no_trade_tickers:
                ticker_upper = ticker.upper()
                if any(
                    ticker_upper == nt or ticker_upper.startswith(nt + "-")
                    for nt in self.config.no_trade_tickers
                ):
                    self._log_deduped_info(
                        "phase.b.entry_blocked_by_config",
                        ticker,
                        ticker=ticker,
                        reason="NO_TRADE_TICKERS",
                    )
                    continue

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
            now_utc = datetime.datetime.now(datetime.timezone.utc)

            buy_trigger = get_buy_trigger_price(self.config, ticker)
            ticker_upper = ticker.upper()
            is_high = "KXHIGH" in ticker_upper
            is_low = "KXLOW" in ticker_upper
            use_nws_window_for_low = True
            sunrise_gate_allowed = True
            if should_evaluate_entry and is_low and self.config.entry_gate_mode == "SUNRISE":
                sunrise_decision = self._sunrise_entry_gate.evaluate(
                    ticker=ticker,
                    now_utc=now_utc,
                )
                if not sunrise_decision.allowed:
                    bracket.sunrise_window_was_open = False
                    sunrise_gate_allowed = False
                else:
                    if not bracket.sunrise_window_was_open:
                        if bracket.falling_knife_guard:
                            logger.info("phase.b.falling_knife_window_reset", ticker=ticker)
                        self._reset_falling_knife_state(bracket)
                        bracket.sunrise_window_was_open = True
                    use_nws_window_for_low = sunrise_decision.use_nws_window_fallback
            self._update_falling_knife_guard(bracket, ticker, price, buy_trigger, now_utc)

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
                self._log_deduped_info(
                    "phase.b.missed_entry",
                    ticker,
                    ticker=ticker,
                    price=price,
                    max_price=self.config.spread_monitor_price,
                )
                continue

            if bracket.falling_knife_guard:
                self._log_deduped_info(
                    "phase.b.falling_knife_blocked",
                    ticker,
                    ticker=ticker,
                    price=price,
                )
                continue

            if not sunrise_gate_allowed:
                continue

            if spread <= self.config.minimum_spread:
                # --- Trade-direction toggle gate ---
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



















































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































    
