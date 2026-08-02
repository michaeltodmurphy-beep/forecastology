from pydantic_settings import BaseSettings, SettingsConfigDict, NoDecode
from typing import Literal, Annotated
import os
import structlog
from dotenv import load_dotenv
from pydantic import field_validator, model_validator

load_dotenv()

logger = structlog.get_logger(__name__)


def _parse_hedge_max_factor(raw: str | None) -> int:
    """Parse HEDGE_MAX_FACTOR from an env-var string.

    Accepts positive integer strings ("1", "3", "5").
    Float strings ("3.0", "2.5") are truncated to int with a warning.
    Missing / empty → returns default of 3.
    Values below 1 are clamped to 1 with a warning.
    """
    if not raw or not raw.strip():
        return 3
    stripped = raw.strip()
    try:
        parsed_float = float(stripped)
        parsed_int = int(parsed_float)
        if parsed_float != parsed_int:
            logger.warning(
                "config.hedge_max_factor_non_integer",
                raw=raw,
                truncated_to=parsed_int,
                message=f"HEDGE_MAX_FACTOR='{raw}' is not an integer; truncating to {parsed_int}",
            )
        if parsed_int < 1:
            logger.warning(
                "config.hedge_max_factor_below_minimum",
                raw=raw,
                clamped_to=1,
                message=f"HEDGE_MAX_FACTOR='{raw}' is below 1; clamping to 1",
            )
            return 1
        return parsed_int
    except (ValueError, TypeError):
        logger.warning(
            "config.hedge_max_factor_invalid",
            raw=raw,
            fallback=3,
            message=f"Unrecognized value for HEDGE_MAX_FACTOR='{raw}'; defaulting to 3",
        )
        return 3


def _parse_initial_contract_count(raw: str | None) -> int:
    """Parse INITIAL_CONTRACT_COUNT from an env-var string.

    Accepts positive integer strings ("1", "4", "10").
    Float strings ("4.0", "2.5") are truncated to int with a warning.
    Missing / empty → returns default of 1.
    Values below 1 are clamped to 1 with a warning.
    Unparseable input logs a warning and returns the default (1).
    """
    if not raw or not raw.strip():
        return 1
    stripped = raw.strip()
    try:
        parsed_float = float(stripped)
        parsed_int = int(parsed_float)
        if parsed_float != parsed_int:
            logger.warning(
                "config.initial_contract_count_non_integer",
                raw=raw,
                truncated_to=parsed_int,
                message=f"INITIAL_CONTRACT_COUNT='{raw}' is not an integer; truncating to {parsed_int}",
            )
        if parsed_int < 1:
            logger.warning(
                "config.initial_contract_count_below_minimum",
                raw=raw,
                clamped_to=1,
                message=f"INITIAL_CONTRACT_COUNT='{raw}' is below 1; clamping to 1",
            )
            return 1
        return parsed_int
    except (ValueError, TypeError):
        logger.warning(
            "config.initial_contract_count_invalid",
            raw=raw,
            fallback=1,
            message=f"Unrecognized value for INITIAL_CONTRACT_COUNT='{raw}'; defaulting to 1",
        )
        return 1


def _parse_trade_toggle(raw: str | None, name: str, default: bool = True) -> bool:
    """Parse a yes/no trade-direction toggle from an env-var string.

    Accepted truthy  : 'yes', 'true', '1'  (case-insensitive)
    Accepted falsy   : 'no',  'false', '0' (case-insensitive)
    Missing / empty  : returns *default* (True)
    Anything else    : logs a warning and returns *default* (True – fail safe)
    """
    if not raw or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in ("yes", "true", "1"):
        return True
    if normalized in ("no", "false", "0"):
        return False
    logger.warning(
        "config.trade_toggle_invalid",
        name=name,
        raw=raw,
        fallback=default,
        message=f"Unrecognized value for {name}='{raw}'; defaulting to {'yes' if default else 'no'}",
    )
    return default


def _parse_positive_int(raw: str | None, name: str, default: int) -> int:
    if not raw or not raw.strip():
        return default
    try:
        parsed = int(raw.strip())
    except (TypeError, ValueError):
        logger.warning(
            "config.positive_int_invalid",
            name=name,
            raw=raw,
            fallback=default,
            message=f"Unrecognized value for {name}='{raw}'; defaulting to {default}",
        )
        return default
    if parsed < 1:
        logger.warning(
            "config.positive_int_below_minimum",
            name=name,
            raw=raw,
            fallback=default,
            message=f"Value for {name} must be >= 1; defaulting to {default}",
        )
        return default
    return parsed


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    kalshi_api_key: str = ''
    kalshi_private_key_path: str = 'kalshi_private_key.pem'
    mysql_database_url: str = 'mysql+aiomysql://user:pass@localhost:3306/forecastology'
    trading_mode: Literal['PAPER', 'LIVE'] = 'PAPER'
    initial_contract_count: int = 1
    monitor_start_price: int
    buy_trigger_price: int
    spread_monitor_price: int
    minimum_spread: int
    stop_loss_price: int
    rest_base_url: str = 'https://external-api.kalshi.com'
    ws_url: str = 'wss://external-api-ws.kalshi.com/trade-api/ws/v2'
    weather_series_prefix: str = 'KXWEATHER'
    # hedge_max_factor controls martingale recovery sizing.
    # It is the TOTAL NUMBER OF ALLOWED BUY LEVELS (counting from 0).
    # Buying is allowed while stop_loss_count < hedge_max_factor.
    # Buy size = initial_contract_count * 2**stop_loss_count.
    # Example: initial=3, factor=3 → counts 0,1,2 allowed → sizes 3,6,12;
    #          max_allowed_qty = 3 * 2^(3-1) = 12.  count >= 3 is blocked.
    hedge_max_factor: int = 3
    eval_price_floor: int = 5
    # DEPRECATED / UNUSED by trading logic. Kept only so existing .env files that
    # still define HEDGE_TRIGGER_PRICE / HEDGE_BUY continue to load, and so .env
    # files that OMIT them do not raise (they now have safe defaults). No code
    # references these for any trading decision after the hedge engine removal.
    hedge_trigger_price: int = 0
    hedge_buy: int = 0
    dry_run: bool = False
    # Trade-direction toggles.  Set LOW_TRADES=no or HIGH_TRADES=no in .env
    # to disable new entry placement for the respective city-temperature family.
    # Existing open positions are always fully managed (SL/exit) regardless of
    # these flags.  Parsed by from_env() via _parse_trade_toggle().
    low_trades: bool = True
    high_trades: bool = True
    no_trade_tickers: Annotated[set[str], NoDecode] = set()
    manage_external_positions: bool = False
    # ── City-local-time entry settle gate ───────────────────────────────────
    # Prevents new buy orders from being placed before the city's local clock
    # reaches the threshold.  Kalshi settles temperature markets overnight, so
    # entries before rollover would be for the *prior* settlement day.
    #
    # ENABLE_LOCAL_SETTLE_GATE=true|false  (default: true)
    # DEFAULT_ENTRY_START_LOCAL=HH:MM      (default: 01:00; all cities except PHX)
    # PHOENIX_ENTRY_START_LOCAL=HH:MM      (default: 00:00; Phoenix MST no DST)
    #
    # Parsed by from_env().  Does NOT affect SL/exit/position management paths.
    enable_local_settle_gate: bool = True
    default_entry_start_local: str = "01:00"
    phoenix_entry_start_local: str = "00:00"
    held_position_price_refresh_seconds: int = 10
    # Interval (ms) for the dedicated held-position SL evaluation loop that runs
    # independently of entry scanning.  Range: 100–250 ms.  Configurable via
    # HELD_POSITIONS_LOOP_INTERVAL_MS env var.  Default 250 ms is intentionally
    # conservative; lower values increase SL check frequency at the cost of more
    # CPU/asyncio scheduling overhead.
    held_positions_loop_interval_ms: int = 250
    max_no_price_cycles: int = 10
    stop_loss_max_unfilled_attempts: int = 3
    enable_fast_sl_exit: bool | None = None
    sl_worker_interval_ms: int = 250
    sl_exit_retry_interval_ms: int = 300
    sl_exit_max_attempts: int = 3
    sl_exit_aggressive_offset_ticks: int = 2
    sl_exit_max_slippage: int = 20
    # Maximum bid-ask spread (in cents) at which the stop-loss is allowed to fire.
    # When the YES spread exceeds this value the bot holds rather than selling into
    # a wide, indecisive book. Set via `max_sl_spread` in dollar format
    # (e.g. `max_sl_spread=0.15` -> 15¢); default 20 is fallback when env is absent.
    max_sl_spread: int = 20
    # Maximum seconds to hold a stop-loss trigger for wide/one-sided spread before
    # escalating and forcing exit anyway. 0 means no hold window (fire immediately).
    sl_spread_hold_max_seconds: int = 120
    # Stop-loss exit mode.
    # PANIC_FLATTEN (default): immediately submit at SL_PANIC_SELL_PRICE (1¢ floor)
    #   so Kalshi matches at the best available bid without a repricing ladder.
    # AGGRESSIVE_LIMIT: repricing ladder capped by SL_EXIT_MAX_SLIPPAGE.
    sl_exit_mode: str = "PANIC_FLATTEN"
    # Panic-flatten sell price floor in cents (default 1¢). A sell at 1¢ becomes
    # immediately marketable — Kalshi fills it at the best available bid.
    sl_panic_sell_price: int = 1
    # Retry interval (ms) between panic-flatten re-submissions (default 250ms).
    sl_panic_retry_ms: int = 250
    # Max retry attempts for panic-flatten exit (default 5).
    sl_panic_max_retries: int = 5
    # Maximum age (ms) of a cached YES ask quote before it is considered stale
    # for PANIC_FLATTEN pre-submit revalidation. Set to 0 to disable the check.
    sl_panic_max_quote_age_ms: int = 30000
    # ── Low-ticker daily close-out at 22:00 ET ──────────────────────────────
    # Automatically flattens all open KXLOW* positions at the configured time
    # (Eastern, DST-aware) every day.  KXHIGH* positions are not touched.
    #
    # LOW_TICKER_DAILY_CLOSEOUT_ENABLED=true|false  (default: true)
    # LOW_TICKER_CLOSEOUT_TIME_ET=HH:MM             (default: 22:00)
    # LOW_TICKER_CLOSEOUT_ON_LATE_START=true|false  (default: true)
    #   When true, if the process starts after the configured closeout time
    #   on a given ET day, perform the closeout once immediately so the
    #   "no Low positions held overnight" intent is honoured.
    #
    # Parsed by from_env().
    low_ticker_daily_closeout_enabled: bool = True
    low_ticker_closeout_time_et: str = "22:00"
    low_ticker_closeout_on_late_start: bool = True
    # ── Low-ticker entry halt after 22:00 ET ────────────────────────────────
    # Prevents new KXLOW* entry orders from being placed at/after the
    # configured Eastern time.  The block lasts for the remainder of that
    # ET trading day; new Low entries resume the following ET day.
    # KXHIGH* entries are completely unaffected.
    #
    # LOW_TICKER_ENTRY_HALT_ENABLED=true|false  (default: true)
    # LOW_TICKER_ENTRY_HALT_TIME_ET=HH:MM       (default: 22:00)
    # LOW_TICKER_10PM_MAX_ASK=0.93             (default: 93¢)
    #   At/after LOW_TICKER_ENTRY_HALT_TIME_ET, apply Low 10 PM halt/closeout
    #   behavior only when YES ask is STRICTLY below this threshold.
    #
    # Parsed by from_env().  Does NOT affect stop-loss / exit paths.
    low_ticker_entry_halt_enabled: bool = True
    low_ticker_entry_halt_time_et: str = "22:00"
    low_ticker_10pm_max_ask: int = 93
    instance_lock_enabled: bool = True
    instance_lock_file: str = "/tmp/forecastology.lock"
    instance_id: str = ""
    log_file: str = "logs/run.log"
    log_max_bytes: int = 104857600
    log_backup_count: int = 10
    # ── Unprotected-position remediation ────────────────────────────────────
    # When a restored/adopted position goes blind (no price feed) for more than
    # this many consecutive SL evaluation cycles, the bot escalates to a
    # logger.critical alert (phase.c.unprotected_escalation) and optionally
    # attempts a protective panic-flatten of the app-owned quantity.
    #
    # SL_UNPROTECTED_MAX_BLIND_CYCLES: number of consecutive no-price cycles
    #   before escalation fires. Default: 30 (at 250 ms cadence ≈ 7.5 seconds).
    # SL_FLATTEN_UNPROTECTED_ON_BLIND: when true, attempt a panic-flatten exit
    #   of app_owned_qty once the escalation threshold is exceeded. Only
    #   app-owned quantity is ever sold — MANAGE_EXTERNAL_POSITIONS semantics
    #   are fully respected. Default: false (conservative; alert only).
    sl_unprotected_max_blind_cycles: int = 30
    sl_flatten_unprotected_on_blind: bool = False

    @field_validator(
        'buy_trigger_price', 'spread_monitor_price', 'minimum_spread',
        'stop_loss_price', 'monitor_start_price',
        'eval_price_floor', 'hedge_trigger_price', 'hedge_buy',
        'max_sl_spread', 'sl_exit_max_slippage', 'low_ticker_10pm_max_ask',
        mode='before'
    )
    @classmethod
    def convert_dollars_to_cents(cls, v):
        """Convert dollar-formatted values from .env to integer cents.
        .env values come as strings like '0.82' -> 82 cents.
        Hardcoded default ints (e.g. 85) are already in cents and left as-is.
        """
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return 0
            # Always treat string inputs as dollars, convert to cents
            return int(float(v) * 100)
        # Already an int or float — it's already in cents
        return int(v)

    @field_validator('no_trade_tickers', mode='before')
    @classmethod
    def parse_no_trade_tickers(cls, v):
        if not v:
            return set()
        if isinstance(v, (set, list)):
            return {str(t).strip().upper() for t in v if str(t).strip()}
        return {t.strip().upper() for t in str(v).split(',') if t.strip()}

    @model_validator(mode='after')
    def normalize_trading_mode(self):
        if self.trading_mode:
            self.trading_mode = self.trading_mode.upper()
        if self.enable_fast_sl_exit is None:
            self.enable_fast_sl_exit = self.trading_mode == "LIVE"
        return self

    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Load config from .env file (or environment variables).
        Prices in .env may be in dollar format (e.g. 0.85) or already in cents.
        Field validators convert them to integer cents automatically.
        """
        dry_run_raw = os.getenv("DRY_RUN", "")
        dry_run = dry_run_raw.strip().lower() in {"1", "true", "yes"} if dry_run_raw else False
        low_trades = _parse_trade_toggle(os.getenv("LOW_TRADES"), "LOW_TRADES", default=True)
        high_trades = _parse_trade_toggle(os.getenv("HIGH_TRADES"), "HIGH_TRADES", default=True)
        manage_external_positions = _parse_trade_toggle(
            os.getenv("MANAGE_EXTERNAL_POSITIONS"), "MANAGE_EXTERNAL_POSITIONS", default=False
        )
        enable_local_settle_gate = _parse_trade_toggle(
            os.getenv("ENABLE_LOCAL_SETTLE_GATE"), "ENABLE_LOCAL_SETTLE_GATE", default=True
        )
        no_trade_tickers_raw = os.getenv("NO_TRADE_TICKERS", "")
        default_entry_start_local = os.getenv("DEFAULT_ENTRY_START_LOCAL", "01:00")
        phoenix_entry_start_local = os.getenv("PHOENIX_ENTRY_START_LOCAL", "00:00")
        hedge_max_factor = _parse_hedge_max_factor(os.getenv("HEDGE_MAX_FACTOR"))
        initial_contract_count = _parse_initial_contract_count(os.getenv("INITIAL_CONTRACT_COUNT"))
        low_ticker_daily_closeout_enabled = _parse_trade_toggle(
            os.getenv("LOW_TICKER_DAILY_CLOSEOUT_ENABLED"),
            "LOW_TICKER_DAILY_CLOSEOUT_ENABLED",
            default=True,
        )
        low_ticker_closeout_time_et = os.getenv("LOW_TICKER_CLOSEOUT_TIME_ET", "22:00")
        low_ticker_closeout_on_late_start = _parse_trade_toggle(
            os.getenv("LOW_TICKER_CLOSEOUT_ON_LATE_START"),
            "LOW_TICKER_CLOSEOUT_ON_LATE_START",
            default=True,
        )
        low_ticker_entry_halt_enabled = _parse_trade_toggle(
            os.getenv("LOW_TICKER_ENTRY_HALT_ENABLED"),
            "LOW_TICKER_ENTRY_HALT_ENABLED",
            default=True,
        )
        low_ticker_entry_halt_time_et = os.getenv("LOW_TICKER_ENTRY_HALT_TIME_ET", "22:00")
        low_ticker_10pm_max_ask = os.getenv("LOW_TICKER_10PM_MAX_ASK", "0.93")
        instance_lock_enabled = _parse_trade_toggle(
            os.getenv("INSTANCE_LOCK_ENABLED"),
            "INSTANCE_LOCK_ENABLED",
            default=True,
        )
        instance_lock_file = os.getenv("INSTANCE_LOCK_FILE") or os.getenv(
            "FORECASTOLOGY_LOCKFILE", "/tmp/forecastology.lock"
        )
        instance_id = os.getenv("INSTANCE_ID", "").strip()
        log_file = os.getenv("LOG_FILE", "logs/run.log")
        log_max_bytes = _parse_positive_int(
            os.getenv("LOG_MAX_BYTES"),
            "LOG_MAX_BYTES",
            default=104857600,
        )
        log_backup_count = _parse_positive_int(
            os.getenv("LOG_BACKUP_COUNT"),
            "LOG_BACKUP_COUNT",
            default=10,
        )
        sl_unprotected_max_blind_cycles = _parse_positive_int(
            os.getenv("SL_UNPROTECTED_MAX_BLIND_CYCLES"),
            "SL_UNPROTECTED_MAX_BLIND_CYCLES",
            default=30,
        )
        sl_flatten_unprotected_on_blind = _parse_trade_toggle(
            os.getenv("SL_FLATTEN_UNPROTECTED_ON_BLIND"),
            "SL_FLATTEN_UNPROTECTED_ON_BLIND",
            default=False,
        )
        return cls(
            dry_run=dry_run,
            low_trades=low_trades,
            high_trades=high_trades,
            no_trade_tickers=no_trade_tickers_raw,
            manage_external_positions=manage_external_positions,
            enable_local_settle_gate=enable_local_settle_gate,
            default_entry_start_local=default_entry_start_local,
            phoenix_entry_start_local=phoenix_entry_start_local,
            hedge_max_factor=hedge_max_factor,
            initial_contract_count=initial_contract_count,
            low_ticker_daily_closeout_enabled=low_ticker_daily_closeout_enabled,
            low_ticker_closeout_time_et=low_ticker_closeout_time_et,
            low_ticker_closeout_on_late_start=low_ticker_closeout_on_late_start,
            low_ticker_entry_halt_enabled=low_ticker_entry_halt_enabled,
            low_ticker_entry_halt_time_et=low_ticker_entry_halt_time_et,
            low_ticker_10pm_max_ask=low_ticker_10pm_max_ask,
            instance_lock_enabled=instance_lock_enabled,
            instance_lock_file=instance_lock_file,
            instance_id=instance_id,
            log_file=log_file,
            log_max_bytes=log_max_bytes,
            log_backup_count=log_backup_count,
            sl_unprotected_max_blind_cycles=sl_unprotected_max_blind_cycles,
            sl_flatten_unprotected_on_blind=sl_flatten_unprotected_on_blind,
        )
