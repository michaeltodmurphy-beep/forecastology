from pydantic_settings import BaseSettings, SettingsConfigDict, NoDecode
from typing import Literal, Annotated
import os
import structlog
from dotenv import load_dotenv
from pydantic import field_validator, model_validator

load_dotenv()

logger = structlog.get_logger(__name__)


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


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

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
    hedge_max_factor: float = 3.0
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
    # AGGRESSIVE_LIMIT (default): repricing ladder capped by SL_EXIT_MAX_SLIPPAGE.
    # PANIC_FLATTEN: immediately submit at SL_PANIC_SELL_PRICE (1¢ floor) to
    #   guarantee fill speed over exit price, then retry rapidly if unfilled.
    sl_exit_mode: str = "AGGRESSIVE_LIMIT"
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

    @field_validator(
        'buy_trigger_price', 'spread_monitor_price', 'minimum_spread',
        'stop_loss_price', 'monitor_start_price',
        'eval_price_floor', 'hedge_trigger_price', 'hedge_buy',
        'max_sl_spread', 'sl_exit_max_slippage',
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
        return cls(
            dry_run=dry_run,
            low_trades=low_trades,
            high_trades=high_trades,
            no_trade_tickers=no_trade_tickers_raw,
            manage_external_positions=manage_external_positions,
            enable_local_settle_gate=enable_local_settle_gate,
            default_entry_start_local=default_entry_start_local,
            phoenix_entry_start_local=phoenix_entry_start_local,
        )
