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


_INTRADAY_EXIT_SCHEDULE_DEFAULT = "12:00:0.85,15:00:0.90,18:00:0.90"


def _parse_intraday_schedule(raw: str | None) -> list[tuple[str, int]]:
    """Parse INTRADAY_EXIT_SCHEDULE into a sorted list of (HH:MM, threshold_cents) tuples.

    Format: comma-separated ``HH:MM:price`` entries where price is in dollars
    (e.g. ``0.85`` → 85¢).  Malformed individual entries are skipped with a
    warning.  Falls back to the built-in default schedule when the whole string
    contains no valid entries.
    """
    default = _parse_intraday_schedule_raw(_INTRADAY_EXIT_SCHEDULE_DEFAULT)
    if not raw or not raw.strip():
        return default
    return _parse_intraday_schedule_raw(raw, fallback=default)


def _parse_intraday_schedule_raw(
    raw: str, fallback: "list[tuple[str, int]] | None" = None
) -> "list[tuple[str, int]]":
    """Low-level parser shared by _parse_intraday_schedule and tests."""
    entries: list[tuple[str, int]] = []
    for part in raw.strip().split(","):
        part = part.strip()
        if not part:
            continue
        segments = part.split(":")
        if len(segments) != 3:
            logger.warning(
                "config.intraday_schedule_entry_malformed",
                entry=part,
                reason="wrong_segment_count",
            )
            continue
        hh, mm, price_str = segments
        try:
            h = int(hh)
            m = int(mm)
            if not (0 <= h <= 23 and 0 <= m <= 59):
                raise ValueError("time out of range")
            time_str = f"{h:02d}:{m:02d}"
        except (ValueError, TypeError):
            logger.warning(
                "config.intraday_schedule_entry_malformed",
                entry=part,
                reason="invalid_time",
            )
            continue
        try:
            price_cents = int(round(float(price_str) * 100))
            if not (1 <= price_cents <= 99):
                raise ValueError("price out of valid range")
        except (ValueError, TypeError):
            logger.warning(
                "config.intraday_schedule_entry_malformed",
                entry=part,
                reason="invalid_price",
            )
            continue
        entries.append((time_str, price_cents))

    if not entries:
        logger.warning(
            "config.intraday_schedule_all_entries_malformed",
            raw=raw,
            fallback="default",
        )
        return fallback if fallback is not None else []

    return sorted(entries, key=lambda x: x[0])


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


def _parse_non_negative_int(raw: str | None, name: str, default: int) -> int:
    if not raw or not raw.strip():
        return default
    try:
        parsed = int(raw.strip())
    except (TypeError, ValueError):
        logger.warning(
            "config.non_negative_int_invalid",
            name=name,
            raw=raw,
            fallback=default,
            message=f"Unrecognized value for {name}='{raw}'; defaulting to {default}",
        )
        return default
    if parsed < 0:
        logger.warning(
            "config.non_negative_int_below_minimum",
            name=name,
            raw=raw,
            fallback=default,
            message=f"Value for {name} must be >= 0; defaulting to {default}",
        )
        return default
    return parsed


def _parse_entry_gate_mode(raw: str | None) -> str:
    if not raw or not raw.strip():
        return "NWS_WINDOW"
    normalized = raw.strip().upper()
    if normalized in ("NWS_WINDOW", "SUNRISE"):
        return normalized
    logger.warning(
        "config.entry_gate_mode_invalid",
        raw=raw,
        fallback="NWS_WINDOW",
        message=f"Unrecognized value for ENTRY_GATE_MODE='{raw}'; defaulting to NWS_WINDOW",
    )
    return "NWS_WINDOW"


def _parse_sunrise_source(raw: str | None) -> str:
    if not raw or not raw.strip():
        return "astral"
    normalized = raw.strip().lower()
    if normalized in ("astral", "api"):
        return normalized
    logger.warning(
        "config.sunrise_source_invalid",
        raw=raw,
        fallback="astral",
        message=f"Unrecognized value for SUNRISE_SOURCE='{raw}'; defaulting to astral",
    )
    return "astral"


def _parse_sunrise_obs_source(raw: str | None) -> str:
    if not raw or not raw.strip():
        return "awc"
    normalized = raw.strip().lower()
    if normalized in ("awc", "nws"):
        return normalized
    logger.warning(
        "config.sunrise_obs_source_invalid",
        raw=raw,
        fallback="awc",
        message=f"Unrecognized value for SUNRISE_OBS_SOURCE='{raw}'; defaulting to awc",
    )
    return "awc"


def _parse_sunrise_obs_max_age_overrides(raw: str | None) -> dict[str, int]:
    if not raw or not raw.strip():
        return {}

    parsed: dict[str, int] = {}
    for part in raw.strip().split(","):
        entry = part.strip()
        if not entry:
            continue
        station_part, sep, minutes_part = entry.partition(":")
        if sep != ":":
            logger.warning(
                "config.sunrise_obs_max_age_override_malformed",
                entry=entry,
                reason="missing_colon",
            )
            continue
        station = station_part.strip().upper()
        if not station:
            logger.warning(
                "config.sunrise_obs_max_age_override_malformed",
                entry=entry,
                reason="missing_station",
            )
            continue
        try:
            minutes = int(minutes_part.strip())
        except (TypeError, ValueError):
            logger.warning(
                "config.sunrise_obs_max_age_override_malformed",
                entry=entry,
                reason="invalid_minutes",
            )
            continue
        if minutes < 1:
            logger.warning(
                "config.sunrise_obs_max_age_override_malformed",
                entry=entry,
                reason="minutes_below_minimum",
            )
            continue
        parsed[station] = minutes
    return parsed


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    kalshi_api_key: str = ''
    kalshi_private_key_path: str = 'kalshi_private_key.pem'
    mysql_database_url: str = 'mysql+aiomysql://user:pass@localhost:3306/forecastology'
    trading_mode: Literal['PAPER', 'LIVE'] = 'PAPER'
    initial_contract_count: int = 1
    monitor_start_price: int
    buy_trigger_price_low: int
    buy_trigger_price_high: int
    # Optional warm-ticker override for LOW series (e.g. Dallas) that bypass the
    # sunrise/NWS entry gate. 0 (default) means "use buy_trigger_price_low".
    # Parsed by from_env() from BUY_TRIGGER_PRICE_LOW_WARM (dollars -> cents).
    buy_trigger_price_low_warm: int = 0
    spread_monitor_price: int
    falling_knife_decay_minutes: int = 10
    minimum_spread: int
    stop_loss_price_ask: int
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
    # Warm-trade series prefixes (uppercase CSV, e.g. KXLOWTDAL). These series
    # bypass the sunrise/NWS entry gate (catching pre-sunrise moves) but still
    # enforce the AM-low deadline (NWS_LOW_DEADLINE_HOUR) and the local settle
    # gate. They also use their own warm buy trigger price. Parsed by from_env().
    warm_trade_tickers: Annotated[set[str], NoDecode] = set()
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
    # ── KXLOW entry gate mode ────────────────────────────────────────────────
    # ENTRY_GATE_MODE selects how KXLOW* entry timing is gated:
    #   - NWS_WINDOW (default): existing forecast-window behavior (GATE_LOW_*)
    #   - SUNRISE: sunrise-based gate; bypasses only the KXLOW NWS low-window gate
    #
    # SUNRISE_STRATEGY_TIME: open gate at sunrise + N minutes (default 30)
    # SUNRISE_ENTRY_WINDOW_MINUTES: gate closes N minutes after open (default 120)
    # SUNRISE_REQUIRE_TEMP_RISING: DEPRECATED — replaced by SUNRISE_TEMP_RISE_REQUIRED.
    #   Still parsed; triggers a deprecation warning if set.
    # SUNRISE_SOURCE: astral|api (default astral)
    # SUNRISE_REQUIRE_AM_LOW: block entry when NWS forecast min for the day is
    #   scheduled at/after NWS_LOW_DEADLINE_HOUR local (default YES / true)
    # NWS_LOW_DEADLINE_HOUR: local hour (0–23) deadline for the day's forecast low
    #   (default 12 noon)
    # SUNRISE_TEMP_RISE_REQUIRED: °F rise above running baseline required to latch
    #   entry permission (default 1.0; 0 disables the check)
    # SUNRISE_TEMP_BASELINE_MINUTES: minutes before sunrise to start baseline
    #   observation window (default 15)
    # SUNRISE_OBS_MAX_AGE_MINUTES: stale-observation threshold in minutes (default 15)
    # SUNRISE_OBS_MAX_AGE_OVERRIDES: per-station stale threshold overrides as
    #   STATION:MINUTES entries, comma-separated (e.g. KNYC:25,KSEA:20)
    entry_gate_mode: Literal["NWS_WINDOW", "SUNRISE"] = "NWS_WINDOW"
    sunrise_strategy_time: int = 30
    sunrise_entry_window_minutes: int = 120
    sunrise_require_temp_rising: bool = True
    sunrise_source: Literal["astral", "api"] = "astral"
    sunrise_require_am_low: bool = True
    nws_low_deadline_hour: int = 12
    sunrise_temp_rise_required: float = 1.0
    sunrise_temp_baseline_minutes: int = 15
    sunrise_obs_max_age_minutes: int = 15
    sunrise_obs_max_age_overrides: Annotated[dict[str, int], NoDecode] = {}
    sunrise_obs_source: Literal["awc", "nws"] = "awc"
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
    # Minimum seconds between non-bypass stop-loss attempts for the same bracket.
    # Watcher/fast paths always pass bypass_cooldown=True and are unaffected.
    sl_execute_cooldown_seconds: int = 5
    sl_worker_interval_ms: int = 250
    sl_exit_retry_interval_ms: int = 300
    sl_exit_max_attempts: int = 3
    sl_exit_aggressive_offset_ticks: int = 2
    sl_exit_max_slippage: int = 20
    # Maximum seconds to hold a stop-loss trigger before escalation in legacy
    # AGGRESSIVE_LIMIT paths. 0 means no hold window (fire immediately).
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
    # ── Low-ticker PM close ──────────────────────────────────────────────────
    # Automatically evaluates all open KXLOW* positions at each ticker's own
    # local LOW_PM_CLOSE_TIME every day. KXHIGH* positions are not touched.
    #
    # LOW_TICKER_DAILY_CLOSEOUT_ENABLED=true|false  (default: true)
    # LOW_TICKER_CLOSEOUT_ON_LATE_START=true|false  (default: true)
    # LOW_PM_CLOSE_TIME=HH:MM                       (default: 22:00)
    # LOW_PM_CLOSE_AMOUNT=<int cents>               (default: 93)
    # PM_TICKERS_CLOSE=<csv series/ticker prefixes> (default: empty)
    #   When true, if the process starts after the configured closeout time
    #   on a given ET day, perform the closeout once immediately so the
    #   "no Low positions held overnight" intent is honoured.
    #
    # Parsed by from_env().
    low_ticker_daily_closeout_enabled: bool = True
    low_ticker_closeout_time_et: str = "22:00"
    low_ticker_closeout_on_late_start: bool = True
    low_pm_close_time: str = "22:00"
    low_pm_close_amount: int = 93
    pm_tickers_close: Annotated[set[str], NoDecode] = set()
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
    # ── Settlement reconciler ────────────────────────────────────────────────
    # Background loop that backfills TradeOutcome rows by querying Kalshi for
    # settled market results.
    #
    # ENABLE_SETTLEMENT_RECONCILER=true|false  (default: true)
    # RECONCILER_INTERVAL_MINUTES=<int>        (default: 60)
    enable_settlement_reconciler: bool = True
    reconciler_interval_minutes: int = 60
    # ── Resting "disaster" limit-sell backstop ───────────────────────────────
    # Places a resting GTC SELL_YES limit order on the exchange a fixed offset
    # below the normal stop-loss trigger price when the bot holds a YES position.
    # The order executes exchange-side with zero round-trip latency, catching
    # gap-downs that occur while the bot is mid-round-trip or during a WS
    # reconnect.  The reactive SL path cancels this order before submitting its
    # own sell to prevent overselling.
    #
    # SL_BACKSTOP_ENABLED=true|false     (default: false — opt-in)
    # SL_BACKSTOP_OFFSET=<dollars>       (default: 0.05 → 5¢)
    #   Backstop resting price = stop_loss_price_ask - sl_backstop_offset,
    #   floored at 1¢.
    sl_backstop_enabled: bool = False
    # Default 5¢ (stored as int cents; converted from dollars via validator)
    sl_backstop_offset: int = 5
    # ── Resting "take-profit" limit-sell ─────────────────────────────────────
    # Places a resting GTC SELL_YES limit order on the exchange at a fixed HIGH
    # price (default 99¢) right after an entry fills.  If the market trades up
    # to that price, the order fills automatically (zero round-trip latency),
    # locking in the profit.  This is the mirror image of the SL backstop and is
    # cancelled before every reactive sell to prevent overselling.
    #
    # PROFIT_TAKE_SELL_ENABLED=true|false  (default: false — opt-in)
    # PROFIT_TAKE_SELL_PRICE=<dollars>     (default: 0.99 → 99¢)
    #
    # Parsed by from_env().
    profit_take_sell_enabled: bool = False
    # Default 99¢ (stored as int cents; converted from dollars via validator)
    profit_take_sell_price: int = 99
    # ── Intraday checkpoint exits ────────────────────────────────────────────
    # At each configured local-time checkpoint, checks the YES ask for held
    # KXLOW* positions.  If the ask is below the checkpoint's threshold, a
    # capital-preservation exit is initiated after a confirmation re-read
    # (~60 s later) to filter single-tick flickers.
    #
    # INTRADAY_EXIT_ENABLED=true|false  (default: true)
    # INTRADAY_EXIT_SCHEDULE=HH:MM:price,...  (default: "12:00:0.85,15:00:0.90,18:00:0.90")
    #   Comma-separated HH:MM:price entries in the ticker's city-local time.
    #   Price is in dollars; parsed to cents (0.85 → 85).  Malformed entries
    #   are skipped with a warning; all-malformed input falls back to the
    #   default schedule.
    # INTRADAY_EXIT_ENTRY_GRACE_MINUTES=N  (default: 90)
    #   Skip checkpoint exits for positions entered within the last N minutes.
    #   Restored positions without a known entry time are treated as past grace.
    #
    # Parsed by from_env().
    intraday_exit_enabled: bool = True
    intraday_exit_schedule: str = _INTRADAY_EXIT_SCHEDULE_DEFAULT
    intraday_exit_entry_grace_minutes: int = 90
    # ── High-water-mark deterioration exit (opt-in) ──────────────────────────
    # Once a held KXLOW* position's ask has reached HWM_ARM_PRICE after local
    # noon, arms a deterioration trigger: if the ask subsequently drops to
    # HWM_EXIT_PRICE or below, the position is exited (same confirmation-read
    # + limit-at-bid mechanics as checkpoint exits).
    #
    # HWM_EXIT_ENABLED=true|false  (default: false — opt-in)
    # HWM_ARM_PRICE=<dollars>      (default: 0.93 → 93¢)
    # HWM_EXIT_PRICE=<dollars>     (default: 0.88 → 88¢)
    #
    # Parsed by from_env().
    hwm_exit_enabled: bool = False
    hwm_arm_price: int = 93
    hwm_exit_price: int = 88

    # ── Partial-fill chaser ─────────────────────────────────────────────────
    # When an entry order partially fills due to insufficient ask liquidity,
    # automatically work a pegged limit bid for the remaining contracts,
    # adjusting every CHASE_INTERVAL_SECONDS, capped at spread_monitor_price.
    #
    # PARTIAL_FILL_CHASE=yes|no     (default: no — deployed dark until tested)
    # CHASE_INTERVAL_SECONDS=<int>  (default: 60)
    # CHASE_MAX_MINUTES=<int>       (default: 30)
    #
    # Parsed by from_env().
    partial_fill_chase: bool = False
    chase_interval_seconds: int = 60
    chase_max_minutes: int = 30

    @field_validator(
        'buy_trigger_price_low', 'buy_trigger_price_high', 'buy_trigger_price_low_warm', 'spread_monitor_price', 'minimum_spread',
        'stop_loss_price_ask', 'monitor_start_price',
        'eval_price_floor', 'hedge_trigger_price', 'hedge_buy',
        'sl_exit_max_slippage', 'low_ticker_10pm_max_ask', 'sl_backstop_offset',
        'hwm_arm_price', 'hwm_exit_price', 'profit_take_sell_price',
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

    @field_validator('no_trade_tickers', 'warm_trade_tickers', 'pm_tickers_close', mode='before')
    @classmethod
    def parse_upper_csv_set(cls, v):
        if not v:
            return set()
        if isinstance(v, (set, list)):
            return {str(t).strip().upper() for t in v if str(t).strip()}
        return {t.strip().upper() for t in str(v).split(',') if t.strip()}

    @model_validator(mode='before')
    @classmethod
    def map_legacy_stop_loss_field(cls, values):
        """Allow legacy constructor/tests using stop_loss_price keyword."""
        if isinstance(values, dict):
            if "stop_loss_price_ask" not in values and "stop_loss_price" in values:
                values["stop_loss_price_ask"] = values["stop_loss_price"]
            if "buy_trigger_price_low" not in values and "buy_trigger_price" in values:
                values["buy_trigger_price_low"] = values["buy_trigger_price"]
            if "buy_trigger_price_high" not in values and "buy_trigger_price" in values:
                values["buy_trigger_price_high"] = values["buy_trigger_price"]
        return values

    @model_validator(mode='after')
    def normalize_trading_mode(self):
        if self.trading_mode:
            self.trading_mode = self.trading_mode.upper()
        if self.enable_fast_sl_exit is None:
            self.enable_fast_sl_exit = self.trading_mode == "LIVE"
        return self

    @property
    def stop_loss_price(self) -> int:
        """Compatibility alias for existing call sites/tests: ask-side SL threshold."""
        return int(self.stop_loss_price_ask)

    @stop_loss_price.setter
    def stop_loss_price(self, value: int) -> None:
        self.stop_loss_price_ask = int(value)

    @property
    def buy_trigger_price(self) -> int:
        """Compatibility alias for callers that still expect a single trigger."""
        return int(self.buy_trigger_price_high)

    @buy_trigger_price.setter
    def buy_trigger_price(self, value: int) -> None:
        parsed = int(value)
        self.buy_trigger_price_low = parsed
        self.buy_trigger_price_high = parsed

    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Load config from .env file (or environment variables).
        Prices in .env may be in dollar format (e.g. 0.85) or already in cents.
        Field validators convert them to integer cents automatically.
        """
        dry_run_raw = os.getenv("DRY_RUN", "")
        dry_run = dry_run_raw.strip().lower() in {"1", "true", "yes"} if dry_run_raw else False
        max_spread_raw = os.getenv("MAX_SPREAD")
        minimum_spread_legacy_raw = os.getenv("MINIMUM_SPREAD")
        if max_spread_raw and max_spread_raw.strip():
            minimum_spread_raw = max_spread_raw
            if minimum_spread_legacy_raw and minimum_spread_legacy_raw.strip():
                logger.warning(
                    "config.max_spread_precedence",
                    message="Both MAX_SPREAD and deprecated MINIMUM_SPREAD are set; using MAX_SPREAD.",
                )
        elif minimum_spread_legacy_raw and minimum_spread_legacy_raw.strip():
            minimum_spread_raw = minimum_spread_legacy_raw
            logger.warning(
                "config.minimum_spread_deprecated",
                message="MINIMUM_SPREAD is deprecated; use MAX_SPREAD instead.",
            )
        else:
            minimum_spread_raw = minimum_spread_legacy_raw
        low_trades = _parse_trade_toggle(os.getenv("LOW_TRADES"), "LOW_TRADES", default=True)
        high_trades = _parse_trade_toggle(os.getenv("HIGH_TRADES"), "HIGH_TRADES", default=True)
        manage_external_positions = _parse_trade_toggle(
            os.getenv("MANAGE_EXTERNAL_POSITIONS"), "MANAGE_EXTERNAL_POSITIONS", default=False
        )
        enable_local_settle_gate = _parse_trade_toggle(
            os.getenv("ENABLE_LOCAL_SETTLE_GATE"), "ENABLE_LOCAL_SETTLE_GATE", default=True
        )
        no_trade_tickers_raw = os.getenv("NO_TRADE_TICKERS", "")
        warm_trade_tickers_raw = os.getenv("WARM_TRADE_TICKERS", "")
        buy_trigger_price_low_warm = os.getenv("BUY_TRIGGER_PRICE_LOW_WARM", "0")
        default_entry_start_local = os.getenv("DEFAULT_ENTRY_START_LOCAL", "01:00")
        phoenix_entry_start_local = os.getenv("PHOENIX_ENTRY_START_LOCAL", "00:00")
        entry_gate_mode = _parse_entry_gate_mode(os.getenv("ENTRY_GATE_MODE"))
        sunrise_strategy_time = _parse_non_negative_int(
            os.getenv("SUNRISE_STRATEGY_TIME"),
            "SUNRISE_STRATEGY_TIME",
            default=30,
        )
        sunrise_entry_window_minutes = _parse_positive_int(
            os.getenv("SUNRISE_ENTRY_WINDOW_MINUTES"),
            "SUNRISE_ENTRY_WINDOW_MINUTES",
            default=120,
        )
        sunrise_require_temp_rising_raw = os.getenv("SUNRISE_REQUIRE_TEMP_RISING")
        if sunrise_require_temp_rising_raw is not None:
            logger.warning(
                "config.sunrise_require_temp_rising_deprecated",
                message=(
                    "SUNRISE_REQUIRE_TEMP_RISING is deprecated; "
                    "use SUNRISE_TEMP_RISE_REQUIRED (float °F, default 1.0) instead. "
                    "The setting is still applied but will be removed in a future release."
                ),
            )
        sunrise_require_temp_rising = _parse_trade_toggle(
            sunrise_require_temp_rising_raw,
            "SUNRISE_REQUIRE_TEMP_RISING",
            default=True,
        )
        sunrise_source = _parse_sunrise_source(os.getenv("SUNRISE_SOURCE"))
        sunrise_require_am_low = _parse_trade_toggle(
            os.getenv("SUNRISE_REQUIRE_AM_LOW"),
            "SUNRISE_REQUIRE_AM_LOW",
            default=True,
        )
        nws_low_deadline_hour = _parse_non_negative_int(
            os.getenv("NWS_LOW_DEADLINE_HOUR"),
            "NWS_LOW_DEADLINE_HOUR",
            default=12,
        )
        if nws_low_deadline_hour > 23:
            logger.warning(
                "config.nws_low_deadline_hour_out_of_range",
                raw=os.getenv("NWS_LOW_DEADLINE_HOUR"),
                fallback=12,
                message="NWS_LOW_DEADLINE_HOUR must be 0–23; defaulting to 12",
            )
            nws_low_deadline_hour = 12
        sunrise_temp_rise_required_raw = os.getenv("SUNRISE_TEMP_RISE_REQUIRED")
        sunrise_temp_rise_required: float = 1.0
        if sunrise_temp_rise_required_raw is not None and sunrise_temp_rise_required_raw.strip():
            try:
                sunrise_temp_rise_required = float(sunrise_temp_rise_required_raw.strip())
                if sunrise_temp_rise_required < 0:
                    logger.warning(
                        "config.sunrise_temp_rise_required_negative",
                        raw=sunrise_temp_rise_required_raw,
                        fallback=1.0,
                        message="SUNRISE_TEMP_RISE_REQUIRED must be >= 0; defaulting to 1.0",
                    )
                    sunrise_temp_rise_required = 1.0
            except ValueError:
                logger.warning(
                    "config.sunrise_temp_rise_required_invalid",
                    raw=sunrise_temp_rise_required_raw,
                    fallback=1.0,
                    message="Invalid SUNRISE_TEMP_RISE_REQUIRED; defaulting to 1.0",
                )
                sunrise_temp_rise_required = 1.0
        sunrise_temp_baseline_minutes = _parse_non_negative_int(
            os.getenv("SUNRISE_TEMP_BASELINE_MINUTES"),
            "SUNRISE_TEMP_BASELINE_MINUTES",
            default=15,
        )
        sunrise_obs_max_age_minutes = _parse_positive_int(
            os.getenv("SUNRISE_OBS_MAX_AGE_MINUTES"),
            "SUNRISE_OBS_MAX_AGE_MINUTES",
            default=15,
        )
        sunrise_obs_max_age_overrides = _parse_sunrise_obs_max_age_overrides(
            os.getenv("SUNRISE_OBS_MAX_AGE_OVERRIDES")
        )
        sunrise_obs_source = _parse_sunrise_obs_source(os.getenv("SUNRISE_OBS_SOURCE"))
        falling_knife_decay_minutes = _parse_non_negative_int(
            os.getenv("FALLING_KNIFE_DECAY_MINUTES"),
            "FALLING_KNIFE_DECAY_MINUTES",
            default=10,
        )
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
        low_pm_close_time = os.getenv("LOW_PM_CLOSE_TIME", "22:00")
        low_pm_close_amount = _parse_positive_int(
            os.getenv("LOW_PM_CLOSE_AMOUNT"),
            "LOW_PM_CLOSE_AMOUNT",
            default=93,
        )
        pm_tickers_close_raw = os.getenv("PM_TICKERS_CLOSE", "")
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
        enable_settlement_reconciler = _parse_trade_toggle(
            os.getenv("ENABLE_SETTLEMENT_RECONCILER"),
            "ENABLE_SETTLEMENT_RECONCILER",
            default=True,
        )
        reconciler_interval_minutes = _parse_positive_int(
            os.getenv("RECONCILER_INTERVAL_MINUTES"),
            "RECONCILER_INTERVAL_MINUTES",
            default=60,
        )
        intraday_exit_enabled = _parse_trade_toggle(
            os.getenv("INTRADAY_EXIT_ENABLED"),
            "INTRADAY_EXIT_ENABLED",
            default=True,
        )
        intraday_exit_schedule = os.getenv(
            "INTRADAY_EXIT_SCHEDULE", _INTRADAY_EXIT_SCHEDULE_DEFAULT
        )
        intraday_exit_entry_grace_minutes = _parse_positive_int(
            os.getenv("INTRADAY_EXIT_ENTRY_GRACE_MINUTES"),
            "INTRADAY_EXIT_ENTRY_GRACE_MINUTES",
            default=90,
        )
        hwm_exit_enabled = _parse_trade_toggle(
            os.getenv("HWM_EXIT_ENABLED"),
            "HWM_EXIT_ENABLED",
            default=False,
        )
        hwm_arm_price = os.getenv("HWM_ARM_PRICE", "0.93")
        hwm_exit_price = os.getenv("HWM_EXIT_PRICE", "0.88")
        partial_fill_chase = _parse_trade_toggle(
            os.getenv("PARTIAL_FILL_CHASE"),
            "PARTIAL_FILL_CHASE",
            default=False,
        )
        chase_interval_seconds = _parse_positive_int(
            os.getenv("CHASE_INTERVAL_SECONDS"),
            "CHASE_INTERVAL_SECONDS",
            default=60,
        )
        chase_max_minutes = _parse_positive_int(
            os.getenv("CHASE_MAX_MINUTES"),
            "CHASE_MAX_MINUTES",
            default=30,
        )
        return cls(
            dry_run=dry_run,
            low_trades=low_trades,
            high_trades=high_trades,
            no_trade_tickers=no_trade_tickers_raw,
            warm_trade_tickers=warm_trade_tickers_raw,
            manage_external_positions=manage_external_positions,
            enable_local_settle_gate=enable_local_settle_gate,
            default_entry_start_local=default_entry_start_local,
            phoenix_entry_start_local=phoenix_entry_start_local,
            entry_gate_mode=entry_gate_mode,
            sunrise_strategy_time=sunrise_strategy_time,
            sunrise_entry_window_minutes=sunrise_entry_window_minutes,
            sunrise_require_temp_rising=sunrise_require_temp_rising,
            sunrise_source=sunrise_source,
            sunrise_require_am_low=sunrise_require_am_low,
            nws_low_deadline_hour=nws_low_deadline_hour,
            sunrise_temp_rise_required=sunrise_temp_rise_required,
            sunrise_temp_baseline_minutes=sunrise_temp_baseline_minutes,
            sunrise_obs_max_age_minutes=sunrise_obs_max_age_minutes,
            sunrise_obs_max_age_overrides=sunrise_obs_max_age_overrides,
            sunrise_obs_source=sunrise_obs_source,
            falling_knife_decay_minutes=falling_knife_decay_minutes,
            hedge_max_factor=hedge_max_factor,
            initial_contract_count=initial_contract_count,
            buy_trigger_price_low=os.environ["BUY_TRIGGER_PRICE_LOW"],
            buy_trigger_price_high=os.environ["BUY_TRIGGER_PRICE_HIGH"],
            buy_trigger_price_low_warm=buy_trigger_price_low_warm,
            minimum_spread=minimum_spread_raw,
            low_ticker_daily_closeout_enabled=low_ticker_daily_closeout_enabled,
            low_ticker_closeout_time_et=low_ticker_closeout_time_et,
            low_ticker_closeout_on_late_start=low_ticker_closeout_on_late_start,
            low_pm_close_time=low_pm_close_time,
            low_pm_close_amount=low_pm_close_amount,
            pm_tickers_close=pm_tickers_close_raw,
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
            enable_settlement_reconciler=enable_settlement_reconciler,
            reconciler_interval_minutes=reconciler_interval_minutes,
            intraday_exit_enabled=intraday_exit_enabled,
            intraday_exit_schedule=intraday_exit_schedule,
            intraday_exit_entry_grace_minutes=intraday_exit_entry_grace_minutes,
            hwm_exit_enabled=hwm_exit_enabled,
            hwm_arm_price=hwm_arm_price,
            hwm_exit_price=hwm_exit_price,
            partial_fill_chase=partial_fill_chase,
            chase_interval_seconds=chase_interval_seconds,
            chase_max_minutes=chase_max_minutes,
        )
