# Forecastology

A Kalshi prediction market trading bot for US temperature bracket markets.

## Overview

Forecastology monitors and trades Kalshi temperature bracket markets across 20 US cities (high/low temperature brackets). It supports two execution modes: **PAPER** (simulated) and **LIVE** (real money via Kalshi REST API).

## Architecture (post-fix)

The primary runtime is a single always-on WebSocket daemon (`run.py`). It owns all live order decisions — entry, stop-loss, and position management.

| Process | File | Role |
|---|---|---|
| **WS Daemon** | `run.py` | ✅ **Primary executor.** Connects to Kalshi WebSocket, runs `TemperatureStrategy`, owns all entry/exit decisions, and runs WebSocket-driven stop-losses via `StopLossWatcher`. |
| **Scanner** | `scanner.py` | ⚠️ **Legacy / standby only.** Fetches markets via REST and places buy orders. **Exits immediately if `run.py` is running** (lockfile guard). Only useful in environments where `run.py` is not deployed. |
| **Monitor** | `monitor.py` | 🔧 **Reconciliation only.** Reads open positions from DB, reconciles prices via REST, handles optional hedge fallback, and cleans up expired positions. Does **not** execute stop-losses. |

### Critical architecture fixes applied

**Fix 1 — Single execution ownership:** `scanner.py` checks for `run.py`'s
process lockfile (`FORECASTOLOGY_LOCKFILE`, default `/tmp/forecastology.lock`)
at startup.  If the daemon is running the scanner exits immediately — no market
scanning, no orders.  This eliminates split-brain execution between the two
processes.

**Fix 2 — Remove `/dev/shm` dependency:** `scanner.py` and `monitor.py` no
longer read from `/dev/shm/forecastology_state.json` for any trading decision.
`scanner.py` fetches today's markets and prices via the Kalshi REST API.
`monitor.py` fetches per-position prices via REST directly.  Shared-state file
reads have been removed entirely.

**Fix 3 — WebSocket-driven stop-loss as primary path:** `run.py` runs a
`StopLossWatcher` that evaluates stop-loss conditions on every WebSocket ticker
update.  It uses an `exit_in_progress` flag to prevent duplicate exits on
repeated ticks or reconnect bursts.  On startup, `_restore_positions()` loads
all open positions from the DB (and from the Kalshi API in LIVE mode) and
registers them with the watcher so stop-loss protection is active from the
first WebSocket message.

### Deployment / ops checklist

When `run.py` is running as an always-on service:

- ✅ **Keep:** `run.py` systemd service (primary runtime)
- ✅ **Keep:** `monitor.py` systemd timer (reconciliation, price updates, hedge)
- ⛔ **Disable** the `scanner.py` systemd timer — it will exit immediately
  anyway due to the lockfile guard, but disabling it avoids unnecessary process
  spawns
- ⛔ **Do not** write to `/dev/shm/forecastology_state.json` — that file is no
  longer read by any component

If migrating from the old shared-state architecture:

1. Stop all scanner timer jobs.
2. Deploy `run.py` as the always-on daemon.
3. Keep `monitor.py` timer for reconciliation.
4. Remove any cron/systemd jobs that write to `/dev/shm`.

## Prerequisites

- Python 3.11+
- MySQL 8.0+ (or MariaDB)
- Kalshi API credentials (API key + RSA private key)

## Setup

```bash
# 1. Clone the repository
git clone https://github.com/michaeltodmurphy-beep/forecastology.git
cd forecastology

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your actual credentials

# 5. Initialize the database
# Create the database and run:
mysql -u <user> -p <database> < db/init_schema.sql
```

## Configuration

All configuration is via environment variables (`.env` file). See `.env.example` for the full list.

Key variables:

| Variable | Description |
|---|---|
| `KALSHI_API_KEY` | Your Kalshi API key |
| `KALSHI_PRIVATE_KEY_PATH` | Path to your RSA private key PEM file (do NOT commit this file) |
| `MYSQL_DATABASE_URL` | SQLAlchemy-compatible MySQL URL |
| `TRADING_MODE` | `PAPER` (simulated) or `LIVE` (real money) |
| `REST_BASE_URL` | Kalshi REST API base URL |
| `WS_URL` | Kalshi WebSocket URL |
| `STOP_LOSS_PRICE` | WebSocket best ask at or below this (cents) triggers the stop-loss sell (default `0.35`) |
| `ENABLE_FAST_SL_EXIT` | Enable immediate async stop-loss execution path (`true` by default in `LIVE`, `false` in `PAPER`) |
| `SL_EXIT_MODE` | Stop-loss exit strategy: `PANIC_FLATTEN` (default, immediate 1¢ floor sell so Kalshi matches at the best available bid) or `AGGRESSIVE_LIMIT` (opt-in repricing ladder) |
| `SL_EXIT_RETRY_INTERVAL_MS` | Fast stop-loss retry interval in milliseconds (default `300`) |
| `SL_EXIT_MAX_ATTEMPTS` | Max fast stop-loss attempts per trigger (default `3`) |
| `SL_EXIT_AGGRESSIVE_OFFSET_TICKS` | Initial sell-price offset (in ticks/cents) from trigger reference for marketable exits (default `2`) |
| `SL_EXIT_MAX_SLIPPAGE` | Max total slippage (dollar format accepted) allowed for fast stop-loss repricing (default `0.20`) |
| `SL_PANIC_SELL_PRICE` | Panic-flatten floor price in cents (default `1`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_RETRY_MS` | Retry interval (ms) between panic-flatten re-submissions (default `250`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_RETRIES` | Max retry attempts for panic-flatten exit (default `5`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_QUOTE_AGE_MS` | Max age (ms) of a cached YES ask quote for PANIC_FLATTEN pre-submit revalidation (default `30000`). Set to `0` to disable the freshness check. Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `FORECASTOLOGY_LOCKFILE` | Path to the run.py process lockfile (default `/tmp/forecastology.lock`) |
| `HEDGE_MAX_FACTOR` | Total number of allowed buy levels per `(series_ticker, date_prefix)` (counting from 0). Buying is allowed while `stop_loss_count < HEDGE_MAX_FACTOR`; default `3` gives sizes `2/4/8` when `INITIAL_CONTRACT_COUNT=2`. With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3` the max is `12` |
| `HEDGE_TRIGGER_PRICE` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `HEDGE_BUY` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `LOW_TRADES` | `yes` (default) / `no` — set to `no` to disable new **Low** ticker entries (existing positions still managed) |
| `HIGH_TRADES` | `yes` (default) / `no` — set to `no` to disable new **High** ticker entries (existing positions still managed) |
| `ENABLE_LOCAL_SETTLE_GATE` | `true` (default) / `false` — enable city-local-time entry gate; blocks new buys before the city's local rollover time |
| `DEFAULT_ENTRY_START_LOCAL` | Local time (`HH:MM`) at/after which new entries are allowed for all cities except Phoenix (default `01:00`) |
| `PHOENIX_ENTRY_START_LOCAL` | Local time (`HH:MM`) at/after which new entries are allowed for Phoenix (default `00:00`; Phoenix observes Mountain Standard Time year-round, no DST) |

### City-local-time entry settle gate

Kalshi settles temperature markets overnight.  By default (`ENABLE_LOCAL_SETTLE_GATE=true`) the bot will **not** open new positions until the city's local clock has passed the configured rollover threshold:

| City group | Example cities | Threshold |
|---|---|---|
| Eastern Time | Atlanta, Boston, Miami, New York City, Philadelphia, Washington DC | 01:00 ET |
| Central Time | Austin, Chicago, Dallas, Houston, Minneapolis, New Orleans, Oklahoma City, San Antonio | 01:00 CT |
| Mountain Time | Denver | 01:00 MT |
| Mountain Standard Time (no DST) | **Phoenix** | **00:00 MST** |
| Pacific Time | Las Vegas, Los Angeles, San Francisco, Seattle | 01:00 PT |

**Behavior examples**:

- NYC at 12:59 AM ET → new buys **blocked** (logs `entry.blocked_local_settle_gate`)
- NYC at 01:00 AM ET → new buys **allowed**
- Phoenix at 11:59 PM MST → new buys **blocked**
- Phoenix at 00:00 AM MST → new buys **allowed**

**This gate applies to new entry orders only.**  Stop-loss execution, panic exits, sell paths, and all position management continue 24/7 regardless of this setting.

## Running

### WebSocket Daemon (main trading loop)

```bash
python run.py
```

### Scanner (standalone, systemd timer)

```bash
python scanner.py
```

> ⚠️ scanner.py exits immediately if `run.py` is already running.

### Monitor (position manager, systemd timer)

```bash
python monitor.py
```

### Bracket Scanner (diagnostic tool)

```bash
python bracket_scanner.py --min-spread 7 --buy-trigger 85
```

## Trading Strategy

The hedge engine has been removed. The strategy is now a simple entry + stop-loss + martingale recovery system keyed by `(series_ticker, date_prefix)`.

### Phase A — Market Monitoring
All temperature bracket markets are monitored via the WebSocket ticker feed (YES ask price and bid-ask spread).

### Phase B — Entry
**Buy signal**: YES ask price ≥ `BUY_TRIGGER_PRICE` (default 85¢) AND bid-ask spread ≤ `MINIMUM_SPREAD` (default 7¢).

Before each buy, the bot looks up `StopLossLedger(series_ticker, date_prefix)` using the market ticker's parsed `YYMMMDD` segment:

- `count = 0` → buy `INITIAL_CONTRACT_COUNT`
- `count = 1` → buy `INITIAL_CONTRACT_COUNT * 2`
- `count = 2` → buy `INITIAL_CONTRACT_COUNT * 4`
- in general: `quantity = INITIAL_CONTRACT_COUNT * 2**count`

`HEDGE_MAX_FACTOR` is the **total number of allowed buy levels** (counting from 0).  Buying is allowed while `count < HEDGE_MAX_FACTOR`; once `count >= HEDGE_MAX_FACTOR`, the series is done for that day and the bot logs `hedge.cap_blocked` + `phase.b.recovery_cap_reached`.

With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the exact cap boundary is:

- `count=0` → buy `2`
- `count=1` → buy `4`
- `count=2` → buy `8`
- `count>=3` → no more buys for that `(series, day)` — max allowed qty = `2 * 2^(3-1) = 8`

With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3` (the production config that triggered this hotfix):

- `count=0` → buy `3`
- `count=1` → buy `6`
- `count=2` → buy `12`
- `count>=3` → no more buys — max allowed qty = `3 * 2^(3-1) = 12`

The general formula: `max_allowed_qty = INITIAL_CONTRACT_COUNT * 2 ** (HEDGE_MAX_FACTOR - 1)`.

High and Low markets are naturally independent because they have different `series_ticker` values (for example `KXHIGHTBOS` vs `KXLOWTBOS`).

### Phase C — Position Management (Stop-Loss)
Stop-loss is driven by the **WebSocket `StopLossWatcher`** inside `run.py`:

- On every `ticker` WebSocket update, `yes_ask` is passed to `StopLossWatcher.on_market_update()`.
- If `yes_ask ≤ STOP_LOSS_PRICE`, the exit handler fires immediately.
- An `exit_in_progress` guard prevents duplicate exits on repeated ticks or reconnect bursts.
- On failure, the guard is reset so the next tick can retry.
- Startup reconciliation (`_restore_positions`) registers all open positions with the watcher so coverage begins from the first WebSocket message.

The `_evaluate_held_positions` loop in the strategy (runs ~every 1s) provides a secondary safety net for cases where the shared WebSocket price feed goes stale, reconnects, or is unavailable for extended periods by falling back to REST quotes for held positions.

### StopLossLedger
`stop_loss_ledger` stores the persistent per-day martingale counter:

- key: `(series_ticker, date_prefix)`
- value: `stop_loss_count`
- date key comes from the market ticker itself, not the current clock

This means any bracket in the same series on the same day inherits the same recovery size. For example, a stop-loss on `KXLOWTBOS-26JUN23-B65.5` makes `KXLOWTBOS-26JUN23-T68` rebuy at the doubled size.

### Worst-Case Per-Series Daily Spend
This is explicitly a martingale. With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the maximum daily sequence for one series is **three** buys at `2 + 4 + 8 = 14` contracts total before the strategy stops buying that series for the day. With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3`, the sequence is `3 + 6 + 12 = 21` contracts (max single order = **12**).

### Watchlist Evaluation Floor (`EVAL_PRICE_FLOOR`)
Brackets priced at or below the floor are skipped early in `_evaluate_watchlist` without emitting a `phase.b.below_trigger` log. Brackets above the floor but below `BUY_TRIGGER_PRICE` still emit `phase.b.below_trigger`.

## Security

- **Never commit your private key** (`*.pem` is in `.gitignore`)
- **Never commit your `.env` file** (`.env` is in `.gitignore`)
- If you accidentally commit credentials, rotate them immediately at Kalshi

## Database Schema

See `db/init_schema.sql` for the full schema. Key tables:

- `positions` — open positions
- `executed_trades` — trade history
- `stop_loss_ledger` — per-(series, day) martingale stop-loss counters
- `station_forecasts` — NWS daily high/low temperature forecast times per station

## Prerequisites

- Python 3.11+
- MySQL 8.0+ (or MariaDB)
- Kalshi API credentials (API key + RSA private key)

## Setup

```bash
# 1. Clone the repository
git clone https://github.com/michaeltodmurphy-beep/forecastology.git
cd forecastology

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your actual credentials

# 5. Initialize the database
# Create the database and run:
mysql -u <user> -p <database> < db/init_schema.sql
```

## Configuration

All configuration is via environment variables (`.env` file). See `.env.example` for the full list.

Key variables:

| Variable | Description |
|---|---|
| `KALSHI_API_KEY` | Your Kalshi API key |
| `KALSHI_PRIVATE_KEY_PATH` | Path to your RSA private key PEM file (do NOT commit this file) |
| `MYSQL_DATABASE_URL` | SQLAlchemy-compatible MySQL URL |
| `TRADING_MODE` | `PAPER` (simulated) or `LIVE` (real money) |
| `REST_BASE_URL` | Kalshi REST API base URL |
| `WS_URL` | Kalshi WebSocket URL |
| `STOP_LOSS_PRICE` | WebSocket best ask at or below this (cents) triggers the stop-loss sell (default `0.35`) |
| `ENABLE_FAST_SL_EXIT` | Enable immediate async stop-loss execution path (`true` by default in `LIVE`, `false` in `PAPER`) |
| `SL_EXIT_MODE` | Stop-loss exit strategy: `PANIC_FLATTEN` (default, immediate 1¢ floor sell so Kalshi matches at the best available bid) or `AGGRESSIVE_LIMIT` (opt-in repricing ladder) |
| `SL_EXIT_RETRY_INTERVAL_MS` | Fast stop-loss retry interval in milliseconds (default `300`) |
| `SL_EXIT_MAX_ATTEMPTS` | Max fast stop-loss attempts per trigger (default `3`) |
| `SL_EXIT_AGGRESSIVE_OFFSET_TICKS` | Initial sell-price offset (in ticks/cents) from trigger reference for marketable exits (default `2`) |
| `SL_EXIT_MAX_SLIPPAGE` | Max total slippage (dollar format accepted) allowed for fast stop-loss repricing (default `0.20`) |
| `SL_SPREAD_HOLD_MAX_SECONDS` | Max seconds to hold an `AGGRESSIVE_LIMIT` stop-loss trigger when spread is wide/one-sided before forcing exit anyway (default `120`; set `0` to fire immediately even on wide spread) |
| `SL_PANIC_SELL_PRICE` | Panic-flatten floor price in cents (default `1`). Sell placed at this price so Kalshi matches at best bid. Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_RETRY_MS` | Retry interval (ms) between panic-flatten re-submissions (default `250`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_RETRIES` | Max retry attempts for panic-flatten exit (default `5`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_QUOTE_AGE_MS` | Max age (ms) of a cached YES ask quote for pre-submit revalidation (default `30000`). Set to `0` to disable. Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `MANAGE_EXTERNAL_POSITIONS` | Ownership safety switch. Default `false`: only APP-owned quantity is managed; manual/external quantity is never sold by stop-loss/exit logic. Set `true` only for legacy/emergency aggregate-position behavior. |
| `HEDGE_MAX_FACTOR` | Total number of allowed buy levels per `(series_ticker, date_prefix)` (counting from 0). Buying is allowed while `stop_loss_count < HEDGE_MAX_FACTOR`; default `3` gives sizes `2/4/8` when `INITIAL_CONTRACT_COUNT=2`. With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3` the max is `12` |
| `HEDGE_TRIGGER_PRICE` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `HEDGE_BUY` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |

### Trade ownership model (APP vs manual)

- Every app-submitted order uses a client order id with `APP_` prefix (`APP_<uuid>`).
- Position ownership is partitioned per ticker:
  - `app_owned`: quantity attributable to app-tracked holdings.
  - `external_manual`: quantity not attributable to app-owned tracking.
- Default (`MANAGE_EXTERNAL_POSITIONS=false`): stop-loss/exit logic only acts on `app_owned` quantity and never sells external/manual quantity.
- Mixed positions are capped on exit to app-owned qty only; if app-owned qty is zero, exits are skipped (`exit.skipped_no_app_qty`).

## Running

### WebSocket Daemon (main trading loop)

```bash
python run.py
```

### Scanner (standalone, systemd timer)

```bash
python scanner.py
```

### Monitor (position manager, systemd timer)

```bash
python monitor.py
```

### Bracket Scanner (diagnostic tool)

```bash
python bracket_scanner.py --min-spread 7 --buy-trigger 85
```

## Trading Strategy

The hedge engine has been removed. The strategy is now a simple entry + stop-loss + martingale recovery system keyed by `(series_ticker, date_prefix)`.

### Phase A — Market Monitoring
All temperature bracket markets are monitored via the WebSocket ticker feed (YES ask price and bid-ask spread).

### Phase B — Entry
**Buy signal**: YES ask price ≥ `BUY_TRIGGER_PRICE` (default 85¢) AND bid-ask spread ≤ `MINIMUM_SPREAD` (default 7¢).

Before each buy, the bot looks up `StopLossLedger(series_ticker, date_prefix)` using the market ticker's parsed `YYMMMDD` segment:

- `count = 0` → buy `INITIAL_CONTRACT_COUNT`
- `count = 1` → buy `INITIAL_CONTRACT_COUNT * 2`
- `count = 2` → buy `INITIAL_CONTRACT_COUNT * 4`
- in general: `quantity = INITIAL_CONTRACT_COUNT * 2**count`

`HEDGE_MAX_FACTOR` is the **total number of allowed buy levels** (counting from 0).  Buying is allowed while `count < HEDGE_MAX_FACTOR`; once `count >= HEDGE_MAX_FACTOR`, the series is done for that day and the bot logs `hedge.cap_blocked` + `phase.b.recovery_cap_reached`.

With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the exact cap boundary is:

- `count=0` → buy `2`
- `count=1` → buy `4`
- `count=2` → buy `8`
- `count>=3` → no more buys for that `(series, day)` — max allowed qty = `2 * 2^(3-1) = 8`

With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3` (the production config that triggered the hotfix):

- `count=0` → buy `3`
- `count=1` → buy `6`
- `count=2` → buy `12`
- `count>=3` → no more buys — max allowed qty = `3 * 2^(3-1) = 12`

The general formula: `max_allowed_qty = INITIAL_CONTRACT_COUNT * 2 ** (HEDGE_MAX_FACTOR - 1)`.

High and Low markets are naturally independent because they have different `series_ticker` values (for example `KXHIGHTBOS` vs `KXLOWTBOS`).

### Phase C — Position Management
When stop-loss trigger conditions are met, the strategy dispatches an immediate per-ticker async stop-loss worker so one ticker's exit path does not block others.

#### Stop-loss exit modes (`SL_EXIT_MODE`)

**`PANIC_FLATTEN` (default)** — immediate floor sell:

- **Trigger condition (strict ASK-only):** `trigger_met = (best_ask_yes is not None) AND (best_ask_yes <= STOP_LOSS_PRICE)`. Bid price, last-trade price, midpoint, and zero-bid-collapse paths are **not** used to trigger PANIC_FLATTEN.
- On trigger, immediately submits a sell at `SL_PANIC_SELL_PRICE` (default 1¢) — a floor-priced order that is immediately marketable, so Kalshi matches it at the **best available bid**
- no slow repricing ladder before the first submit: fill speed is prioritised over exit price and avoids chasing the book down
- **Pre-submit revalidation:** immediately before placing each panic order, the latest cached YES ask is re-checked against `STOP_LOSS_PRICE`. If the ask has risen back above the stop, the submit is **canceled** and the reason is logged as `sl.panic_revalidation_aborted` (`reason="ask_above_stop"`). If the quote is missing or stale (older than `SL_PANIC_MAX_QUOTE_AGE_MS`), the submit proceeds in **degraded mode** (`sl.panic_revalidation_degraded`) — failing to exit is worse than a marginal false positive.
- if unfilled or partially filled, retries every `SL_PANIC_RETRY_MS` up to `SL_PANIC_MAX_RETRIES` attempts, each at the same floor price (with revalidation before each attempt); transient submit errors are also retried with per-attempt logging (`sl.panic_submit_error`)
- stop-loss completion is only treated as terminal after `get_positions()` confirms the remaining app-owned quantity is `0`; exhausted attempts emit `sl.exit_exhausted_unprotected` and re-arm protection instead of silently giving up
- per-ticker task idempotency: repeated triggers while an exit is in-flight are silently suppressed
- structured logs: `sl.panic_triggered`, `sl.panic_revalidation`, `sl.panic_revalidation_degraded`, `sl.panic_revalidation_aborted`, `sl.panic_submit`, `sl.panic_retry`, `sl.panic_submit_error`, `sl.panic_filled` / `sl.panic_failed`
- trade-off: fill speed is prioritised over exit price — you may receive less than 1¢; the intent is to get flat immediately
- units: `STOP_LOSS_PRICE` and the cached YES ask are both stored in **cents** (integer); dollar-format `.env` values (e.g. `STOP_LOSS_PRICE=0.48`) are automatically converted to 48¢ by AppConfig.

**`AGGRESSIVE_LIMIT`** — opt-in repricing ladder:

- aggressive marketable sell relative to trigger price using `SL_EXIT_AGGRESSIVE_OFFSET_TICKS`
- bounded repricing capped by `SL_EXIT_MAX_SLIPPAGE`
- rapid per-ticker retries at `SL_EXIT_RETRY_INTERVAL_MS` up to `SL_EXIT_MAX_ATTEMPTS`
- structured logs: `sl.trigger_detected`, `sl.exit_submit_start`, `sl.exit_submitted`, `sl.exit_fill_observed` / `sl.exit_failed`

Conservative mode remains available by setting `ENABLE_FAST_SL_EXIT=false` (default for PAPER).

Recommended LIVE defaults (`PANIC_FLATTEN`, repository default):

- `ENABLE_FAST_SL_EXIT=true`
- `SL_EXIT_MODE=PANIC_FLATTEN`
- `SL_PANIC_SELL_PRICE=1`
- `SL_PANIC_RETRY_MS=250`
- `SL_PANIC_MAX_RETRIES=5`

Recommended LIVE defaults (`AGGRESSIVE_LIMIT`, backward-compatible):

- `ENABLE_FAST_SL_EXIT=true`
- `SL_EXIT_MODE=AGGRESSIVE_LIMIT`
- `SL_EXIT_RETRY_INTERVAL_MS=250-300`
- `SL_EXIT_MAX_ATTEMPTS=3`
- `SL_EXIT_AGGRESSIVE_OFFSET_TICKS=2`
- `SL_EXIT_MAX_SLIPPAGE=0.20`

### StopLossLedger
`stop_loss_ledger` stores the persistent per-day martingale counter:

- key: `(series_ticker, date_prefix)`
- value: `stop_loss_count`
- date key comes from the market ticker itself, not the current clock

This means any bracket in the same series on the same day inherits the same recovery size. For example, a stop-loss on `KXLOWTBOS-26JUN23-B65.5` makes `KXLOWTBOS-26JUN23-T68` rebuy at the doubled size.

### Worst-Case Per-Series Daily Spend
This is explicitly a martingale. With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the maximum daily sequence for one series is **three** buys at `2 + 4 + 8 = 14` contracts total before the strategy stops buying that series for the day. With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3`, the sequence is `3 + 6 + 12 = 21` contracts (max single order = **12**).

### Watchlist Evaluation Floor (`EVAL_PRICE_FLOOR`)
Brackets priced at or below the floor are skipped early in `_evaluate_watchlist` without emitting a `phase.b.below_trigger` log. Brackets above the floor but below `BUY_TRIGGER_PRICE` still emit `phase.b.below_trigger`.

## Security

- **Never commit your private key** (`*.pem` is in `.gitignore`)
- **Never commit your `.env` file** (`.env` is in `.gitignore`)
- If you accidentally commit credentials, rotate them immediately at Kalshi

## Database Schema

See `db/init_schema.sql` for the full schema. Key tables:

- `positions` — open positions
- `executed_trades` — trade history
- `stop_loss_ledger` — per-(series, day) martingale stop-loss counters
- `station_forecasts` — NWS daily high/low temperature forecast times per station

## NWS Forecast Backend

The `nws/` package provides a production-ready temperature forecast integration with the National Weather Service API. It runs as a background service alongside the main trading loop, keeping the daily high/low forecast times up to date in the database.

### Architecture

```
nws/
├── __init__.py       # Package
├── config.py         # Environment variable loading
├── stations.py       # ICAO station codes for 20 monitored cities
├── client.py         # NWS API client (station → grid → hourly forecast)
├── db.py             # Synchronous SQLAlchemy engine + session context manager
├── gate.py           # is_trading_gate_open() trading gate function
└── scheduler.py      # APScheduler background updater + bootstrap()
```

### NWS Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NWS_USER_AGENT` | _(required)_ | Custom User-Agent for the NWS API (required by their ToS). Example: `forecastology/1.0 (you@yourdomain.com)` |
| `MYSQL_URL` | _(falls back to `MYSQL_DATABASE_URL`)_ | Sync `pymysql` database URL for the NWS scheduler. If unset, `MYSQL_DATABASE_URL` is used with the driver converted to `pymysql`. |
| `HIGH_LOW_UPDATE` | `60` | How often (minutes) to refresh NWS forecast data in the background |
| `GATE_LOW_BEFORE` | `120` | Minutes before the forecasted low to open the trading gate |
| `GATE_LOW_AFTER` | `45` | Minutes after the forecasted low to close the trading gate |
| `GATE_HIGH_BEFORE` | `60` | Minutes before the forecasted high to open the trading gate |
| `GATE_HIGH_AFTER` | `30` | Minutes after the forecasted high to close the trading gate |

### NWS Usage

**Bootstrap at application startup** (call once, non-blocking):

```python
from nws.scheduler import bootstrap, shutdown

# In your main entry point, before the trading loop:
bootstrap()   # initialises DB + immediate update + starts background scheduler

# On clean exit:
shutdown()
```

**Check if the trading gate is open:**

```python
from datetime import datetime, timezone
from nws.gate import is_trading_gate_open

allowed = is_trading_gate_open("KATL", datetime.now(timezone.utc))
```

**Standalone updater run** (for manual refresh or cron):

```python
from nws.scheduler import run_forecast_update_job

run_forecast_update_job()
```

### NWS API Flow

1. `GET /stations/{ICAO}` → lat/lon coordinates (cached per process)
2. `GET /points/{lat},{lon}` → `forecastHourly` URL **and `timeZone`** (IANA name, cached per process)
3. `GET {forecastHourly}` → hourly temperature periods
4. Parse periods to find the **station-local-day** high and low: each period's
   `startTime` is converted to the station's IANA timezone and filtered by the
   station's local calendar date. This ensures UTC day boundaries never split a
   station's effective trading day (e.g. a US/Pacific station at 01:00 UTC is
   still on the previous local calendar day). The resulting high/low times are
   stored as UTC.

### station_forecasts Table

One row per `(station_code, forecast_date_utc)`:

| Column | Type | Description |
|---|---|---|
| `station_code` | VARCHAR(8) | NWS ICAO code, e.g. `KATL` |
| `forecast_date_utc` | DATETIME | UTC midnight of the station's **local** forecast day |
| `high_time_utc` | DATETIME | UTC time of the local-day daily high temperature |
| `low_time_utc` | DATETIME | UTC time of the local-day daily low temperature |
| `updated_at` | DATETIME | Last refresh timestamp |

Unique index on `(station_code, forecast_date_utc)` with upsert semantics.
`forecast_date_utc` is UTC midnight of the station's local calendar today, so
it may differ from the UTC calendar date when the updater runs near midnight UTC.
