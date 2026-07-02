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
| `SL_EXIT_MODE` | Stop-loss exit strategy: `AGGRESSIVE_LIMIT` (default, repricing ladder) or `PANIC_FLATTEN` (immediate 1¢ floor sell) |
| `SL_EXIT_RETRY_INTERVAL_MS` | Fast stop-loss retry interval in milliseconds (default `300`) |
| `SL_EXIT_MAX_ATTEMPTS` | Max fast stop-loss attempts per trigger (default `3`) |
| `SL_EXIT_AGGRESSIVE_OFFSET_TICKS` | Initial sell-price offset (in ticks/cents) from trigger reference for marketable exits (default `2`) |
| `SL_EXIT_MAX_SLIPPAGE` | Max total slippage (dollar format accepted) allowed for fast stop-loss repricing (default `0.20`) |
| `SL_PANIC_SELL_PRICE` | Panic-flatten floor price in cents (default `1`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_RETRY_MS` | Retry interval (ms) between panic-flatten re-submissions (default `250`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_RETRIES` | Max retry attempts for panic-flatten exit (default `5`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_QUOTE_AGE_MS` | Max age (ms) of a cached YES ask quote for PANIC_FLATTEN pre-submit revalidation (default `30000`). Set to `0` to disable the freshness check. Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `FORECASTOLOGY_LOCKFILE` | Path to the run.py process lockfile (default `/tmp/forecastology.lock`) |
| `HEDGE_MAX_FACTOR` | Repurposed as the maximum number of martingale doublings allowed per `(series_ticker, date_prefix)`; default `3` gives sizes `2/4/8/16` when `INITIAL_CONTRACT_COUNT=2` |
| `HEDGE_TRIGGER_PRICE` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `HEDGE_BUY` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |

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
- `count = 3` → buy `INITIAL_CONTRACT_COUNT * 8`
- in general: `quantity = INITIAL_CONTRACT_COUNT * 2**count`

`HEDGE_MAX_FACTOR` is now the **maximum number of doublings**. Buying is allowed while `count <= HEDGE_MAX_FACTOR`; once `count > HEDGE_MAX_FACTOR`, the series is done for that day and the bot logs `phase.b.recovery_cap_reached`.

With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the exact cap boundary is:

- `count=0` → buy `2`
- `count=1` → buy `4`
- `count=2` → buy `8`
- `count=3` → buy `16`
- `count>=4` → no more buys for that `(series, day)`

High and Low markets are naturally independent because they have different `series_ticker` values (for example `KXHIGHTBOS` vs `KXLOWTBOS`).

### Phase C — Position Management (Stop-Loss)
Stop-loss is driven by the **WebSocket `StopLossWatcher`** inside `run.py`:

- On every `ticker` WebSocket update, `yes_ask` is passed to `StopLossWatcher.on_market_update()`.
- If `yes_ask ≤ STOP_LOSS_PRICE`, the exit handler fires immediately.
- An `exit_in_progress` guard prevents duplicate exits on repeated ticks or reconnect bursts.
- On failure, the guard is reset so the next tick can retry.
- Startup reconciliation (`_restore_positions`) registers all open positions with the watcher so coverage begins from the first WebSocket message.

The `_evaluate_held_positions` loop in the strategy (runs ~every 1s) provides a secondary safety net for cases where the WebSocket price feed is unavailable for extended periods.

### StopLossLedger
`stop_loss_ledger` stores the persistent per-day martingale counter:

- key: `(series_ticker, date_prefix)`
- value: `stop_loss_count`
- date key comes from the market ticker itself, not the current clock

This means any bracket in the same series on the same day inherits the same recovery size. For example, a stop-loss on `KXLOWTBOS-26JUN23-B65.5` makes `KXLOWTBOS-26JUN23-T68` rebuy at the doubled size.

### Worst-Case Per-Series Daily Spend
This is explicitly a martingale. With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the maximum daily sequence for one series is four buys at `2 + 4 + 8 + 16 = 30` contracts total before the strategy stops buying that series for the day.

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
| `SL_EXIT_MODE` | Stop-loss exit strategy: `AGGRESSIVE_LIMIT` (default, repricing ladder) or `PANIC_FLATTEN` (immediate 1¢ floor sell) |
| `SL_EXIT_RETRY_INTERVAL_MS` | Fast stop-loss retry interval in milliseconds (default `300`) |
| `SL_EXIT_MAX_ATTEMPTS` | Max fast stop-loss attempts per trigger (default `3`) |
| `SL_EXIT_AGGRESSIVE_OFFSET_TICKS` | Initial sell-price offset (in ticks/cents) from trigger reference for marketable exits (default `2`) |
| `SL_EXIT_MAX_SLIPPAGE` | Max total slippage (dollar format accepted) allowed for fast stop-loss repricing (default `0.20`) |
| `SL_PANIC_SELL_PRICE` | Panic-flatten floor price in cents (default `1`). Sell placed at this price so Kalshi matches at best bid. Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_RETRY_MS` | Retry interval (ms) between panic-flatten re-submissions (default `250`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_RETRIES` | Max retry attempts for panic-flatten exit (default `5`). Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `SL_PANIC_MAX_QUOTE_AGE_MS` | Max age (ms) of a cached YES ask quote for pre-submit revalidation (default `30000`). Set to `0` to disable. Only used when `SL_EXIT_MODE=PANIC_FLATTEN` |
| `HEDGE_MAX_FACTOR` | Repurposed as the maximum number of martingale doublings allowed per `(series_ticker, date_prefix)`; default `3` gives sizes `2/4/8/16` when `INITIAL_CONTRACT_COUNT=2` |
| `HEDGE_TRIGGER_PRICE` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `HEDGE_BUY` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |

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
- `count = 3` → buy `INITIAL_CONTRACT_COUNT * 8`
- in general: `quantity = INITIAL_CONTRACT_COUNT * 2**count`

`HEDGE_MAX_FACTOR` is now the **maximum number of doublings**. Buying is allowed while `count <= HEDGE_MAX_FACTOR`; once `count > HEDGE_MAX_FACTOR`, the series is done for that day and the bot logs `phase.b.recovery_cap_reached`.

With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the exact cap boundary is:

- `count=0` → buy `2`
- `count=1` → buy `4`
- `count=2` → buy `8`
- `count=3` → buy `16`
- `count>=4` → no more buys for that `(series, day)`

High and Low markets are naturally independent because they have different `series_ticker` values (for example `KXHIGHTBOS` vs `KXLOWTBOS`).

### Phase C — Position Management
When stop-loss trigger conditions are met, the strategy dispatches an immediate per-ticker async stop-loss worker so one ticker's exit path does not block others.

#### Stop-loss exit modes (`SL_EXIT_MODE`)

**`AGGRESSIVE_LIMIT` (default)** — repricing ladder:

- aggressive marketable sell relative to trigger price using `SL_EXIT_AGGRESSIVE_OFFSET_TICKS`
- bounded repricing capped by `SL_EXIT_MAX_SLIPPAGE`
- rapid per-ticker retries at `SL_EXIT_RETRY_INTERVAL_MS` up to `SL_EXIT_MAX_ATTEMPTS`
- structured logs: `sl.trigger_detected`, `sl.exit_submit_start`, `sl.exit_submitted`, `sl.exit_fill_observed` / `sl.exit_failed`

**`PANIC_FLATTEN`** — immediate floor sell (recommended for LIVE):

- **Trigger condition (strict ASK-only):** `trigger_met = (best_ask_yes is not None) AND (best_ask_yes <= STOP_LOSS_PRICE)`. Bid price, last-trade price, midpoint, and zero-bid-collapse paths are **not** used to trigger PANIC_FLATTEN.
- On trigger, immediately submits a sell at `SL_PANIC_SELL_PRICE` (default 1¢) — a floor-priced order that Kalshi fills at the best available bid
- no slow repricing ladder before the first submit: fill speed is prioritised over exit price
- **Pre-submit revalidation:** immediately before placing each panic order, the latest cached YES ask is re-checked against `STOP_LOSS_PRICE`. If the ask has risen back above the stop, the submit is **canceled** and the reason is logged as `sl.panic_revalidation_aborted` (`reason="ask_above_stop"`). If the quote is missing or stale (older than `SL_PANIC_MAX_QUOTE_AGE_MS`), the submit proceeds in **degraded mode** (`sl.panic_revalidation_degraded`) — failing to exit is worse than a marginal false positive.
- if unfilled or partially filled, retries every `SL_PANIC_RETRY_MS` up to `SL_PANIC_MAX_RETRIES` attempts, each at the same floor price (with revalidation before each attempt); transient submit errors are also retried with per-attempt logging (`sl.panic_submit_error`)
- per-ticker task idempotency: repeated triggers while an exit is in-flight are silently suppressed
- structured logs: `sl.panic_triggered`, `sl.panic_revalidation`, `sl.panic_revalidation_degraded`, `sl.panic_revalidation_aborted`, `sl.panic_submit`, `sl.panic_retry`, `sl.panic_submit_error`, `sl.panic_filled` / `sl.panic_failed`
- trade-off: fill speed is prioritised over exit price — you may receive less than 1¢; the intent is to get flat immediately
- units: `STOP_LOSS_PRICE` and the cached YES ask are both stored in **cents** (integer); dollar-format `.env` values (e.g. `STOP_LOSS_PRICE=0.48`) are automatically converted to 48¢ by AppConfig.

Conservative mode remains available by setting `ENABLE_FAST_SL_EXIT=false` (default for PAPER).

Recommended LIVE defaults (`PANIC_FLATTEN`):

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
This is explicitly a martingale. With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, the maximum daily sequence for one series is four buys at `2 + 4 + 8 + 16 = 30` contracts total before the strategy stops buying that series for the day.

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
