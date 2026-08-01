# Forecastology

A Kalshi prediction market trading bot for US temperature bracket markets.

## Overview

Forecastology monitors and trades Kalshi temperature bracket markets across 20 US cities (high/low temperature brackets). It supports two execution modes: **PAPER** (simulated) and **LIVE** (real money via Kalshi REST API).

## Architecture (post-fix)

The primary runtime is a single always-on WebSocket daemon (`run.py`). It owns all live order decisions — entry, stop-loss, and position management.

| Process | File | Role |
|---|---|---|
| **WS Daemon** | `run.py` | ✅ **Primary executor.** Connects to Kalshi WebSocket, runs `TemperatureStrategy`, owns all entry/exit decisions, and runs WebSocket-driven stop-losses via `StopLossWatcher`. |
| **Scanner** | `scanner.py` | ⚠️ **Legacy / standby only.** Fetches markets via REST and places buy orders. **Exits immediately if `run.py` is running** (lockfile guard). Buy orders now route through the shared capped executor (V2 payload, `max_buy_qty` enforced). Only useful in environments where `run.py` is not deployed. |
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

**Fix 4 — Restored/adopted positions are fully price-seeded:** On startup,
`_restore_positions()` immediately fetches the current REST price for every
restored or adopted position and seeds `TickerCache` before the first WebSocket
tick arrives.  This ensures the stop-loss watcher has a live price from the
moment the position is active, even for positions from series that were never
in the active watchlist (e.g. positions inherited from a prior bot instance).

**Fix 5 — Unprotected-position escalation:** When a held position goes blind
(no price feed from either WebSocket or REST) for more than
`SL_UNPROTECTED_MAX_BLIND_CYCLES` consecutive SL evaluation cycles, the bot
now escalates to a `logger.critical` alert (`phase.c.unprotected_escalation`)
instead of silently cycling `phase.c.held_position_unprotected` forever.  With
`SL_FLATTEN_UNPROTECTED_ON_BLIND=true` (default `false`), the bot also
attempts a protective panic-flatten of the app-owned quantity — only
`app_owned_qty` is sold; `MANAGE_EXTERNAL_POSITIONS` semantics are always
respected.

**Fix 6 — Scanner routes through shared capped executor:** `scanner.py`'s
`buy_market` now calls `create_executor(...).buy_yes(...)` instead of posting
a raw `httpx` request.  This single change fixes the V2 payload (eliminating
the HTTP 410 rejection from the V1-era format) and ensures the per-market
martingale cap (`max_buy_qty = INITIAL_CONTRACT_COUNT * 2**(HEDGE_MAX_FACTOR-1)`)
is enforced at the executor level — the same limit that applies to the
state-machine buy path.

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
| `INSTANCE_LOCK_ENABLED` | `true` (default) / `false` — enables startup single-instance guard |
| `INSTANCE_LOCK_FILE` | Base path for the run.py startup lock file (default `/tmp/forecastology.lock`). The runtime appends an account hash so two bots for the same account conflict while distinct accounts can run separately. |
| `INSTANCE_ID` | Optional stable lock identity (if unset, derived from `KALSHI_API_KEY_ID`; only a short hash is logged) |
| `LOG_FILE` | Application log file path for built-in rotating file logging (default `logs/run.log`) |
| `LOG_MAX_BYTES` | Max log file size in bytes before rollover (default `104857600` = 100 MB) |
| `LOG_BACKUP_COUNT` | Number of rotated log files to keep (default `10`) |
| `HEDGE_MAX_FACTOR` | Total number of allowed buy levels per `(series_ticker, date_prefix)` (counting from 0). Buying is allowed while `stop_loss_count < HEDGE_MAX_FACTOR`; default `3` gives sizes `2/4/8` when `INITIAL_CONTRACT_COUNT=2`. With `INITIAL_CONTRACT_COUNT=3` and `HEDGE_MAX_FACTOR=3` the max is `12` |
| `HEDGE_TRIGGER_PRICE` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `HEDGE_BUY` | Deprecated and ignored by the trading logic; retained only so older `.env` files still load |
| `LOW_TRADES` | `yes` (default) / `no` — set to `no` to disable new **Low** ticker entries (existing positions still managed) |
| `HIGH_TRADES` | `yes` (default) / `no` — set to `no` to disable new **High** ticker entries (existing positions still managed) |
| `ENABLE_LOCAL_SETTLE_GATE` | `true` (default) / `false` — enable city-local-time entry gate; blocks new `KXLOW*` buys before the city's local rollover time (RESUME half of the STOP/RESUME rule). `KXHIGH*` tickers are never affected. |
| `DEFAULT_ENTRY_START_LOCAL` | Local time (`HH:MM`) at/after which new entries are allowed for all cities except Phoenix (default `01:00`) |
| `PHOENIX_ENTRY_START_LOCAL` | Local time (`HH:MM`) at/after which new entries are allowed for Phoenix (default `00:00`; Phoenix observes Mountain Standard Time year-round, no DST) |
| `LOW_TICKER_DAILY_CLOSEOUT_ENABLED` | `true` (default) / `false` — automatically flatten all open `KXLOW*` positions at `LOW_TICKER_CLOSEOUT_TIME_ET` every day (Eastern, DST-aware). `KXHIGH*` positions are never touched. |
| `LOW_TICKER_CLOSEOUT_TIME_ET` | Wall-clock time (Eastern, `HH:MM`) at which the daily Low-ticker close-out fires (default `22:00`). |
| `LOW_TICKER_CLOSEOUT_ON_LATE_START` | `true` (default) / `false` — if the process starts **after** the configured close-out time on a given ET day, run the close-out once immediately so the "no Low positions held overnight" intent is honoured. |
| `LOW_TICKER_ENTRY_HALT_ENABLED` | `true` (default) / `false` — block new `KXLOW*` entry orders from being placed at or after `LOW_TICKER_ENTRY_HALT_TIME_ET`. High tickers are unaffected. Does **not** affect stop-loss / exit paths. |
| `LOW_TICKER_ENTRY_HALT_TIME_ET` | Wall-clock time (Eastern, `HH:MM`) after which new Low entries are blocked for the remainder of the ET trading day (default `22:00`). |
| `SL_UNPROTECTED_MAX_BLIND_CYCLES` | Number of consecutive no-price SL evaluation cycles before a CRITICAL `phase.c.unprotected_escalation` alert fires for a blind position (default `30`; at 250 ms cadence ≈ 7.5 s). |
| `SL_FLATTEN_UNPROTECTED_ON_BLIND` | `false` (default, conservative) / `true` — when `true`, attempt a protective panic-flatten exit of `app_owned_qty` once the blind-cycle threshold is exceeded. Only `app_owned_qty` is ever sold; `MANAGE_EXTERNAL_POSITIONS` semantics are fully respected. |

### City-local-time entry settle gate (KXLOW* only)

**This gate applies to `KXLOW*` (Low) tickers only.  `KXHIGH*` tickers are never subject to this gate.**

Kalshi settles temperature markets overnight.  By default (`ENABLE_LOCAL_SETTLE_GATE=true`) the bot will **not** open new Low-ticker positions until the city's local clock has passed the configured rollover threshold.  This is the **RESUME** half of the Low-ticker STOP/RESUME rule (see also "Low-ticker entry halt" below):

| City group | Example cities | Resume threshold (local) | Equivalent ET (summer) |
|---|---|---|---|
| Eastern Time | Atlanta, Boston, Miami, New York City, Philadelphia, Washington DC | 01:00 ET | 01:00 ET |
| Central Time | Austin, Chicago, Dallas, Houston, Minneapolis, New Orleans, Oklahoma City, San Antonio | 01:00 CT | 02:00 ET |
| Mountain Time | Denver | 01:00 MT | 03:00 ET |
| Mountain Standard Time (no DST) | **Phoenix** | **00:00 MST** | 02:00 ET |
| Pacific Time | Las Vegas, Los Angeles, San Francisco, Seattle | 01:00 PT | 04:00 ET |

**Behavior examples** (Low tickers only):

- NYC KXLOW at 12:59 AM ET → new buys **blocked** (logs `entry.blocked_local_settle_gate`)
- NYC KXLOW at 01:00 AM ET → new buys **allowed**
- LA KXLOW at 00:30 ET (= 21:30 PT) → new buys **blocked** (ET halt lifted at midnight, but local gate blocks until 01:00 PT)
- LA KXLOW at 01:00 PT (= 04:00 ET) → new buys **allowed**
- Phoenix KXLOW at 11:59 PM MST → new buys **blocked**
- Phoenix KXLOW at 00:00 AM MST → new buys **allowed**

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

## Running safely: single instance

`run.py` now acquires an account-scoped startup file lock before the strategy starts. If a second bot process starts against the same account, startup is denied with `instance.lock_conflict` and a clear fatal message:

> Another Forecastology instance is already running against this account; refusing to start.

Use these env vars to control behavior:

- `INSTANCE_LOCK_ENABLED=true|false` (default `true`)
- `INSTANCE_LOCK_FILE=/tmp/forecastology.lock` (default shown; runtime appends account hash)
- `INSTANCE_ID=<optional stable id>` (optional override for account/environment lock key; default key is derived from `KALSHI_API_KEY_ID`)

Operational cleanup after the incident: remove old duplicate systemd units from prior deployments so they cannot be auto-started again:

```bash
sudo systemctl disable --now kalshibot.service kalshibot-monitor.service
sudo rm -f /etc/systemd/system/kalshibot.service /etc/systemd/system/kalshibot-monitor.service
sudo systemctl daemon-reload
```

## Logging & rotation

The application now includes built-in size-based log rotation via `RotatingFileHandler`, so log growth is bounded even without host-level logrotate.

- `LOG_FILE` (default `logs/run.log`)
- `LOG_MAX_BYTES` (default `104857600`, 100 MB)
- `LOG_BACKUP_COUNT` (default `10`)

As a belt-and-suspenders host safeguard, install the repo logrotate profile:

```bash
sudo cp deploy/logrotate/forecastology /etc/logrotate.d/forecastology
sudo chmod 644 /etc/logrotate.d/forecastology
sudo logrotate -f /etc/logrotate.d/forecastology
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

**Multi-layer hard cap enforcement (choke-point fix):** Every buy path is subject to the same cap regardless of how it reaches the exchange:

1. **`_evaluate_watchlist`** checks `is_allowed` from `hedge_policy` and skips entries where `count >= HEDGE_MAX_FACTOR` — no order is ever computed for a blocked count.
2. **`_execute_entry`** re-validates both `proposed_qty <= max_allowed_qty` **and** `existing_position_qty + proposed_qty <= max_allowed_qty` before calling the executor, and logs `hedge.cap_blocked` at CRITICAL if either check fails.
3. **Executor layer** (`LiveTradeExecutor` / `PaperTradeExecutor`) enforces `max_buy_qty = INITIAL_CONTRACT_COUNT * 2**(HEDGE_MAX_FACTOR-1)` as a per-order backstop **and** a position-aware guard (`existing + proposed <= cap`) at `buy_yes` entry.
4. **`scanner.py` and `monitor.py` buy paths** apply the same position-aware total-cap check before submitting orders, so side-path entries cannot stack a market past cap.
5. **`stop_loss_count` clamp**: `_increment_stop_loss_count_for_market` now clamps the stored count to `hedge_max_factor` so a stale or corrupt ledger row can never produce an oversized quantity on the next sizing calculation. A `hedge.stop_loss_count_clamped` warning is logged when clamping fires.

Root cause of the 16-contract order: `monitor.py`'s `_buy_hedge` computed `qty = pos.quantity` from a live position and POSTed directly to Kalshi with no cap check, bypassing all guards in the state machine. The fix adds an explicit cap check in `_buy_hedge` (step 4) and at the executor layer (step 3) so no order path can bypass the cap.

High and Low markets are naturally independent because they have different `series_ticker` values (for example `KXHIGHTBOS` vs `KXLOWTBOS`).

### Low-ticker daily close-out (22:00 ET)

A dedicated background loop (`_low_ticker_closeout_loop`) runs every 30 seconds and fires the close-out exactly once per Eastern-calendar day at/after the configured time (`LOW_TICKER_CLOSEOUT_TIME_ET`, default `22:00`).

- Only `KXLOW*` tickers are affected; `KXHIGH*` positions are never touched.
- Close-out is routed through the existing `_execute_stop_loss` path so ownership guards (`MANAGE_EXTERNAL_POSITIONS=false`) and idempotency records are fully respected.
- When `LOW_TICKER_CLOSEOUT_ON_LATE_START=true` (default) and the process starts **after** the configured time, the close-out runs once on the first loop iteration so no Low positions are inadvertently held overnight.
- Logs: `lowticker.daily_closeout_start`, per-ticker `lowticker.daily_closeout_ticker_done`, and `lowticker.daily_closeout_complete`.
- This close-out is driven **entirely by Eastern Time** — the ticker's own city timezone plays no role.  A Pacific-time KXLOW ticker is closed out at 22:00 ET (= 19:00 PT).

### Low-ticker STOP/RESUME rule (combined)

Low-ticker trading is controlled by two complementary gates that together form the daily STOP/RESUME cycle:

1. **STOP — global ET halt (22:00 ET)**
   At 22:00 Eastern Time, **all** new Low-ticker entry orders are blocked universally, regardless of the ticker's home city.  Implemented via `is_low_entry_halted_et` in `_evaluate_watchlist` and the `_low_ticker_closeout_loop` that simultaneously flattens open positions.  `KXHIGH*` entries are completely unaffected.

2. **RESUME — per-city local time**
   New Low-ticker entries resume once the ticker's **own** city local clock reaches the resume threshold.  Implemented via `is_entry_allowed` in `core/local_time_gate.py`, called **only for `KXLOW*` tickers** (the `_evaluate_watchlist` call is guarded by `if is_low:`):
   - Standard: 01:00 local time (Eastern, Central, Mountain/Denver, Pacific).
   - Exception: 00:00 local time for Phoenix (`America/Phoenix`, no DST).

   This means a Pacific-time Low ticker remains blocked from 22:00 ET through 01:00 PT (= 04:00 ET), even though the ET halt window technically ended at midnight ET.  The local-time gate covers that 4-hour gap.

| Timezone | Resume (local) | Resume (ET, summer) |
|---|---|---|
| Eastern (`America/New_York`) | 01:00 | 01:00 |
| Central (`America/Chicago`) | 01:00 | 02:00 |
| Mountain (`America/Denver`) | 01:00 | 03:00 |
| Mountain/Phoenix (`America/Phoenix`, no DST) | 00:00 | 02:00 |
| Pacific (`America/Los_Angeles`) | 01:00 | 04:00 |

- Logs: `entry.blocked_low_after_2200_et` (ET halt), `entry.blocked_local_settle_gate` (local resume gate).
- DST-aware via `zoneinfo` / `ZoneInfo`.

### Low-ticker entry halt (22:00 ET)

`_evaluate_watchlist` applies an Eastern-time gate (after the `LOW_TRADES` toggle and before the city-local-time settle gate) that blocks new KXLOW entry orders from `LOW_TICKER_ENTRY_HALT_TIME_ET` (default `22:00`) until the end of that ET calendar day. `KXHIGH` entries are completely unaffected. Stop-loss / exit paths are never blocked.

- Logs `entry.blocked_low_after_2200_et` with `ticker`, `now_et`, `halt_time_et`.
- DST-aware via `ZoneInfo("America/New_York")`.

### Phase C — Position Management (Stop-Loss)
Stop-loss is driven by the **WebSocket `StopLossWatcher`** inside `run.py`:

- On every `ticker` WebSocket update, `yes_ask` is passed to `StopLossWatcher.on_market_update()`.
- If `yes_ask ≤ STOP_LOSS_PRICE`, the exit handler fires immediately.
- An `exit_in_progress` guard prevents duplicate exits on repeated ticks or reconnect bursts.
- On failure, the guard is reset so the next tick can retry.
- Startup reconciliation (`_restore_positions`) registers all open positions with the watcher so coverage begins from the first WebSocket message.
- On restore, the bot immediately seeds `TickerCache` with a REST price for each restored/adopted position so the stop-loss watcher has a live price before the first WebSocket tick arrives.

The `_evaluate_held_positions` loop in the strategy (runs ~every 1s) provides a secondary safety net for cases where the shared WebSocket price feed goes stale, reconnects, or is unavailable for extended periods by falling back to REST quotes for held positions.

#### Unprotected-position remediation

When a restored or adopted position receives no price feed (neither WebSocket nor REST) for an extended period, the bot escalates through a staged response:

1. **Periodic warning** (`phase.c.held_position_unprotected`): logged at most once per 60 s while the position is blind and the no-price cycle count exceeds `max_no_price_cycles`.
2. **CRITICAL escalation** (`phase.c.unprotected_escalation`): once `_consecutive_no_price_cycles >= SL_UNPROTECTED_MAX_BLIND_CYCLES` (default 30, ≈ 7.5 s at 250 ms cadence), a `logger.critical` event is emitted. This fires once on threshold crossing and then re-fires at most once per 300 s if the position remains blind.
3. **Config-gated panic-flatten** (default off): set `SL_FLATTEN_UNPROTECTED_ON_BLIND=true` to attempt a protective panic-flatten exit of `app_owned_qty` when the escalation threshold is crossed. Only app-owned contracts are sold — `MANAGE_EXTERNAL_POSITIONS=false` semantics are always respected. If `app_owned_qty == 0`, no sell is attempted and `phase.c.unprotected_flatten_skipped_no_app_qty` is logged at CRITICAL instead.

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
- Live ownership reconciliation uses Kalshi fill history for `APP_` client-order IDs and aggregates fractional/partial fragments (`count_fp` / fixed-point fields) before classifying ownership.
- Position ownership is partitioned per ticker:
  - `app_owned`: quantity attributable to app-tracked holdings.
  - `external_manual`: quantity not attributable to app-owned tracking.
- `external_manual` is now only the unmatched remainder after APP-fill attribution; APP-attributed quantity is never classified as external.
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

`run.py` now starts this backend automatically in-process (it runs
`bootstrap()` via `asyncio.to_thread(...)` so the async trading loop is not blocked).

```python
from nws.scheduler import bootstrap, shutdown

# In your main entry point, before the trading loop:
await asyncio.to_thread(bootstrap)  # initialises DB + immediate update + scheduler

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
4. Parse periods to find the forecast high/low inside the station's **Kalshi
   trading-day window** (local-time rule, stored in UTC):
   - All stations except `KPHX`: `01:00:00` local → next-day `00:59:59` local
     (`[01:00, next 01:00)` in code).
   - `KPHX` only: `00:00:00` local → `23:59:59` local (`[00:00, next 00:00)`).
   Each period `startTime` is converted to station-local time for filtering,
   while persisted timestamps remain UTC.

### station_forecasts Table

One row per `(station_code, forecast_date_utc)`:

| Column | Type | Description |
|---|---|---|
| `station_code` | VARCHAR(8) | NWS ICAO code, e.g. `KATL` |
| `forecast_date_utc` | DATETIME | UTC midnight of the station-local **trading-day start date** |
| `high_time_utc` | DATETIME | UTC time of the trading-window daily high temperature |
| `low_time_utc` | DATETIME | UTC time of the trading-window daily low temperature |
| `updated_at` | DATETIME | Last refresh timestamp |

Unique index on `(station_code, forecast_date_utc)` with upsert semantics.
`forecast_date_utc` is UTC midnight of the station-local trading-day start date
(for non-`KPHX`, local `00:00`–`00:59:59` still maps to the prior trading day),
so it may differ from the UTC calendar date when the updater runs near rollovers.
