# Forecastology

A Kalshi prediction market trading bot for US temperature bracket markets.

## Overview

Forecastology monitors and trades Kalshi temperature bracket markets across 20 US cities (high/low temperature brackets). It supports two execution modes: **PAPER** (simulated) and **LIVE** (real money via Kalshi REST API).

## Architecture

The system runs as three coordinating processes:

| Process | File | Role |
|---|---|---|
| **WS Daemon** | `run.py` | Connects to Kalshi WebSocket, maintains live order book cache, runs `TemperatureStrategy` |
| **Scanner** | `scanner.py` | Reads shared state from `/dev/shm/forecastology_state.json`, places buy orders when conditions are met (systemd timer, ~every 2s) |
| **Monitor** | `monitor.py` | Reads open positions from DB, checks prices, and triggers stop-losses (systemd timer, ~every 30s) |

> **Note:** `scanner.py` (shared-state version) requires the WS daemon (`run.py`) to write market state to `/dev/shm/forecastology_state.json`. The current `run.py` uses the integrated `TemperatureStrategy` approach which handles scanning internally — the standalone `scanner.py` is for a future decoupled architecture.

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
| `STOP_LOSS_PRICE` | Last trade strictly below this (cents) triggers the stop-loss sell (default `0.35`) |
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
Held positions use the cached **last trade** only:

- `price = cache.get_last_price(ticker)`
- if `price is None` → skip this cycle
- if `price < STOP_LOSS_PRICE` → sell the entire holding through the existing 1¢ IOC reduce-only stop-loss path
- comparison is **strictly `<`**, not `<=`

Immediately before the stop-loss order is sent, the bot increments `StopLossLedger` once per triggered position (guarded so 60-second retries do not double-count the same stop-loss).

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
