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
| **Monitor** | `monitor.py` | Reads open positions from DB and applies stop-loss management (systemd timer, ~every 30s) |

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
| `STOP_LOSS_PRICE` | Last-traded price strictly below this (cents) triggers full stop-loss sell |
| `HEDGE_MAX_FACTOR` | Repurposed as max stop-loss doublings per series/day (default `3`) |
| `HEDGE_TRIGGER_PRICE` | Deprecated/unused by strategy logic (retained only for `.env` compatibility) |
| `HEDGE_BUY` | Deprecated/unused by strategy logic (retained only for `.env` compatibility) |

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

The strategy now uses simple stop-loss + bounded martingale recovery sizing. All hedge/deferred-hedge/top-off logic has been removed.

### Phase A — Market Monitoring
All temperature bracket markets are monitored via the WebSocket ticker feed (YES ask price and bid-ask spread).

### Phase B — Entry
A bracket is eligible to buy when:
- YES ask `>= BUY_TRIGGER_PRICE`
- spread `<= MINIMUM_SPREAD`
- YES ask `> EVAL_PRICE_FLOOR`

Entry size is based on a persistent per-`(series_ticker, date_prefix)` stop-loss counter:

`quantity = INITIAL_CONTRACT_COUNT * 2**count`

Where `count` is read from `stop_loss_ledger` using the market ticker date segment (`YYMMMDD`).

Cap boundary:
- Buy while `count <= HEDGE_MAX_FACTOR`
- Stop buying for that series/day when `count > HEDGE_MAX_FACTOR` (`phase.b.recovery_cap_reached`)

With `INITIAL_CONTRACT_COUNT=2` and `HEDGE_MAX_FACTOR=3`, sizes are:
- count 0 → 2
- count 1 → 4
- count 2 → 8
- count 3 → 16
- count >= 4 → no more buys that day

### Phase C — Held Position Stop-Loss
For each held position, each cycle:
- Read only `cache.get_last_price(ticker)`
- If a price exists and `last_trade < STOP_LOSS_PRICE` (strict `<`), execute full stop-loss sell (1¢ IOC reduce-only path)
- If no last-trade price exists, skip stop-loss for that cycle

No quote/REST fallback is used to trigger stop-loss, and floor-based stop-loss skipping is removed.

### Stop-loss Ledger and Independence
- Stop-loss events increment `stop_loss_ledger` once per triggered stop-loss
- Counter key is `(series_ticker, date_prefix)` parsed from ticker
- Any bracket in the same series/day uses that counter
- High and Low series are naturally independent (`KXHIGHT...` vs `KXLOWT...`)

### Risk Bound (per series/day)
This is a martingale recovery scheme with a hard cap.
For `INITIAL_CONTRACT_COUNT=2`, `HEDGE_MAX_FACTOR=3`, worst-case bought contracts per series/day are:

`2 + 4 + 8 + 16 = 30`

## Security

- **Never commit your private key** (`*.pem` is in `.gitignore`)
- **Never commit your `.env` file** (`.env` is in `.gitignore`)
- If you accidentally commit credentials, rotate them immediately at Kalshi

## Database Schema

See `db/init_schema.sql` for the full schema. Key tables:

- `positions` — open positions
- `executed_trades` — trade history
