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
| **Monitor** | `monitor.py` | Reads open positions from DB, checks prices, manages stop-losses (systemd timer, ~every 30s) |

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
| `STOP_LOSS_PRICE` | Last-traded price strictly below this (cents, dollar format ok) triggers immediate sell (default `0.50`) |
| `BUY_TRIGGER_PRICE` | YES ask at or above this (cents, dollar format ok) triggers an entry (default `0.82`) |
| `INITIAL_CONTRACT_COUNT` | Base number of contracts per entry (default `2`; doubles on each recovery: 2→4→8→16) |
| `HEDGE_MAX_FACTOR` | Maximum number of martingale doublings per (series, day). Entry is blocked when `stop_loss_count > HEDGE_MAX_FACTOR`. With `3`: count 0→buy 2, 1→4, 2→8, 3→16, 4+→no more buys. |
| `MINIMUM_SPREAD` | Max bid-ask spread allowed at entry (cents, dollar format ok) |
| `MONITOR_START_PRICE` | Monitor markets with YES ask ≥ this (cents, dollar format ok) |
| `SPREAD_MONITOR_PRICE` | Maximum submit price for all buys (cents, dollar format ok) |
| `EVAL_PRICE_FLOOR` | Silently skip brackets with YES ask ≤ this in watchlist evaluation (cents, dollar format ok, default `0.05`) |
| `HEDGE_TRIGGER_PRICE` | **Deprecated** — was the hedge trigger. Ignored by trading logic. Field kept for `.env` back-compat (defaults to `0`). |
| `HEDGE_BUY` | **Deprecated** — was the recovery hedge threshold. Ignored by trading logic. Field kept for `.env` back-compat (defaults to `0`). |

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

The strategy uses a **dead-simple stop-loss** backed by a **per-(series, date) martingale entry sizing** system. All hedging has been removed.

### Phase A — Market Monitoring
All temperature bracket markets are monitored via the WebSocket ticker feed (YES ask price and bid-ask spread).

### Phase B — Entry (Martingale Sizing)
**Buy signal**: YES ask price ≥ `BUY_TRIGGER_PRICE` AND bid-ask spread ≤ `MINIMUM_SPREAD`.

Before placing the order the strategy checks the persistent stop-loss ledger for the market's `(series_ticker, date)` key (see below):

| `stop_loss_count` for (series, date) | Entry quantity | State |
|---|---|---|
| 0 | `INITIAL_CONTRACT_COUNT × 1` = 2 | normal |
| 1 | `INITIAL_CONTRACT_COUNT × 2` = 4 | recovering |
| 2 | `INITIAL_CONTRACT_COUNT × 4` = 8 | recovering |
| 3 | `INITIAL_CONTRACT_COUNT × 8` = 16 | final attempt |
| > 3 (i.e. ≥ 4) | **0 — no buy** | done, accept losses for that series that day |

Formula: `quantity = INITIAL_CONTRACT_COUNT × 2^count` when `count ≤ HEDGE_MAX_FACTOR`.

The counter is keyed to the **full Kalshi series ticker** (e.g. `KXLOWTBOS`, `KXHIGHTBOS`), so High and Low are automatically separate — one city's Low stop-loss never affects that city's High entry size, and vice versa. Any bracket in a series can trigger a recovery-sized buy after a stop-loss on a different bracket in the same series.

When the cap is reached (`count > HEDGE_MAX_FACTOR`), the bracket is flagged so the cap log fires only once instead of every evaluation cycle.

### Phase C — Position Management (Simple Stop-Loss)
Every evaluation cycle, each held position is checked using the **last traded price** only:

- `cache.get_last_price(ticker) < STOP_LOSS_PRICE` → **sell the entire holding** at 1¢ limit (IOC, reduce-only). Strictly less-than comparison.
- No last-traded price available → **skip stop-loss this cycle** (no invented or stale price is used).

On stop-loss trigger: the `(series_ticker, date)` counter in `StopLossLedger` is incremented exactly once (guarded by a per-bracket `_stop_loss_counted` flag to prevent double-counting on the 60-second retry throttle).

**Worst-case bounded spend per series/day** (with `INITIAL_CONTRACT_COUNT=2`, `HEDGE_MAX_FACTOR=3`): `2 + 4 + 8 + 16 = 30` contracts across four stop-loss events. After the fourth stop-loss the series is silenced for the rest of the day.

> **This is a martingale.** Each stop-loss doubles the next buy. The cap (`HEDGE_MAX_FACTOR`) strictly limits the total exposure per series per trading day.

### Stop-Loss Ledger (`StopLossLedger`)
Persistent DB table keyed on `(series_ticker, date_prefix)` where `date_prefix` is parsed from the market ticker's own date segment (e.g. `26JUN23` from `KXLOWTBOS-26JUN23-B65.5`). This ensures the counter correctly keys to the market's trading day rather than the local date, avoiding midnight edge cases. The ledger survives bot restarts.

### Watchlist Evaluation Floor (`EVAL_PRICE_FLOOR`)
Reduces log noise by silently skipping brackets whose YES ask price is at or below the floor in `_evaluate_watchlist`. Configured via `EVAL_PRICE_FLOOR` (default `5` cents / `0.05` dollar format).

## Security

- **Never commit your private key** (`*.pem` is in `.gitignore`)
- **Never commit your `.env` file** (`.env` is in `.gitignore`)
- If you accidentally commit credentials, rotate them immediately at Kalshi

## Database Schema

See `db/init_schema.sql` for the full schema. Key tables:

- `positions` — open positions
- `executed_trades` — trade history
- `stop_loss_ledger` — per-(series, date) stop-loss counter driving martingale sizing
