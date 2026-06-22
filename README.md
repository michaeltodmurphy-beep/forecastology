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
| **Monitor** | `monitor.py` | Reads open positions from DB, checks prices, triggers hedges and stop-losses (systemd timer, ~every 30s) |

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

The strategy uses a two-phase, multi-hedge, per-event break-even approach. Each event (`event_ticker`) is tracked independently — a city's High and Low temperature markets have separate ledgers and hedge state.

### Phase A — Market Monitoring
All temperature bracket markets are monitored via the WebSocket ticker feed (YES ask price and bid-ask spread).

### Phase B — Entry
**Buy signal**: YES ask price ≥ `BUY_TRIGGER_PRICE` (default 85¢) AND bid-ask spread ≤ `MINIMUM_SPREAD` (default 7¢). Price is sourced exclusively from the ticker channel `yes_ask`/`yes_ask_dollars` — the NO side is never used to derive YES ask prices.

### Phase C — Position Management

#### Phase-1 Hedge (triggered at low price)
When a held bracket's YES price drops to ≤ `HEDGE_TRIGGER_PRICE` (default 50¢), the strategy hedges by buying the highest-priced sibling bracket in the same event.

- **Pricing**: the hedge target price is the **YES ask** from the ticker-quote cache (`yes_ask`/`yes_ask_dollars`), with a REST fallback using the `yes_ask` field only — no NO-derived values.
- **Sizing** (break-even math):
  - `expected_loss = Q × (avg_entry − stop_loss_price)`
  - `hedge_qty = ⌈expected_loss / (100 − hedge_price)⌉`
  - Capped to `min(raw_qty, original_qty, original_cost / hedge_price)` and floored at 1.
- **Multi-hedge**: an event can be hedged multiple times. If the hedge bracket's price also falls, a new hedge fires into the next highest sibling. The 60-second per-bracket cooldown prevents spam but does not permanently block re-hedging.
- **Ledger**: once an event is hedged, a per-event cash ledger (sourced from `executed_trades`) tracks gross spend and stop-loss proceeds for all brackets in that event.

#### Phase-2 Top-Off (triggered at high price, hedged events only)
When a bracket in a **hedged** event recovers to YES ask ≥ `BUY_TRIGGER_PRICE`, and all sibling brackets for that event have closed (settled or stop-lossed), the strategy tops off the surviving bracket to reach event break-even.

- **Gating**: event must have been hedged; YES ask ≥ `BUY_TRIGGER_PRICE` and ≤ `SPREAD_MONITOR_PRICE`; all other event brackets must be closed; event must not already be at break-even.
- **Sizing** (ledger-based):
  - `remaining_deficit = gross_spend_cents − (Q_current × 100)`
  - `topoff_qty = ⌈remaining_deficit / (100 − yes_ask)⌉` (rounded up so worst case is flat)
- This single formula handles Case B (original bracket recovers while hedge will lose) because the hedge spend is already in `gross_spend_cents`.

#### Stop Loss
If YES price drops to ≤ `STOP_LOSS_PRICE` (default 25¢), the position is sold at 1¢ (GTC) to guarantee a fill.

### Per-Event Circuit-Breaker (`HEDGE_MAX_FACTOR`)
A safety cap prevents a single event from draining the account. Configured via `HEDGE_MAX_FACTOR` (default `5`).

- `max_event_spend = HEDGE_MAX_FACTOR × initial_entry_cost_for_event`
- Basis: **gross spend** (sum of all BUY and HEDGE costs). Stop-loss proceeds do NOT restore headroom.
- When any hedge or top-off order would push gross spend over the cap: the order is not placed, `phase.c.hedge_cap_reached` is logged (with `event_ticker`, `gross_spend_cents`, `max_event_spend_cents`, and `attempted_spend`), and that event stops receiving hedge/top-off orders for the remainder of the day. Other events are unaffected.

## Security

- **Never commit your private key** (`*.pem` is in `.gitignore`)
- **Never commit your `.env` file** (`.env` is in `.gitignore`)
- If you accidentally commit credentials, rotate them immediately at Kalshi

## Database Schema

See `db/init_schema.sql` for the full schema. Key tables:

- `positions` — open positions
- `executed_trades` — trade history
