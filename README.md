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
| `HEDGE_TRIGGER_PRICE` | YES bid at or below this (cents) triggers the hedge/arm logic (default `0.48`) |
| `HEDGE_BUY` | Recovery threshold: deferred-hedge recovery fires only once a sibling's YES ask rises **strictly above** this value (default `0.60`). Normal immediate hedges (best sibling ≥ `HEDGE_TRIGGER_PRICE` at trigger time) are not gated by this value. |
| `STOP_LOSS_PRICE` | YES bid at or below this (cents) triggers the guaranteed stop-loss sell (default `0.35`) |

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

Phase C prices held positions off the **authoritative YES bid/ask quote** from the
WebSocket ticker channel (`cache.get_quote`), not the stale `last_price` (last trade).
This ensures hedge and stop-loss triggers fire correctly even when the last trade is
hours old in thin temperature markets.

**Price resolution priority:**
1. `cache.get_quote(ticker)` → YES bid (realistic exit for a long YES); falls back to YES ask if bid is 0.
2. Positions-API `last_price_cents`.
3. REST `_fetch_market_data_via_rest` → `yes_bid` / `yes_ask` / `price` (60 s per-ticker cooldown).
4. Last known real price (`bracket.last_price` if > 0).

If no real price is available, the cycle is skipped with a `phase.c.no_live_price` warning.
The bot **never manufactures a price** (the old `avg_entry or 83` fallback has been removed).

#### Full lifecycle (sell loser → buy recovery → top-off to break-even)

1. **Hedge trigger** (`HEDGE_TRIGGER_PRICE`, default 48¢): when a held bracket's YES bid falls
   to ≤ 48¢, the bot scans siblings in the same event for the **best (highest) valid YES ask**
   (ignoring brackets priced at or below `EVAL_PRICE_FLOOR`):

   | Best sibling YES ask at trigger time | Action |
   |--------------------------------------|--------|
   | **≥ `HEDGE_TRIGGER_PRICE` (48¢)** — credible winner exists | **Normal hedge**: buy that sibling immediately using break-even sizing (see below). Event added to `_hedged_events`. |
   | **< `HEDGE_TRIGGER_PRICE` (48¢)** — all siblings weak, no credible winner | **Deferred**: arm the event (`hedge_pending`). Do NOT buy a weak sibling. Wait for a winner to emerge (see step 2). |

2. **Armed/deferred state** (`hedge_pending`): every subsequent cycle, armed events are
   re-scanned. As soon as a sibling's YES ask rises **strictly above `HEDGE_BUY` (60¢)** the
   deferred hedge (recovery) fires: the single highest-priced qualifying sibling is bought at
   `initial_contract_count` quantity. `hedge_pending` is cleared and the event enters the hedged
   state. The 60-second per-event cooldown prevents spam.

   > **Why the two thresholds?** `HEDGE_TRIGGER_PRICE` (48¢) is the "credible winner" bar at
   > hedge time — if any sibling is already there, hedge immediately. `HEDGE_BUY` (60¢) is the
   > "risen enough to be a real winner" bar for the deferred path — wait until a sibling climbs
   > convincingly before committing to the recovery buy.

3. **Stop-loss backstop** (`STOP_LOSS_PRICE`, default 35¢): whenever the original held
   bracket's price ≤ 35¢, the bot **sells it at market (1¢ limit, takes best available bid)**
   to realize the loss, **regardless of `hedge_pending` or hedge state**. The armed state
   (`hedge_pending`) is preserved after the stop-loss executes, so a recovery bracket can still
   be bought on a later cycle once a sibling clears 60¢ (via the secondary loop in
   `_evaluate_held_positions`, which runs even when `active_positions` is empty).

4. **Top-off at 82¢** (`BUY_TRIGGER_PRICE`): when a bracket in a hedged event recovers to
   YES ask ≥ 82¢ and all other event brackets are closed, the ledger-based top-off fires.
   Because the ledger (`_event_ledger`) already includes the realized 35¢ stop-loss proceeds
   and the 60¢ recovery-buy cost, one top-off calculation reconciles all prior legs
   automatically.
   - `remaining_deficit = gross_spend_cents − (Q_current × 100)`
   - `topoff_qty = ⌈remaining_deficit / (100 − yes_ask)⌉`

**Single-order-per-event guarantee**: at most ONE hedge/recovery order is placed per event
(until the top-off phase). Once an event is in `_hedged_events`, neither the main hedge branch
nor the secondary recovery loop will place another hedge order; only `_execute_topoff` may
buy more (into the same surviving bracket).

**90¢ buy ceiling**: all buys (entry, hedge, recovery, top-off) submit at
`max_price = SPREAD_MONITOR_PRICE` (default 90¢). Kalshi fills at the best available ask ≤ 90¢.
If a winner has already risen above 90¢ when detected, the order will not fill at that level —
do not chase. The stop-loss on the loser still protects the downside.

**Floor guard**: brackets priced at or below `EVAL_PRICE_FLOOR` (default 5¢) are never
chosen as hedge or recovery targets. This prevents buying dead/settled brackets.

**Ledger honesty**: all sizing and break-even math uses the actual `result.fill_price`
returned by the executor, never the 90¢ submit ceiling.

#### Phase-1 Hedge (triggered at low price)
When a held bracket's YES price drops to ≤ `HEDGE_TRIGGER_PRICE`, the strategy applies the
**conditional hedge rule**:

- **Normal hedge (best sibling ≥ 48¢)**: buy the highest-priced sibling immediately using
  break-even sizing. Max buy price = `SPREAD_MONITOR_PRICE` (90¢).
- **Deferred hedge (all siblings < 48¢)**: arm the event (`hedge_pending`); wait for a sibling
  to rise strictly above `HEDGE_BUY` (60¢), then buy that sibling as a recovery bracket.

- **Pricing**: the hedge target price is the **YES ask** from the ticker-quote cache
  (`yes_ask`/`yes_ask_dollars`), with a REST fallback using the `yes_ask` field only — no
  NO-derived values, no orderbook `best_ask`.
- **Sizing** (break-even math, normal hedge path only):
  - `expected_loss = Q × (avg_entry − stop_loss_price)`
  - `hedge_qty = ⌈expected_loss / (100 − hedge_price)⌉`
  - Capped to `min(raw_qty, original_qty, original_cost / hedge_price)` and floored at 1.
- **Ledger**: once an event is hedged, a per-event cash ledger (sourced from
  `executed_trades`) tracks gross spend and stop-loss proceeds for all brackets in that event.

#### Phase-2 Top-Off (triggered at high price, hedged events only)
When a bracket in a **hedged** event recovers to YES ask ≥ `BUY_TRIGGER_PRICE`, and all
sibling brackets for that event have closed (settled or stop-lossed), the strategy tops off
the surviving bracket to reach event break-even.

- **Gating**: event must have been hedged; YES ask ≥ `BUY_TRIGGER_PRICE` and ≤
  `SPREAD_MONITOR_PRICE`; all other event brackets must be closed; event must not already be
  at break-even.
- **Sizing** (ledger-based):
  - `remaining_deficit = gross_spend_cents − (Q_current × 100)`
  - `topoff_qty = ⌈remaining_deficit / (100 − yes_ask)⌉` (rounded up so worst case is flat)
- This single formula handles Case B (original bracket recovers while hedge will lose) because
  the hedge spend is already in `gross_spend_cents`. It also reconciles the 35¢ stop-loss
  realized loss automatically.

#### Stop Loss
If YES bid/ask drops to ≤ `STOP_LOSS_PRICE` (default 35¢), the position is **sold at 1¢
(marketable limit — accepts the best available bid)** to guarantee a fill. This fires
**independently of `hedge_pending`** — an armed/deferred hedge never delays or prevents the
stop-loss backstop. After the stop-loss executes, the event stays armed so a recovery bracket
can still be bought once a sibling clears 60¢.

### Per-Event Circuit-Breaker (`HEDGE_MAX_FACTOR`)
A safety cap prevents a single event from draining the account. Configured via
`HEDGE_MAX_FACTOR` (default `5`).

- `max_event_spend = HEDGE_MAX_FACTOR × initial_entry_cost_for_event`
- Basis: **gross spend** (sum of all BUY and HEDGE costs). Stop-loss proceeds do NOT restore
  headroom.
- When any hedge or top-off order would push gross spend over the cap: the order is not
  placed, `phase.c.hedge_cap_reached` is logged (with `event_ticker`, `gross_spend_cents`,
  `max_event_spend_cents`, and `attempted_spend`), and that event stops receiving hedge/top-off
  orders for the remainder of the day. Other events are unaffected.

### Watchlist Evaluation Floor (`EVAL_PRICE_FLOOR`)
Reduces log noise and speeds up the watchlist loop by silently skipping brackets whose YES ask
price is at or below the floor. Configured via `EVAL_PRICE_FLOOR` (default `5` cents; dollar
format `0.05` is also accepted).

- Brackets priced ≤ floor are skipped early in `_evaluate_watchlist` without emitting a
  `phase.b.below_trigger` log. Their `last_price` is still updated.
- Brackets priced above the floor but below `BUY_TRIGGER_PRICE` continue to emit
  `phase.b.below_trigger` exactly as before.
- **WebSocket subscriptions are unchanged**: all market data keeps flowing so hedge/top-off
  logic (`_execute_hedge`, `_execute_topoff`, `_find_next_bracket`) can still see every
  sibling bracket in an event.
- The default 5¢ floor only suppresses truly inert brackets; any bracket that could
  realistically recover toward the 82¢ buy trigger remains fully evaluated and logged.

## Security

- **Never commit your private key** (`*.pem` is in `.gitignore`)
- **Never commit your `.env` file** (`.env` is in `.gitignore`)
- If you accidentally commit credentials, rotate them immediately at Kalshi

## Database Schema

See `db/init_schema.sql` for the full schema. Key tables:

- `positions` — open positions
- `executed_trades` — trade history
