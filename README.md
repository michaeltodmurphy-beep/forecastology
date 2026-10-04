# Forecastology

Forecastology is a Python bot for Kalshi daily US low-temperature (`KXLOWT*`) and high-temperature (`KXHIGHT*`) bracket markets, not a weather forecasting model.
It combines exchange quotes with NWS forecasts, city-local timing, and optional observed-temperature checks to decide when to buy YES contracts.
The primary daemon manages entries, stop-losses, recovery sizing, closeouts, and settlement reconciliation in one asynchronous runtime.
`PAPER` simulates execution; `LIVE` submits real-money orders, and martingale recovery can increase losses rapidly.
The repository's supported series are not evidence that any particular number of cities, contracts, or instances is deployed.

Historical fixes and postmortems are preserved in [docs/CHANGELOG.md](docs/CHANGELOG.md).

## Architecture and entrypoints

| Entrypoint / component | Responsibility |
|---|---|
| `python run.py` | Primary always-on daemon: loads `AppConfig`, configures logs, acquires an account-scoped instance lock, initializes the DB and NWS backend, connects WebSocket, then starts strategy and stop-loss watcher. |
| `core/state_machine.py` → `TemperatureStrategy` | Watchlist selection, ordered entry gates, persistent recovery ledger, ownership tracking, held-position safety loops, closeouts, and outcome reconciliation. |
| `data/websocket_manager.py`, `data/ticker_cache.py` | Exchange stream and quote cache; held positions also have REST price fallback. |
| `execution/sl_watcher.py` → `StopLossWatcher` | WebSocket-driven exit dispatch with duplicate-exit protection; strategy loops provide independent backstops. |
| `execution/factory.py`, `execution/live.py`, `execution/paper.py` | Select LIVE/PAPER execution; enforce order and position caps, buy ceiling, dry-run handling, and fill reporting. |
| `nws/scheduler.py`, `nws/gate.py`, `core/sunrise_gate.py` | Background forecast refresh, persisted daily decisions, forecast-window and sunrise/observation gates. |
| `app/database.py`, `app/models.py`, `db/init_schema.sql` | Async trading persistence; NWS uses a separate synchronous connection. Positions, fills, stop-loss ledger, forecasts, daily gate decisions, and outcomes survive restarts. |
| `python scanner.py` | Legacy REST buy scanner. Checks the legacy lock path and, after config loads, the daemon's account-scoped lock when instance locking is enabled. Not an equivalent implementation of the daemon's weather/observation gates; do not intentionally run alongside `run.py`. |
| `python monitor.py` | Optional legacy REST reconciliation/expiry process; does **not** execute primary stop-losses. It can still submit hedge buys when legacy hedge settings permit them: it is not strictly read-only. |
| `python bracket_scanner.py --min-spread 7 --buy-trigger 85` | Diagnostic bracket/quote watcher; CLI price/spread arguments are integer cents. |
| `python -m reports.city_pnl --days 60 --family LOW` | Historical realized outcome report; supports `--bucket`, `--count`, and `--csv`. Not a deployment inventory or guarantee of profitability. |

Use one primary daemon per account/environment. The daemon's lock is scoped with
an identity hash. The scanner checks **both** the legacy lock path and the
configured daemon's real account-scoped path after loading config, and exits if
either is held. Keep instance locking enabled and matching identities/paths;
disabling it removes that scoped protection.
Neither trading decisions nor reconciliation depend on
`/dev/shm/forecastology_state.json`.

## Trade lifecycle and gate order

### Phase A — Monitoring, startup, and observation

Configuration and lock validation precede trading. `LIVE` rejects demo URLs and
requires an API key; authenticated market data may require a valid key/private
key even in PAPER mode. Startup restores DB/API positions, reconstructs
app-owned quantity, registers held positions with the watcher, and attempts
REST quote seeding. Adoption does not mean all account quantity is app-owned.
Entry scanning must not be used as a substitute for held-position protection.

### Phase B — Entry screening: evaluation order

The first failing gate stops that candidate; later gates may not be evaluated.
The table follows `_evaluate_watchlist`, not an alphabetical list of settings.
Some gates are conditional on family, mode, or bracket state.

| Order | Check / controlling environment | Failure semantics |
|---|---|---|
| 1 | Entry-candidate state and `NO_TRADE_TICKERS` prefix exclusion | Excluded new candidates skip immediately. Already-entered/non-monitoring brackets do not become fresh entries. Existing holdings remain managed. |
| 2 | Quote acquisition: ticker cache, then bounded REST fallback | No usable ask skips; missing spread later also skips. Stream subscription is not proof of usable data. |
| 3 | Warm LOW series (`WARM_TRADE_TICKERS`) or `ENTRY_GATE_MODE=SUNRISE` timing | Warm series bypass sunrise/NWS entry timing, **not** AM-low deadline or local settle safety. Normal sunrise requires the configured window, optional AM-low decision, and temperature-rise latch; denied decisions defer entry. Unknown station/timezone metadata can fall back to NWS-window behavior. |
| 4 | Continuous LOW daily-brief keyword check (`AM_LOW_FORECAST`) | Any stored matching keyword blocks all new buys/adds for that city/day, including chase top-ups; existing quantity is left untouched. No snapshot yet permits entry (fail open). This check runs before the non-entry-state early return. |
| 5 | Candidate eligibility, known HIGH/LOW family, usable spread, non-dead book, `EVAL_PRICE_FLOOR` | Unknown family, one-sided/dead books, or ask at/below floor skip. A missing spread never passes the spread gate. |
| 6 | Family trigger (`BUY_TRIGGER_PRICE_LOW`, `BUY_TRIGGER_PRICE_HIGH`; warm override) and ceiling (`SPREAD_MONITOR_PRICE`) | Ask must be at/above trigger and at/below ceiling. Above ceiling is a missed entry, not permission to chase higher. Calibration is opt-in, station-specific, and adjusts LOW bracket-line permission checks. |
| 7 | Falling-knife latch (`FALLING_KNIFE_DECAY_MINUTES`) and earlier sunrise verdict | A latched falling-knife condition blocks re-entry. Continuous time below the ceiling can clear it; `0` disables decay. Sunrise verdict is enforced before sizing. |
| 8 | City-local spread band (`SUNRISE_MAX_SPREAD`, `MIDAM_MAX_SPREAD`, `PM_MAX_SPREAD`) | Ask minus bid must be no greater than the active cap. Set all three: there is no legacy `MAX_SPREAD` fallback. A listed city's positive `SUNRISE_MAX_SPREAD_TIGHT` applies only to the sunrise band. |
| 9 | `HIGH_TRADES` / `LOW_TRADES` | Disabled family blocks new entry only, never exits. |
| 10 | LOW Eastern halt (`LOW_TICKER_ENTRY_HALT_ENABLED`, time, `LOW_TICKER_10PM_MAX_ASK`) | From the halt time through the ET day's end, block **only** when ask is strictly below the ask threshold. HIGH is unaffected. |
| 11 | LOW city-local resume (`ENABLE_LOCAL_SETTLE_GATE`, start times) | Block before `01:00` local by default; Phoenix resumes at `00:00` MST year-round. HIGH is unaffected. This is distinct from sunrise timing and ET halt. |
| 12 | LOW observed-day breach (`BLOCK_ENTRY_WHEN_BELOW_BRACKET`) | In the sunrise-mode path, block a known breach: whole-degree day minimum `<` the B-line or `<=` the T-line, after optional station calibration. Missing observations do not establish a breach (fail open); retained known minima still matter. |
| 13 | LOW observed reachability (`BLOCK_ENTRY_WHEN_BRACKET_UNREACHED`) | Optional sunrise-mode guard: B windows must actually have been reached; open-ended upper T is exempt. Missing data/errors fail open. |
| 14 | LOW remaining-day forecast breach (`BLOCK_ENTRY_WHEN_FORECAST_DIPS_BELOW_BRACKET`) | Optional sunrise-mode guard with a fixed 1°F cushion; unavailable forecast/parse data fail open. |
| 15 | LOW morning forecast breach (`BLOCK_ENTRY_WHEN_MORNING_FORECAST_DIPS_BELOW_BRACKET`) | Optional sunrise-mode guard over sunrise through `NWS_LOW_DEADLINE_HOUR`, inclusive; no cushion. Missing data/errors fail open. |
| 16 | NWS forecast-time window (`GATE_LOW_*`, `GATE_HIGH_*`) | When applicable, missing forecast data, closed window, or evaluation exception blocks. SUNRISE LOW normally bypasses the LOW window; HIGH retains it. Warm LOW bypasses timing. |
| 17 | Recovery ledger, existing app-owned quantity, `HEDGE_MAX_FACTOR`, cycle duplicate guard | Size `INITIAL_CONTRACT_COUNT * 2**stop_loss_count`; block at `count >= HEDGE_MAX_FACTOR`, already-satisfied initial quantity, or position-total cap. At most one entry for `(series, ticker-date, recovery-count)` per sweep. |

The sunrise rise check uses whole-degree °F and requires the rise to hold on the
two most recent observations. Before a latch, unavailable/stale observations
block; a previously earned latch can survive a feed gap within the day/window.
`SUNRISE_REQUIRE_AM_LOW=yes` blocks unavailable complete forecast decisions and
lows at/after the deadline. A post-morning **partial** forecast that would block
instead fails open and is not locked/persisted. Complete decisions can be
snapshotted at `AM_LOW_SNAPSHOT_LOCAL_HOUR` and restored from the DB. Daily-brief
keywords are a separate write-once snapshot, with startup catch-up when needed.
Keyword matching is case-insensitive whole-word; configured `thunderstorm`
also matches `thunderstorms`. Direct `no`/`without` prefixes and supported
`not expected`/`ending` suffixes negate only that occurrence; a later positive
occurrence still blocks. Snapshot selection uses today's period **start**
hour before `NWS_LOW_DEADLINE_HOUR`, not the catch-up time: a 06:00 `Today`
period remains eligible at 13:00, while 18:00 `Tonight` is excluded at the
default noon deadline. Missing city mapping fails open.

The sunrise spread band lasts through local 09:00; mid-AM covers 09:01–12:00;
PM starts 12:01 and remains active for the rest of the day. Timing gates still
determine whether any entry is permitted during those bands.

### Final submission boundary

`_execute_entry` rechecks the applicable sunrise/NWS timing gate without trusting
the earlier cached NWS verdict; NWS closed/error results return the bracket to
monitoring. The submission gates run in this order:

1. Applicable sunrise/NWS timing revalidation: closed/error blocks; warm LOW
   bypasses timing, not the watchlist's AM-low/local-settle checks.
2. Known family: unknown HIGH/LOW family blocks rather than synthesizing a price.
3. Explicit `_fetch_market_data_via_rest` refresh, even with a supplied book:
   use the real REST YES ask, including an ask derived from the NO bid.
   Only when REST ask is unavailable may an **actual cached ask** provide the
   bounded-limit fallback. No trigger or last-trade price is substituted.
   Missing/invalid executable ask blocks and marks the entry pending.
4. Ask ceiling: above `SPREAD_MONITOR_PRICE` blocks, marks `pending_entry=true`,
   and preserves crossed state so the qualifying entry can be retried later.
5. Proposed quantity hard cap: oversized orders block regardless of lookup policy.
6. Known/exchange position-total cap: measured or known quantity plus proposal
   above cap blocks; failed LIVE verification uses the policy below.
7. Enabled event aggregate quantity/cost caps: measured breaches block; unavailable
   exposure uses the explicit open/closed policy below.
8. Shared executor backstops recheck quantity, position total, order price and
   payload ceiling; LIVE dry-run suppresses submission.

- `ENTRY_CROSS_SPREAD_TO_CEILING=true` (default) bids at `max_price`, normally
  `SPREAD_MONITOR_PRICE`, rather than the earlier cached ask. This permits fills
  anywhere up to the ceiling, **not** above it; a fresh ask above ceiling remains
  blocked. `false` preserves the order-price limit capped at `max_price`.
- `POSITION_CAP_FAIL_CLOSED=false` (default): LIVE position verification retries
  three times; if still unavailable, use known quantity plus proposed quantity
  for the guard rather than blocking solely on lookup failure. The proposed
  quantity cap remains enforced. `true` blocks unverifiable position exposure.
  Fail-open fallback cannot prove that unknown/manual holdings are absent.
- `EVENT_MAX_CONTRACTS` and `EVENT_MAX_COST_CENTS`: each is disabled at `0`
  (default) **or negative**, and enabled only by an explicit positive value.
  There is no automatic event cap derived from the recovery ladder. Enabled
  checks aggregate city/day brackets using DB positions and known/in-flight
  exposure; cost is cents. With ceiling crossing enabled, proposed order cost
  reserves the **ceiling**, not the earlier ask; pending exposure is also
  conservatively priced. With crossing disabled, proposed cost uses
  `min(ask, ceiling)`; held positions retain actual average cost basis.
  The same exposure helper governs daemon, scanner, and monitor buys. With
  both caps disabled, no event-cap DB lookup is performed. Existing exposure
  excludes zero quantities and strictly-past **city-local** ticker dates
  (unknown series fall back to Eastern); today's, future, and malformed dates
  remain counted. Proposed quantity/cost is still checked for every target
  date. This uses ticker dates, not ORM market status/expiry fields.
- `EVENT_EXPOSURE_FAIL_CLOSED=false` (default): an enabled event-cap lookup
  failure permits the buy with a CRITICAL unverifiable-exposure event. `true` blocks.
  The event is `entry.event_exposure_unverifiable`, with
  `action=event_exposure_cap_bypassed` or `action=event_exposure_cap_blocked`.
  A successfully measured cap breach blocks regardless of that switch and logs
  WARNING `entry.event_exposure_cap_blocked` with `reason=contracts` or `reason=cost`.

Ceiling-crossing and position-lookup policy are wired through the shared executor
factory for daemon, scanner, and monitor buys; `OrderRequest.known_position_qty`
carries caller-known holdings into fallback verification. These executor guards
do **not** make legacy callers equivalent to the daemon's weather-gate pipeline.

Sizing is keyed by the ticker's parsed date, not the current wall clock. With
initial `1` and factor `3`, recovery sizes are `1, 2, 4`; a single-order/position
cap of `4` is **not** a lifetime daily spend cap (the sequence totals `7`).
HIGH and LOW series have separate ledger keys. PAPER fills and LIVE fills are
not interchangeable evidence of liquidity; record actual partial/zero fills.
Optional partial-fill chasing must stop when position lifecycle or gate safety
disallows further buys and never authorizes prices above the ceiling.

### Phase C — Holding, exits, and settlement

- WebSocket watcher and independent held-position loops evaluate stop-losses.
  The supported trigger is **YES ask** `<= STOP_LOSS_PRICE_ASK`; the example's
  `STOP_LOSS_PRICE_BID` is ignored, not a second implemented trigger.
  `ASK_SPREAD_PROTECTION` can replace an isolated low ask with the next distinct
  corroborated ask for stop-loss evaluation.
- `PANIC_FLATTEN` submits a marketable floor-price sell (default 1¢); actual
  execution depends on available bids and can partially fill or fail.
  `AGGRESSIVE_LIMIT` is the opt-in bounded repricing path. Retry/idempotency and
  ownership checks protect against repeated ticks and overselling.
- Default `MANAGE_EXTERNAL_POSITIONS=false` limits managed exits to app-owned
  quantity. Enabling aggregate management can sell manual/external holdings.
  Blind holdings produce warnings/escalation; automatic blind flatten is opt-in.
- Optional resting disaster/take-profit sells are cancelled before reactive
  sells. They are exchange orders, not guarantees of a fill during a gap.
- LOW checkpoint exits use city-local schedule, grace, confirmation reread, and
  optional spread limit. HWM arms after local noon and fires on deterioration.
  `INTRADAY_EXIT_EXCLUDE` disables **both** checkpoint and HWM behavior, not SL.
- LOW PM close is separate: at `LOW_PM_CLOSE_TIME` in **each ticker's timezone**,
  matching `PM_TICKERS_CLOSE` prefixes close regardless of ask; others close only
  at ask **strictly below** `LOW_PM_CLOSE_AMOUNT`. HIGH is unaffected.
- Settlement reconciliation backfills outcomes from exchange results. DB/API
  failures are logged; no entry gate should suppress sells or reconciliation.

## Configuration reference

`app/config.py` loads `.env` using dotenv; existing process environment wins.
`AppConfig.from_env()` parses additional toggles/CSV values. NWS-specific values
are loaded separately by `nws/config.py`. Restart the service after changes.

The template uses a mixed layout. Below, credentials are separate from seven
functional groups covering its sections/comments, with additional runtime
settings under their related groups.
**Default** means unset runtime value, **example** means the template explicitly
overrides it; copying the template is not the same as using defaults.
For dollar-price fields use `0.85`, not `85`: environment **strings are multiplied
by 100**, even integer-looking strings. Prices inside the application are cents.
Use integer cents only where explicitly marked (e.g. event cost, PM amount,
panic floor, checkpoint spread). `true/false` is the safest boolean spelling;
custom trade toggles also accept `yes/no/1/0`, and invalid custom toggles warn and
fall back to their documented default. Positive/nonnegative parsers warn and
fall back on invalid input; required settings can prevent startup.

### Credentials, database, and runtime

| Variable | Default / example | Meaning |
|---|---|---|
| `KALSHI_API_KEY` | Default empty; example placeholder | Kalshi key ID used for API authentication, not a bearer secret. Required for LIVE. |
| `KALSHI_PRIVATE_KEY_PATH` | `kalshi_private_key.pem` | Readable RSA PEM path; protect file permissions and never commit it. |
| `MYSQL_DATABASE_URL` | Replace template/default placeholder | Async SQLAlchemy URL using `mysql+aiomysql://`, then `USER:PASSWORD@HOST:3306/forecastology`; URL-encode password characters. |
| `TRADING_MODE` | `PAPER` | `PAPER` simulated or `LIVE` real-money execution; use uppercase. |
| `DRY_RUN` | `false` | Suppress live order submission; not a replacement for PAPER. |
| `REST_BASE_URL` | `https://external-api.kalshi.com` | REST origin; API paths are appended by clients. |
| `WS_URL` | `wss://external-api-ws.kalshi.com/trade-api/ws/v2` | Authenticated WebSocket endpoint. LIVE rejects demo endpoints. |
| `WEATHER_SERIES_PREFIX` | `KXWEATHER` | Weather-series configuration prefix; not a count of supported cities. |
| `KALSHI_API_KEY_ID` | Unset | Lock identity when `INSTANCE_ID` is absent; does not replace `KALSHI_API_KEY` authentication. With neither identity set, runtime hashes the shared string `default`. |

### 1. Entry prices, spreads, recovery, and exposure

| Variable | Default / example | Meaning |
|---|---|---|
| `INITIAL_CONTRACT_COUNT` | `1` | Initial contracts; positive integer. Fractional strings truncate; below 1 clamps to 1. |
| `MONITOR_START_PRICE` | Required; example `0.80` | Dollar price for monitoring threshold. |
| `BUY_TRIGGER_PRICE_LOW` | Required; example `0.85` | LOW dollar entry trigger. Legacy single `BUY_TRIGGER_PRICE` is not an env fallback. |
| `BUY_TRIGGER_PRICE_HIGH` | Required; example `0.85` | HIGH dollar entry trigger. |
| `BUY_TRIGGER_PRICE_LOW_WARM` | `0` | Dollar override for warm LOW series; zero uses standard LOW trigger. |
| `SPREAD_MONITOR_PRICE` | Required; example `0.90` | Hard dollar buy ceiling, not maximum bid/ask spread. |
| `ENTRY_CROSS_SPREAD_TO_CEILING` | `true` | Submit buy limit at supplied `max_price`; false uses order price capped there. |
| `FALLING_KNIFE_DECAY_MINUTES` | `10` | Continuous minutes below ceiling to clear guard; `0` retains latch without decay. |
| `SUNRISE_MAX_SPREAD` | `0`; example `0.04` | Dollar spread cap through local 09:00; explicitly set it. |
| `MIDAM_MAX_SPREAD` | `0`; example `0.05` | Dollar spread cap 09:01–12:00; explicitly set it. |
| `PM_MAX_SPREAD` | `0`; example `0.07` | Dollar spread cap from 12:01; explicitly set it. |
| `SUNRISE_MAX_SPREAD_TIGHT` | `0` | Positive dollar sunrise-only cap for selected cities; zero disables. |
| `SUNRISE_MAX_SPREAD_TIGHT_CITIES` | Empty | CSV lowercase series prefixes, e.g. `kxlowtlv,kxlowtchi`; no matching city without a prefix. |
| `HEDGE_MAX_FACTOR` | `3`; example `5` | Total recovery levels, including initial entry. Invalid falls back to 3; fractions truncate; values below 1 clamp. |
| `POSITION_CAP_FAIL_CLOSED` | `false` | After three failed LIVE lookups, true blocks; false falls back to known quantity while preserving quantity guards. |
| `EVENT_MAX_CONTRACTS` | `0` | Positive aggregate event contract cap; zero/negative disables. |
| `EVENT_MAX_COST_CENTS` | `0` | Positive aggregate event cost-basis cap in integer cents; zero/negative disables. |
| `EVENT_EXPOSURE_FAIL_CLOSED` | `false` | Block enabled-cap entries on unverifiable exposure only when true; measured breaches always block. |
| `EVAL_PRICE_FLOOR` | `0.05` | Ask at/below dollar floor skips early; hedging/held quote streams remain available. |
| `HEDGE_TRIGGER_PRICE` | `0`; example `0.50` | Deprecated for primary strategy. Legacy `monitor.py` still reads this threshold for hedge buys; do not treat that process as read-only. |
| `HEDGE_BUY` | `0`; example `0.60` | Deprecated compatibility value; primary strategy no longer uses the old hedge engine. |
| `LOW_TRADES` | `yes` | Enable new LOW entries; existing holdings still managed. |
| `HIGH_TRADES` | `yes` | Enable new HIGH entries; existing holdings still managed. |
| `NO_TRADE_TICKERS` | Empty | CSV uppercase ticker/series prefixes excluding new candidates. |
| `WARM_TRADE_TICKERS` | Empty | CSV LOW prefixes bypassing sunrise/NWS timing, retaining AM-low/local-settle checks. |
| `MANAGE_EXTERNAL_POSITIONS` | `false` | True permits management of aggregate manual/external holdings; false app-owned only. |
| `PARTIAL_FILL_CHASE` | `no` | Opt-in entry remainder chaser; never authorizes buying past initial contract count. |
| `CHASE_INTERVAL_SECONDS` | `60` | Positive repricing/fill-poll cadence. |
| `CHASE_MAX_MINUTES` | `30` | Positive time limit when not chasing until gate close. |
| `CHASE_UNTIL_GATE_CLOSE` | `yes` | Work remainder until gate close/lifecycle end instead of ordinary minute limit. |
| `CHASE_TAKE_AT_CEILING` | `yes` | Lift an ask at/below ceiling; false uses maker bid+1 capped at ceiling. |

### 2. Stop-loss, PANIC_FLATTEN, and ask-spread spoof protection

| Variable | Default / example | Meaning |
|---|---|---|
| `STOP_LOSS_PRICE_ASK` | Required; example `0.25` | Dollar ask threshold; equality triggers. |
| `STOP_LOSS_PRICE_BID` | Example `0.25`; ignored | Present in template but not an `AppConfig` field or supported bid trigger. |
| `ENABLE_FAST_SL_EXIT` | LIVE true / PAPER false; example true | Enable immediate asynchronous exit path. |
| `HELD_POSITION_PRICE_REFRESH_SECONDS` | `10` | Held-position REST quote refresh interval, seconds. |
| `HELD_POSITIONS_LOOP_INTERVAL_MS` | `100` | Independent held SL loop cadence, milliseconds; intended 50–250 ms. |
| `MAX_NO_PRICE_CYCLES` | `10` | No-price cycles before ordinary held-position protection warnings. |
| `STOP_LOSS_MAX_UNFILLED_ATTEMPTS` | `3` | Limit for unfilled stop-loss attempts before escalation. |
| `SL_EXECUTE_COOLDOWN_SECONDS` | `5` | Non-bypass exit cooldown; fast/watcher bypass paths unaffected. |
| `SL_WORKER_INTERVAL_MS` | `100` | Stop-loss watcher worker polling interval. |
| `SL_EXIT_MODE` | `PANIC_FLATTEN` | Alternative `AGGRESSIVE_LIMIT` enables repricing ladder. |
| `SL_EXIT_RETRY_INTERVAL_MS` | `120`; example `300` | Fast aggressive-exit retry cadence. |
| `SL_EXIT_MAX_ATTEMPTS` | `3` | Fast aggressive-exit attempt limit. |
| `SL_EXIT_AGGRESSIVE_OFFSET_TICKS` | `2` | Initial aggressive sell offset, cents/ticks. |
| `SL_EXIT_MAX_SLIPPAGE` | `0.20` | Dollar maximum repricing slippage. |
| `SL_SPREAD_HOLD_MAX_SECONDS` | `120` | Legacy aggressive-mode hold window; `0` fires without waiting. |
| `SL_PANIC_SELL_PRICE` | `1` | Integer-cent floor price for panic sell, not a promised fill price. |
| `SL_PANIC_RETRY_MS` | `100`; example `250` | Panic resubmission interval. |
| `SL_PANIC_MAX_RETRIES` | `5` | Panic retry limit. |
| `SL_PANIC_MAX_QUOTE_AGE_MS` | `30000` | Cached ask age limit before panic revalidation; `0` disables freshness check. |
| `ASK_SPREAD_PROTECTION` | `0.05` | Dollar gap to next distinct ask that identifies an outlier; `0` disables. |
| `SL_BACKSTOP_ENABLED` | `false` | Opt-in resting disaster GTC sell, cancelled before reactive sell. |
| `SL_BACKSTOP_OFFSET` | `0.05` | Dollar offset below SL ask threshold; resting price floored at 1¢. |
| `PROFIT_TAKE_SELL_ENABLED` | `false` | Opt-in resting take-profit GTC sell. |
| `PROFIT_TAKE_SELL_PRICE` | `0.99` | Dollar take-profit sell price; cancelled before reactive sells. |

### 3. Startup single-instance safety lock and built-in log rotation

| Variable | Default | Meaning |
|---|---|---|
| `INSTANCE_LOCK_ENABLED` | `true` | Startup single-instance guard; disabling risks duplicate execution. |
| `INSTANCE_LOCK_FILE` | `/tmp/forecastology.lock` | Base lock path; runtime adds account hash. Configure a shared readable/writable path appropriate to deployment. |
| `FORECASTOLOGY_LOCKFILE` | `/tmp/forecastology.lock` | Legacy base-path fallback when `INSTANCE_LOCK_FILE` unset; scanner checks this legacy path and the configured enabled daemon's scoped lock. |
| `INSTANCE_ID` | Empty | Stable account/environment lock identity; overrides `KALSHI_API_KEY_ID`, otherwise identity is `default`. Only hash logged. |
| `LOG_FILE` | `logs/run.log` | Rotating file path, relative to working directory. |
| `LOG_MAX_BYTES` | `104857600` | Rotation threshold in bytes (100 MiB). |
| `LOG_BACKUP_COUNT` | `10` | Number of rotated backups. |
| `LOG_TO_CONSOLE` | `true` | Emit console events (systemd captures these). |
| `LOG_TO_FILE` | `true` | Emit file events; avoid merging both sinks into the same file. |

### 4. City-local-time entry settle gate and Low-ticker PM / ET behavior

| Variable | Default | Meaning |
|---|---|---|
| `ENABLE_LOCAL_SETTLE_GATE` | `true` | LOW-only local rollover/resume gate; never suppresses exits. |
| `DEFAULT_ENTRY_START_LOCAL` | `01:00` | Local `HH:MM` start outside Phoenix. |
| `PHOENIX_ENTRY_START_LOCAL` | `00:00` | Phoenix `HH:MM` start, MST without DST. |
| `LOW_TICKER_DAILY_CLOSEOUT_ENABLED` | `true` | Enable LOW PM close evaluation. |
| `LOW_TICKER_CLOSEOUT_ON_LATE_START` | `true` | Evaluate already-past PM close on first startup loop; false skips late-start evaluation. |
| `LOW_PM_CLOSE_TIME` | `22:00` | Per-ticker **local** `HH:MM` close evaluation time. |
| `LOW_PM_CLOSE_AMOUNT` | `93` | Integer-cent ask threshold, strict `<`, except override prefixes. |
| `PM_TICKERS_CLOSE` | Empty | CSV ticker/series prefixes closing regardless of ask at PM time. |
| `LOW_TICKER_CLOSEOUT_TIME_ET` | `22:00` | Legacy compatibility setting; current PM timing uses `LOW_PM_CLOSE_TIME`. |
| `LOW_TICKER_ENTRY_HALT_ENABLED` | `true` | Enable separate LOW **ET** late-entry halt. |
| `LOW_TICKER_ENTRY_HALT_TIME_ET` | `22:00` | Eastern `HH:MM` halt until ET day's end. |
| `LOW_TICKER_10PM_MAX_ASK` | `0.93` | Dollar ask threshold; ET entry halt applies only at strict `<`. |

### 5. NWS forecast backend

| Variable | Default / example | Meaning |
|---|---|---|
| `NWS_USER_AGENT` | Required; example contact placeholder | NWS custom identifying User-Agent with real operator contact. |
| `MYSQL_URL` | Falls back to `MYSQL_DATABASE_URL` | Sync scheduler DB URL; aiomysql driver converted to pymysql. Use the same intended DB. |
| `HIGH_LOW_UPDATE` | `60` | Forecast refresh interval, minutes. |
| `GATE_LOW_BEFORE` | `120` | LOW NWS window minutes before forecast low. |
| `GATE_LOW_AFTER` | `45` | LOW NWS window minutes after forecast low. |
| `GATE_HIGH_BEFORE` | `60` | HIGH NWS window minutes before forecast high. |
| `GATE_HIGH_AFTER` | `30` | HIGH NWS window minutes after forecast high. |
| `ENTRY_GATE_MODE` | `NWS_WINDOW` | `NWS_WINDOW` or `SUNRISE` (LOW only); invalid mode warns/falls back. |
| `SUNRISE_STRATEGY_TIME` | `30` | Minutes after sunrise to open LOW window. |
| `SUNRISE_ENTRY_WINDOW_MINUTES` | `120` | Window length after open, minutes. |
| `SUNRISE_REQUIRE_TEMP_RISING` | `true` | Deprecated, still parsed with warning; replace with rise-required amount. |
| `SUNRISE_SOURCE` | `astral` | `astral` local calculation or `api`; invalid source falls back. |
| `SUNRISE_REQUIRE_AM_LOW` | `yes` | Require daily forecast minimum before local deadline; partial-forecast exception described above. |
| `NWS_LOW_DEADLINE_HOUR` | `12` | Local hour 0–23, exclusive AM-low deadline; also bounds morning gate. |
| `AM_LOW_SNAPSHOT_LOCAL_HOUR` | `03:00`; commented example `04:00` | Local `HH`/`HH:MM` snapshot time; consumers use hour. |
| `AM_LOW_FORECAST` | Empty | CSV case-insensitive whole-word daily-brief keywords, with occurrence-level negation handling and thunderstorm/plural matching; empty disables. |
| `AM_LOW_FORECAST_KEYWORDS` | Empty | Pydantic field-derived name; normal `from_env()` explicitly supplies keywords from `AM_LOW_FORECAST`. Use `AM_LOW_FORECAST`, not this internal field name. |
| `SUNRISE_TEMP_RISE_REQUIRED` | `1.0` | Required °F rise above running minimum; `0` disables. Negative/invalid falls back; subdegree values warn. |
| `SUNRISE_TEMP_BASELINE_MINUTES` | `15` | Minutes before sunrise to begin baseline; nonnegative. |
| `SUNRISE_OBS_MAX_AGE_MINUTES` | `15` | Positive observation-age limit, minutes. |
| `SUNRISE_OBS_MAX_AGE_OVERRIDES` | Empty | CSV `STATION:MINUTES`, e.g. `KNYC:25,KSEA:20`; positive station limits. |
| `SUNRISE_OBS_SOURCE` | `awc` | `awc` METAR with NWS fallback, or `nws` only. |
| `ENTRY_OBS_CALIBRATION_ENABLED` | `no` | Opt-in LOW per-station bracket-line calibration. |
| `ENTRY_OBS_CALIBRATION_OFFSETS` | Empty | CSV `STATION:+/-float` °F offsets, e.g. `KSEA:+1.0`; unlisted stations unchanged. |
| `BLOCK_ENTRY_WHEN_BELOW_BRACKET` | `yes` | Sunrise-mode LOW observed-day breach guard; unavailable evidence fails open. |
| `BLOCK_ENTRY_WHEN_BRACKET_UNREACHED` | `no` | Sunrise-mode LOW observed reachability guard; unavailable evidence fails open. |
| `BLOCK_ENTRY_WHEN_FORECAST_DIPS_BELOW_BRACKET` | `no` | Sunrise-mode LOW remaining-day forecast guard, 1°F cushion; errors fail open. |
| `BLOCK_ENTRY_WHEN_MORNING_FORECAST_DIPS_BELOW_BRACKET` | `no` | Sunrise-mode LOW morning forecast guard, no cushion; errors fail open. |

NWS trading-day windows are `[01:00 local, next 01:00)` except Phoenix
`[00:00 local, next 00:00)`. Forecast times are persisted in UTC; forecast-date
keys identify the station-local trading-day start, not necessarily today's UTC
calendar date. Host timezone is not the city timezone.

### 6. Unprotected-position remediation (Bug A fix)

| Variable | Default | Meaning |
|---|---|---|
| `SL_UNPROTECTED_MAX_BLIND_CYCLES` | `30` | Consecutive missing-price cycles before CRITICAL escalation; elapsed time depends on actual loop cadence. |
| `SL_FLATTEN_UNPROTECTED_ON_BLIND` | `false` | Opt-in protective panic flatten of app-owned quantity after blind escalation. |
| `SL_UNPROTECTED_STARTUP_ALERT_SECONDS` | `30` | Startup wall-clock blind alert delay after reconciliation; `0` disables. |
| `ENABLE_SETTLEMENT_RECONCILER` | `true` | Background outcome settlement backfill. |
| `RECONCILER_INTERVAL_MINUTES` | `60` | Positive settlement reconciliation interval. |

### 7. Intraday checkpoint + HWM exit exclusion

| Variable | Default / example | Meaning |
|---|---|---|
| `INTRADAY_EXIT_ENABLED` | `true` | Enable LOW checkpoint exits. |
| `INTRADAY_EXIT_SCHEDULE` | `12:00:0.85,15:00:0.90,18:00:0.90` | CSV local `HH:MM:dollar-price`; malformed entries warn/skip, all malformed falls back. |
| `INTRADAY_EXIT_ENTRY_GRACE_MINUTES` | `90` | Skip checkpoints within grace since entry; restored unknown entry time treated as past grace. |
| `INTRADAY_EXIT_SPREAD` | `0` | Integer-cent exit spread limit; `0` disables. Wide spreads defer checkpoint exit. |
| `INTRADAY_EXIT_EXCLUDE` | Empty | CSV literal prefixes excluding checkpoint **and** HWM only; typos may match nothing. |
| `HWM_EXIT_ENABLED` | `false`; commented example true | Enable LOW deterioration exit after local noon. |
| `HWM_ARM_PRICE` | `0.93` | Dollar ask level that arms HWM. |
| `HWM_EXIT_PRICE` | `0.88`; commented example `0.84` | Dollar ask level at/below which armed HWM fires. |

## Install and configure

Prerequisites: Python 3.11+, MySQL 8/MariaDB, Kalshi RSA credentials, network access
to exchange and weather APIs, and a correctly synchronized host clock.

```bash
git clone https://github.com/michaeltodmurphy-beep/forecastology.git
cd forecastology
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
# Install your RSA PEM at KALSHI_PRIVATE_KEY_PATH, then chmod 600 that file.
# Edit .env: replace credential/DB/contact placeholders; begin with PAPER.
# Create the database and a scoped user first, then initialize the base schema:
mysql -u <database-user> -p <database-name> < db/init_schema.sql
python run.py
```

Do not paste secrets into command history or commit `.env`/PEM files. Set an
async `MYSQL_DATABASE_URL` and a matching sync `MYSQL_URL` if explicitly used.
Startup creates supplemental NWS/daily-decision model tables; its DB user needs
appropriate schema permissions. Back up existing production data before upgrades.
Keep `TRADING_MODE=PAPER` until logs, permissions, quote coverage, gates, fills,
and restart ownership have been reviewed. Switching to LIVE is an operator
decision, not an automatic migration step.

## systemd deployment

No service unit is shipped. The following is an **example**, not a claim that
these units are installed. Adapt user and paths; keep the daemon's working
directory at the checkout so dotenv and relative paths resolve consistently.

```ini
# /etc/systemd/system/forecastology.service
[Unit]
Description=Forecastology Kalshi temperature daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=forecastology
WorkingDirectory=/opt/forecastology
ExecStart=/opt/forecastology/.venv/bin/python /opt/forecastology/run.py
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=10
UMask=0077

[Install]
WantedBy=multi-user.target
```

Dotenv loads `/opt/forecastology/.env` here. Do not blindly use the template as
systemd `EnvironmentFile`: its unquoted values (such as NWS User-Agent) follow
dotenv conventions. If injecting process environment instead, use a properly
formatted protected systemd environment file; injected values override dotenv.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now forecastology.service
sudo systemctl status forecastology.service --no-pager
sudo journalctl -u forecastology.service -n 100 --no-pager
# After an environment or application change:
sudo systemctl restart forecastology.service
```

- Disable old scanner timers/cron and duplicate daemon units before enabling the
  replacement. Inventory actual unit names; historical names are not universal.
- Do not auto-enable `monitor.py`: audit its legacy hedge-buy behavior first.
  If needed, run as a deliberate one-shot/timer using the same venv/config.
- Give the service user access to DB, PEM, logs, and a shared instance-lock
  directory. Do not evade a lock conflict by changing identity or lock path.
- Built-in rotation is normally sufficient. An optional host profile exists at
  `deploy/logrotate/forecastology`; review its paths/ownership before installing.
  Avoid sending journal/console output back into `LOG_FILE`.
- Verify startup, subscriptions, restored ownership, held quote coverage, and
  actual exchange open orders after restart. Service-active alone proves none
  of those properties. Never remove a lock to bypass a genuinely running bot.

## Development and tests

The existing pytest suite includes configuration, execution caps, forecast/
sunrise gates, ownership, closeouts, and DB-isolation checks. Commands below are
for operators/developers; documentation changes alone do not require a run.

```bash
source .venv/bin/activate
python -m pip install pytest pytest-asyncio greenlet
python -m pytest tests/test_config.py tests/test_nws_gate.py tests/test_sunrise_gate.py -q
python -m pytest tests -q
```

`tests/conftest.py` routes NWS DB sessions to per-test in-memory SQLite and
overrides NWS MySQL URLs; `tests/test_db_isolation.py` checks that fixture
behavior. Do not infer that arbitrary standalone scripts or unreviewed tests
are safe against production credentials. Use dedicated development credentials
and DBs; pytest dependencies are not all listed in `requirements.txt`.

## Troubleshooting: structured events

Read structured `event`, `ticker`, reason/action, station, timestamps, quantities,
and configured limits together. A blocked entry is not necessarily a bug.
`phase.b.decision` is change-driven, so repeated identical verdicts need not
produce repeated lines. `entry.blocked_summary` summarizes blocked reasons
**per evaluation cycle**, including `counts_by_reason`, `total_blocks`,
`blocked_ticker_count`, `am_low_blocked_city_count`, and `cycle_completed`.
It is not a fill count or deployment inventory; one ticker can have more than
one recorded gate block, so `total_blocks` need not equal the unique ticker count.

| Symptom / events | Check |
|---|---|
| Startup denied: `instance.lock_conflict` | Find the actual process/unit using the same account identity. Stop duplicate service/cron; do not disable the lock as a fix. |
| Startup/config: `app.config_loaded`, `app.logging_configured`, `config.trade_toggle_invalid`, `config.hedge_max_factor_invalid` | Confirm effective mode, dotenv working directory, injected env overrides, required prices, and parser warnings. |
| No entry: `entry.blocked_summary`, `phase.b.decision`, `phase.b.entry_blocked_by_config` | Inspect first blocked reason; compare family toggles, excluded prefixes, bracket state, ask, ceiling, and active spread band. |
| Price/spread: `phase.b.below_trigger`, `phase.b.missed_entry`, `phase.b.spread_too_wide`, `phase.b.falling_knife_blocked` | Compare fresh quote vs trigger/ceiling; all three spread settings are dollar inputs. Check latch/decay rather than raising limits blindly. |
| Timing: `entry.blocked_low_after_2200_et`, `entry.blocked_local_settle_gate` | Distinguish ET halt from city-local rollover and sunrise. Check ticker family/timezone/date and strict ask threshold. |
| Forecast window: `entry.blocked_nws_temp_gate_no_data`, `entry.blocked_nws_temp_gate_error`, `entry.blocked_nws_gate_final` | Inspect NWS User-Agent, network, station forecast rows/freshness, ticker trading-day date, and final `decision_reason`. |
| Sunrise: `sunrise.am_low_check`, `sunrise.am_low_partial_forecast_fail_open`, `sunrise.obs_unavailable`, `sunrise.temp_rising_blocked` | Inspect persisted decision and partial coverage; check source, observation age overrides, whole-degree two-observation rise, and local sunrise window. |
| Daily brief: `phase.b.entry_blocked_am_low_forecast`, `am_low_brief.no_snapshot_yet`, `nws.daily_brief.catchup_scheduled` | Check keyword list and today's write-once snapshot; no snapshot is fail open, not a proof of favorable forecast. |
| Bracket evidence: `entry.blocked_day_min_below_bracket`, `entry.blocked_bracket_unreached`, `entry.blocked_forecast_dips_below_bracket`, `entry.blocked_morning_forecast_dip_below_bracket` | Compare whole-degree line/kind, calibration station offsets, day minimum, and forecast window; these gates are independent. |
| Cap: `hedge.cap_blocked`, `phase.b.recovery_cap_reached`, `live.position_cap_unverifiable`, `entry.event_exposure_cap_blocked`, `entry.event_exposure_unverifiable` | Compare ledger count, known/exchange quantity, proposed quantity, explicit positive event limits, and fail-closed switches. Fail-open warnings signal incomplete verification. |
| Filled but persistence failed: `phase.b.entry_db_error`, `phase.b.untracked_fill_adopted` | Compare exchange fills/orders with DB ownership before restarting or buying again. A DB exception does not undo an exchange fill. |
| Blind holding: `phase.c.held_position_unprotected`, `phase.c.unprotected_escalation` | Check WS subscription, REST response, ticker date/settlement status, actual app-owned quantity, and blind-remediation settings immediately. |
| PM close: `lowticker.daily_closeout_start`, `lowticker.daily_closeout_ticker_done`, `lowticker.daily_closeout_complete` | Check per-ticker local time, strict threshold vs override prefixes, ownership, and actual fill/remainder. |
| Exclusion typo: `config.intraday_exit_exclude_effective`, `config.intraday_exit_exclude_unmatched_prefix` | Compare literal prefixes; e.g. `KXLOWTSATX`, not `KXLOWSATX`. A plausible typo can still match nothing. |

### Operator checklist

1. Establish whether the issue is missing data, a legitimate gate, an order
   rejection/partial fill, persistence failure, or an unprotected holding.
2. Verify one executor process, account identity, actual exchange holdings/open
   orders, and app-owned quantities; never assume a failed request means no fill.
3. Check effective env vs `.env.example` vs runtime defaults, including dollar
   versus cent units and explicit opt-in event caps.
4. Check clock synchronization, ticker date, city timezone, local gate windows,
   forecast coverage, daily snapshots, and observation age/source.
5. Read the per-cycle summary and first blocked reason before loosening guards.
   Confirm final-boundary events as well as watchlist decisions.
6. For blind positions or failed exits, prioritize exposure/quote recovery and
   exchange reconciliation over entry tuning; retain ownership limits.
7. After a change, restart once and verify actual quotes, protection, orders,
   and persistence. Do not claim success from service status or PAPER fills.
