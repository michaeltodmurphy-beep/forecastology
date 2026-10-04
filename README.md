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
Matching changes affect **newly computed** snapshots only: deployment does not
rewrite existing same-day decisions or clear their day-long gate cache.
Correcting a stored false-positive block requires an explicit forced snapshot
rerun **and** a gate-cache/process refresh; restarting alone does not rewrite it.

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
  both caps disabled, no event-cap DB lookup is performed. Zero-quantity DB rows
  and **unverified stale DB Position rows** with strictly-past city-local ticker
  dates are excluded (unknown series fall back to Eastern); today's, future,
  and malformed DB dates remain counted. Caller-verified target quantity,
  known in-memory holdings, and live/in-flight orders **always count**, even
  for past dates with delayed settlement. Proposed quantity/cost is checked
  for every target date. This uses ticker dates, not ORM market status/expiry fields.
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

Legacy scanner accounting persists the executor-reported fill quantity, price,
and cost, including partial fills; zero fills do not create positions.
Monitor hedge caps use the maximum of standalone target quantity and summed
parent hedge quantities, plus same-cycle fills. A partial hedge records
`hedge_market_ticker` immediately to identify the actual target, but is incomplete
until `hedge_quantity >=` parent quantity. Retries buy only the remainder on that
same target; the presence of a hedge ticker alone does not prove full protection.

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

### Credentials

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `KALSHI_API_KEY` | String | Empty; example placeholder | Kalshi key ID used for authentication, not a bearer secret. Required for LIVE. |
| `KALSHI_PRIVATE_KEY_PATH` | Path string | `kalshi_private_key.pem` | RSA PEM path; protect permissions and never commit it. |
| `MYSQL_DATABASE_URL` | URL string | Replace template/default placeholder | Async SQLAlchemy URL: `mysql+aiomysql://` followed by `USER:PASSWORD@HOST:3306/forecastology`; URL-encode password characters. |

### market selection

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `WEATHER_SERIES_PREFIX` | String | `KXWEATHER` | Weather-series prefix; not a count of supported cities. |
| `LOW_TRADES` | Boolean toggle | `yes` | Enable new LOW entries; existing holdings still managed. |
| `HIGH_TRADES` | Boolean toggle | `yes` | Enable new HIGH entries; existing holdings still managed. |
| `NO_TRADE_TICKERS` | CSV → set of strings | Empty | Uppercase ticker/series prefixes excluding new candidates. |
| `WARM_TRADE_TICKERS` | CSV → set of strings | Empty | LOW prefixes bypassing sunrise/NWS timing, retaining AM-low/local-settle checks. |

### entry gate/timing

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `ENABLE_LOCAL_SETTLE_GATE` | Boolean toggle | `true` | LOW-only local rollover/resume gate; never suppresses exits. |
| `DEFAULT_ENTRY_START_LOCAL` | `HH:MM` string | `01:00` | Local entry start outside Phoenix. |
| `PHOENIX_ENTRY_START_LOCAL` | `HH:MM` string | `00:00` | Phoenix start, MST without DST. |
| `LOW_TICKER_ENTRY_HALT_ENABLED` | Boolean toggle | `true` | Enable separate LOW ET late-entry halt. |
| `LOW_TICKER_ENTRY_HALT_TIME_ET` | `HH:MM` string | `22:00` | Eastern halt until ET day's end. |
| `LOW_TICKER_10PM_MAX_ASK` | Dollar string → integer cents | `0.93` | ET entry halt applies only at ask strictly below this threshold. |
| `GATE_LOW_BEFORE` | Integer | `120` | LOW NWS window minutes before forecast low. |
| `GATE_LOW_AFTER` | Integer | `45` | LOW NWS window minutes after forecast low. |
| `GATE_HIGH_BEFORE` | Integer | `60` | HIGH NWS window minutes before forecast high. |
| `GATE_HIGH_AFTER` | Integer | `30` | HIGH NWS window minutes after forecast high. |
| `ENTRY_GATE_MODE` | Enum string | `NWS_WINDOW` | `NWS_WINDOW` or `SUNRISE` (LOW only); invalid mode warns/falls back. |
| `SUNRISE_STRATEGY_TIME` | Nonnegative integer | `30` | Minutes after sunrise to open LOW window. |
| `SUNRISE_ENTRY_WINDOW_MINUTES` | Positive integer | `120` | Window length after open, minutes. |
| `SUNRISE_REQUIRE_TEMP_RISING` | Boolean toggle | `true` | Deprecated; still parsed with warning. Replace with rise-required amount. |
| `SUNRISE_SOURCE` | Enum string | `astral` | `astral` local calculation or `api`; invalid source falls back. |
| `SUNRISE_REQUIRE_AM_LOW` | Boolean toggle | `yes` | Require forecast minimum before local deadline; partial-forecast exception above. |
| `NWS_LOW_DEADLINE_HOUR` | Integer, 0–23 | `12` | Exclusive AM-low local-hour deadline; also bounds morning gate. |
| `AM_LOW_SNAPSHOT_LOCAL_HOUR` | `HH`/`HH:MM` string | `03:00`; commented example `04:00` | Local snapshot time; consumers use hour. |
| `AM_LOW_FORECAST` | CSV → set of strings | Empty | Whole-word, case-insensitive daily-brief keywords with occurrence-level negation and thunderstorm/plural matching; empty disables. |
| `AM_LOW_FORECAST_KEYWORDS` | Set of strings | Empty | Internal Pydantic field-derived name; `from_env()` explicitly supplies `AM_LOW_FORECAST`. Configure `AM_LOW_FORECAST`, not this internal name. |
| `SUNRISE_TEMP_RISE_REQUIRED` | Float, °F | `1.0` | Rise above running minimum; `0` disables. Negative/invalid falls back; subdegree values warn. |
| `SUNRISE_TEMP_BASELINE_MINUTES` | Nonnegative integer | `15` | Minutes before sunrise to begin baseline. |
| `SUNRISE_OBS_MAX_AGE_MINUTES` | Positive integer | `15` | Observation-age limit in minutes. |
| `SUNRISE_OBS_MAX_AGE_OVERRIDES` | CSV → dictionary of integers | Empty | `STATION:MINUTES`, e.g. `KNYC:25,KSEA:20`; positive station limits. |
| `SUNRISE_OBS_SOURCE` | Enum string | `awc` | `awc` METAR with NWS fallback, or `nws` only. |
| `ENTRY_OBS_CALIBRATION_ENABLED` | Boolean toggle | `no` | Opt-in LOW per-station bracket-line calibration. |
| `ENTRY_OBS_CALIBRATION_OFFSETS` | CSV → dictionary of floats | Empty | `STATION:+/-float` °F offsets, e.g. `KSEA:+1.0`; unlisted stations unchanged. |
| `BLOCK_ENTRY_WHEN_BELOW_BRACKET` | Boolean toggle | `yes` | Sunrise-mode LOW observed-day breach guard; unavailable evidence fails open. |
| `BLOCK_ENTRY_WHEN_BRACKET_UNREACHED` | Boolean toggle | `no` | Sunrise-mode LOW reachability guard; unavailable evidence fails open. |
| `BLOCK_ENTRY_WHEN_FORECAST_DIPS_BELOW_BRACKET` | Boolean toggle | `no` | Sunrise-mode LOW remaining-day forecast guard, 1°F cushion; errors fail open. |
| `BLOCK_ENTRY_WHEN_MORNING_FORECAST_DIPS_BELOW_BRACKET` | Boolean toggle | `no` | Sunrise-mode LOW morning forecast guard, no cushion; errors fail open. |

NWS trading-day windows are `[01:00 local, next 01:00)` except Phoenix
`[00:00 local, next 00:00)`. Forecast times are persisted in UTC; forecast-date
keys identify the station-local trading-day start, not necessarily today's UTC
calendar date. Host timezone is not the city timezone.

### pricing & sizing

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `INITIAL_CONTRACT_COUNT` | Positive integer | `1` | Initial contracts. Fractional strings truncate; below 1 clamps to 1. |
| `MONITOR_START_PRICE` | Dollar string → integer cents | Required; example `0.80` | Monitoring threshold. |
| `BUY_TRIGGER_PRICE_LOW` | Dollar string → integer cents | Required; example `0.85` | LOW entry trigger; legacy single `BUY_TRIGGER_PRICE` is not an env fallback. |
| `BUY_TRIGGER_PRICE_HIGH` | Dollar string → integer cents | Required; example `0.85` | HIGH entry trigger. |
| `BUY_TRIGGER_PRICE_LOW_WARM` | Dollar string → integer cents | `0` | Warm LOW override; zero uses standard LOW trigger. |
| `SPREAD_MONITOR_PRICE` | Dollar string → integer cents | Required; example `0.90` | Hard buy ceiling, not maximum bid/ask spread. |
| `ENTRY_CROSS_SPREAD_TO_CEILING` | Boolean | `true` | Bid at supplied `max_price`; false uses order price capped there. |
| `FALLING_KNIFE_DECAY_MINUTES` | Nonnegative integer | `10` | Continuous minutes below ceiling to clear guard; `0` retains latch without decay. |
| `SUNRISE_MAX_SPREAD` | Dollar string → integer cents | `0`; example `0.04` | Spread cap through local 09:00; explicitly set it. |
| `MIDAM_MAX_SPREAD` | Dollar string → integer cents | `0`; example `0.05` | Spread cap 09:01–12:00; explicitly set it. |
| `PM_MAX_SPREAD` | Dollar string → integer cents | `0`; example `0.07` | Spread cap from 12:01; explicitly set it. |
| `SUNRISE_MAX_SPREAD_TIGHT` | Dollar string → integer cents | `0`; commented example `0.20` | Positive sunrise-only cap for selected cities; zero disables. |
| `SUNRISE_MAX_SPREAD_TIGHT_CITIES` | CSV → set of strings | Empty; commented example `kxlowtlv` | Lowercase series prefixes, e.g. `kxlowtlv,kxlowtchi`; no city matches without a prefix. |
| `HEDGE_MAX_FACTOR` | Positive integer | `3`; example `5` | Total recovery levels, including initial. Invalid falls back to 3; fractions truncate; below 1 clamps. |
| `POSITION_CAP_FAIL_CLOSED` | Boolean | `false` | After three failed LIVE lookups, true blocks; false uses known quantity with quantity guards. |
| `EVENT_MAX_CONTRACTS` | Integer | `0` | Positive aggregate event contract cap; zero/negative disables. |
| `EVENT_MAX_COST_CENTS` | Integer cents | `0` | Positive aggregate event cost-basis cap; zero/negative disables. |
| `EVENT_EXPOSURE_FAIL_CLOSED` | Boolean | `false` | True blocks enabled-cap entries on unverifiable exposure; measured breaches always block. |
| `EVAL_PRICE_FLOOR` | Dollar string → integer cents | `0.05` | Ask at/below floor skips early; held quote streams remain available. |
| `HEDGE_TRIGGER_PRICE` | Dollar string → integer cents | `0`; example `0.50` | Deprecated for primary strategy; legacy monitor still uses this for hedge buys and is not read-only. |
| `HEDGE_BUY` | Dollar string → integer cents | `0`; example `0.60` | Deprecated compatibility value; primary strategy no longer uses the old hedge engine. |
| `PARTIAL_FILL_CHASE` | Boolean toggle | `no` | Opt-in entry remainder chaser; never authorizes buying past initial count. |
| `CHASE_INTERVAL_SECONDS` | Positive integer | `60` | Repricing/fill-poll cadence, seconds. |
| `CHASE_MAX_MINUTES` | Positive integer | `30` | Time limit when not chasing until gate close. |
| `CHASE_UNTIL_GATE_CLOSE` | Boolean toggle | `yes` | Work remainder until gate close/lifecycle end instead of ordinary minute limit. |
| `CHASE_TAKE_AT_CEILING` | Boolean toggle | `yes` | Lift ask at/below ceiling; false uses maker bid+1 capped at ceiling. |

### stop-loss

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `MANAGE_EXTERNAL_POSITIONS` | Boolean toggle | `false` | True permits aggregate manual/external holdings to be managed; false app-owned only. |
| `STOP_LOSS_PRICE_ASK` | Dollar string → integer cents | Required; example `0.25` | Ask threshold; equality triggers. |
| `STOP_LOSS_PRICE_BID` | Ignored dollar string | Example `0.25`; no runtime default | Template-only compatibility input, not a supported bid trigger or AppConfig field. |
| `ENABLE_FAST_SL_EXIT` | Optional Boolean → mode default | LIVE true / PAPER false; example true | Immediate asynchronous exit path; unset field resolves by mode. |
| `HELD_POSITION_PRICE_REFRESH_SECONDS` | Integer | `10` | Held REST quote refresh interval, seconds. |
| `HELD_POSITIONS_LOOP_INTERVAL_MS` | Integer | `100` | Held SL loop cadence, milliseconds; intended 50–250 ms. |
| `MAX_NO_PRICE_CYCLES` | Integer | `10` | No-price cycles before ordinary protection warnings. |
| `STOP_LOSS_MAX_UNFILLED_ATTEMPTS` | Integer | `3` | Unfilled SL attempt limit before escalation. |
| `SL_EXECUTE_COOLDOWN_SECONDS` | Integer | `5` | Non-bypass exit cooldown; fast/watcher bypass paths unaffected. |
| `SL_WORKER_INTERVAL_MS` | Integer | `100` | Watcher worker polling interval. |
| `SL_EXIT_MODE` | String | `PANIC_FLATTEN` | Alternative `AGGRESSIVE_LIMIT` enables repricing ladder. |
| `SL_EXIT_RETRY_INTERVAL_MS` | Integer | `120`; example `300` | Fast aggressive-exit retry cadence. |
| `SL_EXIT_MAX_ATTEMPTS` | Integer | `3` | Fast aggressive-exit attempt limit. |
| `SL_EXIT_AGGRESSIVE_OFFSET_TICKS` | Integer ticks/cents | `2` | Initial aggressive sell offset. |
| `SL_EXIT_MAX_SLIPPAGE` | Dollar string → integer cents | `0.20` | Maximum repricing slippage. |
| `SL_SPREAD_HOLD_MAX_SECONDS` | Integer | `120` | Legacy aggressive hold window; `0` fires without waiting. |
| `SL_PANIC_SELL_PRICE` | Integer cents | `1` | Panic sell floor, not a promised fill price. |
| `SL_PANIC_RETRY_MS` | Integer | `100`; example `250` | Panic resubmission interval. |
| `SL_PANIC_MAX_RETRIES` | Integer | `5` | Panic retry limit. |
| `SL_PANIC_MAX_QUOTE_AGE_MS` | Integer | `30000` | Ask age limit before panic revalidation; `0` disables freshness check. |
| `ASK_SPREAD_PROTECTION` | Dollar string → integer cents | `0.05` | Outlier gap to next distinct ask; `0` disables. |
| `SL_BACKSTOP_ENABLED` | Boolean | `false` | Opt-in resting disaster GTC sell, cancelled before reactive sell. |
| `SL_BACKSTOP_OFFSET` | Dollar string → integer cents | `0.05` | Offset below SL ask threshold; resting price floored at 1¢. |
| `SL_UNPROTECTED_MAX_BLIND_CYCLES` | Positive integer | `30` | Missing-price cycles before CRITICAL escalation; elapsed time depends on cadence. |
| `SL_FLATTEN_UNPROTECTED_ON_BLIND` | Boolean toggle | `false` | Opt-in panic flatten of app-owned quantity after blind escalation. |
| `SL_UNPROTECTED_STARTUP_ALERT_SECONDS` | Nonnegative integer | `30` | Wall-clock blind alert delay after reconciliation; `0` disables. |

### intraday exits

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `INTRADAY_EXIT_ENABLED` | Boolean toggle | `true` | Enable LOW checkpoint exits. |
| `INTRADAY_EXIT_SCHEDULE` | CSV string → time/cent pairs | `12:00:0.85,15:00:0.90,18:00:0.90` | Local `HH:MM:dollar-price`; malformed entries warn/skip, all malformed falls back. |
| `INTRADAY_EXIT_ENTRY_GRACE_MINUTES` | Positive integer | `90` | Grace since entry; restored unknown time treated as past grace. |
| `INTRADAY_EXIT_SPREAD` | Nonnegative integer cents | `0` | Exit spread limit; `0` disables. Wide spreads defer checkpoint exit. |
| `INTRADAY_EXIT_EXCLUDE` | CSV → set of strings | Empty | Literal prefixes excluding checkpoint **and** HWM only; typos can match nothing. |
| `HWM_EXIT_ENABLED` | Boolean toggle | `false`; commented example true | LOW deterioration exit after local noon. |
| `HWM_ARM_PRICE` | Dollar string → integer cents | `0.93` | Ask level that arms HWM. |
| `HWM_EXIT_PRICE` | Dollar string → integer cents | `0.88`; commented example `0.84` | Armed HWM fires at/below this ask level. |
| `PROFIT_TAKE_SELL_ENABLED` | Boolean | `false` | Opt-in resting take-profit GTC sell. |
| `PROFIT_TAKE_SELL_PRICE` | Dollar string → integer cents | `0.99` | Take-profit price; resting order cancelled before reactive sells. |

### EOD closeout

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `LOW_TICKER_DAILY_CLOSEOUT_ENABLED` | Boolean toggle | `true` | Enable LOW PM close evaluation. |
| `LOW_TICKER_CLOSEOUT_ON_LATE_START` | Boolean toggle | `true` | Evaluate past PM close on first startup loop; false skips late-start evaluation. |
| `LOW_PM_CLOSE_TIME` | `HH:MM` string | `22:00` | Per-ticker **local** close evaluation time. |
| `LOW_PM_CLOSE_AMOUNT` | Positive integer cents | `93` | Strict ask `<` threshold, except override prefixes. |
| `PM_TICKERS_CLOSE` | CSV → set of strings | Empty | Ticker/series prefixes closing regardless of ask at PM time. |
| `LOW_TICKER_CLOSEOUT_TIME_ET` | `HH:MM` string | `22:00` | Legacy compatibility setting; PM timing uses `LOW_PM_CLOSE_TIME`. |

### plumbing

| Variable | Type | Default / example | Meaning |
|---|---|---|---|
| `TRADING_MODE` | Enum string | `PAPER` | `PAPER` simulated or `LIVE` real-money execution; use uppercase. |
| `DRY_RUN` | Boolean toggle | `false` | Suppress live submission; not a replacement for PAPER. |
| `REST_BASE_URL` | URL string | `https://external-api.kalshi.com` | REST origin; clients append paths. |
| `WS_URL` | URL string | `wss://external-api-ws.kalshi.com/trade-api/ws/v2` | Authenticated WebSocket endpoint; LIVE rejects demo URLs. |
| `NWS_USER_AGENT` | String | Empty; example contact placeholder | Required for weather requests; identify app and real operator contact. |
| `MYSQL_URL` | URL string | Falls back to `MYSQL_DATABASE_URL` | Sync scheduler DB URL; aiomysql driver converted to pymysql. Use the same intended DB. |
| `HIGH_LOW_UPDATE` | Integer | `60` | Forecast refresh interval, minutes. |
| `INSTANCE_LOCK_ENABLED` | Boolean toggle | `true` | Single-instance guard; disabling risks duplicate execution. |
| `INSTANCE_LOCK_FILE` | Path string | `/tmp/forecastology.lock` | Base lock path; runtime adds account hash. Use a shared accessible path. |
| `FORECASTOLOGY_LOCKFILE` | Path string | `/tmp/forecastology.lock` | Legacy fallback when `INSTANCE_LOCK_FILE` unset; scanner checks legacy and configured enabled scoped lock. |
| `INSTANCE_ID` | String | Empty | Stable lock identity; overrides `KALSHI_API_KEY_ID`, otherwise identity is `default`. Only hash logged. |
| `KALSHI_API_KEY_ID` | String | Unset | Lock identity when `INSTANCE_ID` absent, not an authentication replacement; neither set means shared `default`. |
| `LOG_FILE` | Path string | `logs/run.log` | Rotating log path, relative to working directory. |
| `LOG_MAX_BYTES` | Positive integer | `104857600` | Rotation threshold, 100 MiB. |
| `LOG_BACKUP_COUNT` | Positive integer | `10` | Rotated backup count. |
| `LOG_TO_CONSOLE` | Boolean toggle | `true` | Console events, captured by systemd. |
| `LOG_TO_FILE` | Boolean toggle | `true` | File events; avoid merging both sinks into the same file. |
| `ENABLE_SETTLEMENT_RECONCILER` | Boolean toggle | `true` | Background outcome settlement backfill. |
| `RECONCILER_INTERVAL_MINUTES` | Positive integer | `60` | Settlement reconciliation interval. |

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
python -m pytest -q
```

`tests/conftest.py` routes NWS DB sessions to per-test in-memory SQLite and
overrides NWS MySQL URLs; `tests/test_db_isolation.py` checks that fixture
behavior. Do not infer that arbitrary standalone scripts or unreviewed tests
are safe against production credentials. Use dedicated development credentials
and DBs; pytest dependencies are not all listed in `requirements.txt`.
Live credentials are not required for the ordinary suite. The root
`test_ws.py` smoke test skips when `kalshi_private_key.pem` is absent; when that
file exists it attempts an authenticated exchange WebSocket connection.
Keep production PEMs out of a development checkout used for full-suite runs.

## Troubleshooting: structured events

Read structured `event`, `ticker`, reason/action, station, timestamps, quantities,
and configured limits together. A blocked entry is not necessarily a bug.
`phase.b.decision` is change-driven, so repeated identical verdicts need not
produce repeated lines. `entry.blocked_summary` summarizes blocked reasons
**per evaluation cycle**, including `counts_by_reason`, `total_blocks`,
`blocked_ticker_count`, `am_low_blocked_city_count`, and `cycle_completed`.
It is not a fill count or deployment inventory; one ticker can have more than
one recorded gate block, so `total_blocks` need not equal the unique ticker count.
Reason keys are stable ledger gate IDs; each ticker/reason counts once per cycle
even when individual log messages are deduplicated. Held informational skips
are excluded. Empty, error, and cancelled cycles also emit a summary;
`cycle_completed` distinguishes a completed sweep. Submission outcomes include
`submission_quote`, `price_ceiling`, `sunrise_final`, `nws_temp_window_final`,
`position_cap`, `position_lookup_error`, `event_exposure`, `no_fill_ioc`,
`execution_rejected`, and `execution_error`.

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
