# core/state_machine.py
import asyncio
import datetime
import math
import re
import structlog
from typing import Optional
from core.types import (
    Phase, MarketBracket, OrderRequest, OrderSide, OrderBook, OrderBookLevel,
)
from core.constants import WEATHER_CATEGORY, get_eastern_today_date_prefix
from data.ticker_cache import TickerCache
from data.websocket_manager import WebSocketManager
from execution.base import BaseExecutor, ExecutionResult
from app.database import DatabaseManager
from app.config import AppConfig
from app.signing import load_private_key
from app.models import (
    StreamedTicker, StreamedTrade, ExecutedTrade, TradeAction, TradeStatus,
    Position as PositionModel, PortfolioSnapshot,
)
from sqlalchemy import select, delete

MONTH_MAP = {1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"}
MONTH_ORDINAL = {m: i+1 for i, m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}

logger = structlog.get_logger(__name__)


class TemperatureStrategy:
    """
    Core state machine for daily high/low temperature market brackets.

    Phase A: Market Monitoring
    Phase B: Trade Entry (with spread check)
    Phase C: Position Management (Hedge & Stop Loss)
    """

    def __init__(
        self,
        config: AppConfig,
        cache: TickerCache,
        ws_manager: WebSocketManager,
        executor: BaseExecutor,
        db: DatabaseManager,
    ):
        self.config = config
        self.cache = cache
        self.ws = ws_manager
        self.executor = executor
        self.db = db

        # Cached loaded date flags (set of date strings already loaded)
        # State: market_ticker -> MarketBracket
        self.brackets: dict[str, MarketBracket] = {}

        # Active positions we hold
        self.active_positions: dict[str, MarketBracket] = {}

        # Watchlist: markets whose price >= monitor_start
        self.watchlist: dict[str, MarketBracket] = {}

        # Cached private key to avoid repeated file reads
        self._private_key = load_private_key(config.kalshi_private_key_path)

        # Running flag
        self._running = False

        # Per-event hedge state: set of event_tickers that have been hedged at least once.
        # Only events in this set get top-off / break-even logic applied.
        self._hedged_events: set[str] = set()

        # Per-event circuit-breaker: once an event's gross spend would exceed
        # hedge_max_factor * initial_cost, add it here to stop further hedging/top-off.
        self._cap_reached_events: set[str] = set()

        # Per-event armed/deferred hedge state: when no qualifying sibling is
        # available at hedge time, the event is "armed" here.  Cleared once the
        # deferred hedge fills or the event closes.
        self._pending_hedge_events: set[str] = set()
        # Cooldown tracker for the secondary deferred-hedge retry loop
        # (handles events whose original bracket was already stop-lossed).
        self._pending_hedge_last_attempt: dict[str, float] = {}

    async def start(self):
        """Register WebSocket handlers and start the strategy loop."""
        self._running = True

        # Register handlers for WebSocket message types
        self.ws.on_message("ticker", self._handle_ticker)
        self.ws.on_message("trade", self._handle_trade)
        self.ws.on_message("orderbook_snapshot", self._handle_orderbook_snapshot)
        self.ws.on_message("orderbook_delta", self._handle_orderbook_delta)
        self.ws.on_message("market_lifecycle_v2", self._handle_lifecycle)

        # One-time REST discovery at startup to get the full list of existing markets.
        # After this, all updates (new markets, price changes) come via WebSocket.
        active_markets = await self.executor.get_active_markets()
        for m in active_markets:
            ticker = m.get("ticker", "")
            if ticker and ("KXHIGH" in ticker.upper() or "KXLOW" in ticker.upper()):
                if ticker not in self.brackets:
                    self.brackets[ticker] = MarketBracket(
                        market_ticker=ticker,
                        event_ticker=m.get("event_ticker", ""),
                        series_ticker=m.get("series_ticker", ""),
                        bracket_label=m.get("title", ""),
                        phase=Phase.MONITORING,
                    )

        tickers = list(self.brackets.keys())
        logger.info("strategy.discovered_markets", count=len(tickers))

        # Subscribe to ALL markets via WebSocket — no ticker filter so we get
        # price data for every market. New temperature brackets are auto-detected
        # as they appear in the data or via lifecycle events.
        await self.ws.subscribe("orderbook_snapshot")
        await self.ws.subscribe("orderbook_delta")
        await self.ws.subscribe("market_lifecycle_v2")
        await self.ws.subscribe("ticker")
        await self.ws.subscribe("trade")

        # Restore positions BEFORE starting the strategy loop, so we don't
        # attempt to re-buy markets we already hold.
        await self._restore_positions()

        # Start the strategy evaluation loop
        asyncio.create_task(self._strategy_loop())

        logger.info("strategy.started",
                     monitor_start=self.config.monitor_start_price,
                     buy_trigger=self.config.buy_trigger_price,
                     minimum_spread=self.config.minimum_spread,
                     spread_monitor=self.config.spread_monitor_price,
                     hedge_trigger=self.config.hedge_trigger_price,
                     stop_loss=self.config.stop_loss_price,
                     mode=self.config.trading_mode,
                     restored_positions=len(self.active_positions))

        # Start DB cleanup task (runs hourly)
        asyncio.create_task(self._db_cleanup_loop())

    async def _restore_positions(self):
        """
        On startup, re-populate active_positions from the database
        so that position management (hedge, stop-loss) continues
        across restarts.  Also mark restored brackets as crossed_buy
        so the strategy does not attempt to re-enter them.
        """
        async with await self.db.get_session() as session:
            # Only restore positions from the last 3 days (old settled positions
            # cause noise on every restart as they get immediately cleaned up).
            three_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=3)
            result = await session.execute(
                select(PositionModel).where(
                    PositionModel.quantity > 0,
                    PositionModel.position_ts >= three_days_ago
                )
            )
            db_positions = result.scalars().all()
        db_by_ticker = {pos.market_ticker: pos for pos in db_positions}

        # In LIVE mode, also fetch positions directly from Kalshi API
        if self.config.trading_mode == "LIVE":
            try:
                api_positions = await self.executor.get_positions()
                for ticker, pos_data in api_positions.items():
                    # Skip empty/zero-quantity positions
                    qty = int(float(pos_data.get("count", 0)))
                    if qty <= 0:
                        continue
                    bracket = self.brackets.get(ticker)
                    if bracket is None:
                        bracket = MarketBracket(
                            market_ticker=ticker,
                            event_ticker="",
                            series_ticker="",
                            bracket_label="",
                            phase=Phase.HOLDING,
                        )
                        self.brackets[ticker] = bracket
                    bracket.phase = Phase.HOLDING
                    bracket.crossed_buy = True
                    bracket.position_quantity = qty
                    entry = pos_data.get("average_fill_cost_cents", 0) or 0
                    entry_source = "api"
                    if entry <= 0:
                        db_pos = db_by_ticker.get(ticker)
                        db_entry = (db_pos.avg_entry_price or 0) if db_pos else 0
                        if db_entry > 0:
                            entry = db_entry
                            entry_source = "db"
                        else:
                            entry_source = "none"
                    if entry > 0:
                        bracket.avg_entry = entry
                        bracket.last_price = entry
                    elif not bracket.avg_entry or bracket.avg_entry <= 0:
                        bracket.avg_entry = 0
                    self.active_positions[ticker] = bracket
                    logger.info("strategy.restored_live_position", ticker=ticker,
                                qty=qty, entry=bracket.avg_entry, entry_source=entry_source)
            except Exception as e:
                logger.error("strategy.restore_positions_error", error=str(e))

        for pos in db_positions:
            ticker = pos.market_ticker
            bracket = self.brackets.get(ticker)
            if bracket is None:
                bracket = MarketBracket(
                    market_ticker=ticker,
                    event_ticker=pos.event_ticker or "",
                    series_ticker=pos.series_ticker or "",
                    bracket_label="",
                    phase=Phase.HOLDING,
                )
                self.brackets[ticker] = bracket

            bracket.phase = Phase.HOLDING
            bracket.crossed_buy = True
            bracket.position_quantity = pos.quantity
            bracket.avg_entry = pos.avg_entry_price or 0
            bracket.last_price = pos.last_price
            bracket.hedge_market = pos.hedge_market_ticker
            bracket.hedge_quantity = pos.hedge_quantity

            self.active_positions[ticker] = bracket
            logger.info("strategy.restored_position", ticker=ticker,
                        qty=pos.quantity, entry=bracket.avg_entry,
                        hedge_market=bracket.hedge_market)

            # Restore armed/deferred hedge state for this event
            if getattr(pos, 'hedge_pending', 0) == 1 and pos.event_ticker:
                self._pending_hedge_events.add(pos.event_ticker)
                logger.info("strategy.restored_hedge_pending",
                            ticker=ticker, event_ticker=pos.event_ticker)

    async def _ensure_bracket(self, market_ticker: str, event_ticker: str = "", series_ticker: str = "", bracket_label: str = ""):
        """Create a new MarketBracket if the ticker is a temperature market and unknown."""
        if market_ticker in self.brackets:
            return
        today_prefix = get_eastern_today_date_prefix(days_offset=0)
        if today_prefix not in market_ticker:
            return
        # Only track KXHIGH/KXLOW temperature markets
        if not ("KXHIGH" in market_ticker.upper() or "KXLOW" in market_ticker.upper()):
            return
        self.brackets[market_ticker] = MarketBracket(
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
            bracket_label=bracket_label,
            phase=Phase.MONITORING,
        )
        logger.debug("strategy.new_bracket_discovered", ticker=market_ticker, label=bracket_label)

    async def _handle_ticker(self, msg: dict):
        """Process ticker updates from WebSocket."""
        ticker_data = msg.get("msg", msg)
        market_ticker = ticker_data.get("market_ticker") or ticker_data.get("ticker")
        if not market_ticker:
            return

        # Auto-discover new temperature markets
        await self._ensure_bracket(market_ticker)

        last_price_raw = ticker_data.get("last_price")
        # Prefer *_dollars variants (authoritative); fall back to bare fields
        yes_bid_raw = ticker_data.get("yes_bid_dollars") or ticker_data.get("yes_bid")
        yes_ask_raw = ticker_data.get("yes_ask_dollars") or ticker_data.get("yes_ask")

        # Convert dollars to cents
        last_price = round(float(last_price_raw) * 100) if last_price_raw is not None else None
        yes_bid = round(float(yes_bid_raw) * 100) if yes_bid_raw is not None else None
        yes_ask = round(float(yes_ask_raw) * 100) if yes_ask_raw is not None else None

        if last_price is not None:
            self.cache.update_last_price(market_ticker, last_price)

        # Cache YES bid/ask from ticker channel — this is the authoritative price source
        if yes_bid is not None and yes_ask is not None:
            self.cache.update_quote(market_ticker, yes_bid, yes_ask)

        # Update brackets in state
        if market_ticker in self.brackets:
            bracket = self.brackets[market_ticker]
            bracket.last_price = last_price

    async def _handle_trade(self, msg: dict):
        """Process trade updates - log to database."""
        trade_data = msg.get("msg", msg)
        market_ticker = trade_data.get("market_ticker")
        price = trade_data.get("price")
        quantity = trade_data.get("quantity")
        side = trade_data.get("side")
        trade_ts = trade_data.get("ts")

        if not market_ticker or price is None:
            return

        # Update last price from trades too
        self.cache.update_last_price(market_ticker, price)

        # Log to database
        async with await self.db.get_session() as session:
            st = StreamedTrade(
                market_ticker=market_ticker,
                price=price,
                quantity=quantity or 0,
                side=side,
                trade_ts=datetime.datetime.fromtimestamp(trade_ts / 1000) if trade_ts else datetime.datetime.utcnow(),
            )
            session.add(st)
            await session.commit()

    async def _handle_orderbook_snapshot(self, msg: dict):
        """Process orderbook snapshot - initialize cache baseline price."""
        data = msg.get("msg", msg)
        market_ticker = data.get("market_ticker")
        if not market_ticker:
            return
        
        # Auto-discover new temperature markets
        await self._ensure_bracket(market_ticker, bracket_label=data.get("title", ""))
        
        self.cache.update_orderbook_snapshot(market_ticker, data)
        
        ob = self.cache.get_orderbook(market_ticker)
        if ob and ob.best_ask is not None:
            price = ob.best_ask
            self.cache.update_last_price(market_ticker, price)
            
            # Record the initial snapshot price
            if market_ticker in self.brackets:
                bracket = self.brackets[market_ticker]
                bracket.last_price = price

    async def _handle_orderbook_delta(self, msg: dict):
        """Process orderbook delta - update cached price."""
        data = msg.get("msg", msg)
        market_ticker = data.get("market_ticker")
        if not market_ticker:
            return
        
        # Auto-discover new temperature markets
        await self._ensure_bracket(market_ticker)
        
        self.cache.update_orderbook_delta(market_ticker, data)
        
        ob = self.cache.get_orderbook(market_ticker)
        if not ob:
            return
            
        current_price = ob.best_ask
        if current_price is not None:
            self.cache.update_last_price(market_ticker, current_price)
            
            if market_ticker in self.brackets:
                self.brackets[market_ticker].last_price = current_price

    async def _handle_lifecycle(self, msg: dict):
        """Handle market lifecycle events (new markets, status changes)."""
        data = msg.get("msg", msg)
        event_type = data.get("type", "")

        if event_type == "created":
            market_ticker = data.get("market_ticker", "")
            event_ticker = data.get("event_ticker", "")
            series_ticker = data.get("series_ticker", "")
            today_prefix = get_eastern_today_date_prefix(days_offset=0)

            if market_ticker and today_prefix not in market_ticker:
                return

            # Before adding the bracket, check if this is a NEW event
            # that we don't have brackets for yet. If so, fetch ALL of them.
            if event_ticker and series_ticker:
                known_events = {b.event_ticker for b in self.brackets.values() if b.event_ticker}
                if event_ticker not in known_events:
                    import httpx
                    from app.signing import load_private_key, build_auth_headers
                    private_key = load_private_key(self.config.kalshi_private_key_path)
                    headers = build_auth_headers(private_key, self.config.kalshi_api_key, "GET", "/trade-api/v2/markets")
                    url = f"{self.config.rest_base_url}/trade-api/v2/markets"
                    try:
                        async with httpx.AsyncClient(timeout=5.0) as client:
                            resp = await client.get(url, headers=headers, params={"event_ticker": event_ticker, "limit": 100})
                            if resp.status_code in (200, 201):
                                all_markets = resp.json().get("markets", [])
                                count = 0
                                for m in all_markets:
                                    t = m.get("ticker", "")
                                    if t:
                                        existed = t in self.brackets
                                        await self._ensure_bracket(
                                            t,
                                            event_ticker=event_ticker,
                                            series_ticker=series_ticker,
                                            bracket_label=m.get("title", ""),
                                        )
                                        if not existed and t in self.brackets:
                                            count += 1
                                logger.info("strategy.new_event_brackets",
                                            event_ticker=event_ticker, count=count)
                    except Exception as e:
                        logger.error("strategy.new_event_brackets_error",
                                      event_ticker=event_ticker, error=str(e))

            if market_ticker:
                await self._ensure_bracket(
                    market_ticker,
                    event_ticker=event_ticker,
                    series_ticker=series_ticker,
                    bracket_label=data.get("title", ""),
                )

    async def _strategy_loop(self):
        """
        Main strategy evaluation loop runs every ~1 second.
        Evaluates all brackets and transitions phases.
        """
        while self._running:
            try:
                await asyncio.wait_for(self._evaluate_watchlist(), timeout=30.0)
                await asyncio.wait_for(self._evaluate_held_positions(), timeout=30.0)
                await asyncio.wait_for(self._log_periodic_snapshot(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.error("strategy.loop_timeout", msg="A strategy step timed out and was skipped")
            except Exception as e:
                logger.error("strategy.loop_error", error=str(e), exc_info=True)
            await asyncio.sleep(1)

    async def _fetch_live_prices(self, tickers: list[str]) -> dict[str, OrderBook]:
        """
        Get live prices from WebSocket cache.
        Uses orderbook cache (if available) and ticker last_price cache.
        Does NOT make REST calls — that would be too slow/rate-limited.
        """
        results = {}
        if not tickers:
            return results

        for t in tickers:
            ob = self.cache.get_orderbook(t)
            if ob and ob.best_ask is not None and ob.best_bid is not None:
                results[t] = ob
            else:
                # Check ticker cache for a last_price we can use
                lp = self.cache.get_last_price(t)
                if lp and lp > 0:
                    from core.types import OrderBookLevel
                    level = OrderBookLevel(price=lp, quantity=1, order_count=0)
                    results[t] = OrderBook(yes_bids=[level], yes_asks=[level])

        return results

    async def _evaluate_watchlist(self):
        """
        Simple entry check: every cycle, loop all brackets.
        Uses WebSocket ticker quote for prices (primary, instant).
        Falls back to REST for brackets that have no cached quote data.
        Max 5 REST calls per cycle to avoid rate limits.
        """
        rest_calls_this_cycle = 0
        max_rest_per_cycle = 5

        for ticker, bracket in list(self.brackets.items()):
            if bracket.crossed_buy or bracket.phase != Phase.MONITORING:
                continue

            price = None
            spread = None
            rest_data = None

            # Primary source: ticker channel quote (yes_ask as price, yes_ask - yes_bid as spread)
            quote = self.cache.get_quote(ticker)
            if quote is not None:
                yes_bid_q, yes_ask_q = quote
                price = yes_ask_q
                spread = yes_ask_q - yes_bid_q

            # Fallback: REST endpoint
            if price is None and rest_calls_this_cycle < max_rest_per_cycle:
                rest_data = await self._fetch_market_data_via_rest(ticker)
                rest_calls_this_cycle += 1
                if rest_data:
                    if "yes_ask" in rest_data and "yes_bid" in rest_data:
                        price = rest_data["yes_ask"]
                        spread = rest_data["yes_ask"] - rest_data["yes_bid"]
                    elif "yes_ask" in rest_data:
                        price = rest_data["yes_ask"]
                    elif "price" in rest_data:
                        price = rest_data["price"]
                    if spread is None and rest_data and "spread" in rest_data:
                        spread = rest_data["spread"]

            # Skip if we don't have both price (yes_ask) and spread
            if price is None or spread is None:
                continue

            bracket.last_price = price

            # Skip near-dead brackets early (quietly) — they will never reach buy_trigger.
            # Data still flows via WebSocket so hedge/top-off logic is unaffected.
            if price <= self.config.eval_price_floor:
                continue

            if price < self.config.buy_trigger_price:
                logger.debug("phase.b.below_trigger", ticker=ticker, price=price,
                             buy_trigger=self.config.buy_trigger_price)
                continue

            if price > self.config.spread_monitor_price:
                # Price above the maximum we're willing to enter; log and skip
                logger.info("phase.b.missed_entry", ticker=ticker,
                            price=price, max_price=self.config.spread_monitor_price)
                continue

            if spread <= self.config.minimum_spread:
                bracket.crossed_buy = True
                spread_note = "crossed" if spread == 0 else "tight" if spread <= 3 else "normal"
                logger.info("phase.b.buying", ticker=ticker,
                            label=bracket.bracket_label, price=price, spread=spread,
                            spread_note=spread_note)
                await self._execute_entry(bracket)
            else:
                logger.info("phase.b.spread_too_wide", ticker=ticker,
                            price=price, spread=spread)

    async def _execute_entry(self, bracket: MarketBracket, ob: Optional[OrderBook] = None):
        """
        Execute the initial buy order.
        Buy INITIAL_CONTRACT_COUNT at the lowest ask (fetched live from Kalshi API).
        """
        if ob is None:
            prices = await self._fetch_live_prices([bracket.market_ticker])
            ob = prices.get(bracket.market_ticker)
        price = self.config.buy_trigger_price
        if ob and ob.yes_asks:
            price = ob.yes_asks[0].price

        import uuid
        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.BUY_YES,
            price=price,
            quantity=self.config.initial_contract_count,
        )

        # Use spread_monitor_price as max price to ensure quick fill
        max_price = self.config.spread_monitor_price
        result = await self.executor.buy_yes(order, max_price=max_price)

        # Log to database
        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=bracket.market_ticker,
                action=TradeAction.BUY,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.commit()

        if result.success:
            bracket.phase = Phase.HOLDING
            bracket.position_quantity = result.fill_quantity
            bracket.avg_entry = result.fill_price
            self.active_positions[bracket.market_ticker] = bracket
            logger.info("phase.b.entry_filled", ticker=bracket.market_ticker,
                        price=result.fill_price, qty=result.fill_quantity,
                        cost=result.total_cost_cents,
                        active_count=len(self.active_positions),
                        active_keys=list(self.active_positions.keys()))

            # Update positions table (upsert)
            async with await self.db.get_session() as session:
                existing = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = existing.scalar_one_or_none()
                if pos:
                    # Update existing position if found
                    pos.quantity = pos.quantity + result.fill_quantity
                    pos.avg_entry_price = result.fill_price
                    pos.last_price = result.fill_price
                else:
                    # Insert new position
                    pos = PositionModel(
                        market_ticker=bracket.market_ticker,
                        event_ticker=bracket.event_ticker,
                        series_ticker=bracket.series_ticker,
                        side="yes",
                        quantity=result.fill_quantity,
                        avg_entry_price=result.fill_price,
                        last_price=result.fill_price,
                    )
                    session.add(pos)
                
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error("phase.b.entry_db_error", ticker=bracket.market_ticker, error=str(e))
        else:
            bracket.phase = Phase.MONITORING
            logger.warning("phase.b.entry_failed", ticker=bracket.market_ticker,
                           notes=result.notes)

    async def _evaluate_held_positions(self):
        """
        Phase C: Position Management.
        For each held position, get current price from positions API.
        Only trigger hedge/stop-loss on actively priced markets.

        Also runs the secondary recovery loop for armed events whose original
        bracket has already been stop-lossed (no longer in active_positions).
        That loop does not need the positions API, so it runs even when
        active_positions is empty.
        """
        if not self.active_positions and not self._pending_hedge_events:
            return

        api_positions: dict = {}
        if self.active_positions:
            try:
                api_positions = await self.executor.get_positions()
            except Exception as e:
                logger.error("phase.c.get_positions_failed", error=str(e))
                # Fall through to secondary loop; skip main position loop below.
                api_positions = {}

        for ticker, bracket in list(self.active_positions.items()):
            pos_data = api_positions.get(ticker)
            if not pos_data:
                now_ts = asyncio.get_event_loop().time()
                last_seen = getattr(bracket, '_last_seen_in_api', 0)
                if last_seen == 0:
                    # First check for this bracket; record time and skip removal
                    bracket._last_seen_in_api = now_ts
                    continue
                grace = 30  # seconds grace period before considering the position gone
                if now_ts - last_seen < grace:
                    logger.debug("phase.c.position_missing_within_grace", ticker=ticker,
                                 seconds_absent=int(now_ts - last_seen))
                    continue
                # Grace period expired – treat as settled
                logger.warning("phase.c.position_not_in_api_after_grace", ticker=ticker,
                               qty=bracket.position_quantity, phase=bracket.phase.name)
                bracket.phase = Phase.CLOSED
                self.active_positions.pop(ticker, None)
                self.brackets.pop(ticker, None)
                async with await self.db.get_session() as session:
                    await session.execute(
                        delete(PositionModel).where(PositionModel.market_ticker == ticker)
                    )
                    await session.commit()
                continue

            # Mark that we have a live API reading for this position
            bracket._last_seen_in_api = asyncio.get_event_loop().time()

            # Check if Kalshi has already settled this position (count = 0).
            # If Kalshi says we hold zero, the market has closed and the
            # position should be cleaned up.
            api_count = pos_data.get("count", 1)
            if api_count == 0:
                logger.info("phase.c.position_settled", ticker=ticker,
                           qty=bracket.position_quantity, api_count=api_count)
                bracket.phase = Phase.CLOSED
                self.active_positions.pop(ticker, None)
                self.brackets.pop(ticker, None)
                # Also remove from DB
                async with await self.db.get_session() as session:
                    await session.execute(
                        delete(PositionModel).where(PositionModel.market_ticker == ticker)
                    )
                    await session.commit()
                continue

            # Get current price from the authoritative YES bid/ask quote.
            # Priority:
            #   1) cache.get_quote → YES bid (realistic exit for a YES long);
            #      if bid is 0/missing, use YES ask.
            #   2) Positions API last_price_cents.
            #   3) REST _fetch_market_data_via_rest → yes_bid / yes_ask / price
            #      (rate-limited to once per 60 s per ticker).
            #   4) Last known real price (bracket.last_price) if > 0.
            # NEVER use avg_entry or any invented fallback — if no real price is
            # available, skip trading decisions this cycle.
            current_price: Optional[int] = None

            quote = self.cache.get_quote(ticker)
            if quote:
                yes_bid, yes_ask = quote
                if yes_bid and yes_bid > 0:
                    current_price = yes_bid
                elif yes_ask and yes_ask > 0:
                    current_price = yes_ask
            if current_price and current_price > 0:
                bracket.last_price = current_price

            if not current_price or current_price <= 0:
                api_last_price_cents = pos_data.get("last_price_cents")
                if api_last_price_cents and api_last_price_cents > 0:
                    current_price = api_last_price_cents
                    bracket.last_price = current_price

            if not current_price or current_price <= 0:
                # Rate limit: only REST-fetch once per 60 seconds per ticker.
                now = asyncio.get_event_loop().time()
                last_rest = getattr(bracket, '_last_rest_price_fetch', 0)
                if now - last_rest >= 60:
                    bracket._last_rest_price_fetch = now
                    rest_data = await self._fetch_market_data_via_rest(ticker)
                    if rest_data:
                        rest_price = (rest_data.get("yes_bid")
                                      or rest_data.get("yes_ask")
                                      or rest_data.get("price"))
                        if rest_price and rest_price > 0:
                            current_price = rest_price
                            bracket.last_price = current_price

            if not current_price or current_price <= 0:
                # Use last known real price as a final fallback
                if bracket.last_price and bracket.last_price > 0:
                    current_price = bracket.last_price

            if not current_price or current_price <= 0:
                # No real price available — skip trading decisions entirely.
                # Never manufacture a price above the triggers.
                logger.warning("phase.c.no_live_price", ticker=ticker,
                               entry=bracket.avg_entry)
                continue

            # Log price only when it changes from last logged value
            last_logged = getattr(bracket, '_last_logged_price', None)
            if current_price != last_logged:
                bracket._last_logged_price = current_price
                logger.debug("phase.c.price", ticker=ticker,
                            current_price=current_price,
                            entry=bracket.avg_entry,
                            hedge_trigger=self.config.hedge_trigger_price)

            # Check Phase-2 top-off for hedged events (fires at HIGH price, before stop-loss/hedge checks)
            if (bracket.event_ticker in self._hedged_events
                    and bracket.event_ticker not in self._cap_reached_events
                    and bracket.phase == Phase.HOLDING):
                quote = self.cache.get_quote(ticker)
                topoff_ask = quote[1] if quote else None
                if (topoff_ask is not None
                        and topoff_ask >= self.config.buy_trigger_price
                        and topoff_ask <= self.config.spread_monitor_price):
                    # Only top-off when all other event brackets are already closed
                    open_siblings = [t for t, b in self.active_positions.items()
                                     if b.event_ticker == bracket.event_ticker and t != ticker]
                    if not open_siblings:
                        now_topoff = asyncio.get_event_loop().time()
                        last_topoff = getattr(bracket, '_last_topoff_attempt', 0)
                        if now_topoff - last_topoff >= 60:
                            bracket._last_topoff_attempt = now_topoff
                            await self._execute_topoff(bracket, topoff_ask)

            # Check Hedge trigger. Fires when:
            #   a) price has weakened to <= hedge_trigger (initial trigger), OR
            #   b) the event is already armed (hedge_pending) and we are retrying.
            # Guard: never fire if the event already has a filled hedge/recovery — only
            # _execute_topoff may place further buys after the event is hedged.
            hedge_triggered = (
                bracket.event_ticker not in self._hedged_events
                and (
                    (
                        current_price <= self.config.hedge_trigger_price
                        and bracket.phase in (Phase.HOLDING, Phase.HEDGED)
                        and bracket.event_ticker not in self._cap_reached_events
                        and current_price > 0
                    ) or (
                        bracket.event_ticker in self._pending_hedge_events
                        and bracket.phase in (Phase.HOLDING, Phase.HEDGED)
                        and bracket.event_ticker not in self._cap_reached_events
                    )
                )
            )

            hedge_placed = False
            if hedge_triggered:
                # Cooldown: only try hedge once per 60 seconds to prevent spam
                now = asyncio.get_event_loop().time()
                last_attempt = getattr(bracket, '_last_hedge_attempt', 0)
                if now - last_attempt >= 60:
                    bracket._last_hedge_attempt = now
                    logger.info("phase.c.hedge_triggered", ticker=ticker,
                                last_price=current_price,
                                hedge_trigger=self.config.hedge_trigger_price,
                                hedge_pending=(bracket.event_ticker in self._pending_hedge_events))
                    hedge_placed = await self._execute_hedge(bracket)

            # GUARANTEED STOP-LOSS BACKSTOP — fires independently of hedge/arm state.
            # Must run even when the hedge was deferred this cycle.
            if current_price <= self.config.stop_loss_price:
                if bracket.position_quantity <= 0:
                    bracket.phase = Phase.CLOSED
                    self.active_positions.pop(ticker, None)
                    self.brackets.pop(ticker, None)
                    logger.info("phase.c.stop_loss_zero_qty", ticker=ticker)
                    continue
                eval_price_floor = getattr(self.config, "eval_price_floor", 2) or 2
                if current_price <= eval_price_floor:
                    logger.warning("phase.c.stop_loss_skipped_resolved_market",
                                   ticker=ticker,
                                   last_price=current_price,
                                   qty=bracket.position_quantity)
                    continue
                # NOTE: cost-basis guard intentionally removed. A hard stop is an absolute
                # price floor: if price <= stop_loss_price we exit regardless of whether the
                # entry price is known. _execute_stop_loss sells position_quantity at 1¢ and
                # does not require avg_entry.
                logger.warning("phase.c.stop_loss_triggered", ticker=ticker,
                               last_price=current_price, stop_loss=self.config.stop_loss_price)
                await self._execute_stop_loss(bracket)
                continue

            # Only skip further checks this cycle if the hedge actually placed an order
            if hedge_placed:
                continue

        # Secondary retry: attempt deferred hedges for armed events whose original
        # bracket has already been stop-lossed (no longer in active_positions).
        for event_ticker in list(self._pending_hedge_events):
            if event_ticker in self._cap_reached_events:
                continue
            if event_ticker in self._hedged_events:
                continue  # top-off will handle recovery
            if any(b.event_ticker == event_ticker for b in self.active_positions.values()):
                continue  # handled by the main loop above

            # Per-event cooldown for orphaned recovery attempts
            now = asyncio.get_event_loop().time()
            last_recovery = self._pending_hedge_last_attempt.get(event_ticker, 0)
            if now - last_recovery < 60:
                continue
            self._pending_hedge_last_attempt[event_ticker] = now

            # Scan for a qualifying sibling: YES ask strictly > hedge_buy (60¢) AND
            # > eval_price_floor.  A cheap/weak sibling is never the right recovery
            # target — we wait until a real winner emerges (rising above 60¢).
            best_ticker: Optional[str] = None
            best_ask = -1
            best_seen_ask = -1  # track highest seen for logging even if none qualifies
            for t, b in self.brackets.items():
                if b.event_ticker != event_ticker:
                    continue
                q = self.cache.get_quote(t)
                ask = q[1] if q else None
                if ask is None:
                    rest_d = await self._fetch_market_data_via_rest(t)
                    ask = rest_d.get("yes_ask") if rest_d else None
                if ask is not None and ask > 0:
                    if ask > best_seen_ask:
                        best_seen_ask = ask
                    # Qualify only when ask has RISEN above hedge_buy and is above the floor.
                    if ask > self.config.hedge_buy and ask > self.config.eval_price_floor and ask > best_ask:
                        best_ask = ask
                        best_ticker = t

            if best_ticker is None or best_ask <= 0:
                logger.info("phase.c.hedge_deferred", event_ticker=event_ticker,
                            reason="no_qualifying_sibling_above_hedge_buy",
                            best_sibling_price=best_seen_ask if best_seen_ask > 0 else None)
                continue

            # Place recovery buy at initial_contract_count quantity
            recovery_qty = max(self.config.initial_contract_count, 1)
            order = OrderRequest(
                market_ticker=best_ticker,
                side=OrderSide.BUY_YES,
                price=best_ask,
                quantity=recovery_qty,
                is_hedge=True,
            )
            result = await self.executor.buy_yes(order, max_price=self.config.spread_monitor_price)

            async with await self.db.get_session() as session:
                et = ExecutedTrade(
                    market_ticker=best_ticker,
                    action=TradeAction.HEDGE,
                    side="yes",
                    price=result.fill_price,
                    quantity=result.fill_quantity,
                    total_cost_cents=result.total_cost_cents,
                    trade_mode=self.config.trading_mode,
                    status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                    kalshi_order_id=result.order_id or None,
                    notes=result.notes,
                )
                session.add(et)
                await session.commit()

            if result.success:
                # Add recovery bracket as active position
                recovery_bracket = self.brackets.get(best_ticker)
                if recovery_bracket is None:
                    recovery_bracket = MarketBracket(
                        market_ticker=best_ticker,
                        event_ticker=event_ticker,
                        series_ticker="",
                        bracket_label="",
                        phase=Phase.HOLDING,
                    )
                    self.brackets[best_ticker] = recovery_bracket
                recovery_bracket.phase = Phase.HOLDING
                recovery_bracket.crossed_buy = True
                recovery_bracket.position_quantity = result.fill_quantity
                recovery_bracket.avg_entry = result.fill_price
                recovery_bracket.last_price = result.fill_price
                self.active_positions[best_ticker] = recovery_bracket
                self._hedged_events.add(event_ticker)
                self._pending_hedge_events.discard(event_ticker)
                self._pending_hedge_last_attempt.pop(event_ticker, None)

                async with await self.db.get_session() as session:
                    existing_rec = await session.execute(
                        select(PositionModel).where(PositionModel.market_ticker == best_ticker)
                    )
                    rec_pos = existing_rec.scalar_one_or_none()
                    if rec_pos:
                        rec_pos.quantity = rec_pos.quantity + result.fill_quantity
                        rec_pos.avg_entry_price = result.fill_price
                        rec_pos.last_price = result.fill_price
                    else:
                        rec_pos = PositionModel(
                            market_ticker=best_ticker,
                            event_ticker=event_ticker,
                            series_ticker="",
                            side="yes",
                            quantity=result.fill_quantity,
                            avg_entry_price=result.fill_price,
                            last_price=result.fill_price,
                        )
                        session.add(rec_pos)
                    await session.commit()

                logger.info("phase.c.recovery_hedge_filled",
                            event_ticker=event_ticker, recovery_ticker=best_ticker,
                            qty=result.fill_quantity, price=result.fill_price)
            else:
                logger.warning("phase.c.recovery_hedge_failed",
                               event_ticker=event_ticker, notes=result.notes)

    async def _execute_hedge(self, bracket: MarketBracket) -> bool:
        """
        When price drops to HEDGE_TRIGGER_PRICE, buy the highest sibling bracket
        whose YES ask is within HEDGE_BUY (≤ 60¢).  Returns True if an order was
        placed this call, False if the hedge was deferred (event armed) or failed.

        Break-even calculation:
        - We own position_quantity of the current bracket at avg_entry price.
        - If price hits stop loss, we lose: position_quantity * (avg_entry - stop_loss_price)
        - Let P_hedge = price we pay for hedge
        - Each hedge contract pays (100 - P_hedge) profit if hedge resolves Yes.
        - hedge_quantity = ceil(expected_loss / (100 - P_hedge))
        """
        # Find the next highest bracket ticker in the same event
        next_bracket_ticker = await self._find_next_bracket(bracket)
        if not next_bracket_ticker:
            logger.warning("phase.c.hedge_no_bracket_found", ticker=bracket.market_ticker)
            return False

        # Scan all siblings for the highest valid YES ask (ignore price <= eval_price_floor).
        # Use YES ask from ticker-quote cache (authoritative source); fall back to
        # REST yes_ask.  Never use orderbook best_ask (may be empty when NO side
        # is not tracked) and never use any NO-derived value.
        best_sibling_ask = -1
        best_sibling_ticker: Optional[str] = None
        for sibling_ticker, b in self.brackets.items():
            if b.event_ticker != bracket.event_ticker or sibling_ticker == bracket.market_ticker:
                continue
            quote = self.cache.get_quote(sibling_ticker)
            ask = quote[1] if quote else None  # yes_ask_cents from ticker channel
            if ask is None:
                rest_data = await self._fetch_market_data_via_rest(sibling_ticker)
                ask = rest_data.get("yes_ask") if rest_data else None
            # Only consider siblings priced strictly above eval_price_floor (never buy dead
            # brackets at 1¢ etc.).
            if ask is not None and ask > self.config.eval_price_floor and ask > best_sibling_ask:
                best_sibling_ask = ask
                best_sibling_ticker = sibling_ticker

        if best_sibling_ask < self.config.hedge_trigger_price:
            # All siblings are weak (< hedge_trigger_price, e.g. 48¢) or no priced
            # sibling found — there is no credible winner yet.  Arm the event and wait;
            # the secondary recovery loop will buy once a sibling's ask rises > hedge_buy
            # (60¢).  The stop-loss backstop remains fully active regardless.
            self._pending_hedge_events.add(bracket.event_ticker)

            # Persist the armed state to the original bracket's position row so it
            # survives a restart.
            async with await self.db.get_session() as session:
                result_db = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = result_db.scalar_one_or_none()
                if pos:
                    pos.hedge_pending = 1
                    await session.commit()

            logger.info("phase.c.hedge_deferred",
                        ticker=bracket.market_ticker,
                        event_ticker=bracket.event_ticker,
                        best_sibling_price=best_sibling_ask if best_sibling_ask > 0 else None,
                        hedge_buy=self.config.hedge_buy)
            return False

        # Normal hedge: best sibling has a credible ask (>= hedge_trigger_price).
        # Proceed with break-even sizing using the existing formula.
        next_bracket_ticker = best_sibling_ticker
        hedge_price = best_sibling_ask

        # Guard: skip hedging if we don't have a valid entry price
        if not bracket.avg_entry or bracket.avg_entry <= 0:
            logger.warning("phase.c.hedge_no_entry_price",
                           ticker=bracket.market_ticker,
                           qty=bracket.position_quantity)
            return False

        # Calculate expected loss if price continues to stop loss
        expected_loss = bracket.position_quantity * (bracket.avg_entry - self.config.stop_loss_price)

        # Calculate hedge quantity to break even
        hedge_profit_per_contract = 100 - hedge_price
        if hedge_profit_per_contract > 0 and expected_loss > 0:
            raw_hedge_qty = (expected_loss + hedge_profit_per_contract - 1) // hedge_profit_per_contract  # ceiling
        else:
            raw_hedge_qty = bracket.position_quantity  # fallback

        max_by_quantity = bracket.position_quantity
        original_cost = bracket.position_quantity * bracket.avg_entry
        max_by_cost = original_cost // hedge_price if hedge_price > 0 else bracket.position_quantity
        capped_hedge_qty = min(raw_hedge_qty, max_by_quantity, max_by_cost)
        hedge_qty = max(capped_hedge_qty, 1)
        if capped_hedge_qty == raw_hedge_qty:
            cap_reason = "formula"
        elif capped_hedge_qty == max_by_quantity and max_by_quantity <= max_by_cost:
            cap_reason = "quantity"
        else:
            cap_reason = "cost"

        logger.info("phase.c.hedge_quantity_calc",
                    ticker=bracket.market_ticker,
                    expected_loss=expected_loss,
                    hedge_price=hedge_price,
                    raw_qty=raw_hedge_qty,
                    capped_qty=hedge_qty,
                    cap_reason=cap_reason)

        # Circuit-breaker: check per-event spend cap before placing the order
        order_cost = hedge_qty * hedge_price
        ledger = await self._event_ledger(bracket.event_ticker)
        initial_cost = ledger["initial_cost_cents"]
        if initial_cost > 0:
            max_event_spend = int(self.config.hedge_max_factor * initial_cost)
            if ledger["gross_spend_cents"] + order_cost > max_event_spend:
                self._cap_reached_events.add(bracket.event_ticker)
                logger.warning("phase.c.hedge_cap_reached",
                               event_ticker=bracket.event_ticker,
                               gross_spend_cents=ledger["gross_spend_cents"],
                               max_event_spend_cents=max_event_spend,
                               attempted_spend=order_cost)
                return False

        order = OrderRequest(
            market_ticker=next_bracket_ticker,
            side=OrderSide.BUY_YES,
            price=hedge_price,
            quantity=hedge_qty,
            is_hedge=True,
        )

        result = await self.executor.buy_yes(order, max_price=self.config.spread_monitor_price)

        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=next_bracket_ticker,
                action=TradeAction.HEDGE,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.commit()

        if result.success:
            bracket.phase = Phase.HEDGED
            bracket.hedge_market = next_bracket_ticker
            bracket.hedge_quantity = result.fill_quantity

            # Register this event as hedged so the ledger/top-off logic activates
            self._hedged_events.add(bracket.event_ticker)
            # Clear any armed/deferred state now that the hedge has filled
            self._pending_hedge_events.discard(bracket.event_ticker)

            # Add the hedge bracket as its own position in active_positions
            # so it gets stop-loss monitoring and portfolio tracking.
            hedge_bracket = self.brackets.get(next_bracket_ticker)
            if hedge_bracket is None:
                hedge_bracket = MarketBracket(
                    market_ticker=next_bracket_ticker,
                    event_ticker=bracket.event_ticker,
                    series_ticker=bracket.series_ticker,
                    bracket_label="",
                    phase=Phase.HOLDING,
                )
                self.brackets[next_bracket_ticker] = hedge_bracket
            hedge_bracket.phase = Phase.HOLDING
            hedge_bracket.crossed_buy = True
            hedge_bracket.position_quantity = result.fill_quantity
            hedge_bracket.avg_entry = result.fill_price
            hedge_bracket.last_price = result.fill_price
            self.active_positions[next_bracket_ticker] = hedge_bracket

            logger.info("phase.c.hedge_filled", ticker=bracket.market_ticker,
                        hedge_ticker=next_bracket_ticker, hedge_qty=result.fill_quantity,
                        hedge_price=result.fill_price)

            # Update positions table (original position's hedge fields)
            # Also persist the hedge position itself so it survives restarts.
            async with await self.db.get_session() as session:
                result_db = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = result_db.scalar_one_or_none()
                if pos:
                    pos.hedge_market_ticker = next_bracket_ticker
                    pos.hedge_quantity = result.fill_quantity
                    pos.hedge_pending = 0  # clear deferred state on successful fill

                # Upsert hedge position row so it survives restarts
                existing_hedge = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == next_bracket_ticker)
                )
                hedge_pos = existing_hedge.scalar_one_or_none()
                if hedge_pos:
                    hedge_pos.quantity = hedge_pos.quantity + result.fill_quantity
                    hedge_pos.avg_entry_price = result.fill_price
                    hedge_pos.last_price = result.fill_price
                else:
                    hedge_pos = PositionModel(
                        market_ticker=next_bracket_ticker,
                        event_ticker=bracket.event_ticker,
                        series_ticker=bracket.series_ticker,
                        side="yes",
                        quantity=result.fill_quantity,
                        avg_entry_price=result.fill_price,
                        last_price=result.fill_price,
                    )
                    session.add(hedge_pos)

                await session.commit()

            return True
        else:
            logger.warning("phase.c.hedge_failed", ticker=bracket.market_ticker,
                           notes=result.notes)
            return False

    async def _execute_stop_loss(self, bracket: MarketBracket):
        """Execute a stop-loss: sell position at market (1¢) to guarantee fill."""
        now = asyncio.get_event_loop().time()
        last_attempt = getattr(bracket, '_last_stop_loss_attempt', 0)
        if now - last_attempt < 60:
            return
        bracket._last_stop_loss_attempt = now

        price = 1  # Sell at minimum price to guarantee fill at stop loss

        import uuid
        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.SELL_YES,
            price=price,
            quantity=bracket.position_quantity,
        )

        result = await self.executor.sell_yes(order)

        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=bracket.market_ticker,
                action=TradeAction.STOP_LOSS,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.commit()

        live_count = bracket.position_quantity
        try:
            positions = await self.executor.get_positions()
            live_position = positions.get(bracket.market_ticker)
            if not live_position:
                live_count = 0
            else:
                live_count = max(int(live_position.get("count", 0) or 0), 0)
        except Exception as e:
            logger.warning("phase.c.stop_loss_verify_failed", ticker=bracket.market_ticker, error=str(e))
            live_count = max(bracket.position_quantity - result.fill_quantity, 0)

        if live_count == 0:
            # Remove from positions table
            async with await self.db.get_session() as session:
                await session.execute(
                    delete(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                await session.commit()
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            logger.info("phase.c.stop_loss_executed", ticker=bracket.market_ticker,
                        price=result.fill_price, proceeds=-result.total_cost_cents)
            # If the event was armed (hedge_pending), keep it in the in-memory set
            # even after the original is removed — a recovery bracket may still be
            # bought at HEDGE_BUY (60¢) on a subsequent cycle via the secondary loop.
            # The DB row is gone, so in-memory retention is the only state source now.
        else:
            bracket.position_quantity = live_count
            logger.warning(
                "phase.c.stop_loss_partial_or_unfilled",
                ticker=bracket.market_ticker,
                attempted_qty=order.quantity,
                filled_qty=result.fill_quantity,
                remaining_count=live_count,
                notes=result.notes,
                last_price=bracket.last_price,
            )

    async def _event_ledger(self, event_ticker: str) -> dict:
        """
        Build a cash ledger for an event from executed_trades.

        Returns a dict with:
          - initial_cost_cents: total_cost_cents of the first BUY for this event
          - gross_spend_cents: sum of total_cost_cents for BUY and HEDGE actions
          - stop_loss_proceeds_cents: sum of proceeds from STOP_LOSS actions
          - open_tickers: set of market tickers still in active_positions
          - closed_tickers: set of market tickers known for this event but not open
        """
        # Collect all known market tickers for this event from in-memory state
        event_market_tickers = [
            t for t, b in self.brackets.items()
            if b.event_ticker == event_ticker
        ]
        for t, b in self.active_positions.items():
            if b.event_ticker == event_ticker and t not in event_market_tickers:
                event_market_tickers.append(t)

        if not event_market_tickers:
            return {
                "initial_cost_cents": 0,
                "gross_spend_cents": 0,
                "stop_loss_proceeds_cents": 0,
                "open_tickers": set(),
                "closed_tickers": set(),
            }

        async with await self.db.get_session() as session:
            result = await session.execute(
                select(ExecutedTrade).where(
                    ExecutedTrade.market_ticker.in_(event_market_tickers)
                ).order_by(ExecutedTrade.executed_at)
            )
            trades = result.scalars().all()

        initial_cost_cents = 0
        gross_spend_cents = 0
        stop_loss_proceeds_cents = 0
        first_buy_seen = False

        for trade in trades:
            if trade.action in (TradeAction.BUY, TradeAction.HEDGE):
                cost = trade.total_cost_cents or 0
                gross_spend_cents += cost
                if not first_buy_seen and trade.action == TradeAction.BUY:
                    initial_cost_cents = cost
                    first_buy_seen = True
            elif trade.action == TradeAction.STOP_LOSS:
                # total_cost_cents for a sell is negative (proceeds returned);
                # abs() gives the cash received.
                stop_loss_proceeds_cents += abs(trade.total_cost_cents or 0)

        open_tickers = set(self.active_positions.keys()) & set(event_market_tickers)
        closed_tickers = set(event_market_tickers) - open_tickers

        return {
            "initial_cost_cents": initial_cost_cents,
            "gross_spend_cents": gross_spend_cents,
            "stop_loss_proceeds_cents": stop_loss_proceeds_cents,
            "open_tickers": open_tickers,
            "closed_tickers": closed_tickers,
        }

    async def _execute_topoff(self, bracket: MarketBracket, yes_ask: int):
        """
        Phase 2: Top-off the surviving bracket so the event reaches break-even.

        Sizes from the per-event ledger:
          remaining_deficit = gross_spend_cents - (Q_current * 100)
          topoff_qty = ceil(remaining_deficit / (100 - yes_ask))

        Only fires when all other brackets in the event are closed and the
        event's gross spend is not already covered.
        """
        event_ticker = bracket.event_ticker
        ledger = await self._event_ledger(event_ticker)

        gross_spend = ledger["gross_spend_cents"]
        q_current = bracket.position_quantity
        remaining_deficit = gross_spend - (q_current * 100)

        if remaining_deficit <= 0:
            logger.debug("phase.c.topoff_already_break_even",
                         event_ticker=event_ticker,
                         gross_spend=gross_spend,
                         q_current=q_current)
            return

        profit_per_contract = 100 - yes_ask
        if profit_per_contract <= 0:
            logger.warning("phase.c.topoff_price_too_high",
                           event_ticker=event_ticker,
                           ticker=bracket.market_ticker,
                           yes_ask=yes_ask)
            return

        topoff_qty = math.ceil(remaining_deficit / profit_per_contract)

        # Circuit-breaker check
        order_cost = topoff_qty * yes_ask
        initial_cost = ledger["initial_cost_cents"]
        if initial_cost > 0:
            max_event_spend = int(self.config.hedge_max_factor * initial_cost)
            if gross_spend + order_cost > max_event_spend:
                self._cap_reached_events.add(event_ticker)
                logger.warning("phase.c.hedge_cap_reached",
                               event_ticker=event_ticker,
                               gross_spend_cents=gross_spend,
                               max_event_spend_cents=max_event_spend,
                               attempted_spend=order_cost)
                return

        logger.info("phase.c.topoff_triggered",
                    ticker=bracket.market_ticker,
                    event_ticker=event_ticker,
                    gross_spend=gross_spend,
                    q_current=q_current,
                    remaining_deficit=remaining_deficit,
                    topoff_qty=topoff_qty,
                    yes_ask=yes_ask)

        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.BUY_YES,
            price=yes_ask,
            quantity=topoff_qty,
        )

        result = await self.executor.buy_yes(order, max_price=self.config.spread_monitor_price)

        async with await self.db.get_session() as session:
            et = ExecutedTrade(
                market_ticker=bracket.market_ticker,
                action=TradeAction.BUY,
                side="yes",
                price=result.fill_price,
                quantity=result.fill_quantity,
                total_cost_cents=result.total_cost_cents,
                trade_mode=self.config.trading_mode,
                status=TradeStatus.FILLED if result.success else TradeStatus.REJECTED,
                kalshi_order_id=result.order_id or None,
                notes=result.notes,
            )
            session.add(et)
            await session.commit()

        if result.success:
            old_qty = bracket.position_quantity
            bracket.position_quantity = old_qty + result.fill_quantity
            if bracket.position_quantity > 0:
                bracket.avg_entry = (
                    (bracket.avg_entry * old_qty + result.fill_price * result.fill_quantity)
                    // bracket.position_quantity
                )

            logger.info("phase.c.topoff_filled",
                        ticker=bracket.market_ticker,
                        qty=result.fill_quantity,
                        price=result.fill_price,
                        new_qty=bracket.position_quantity)

            # Sync updated quantity/price to positions table
            async with await self.db.get_session() as session:
                existing = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = existing.scalar_one_or_none()
                if pos:
                    pos.quantity = bracket.position_quantity
                    pos.avg_entry_price = bracket.avg_entry
                    pos.last_price = result.fill_price
                    await session.commit()
        else:
            logger.warning("phase.c.topoff_failed",
                           ticker=bracket.market_ticker,
                           notes=result.notes)

    async def _find_next_bracket(self, bracket: MarketBracket) -> Optional[str]:
        """
        Find the next highest bracket in the same event/series by parsing the
        market ticker. Ticker format: KXLOWTCITY-YYMMDD-T## (less than ##°) or
        KXLOWTCITY-YYMMDD-B##.# (between ## and ##+1°).
        """
        ticker = bracket.market_ticker
        event_ticker = bracket.event_ticker

        # Extract the bracket type (T or B) and temperature value from ticker
        match = re.match(r'^(KXHIGHT|KXLOWT)(.+)-(\d{2}[A-Z]{3}\d{2})-(T\d+|B\d+\.?\d*)$', ticker)
        if not match:
            logger.warning("strategy.cannot_parse_ticker",
                          ticker=ticker, event_ticker=event_ticker)
            return None

        prefix, city, date_str, bracket_code = match.groups()
        bracket_type = bracket_code[0]  # 'T' or 'B'
        value_str = bracket_code[1:]     # numeric part

        try:
            if bracket_type == 'T':
                current_val = int(float(value_str))
                next_val = current_val + 1
                next_code = f"T{next_val}"
            else:
                # "B" values like 81.5 = between 81 and 82
                current_val = float(value_str)
                next_val = current_val + 1.0
                if next_val == int(next_val):
                    next_code = f"T{int(next_val)}"
                else:
                    next_code = f"B{next_val:.1f}"
        except (ValueError, TypeError):
            logger.warning("strategy.cannot_parse_bracket_value",
                          ticker=ticker, bracket_code=bracket_code)
            return None

        next_ticker = f"{prefix}{city}-{date_str}-{next_code}"
        if next_ticker in self.brackets:
            return next_ticker

        # Fallback: find any bracket in same event with a higher value
        candidates = []
        for t, b in self.brackets.items():
            if b.event_ticker == event_ticker and t != ticker:
                m = re.match(r'^(KXHIGHT|KXLOWT)(.+)-(\d{2}[A-Z]{3}\d{2})-(T\d+|B\d+\.?\d*)$', t)
                if m:
                    bc = m.group(4)
                    try:
                        if bc[0] == 'T':
                            cand_val = int(bc[1:])
                        else:
                            cand_val = float(bc[1:])
                        candidates.append((cand_val, t))
                    except ValueError:
                        continue

        if candidates:
            try:
                current_numeric = int(float(value_str)) if bracket_type == 'T' else float(value_str)
            except ValueError:
                return None
            candidates.sort()
            for cand_val, cand_ticker in candidates:
                if cand_val > current_numeric:
                    logger.info("strategy.next_bracket_found",
                               ticker=ticker, next_ticker=cand_ticker,
                               next_val=cand_val)
                    return cand_ticker

        logger.warning("strategy.next_bracket_not_found",
                       ticker=ticker, event_ticker=event_ticker)
        return None

    async def _fetch_market_data_via_rest(self, ticker: str) -> Optional[dict]:
        """
        Fetch current market data via Kalshi REST /markets/{ticker} endpoint.
        Returns dict with price, yes_ask, yes_bid, spread in cents, or None.
        """
        import httpx
        from app.signing import build_auth_headers
        markets_path = f"/trade-api/v2/markets/{ticker}"
        markets_url = f"{self.config.rest_base_url}{markets_path}"
        try:
            rest_headers = build_auth_headers(self._private_key, self.config.kalshi_api_key, "GET", markets_path)
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(markets_url, headers=rest_headers)
                if resp.status_code == 200:
                    mkt = resp.json().get("market", {})
                    result = {}

                    ya = mkt.get("yes_ask_dollars") or mkt.get("yes_ask")
                    if ya and float(ya) > 0:
                        result["yes_ask"] = round(float(ya) * 100)

                    yb = mkt.get("yes_bid_dollars") or mkt.get("yes_bid")
                    if yb and float(yb) > 0:
                        result["yes_bid"] = round(float(yb) * 100)

                    lp = mkt.get("last_price_dollars") or mkt.get("last_price")
                    if lp and float(lp) > 0:
                        result["price"] = round(float(lp) * 100)
                    elif "yes_ask" in result:
                        result["price"] = result["yes_ask"]

                    if "yes_ask" in result and "yes_bid" in result:
                        result["spread"] = result["yes_ask"] - result["yes_bid"]

                    return result if result else None
        except Exception as e:
            logger.warning("rest.fetch_failed", ticker=ticker, error=str(e))
        return None

    async def _log_periodic_snapshot(self):
        """Log portfolio snapshot every 60 seconds."""
        if not hasattr(self, '_snapshot_counter'):
            self._snapshot_counter = 0
        self._snapshot_counter += 1
        if self._snapshot_counter % 60 != 0:  # ~60 seconds
            return

        balance = await self.executor.get_balance()
        total_risk = sum(
            b.position_quantity * (100 - (b.last_price or 0))
            for b in self.active_positions.values()
        )

        async with await self.db.get_session() as session:
            ps = PortfolioSnapshot(
                cash_balance_cents=balance,
                total_positions=len(self.active_positions),
                total_risk_cents=total_risk,
            )
            session.add(ps)
            await session.commit()

        # Sync in-memory prices to DB positions
        async with await self.db.get_session() as session:
            for ticker, bracket in self.active_positions.items():
                if bracket.last_price is not None:
                    result = await session.execute(
                        select(PositionModel).where(PositionModel.market_ticker == ticker)
                    )
                    pos = result.scalar_one_or_none()
                    if pos:
                        pos.last_price = bracket.last_price
            await session.commit()

        logger.info("strategy.snapshot", balance=balance,
                    positions=len(self.active_positions), risk=total_risk,
                    active_tickers=list(self.active_positions.keys()),
                    total_brackets=len(self.brackets),
                    bracket_count_by_series={
                        'KXHIGHT': len([t for t in self.brackets if 'HIGHT' in t or 'HIGH' in t]),
                        'KXLOWT': len([t for t in self.brackets if 'LOWT' in t or 'LOW' in t]),
                    },
                    phase_details={t: (b.phase.name, b.crossed_buy) for t, b in self.brackets.items() if b.phase != Phase.MONITORING or b.crossed_buy})

    async def _db_cleanup_loop(self):
        """Delete old trades every hour to prevent disk bloat."""
        while self._running:
            try:
                async with await self.db.get_session() as session:
                    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
                    
                    # Delete trades older than 24 hours
                    await session.execute(
                        delete(StreamedTrade).where(StreamedTrade.trade_ts < cutoff)
                    )
                    
                    await session.commit()
                    logger.info("db.cleanup", hours_retained=24)
            except Exception as e:
                logger.error("db.cleanup_error", error=str(e))
            
            await asyncio.sleep(3600)  # run every hour

    async def stop(self):
        self._running = False
        logger.info("strategy.stopped")
