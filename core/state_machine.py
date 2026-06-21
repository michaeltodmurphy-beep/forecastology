# core/state_machine.py
import asyncio
import datetime
import re
import structlog
from typing import Optional
from core.types import (
    Phase, MarketBracket, OrderRequest, OrderSide, OrderBook, OrderBookLevel,
)
from core.constants import WEATHER_CATEGORY
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
                    # If we got a real entry price from the API, use it;
                    # otherwise the DB restore will fill it in below
                    if entry > 0:
                        bracket.avg_entry = entry
                        bracket.last_price = entry
                    logger.info("strategy.restored_live_position", ticker=ticker,
                                qty=qty, entry=entry)
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

    async def _ensure_bracket(self, market_ticker: str, event_ticker: str = "", series_ticker: str = "", bracket_label: str = ""):
        """Create a new MarketBracket if the ticker is a temperature market and unknown."""
        if market_ticker in self.brackets:
            return
        # Only track KXHIGH/KXLOW temperature markets
        if not ("KXHIGH" in market_ticker.upper() or "KXLOW" in market_ticker.upper()):
            return
        # Only track today's markets — tomorrow's haven't settled yet
        # Ticker format: KXHIGH|KXLOW+CITY-DDMMMYY-T## or B##.#
        parts = market_ticker.split('-')
        if len(parts) >= 2:
            date_str = parts[1]
            # Parse date like 26JUN19
            import datetime
            months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
            try:
                year = 2000 + int(date_str[:2])
                month = months.get(date_str[2:5], 0)
                day = int(date_str[5:])
                ticker_date = datetime.date(year, month, day)
                today = datetime.date.today()
                if ticker_date != today:
                    return  # Skip non-today markets
            except (ValueError, IndexError):
                pass
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
        yes_bid_raw = ticker_data.get("yes_bid")
        yes_ask_raw = ticker_data.get("yes_ask")

        # Convert dollars to cents
        last_price = round(float(last_price_raw) * 100) if last_price_raw is not None else None
        yes_bid = round(float(yes_bid_raw) * 100) if yes_bid_raw is not None else None
        yes_ask = round(float(yes_ask_raw) * 100) if yes_ask_raw is not None else None

        if last_price is not None:
            self.cache.update_last_price(market_ticker, last_price)

        # Log ticker to database
        async with await self.db.get_session() as session:
            st = StreamedTicker(
                market_ticker=market_ticker,
                last_price=last_price,
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                ticker_ts=datetime.datetime.utcnow(),
            )
            session.add(st)
            await session.commit()

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
            if market_ticker:
                await self._ensure_bracket(
                    market_ticker,
                    event_ticker=data.get("event_ticker", ""),
                    series_ticker=data.get("series_ticker", ""),
                    bracket_label=data.get("title", ""),
                )

    async def _strategy_loop(self):
        """
        Main strategy evaluation loop runs every ~1 second.
        Evaluates all brackets and transitions phases.
        """
        while self._running:
            try:
                await self._evaluate_watchlist()       # Phase A -> B
                await self._evaluate_held_positions()  # Phase C
                await self._log_periodic_snapshot()
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
        Uses WebSocket ticker/orderbook cache for prices (instant).
        If price >= buy_trigger and spread <= minimum_spread,
        and we haven't already bought this market -> buy.
        No REST calls — if cache doesn't have price data, skip.
        """
        for ticker, bracket in list(self.brackets.items()):
            if bracket.crossed_buy or bracket.phase != Phase.MONITORING:
                continue

            # Use WebSocket cache only (fast, no REST calls)
            ob = self.cache.get_orderbook(ticker)
            price = ob.best_ask if ob and ob.best_ask is not None else None
            spread = ob.spread if ob and ob.spread is not None else 0

            if price is None:
                # Check ticker cache (also from WebSocket)
                price = self.cache.get_last_price(ticker)

            if price is None:
                continue  # No cache data yet, try next cycle

            bracket.last_price = price

            if price < self.config.buy_trigger_price:
                continue

            if price > self.config.spread_monitor_price:
                bracket.crossed_buy = True
                logger.info("phase.b.missed_entry", ticker=ticker,
                            price=price, max_price=self.config.spread_monitor_price)
                continue

            if spread <= self.config.minimum_spread:
                bracket.crossed_buy = True
                logger.info("phase.b.buying", ticker=ticker,
                            label=bracket.bracket_label, price=price, spread=spread)
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
        """
        if not self.active_positions:
            return

        try:
            api_positions = await self.executor.get_positions()
        except Exception:
            return

        for ticker, bracket in list(self.active_positions.items()):
            pos_data = api_positions.get(ticker)
            if not pos_data:
                logger.warning("phase.c.position_not_in_api", ticker=ticker,
                               qty=bracket.position_quantity, phase=bracket.phase.name)
                bracket.phase = Phase.CLOSED
                self.active_positions.pop(ticker, None)
                self.brackets.pop(ticker, None)
                continue

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

            # Get current price from Kalshi positions API response or WebSocket ticker cache.
            # Priority: 1) Ticker last_price (most reliable for thin/closed markets)
            #           2) Positions API last_price (fallback)
            #           3) Orderbook best_ask (for actively traded markets)
            #           4) Last known price (stale, don't trigger trading)
            ticker_last_price = self.cache.get_last_price(ticker)
            api_last_price_cents = pos_data.get("last_price_cents")

            if ticker_last_price and ticker_last_price > 0:
                current_price = ticker_last_price
                bracket.last_price = current_price
            elif api_last_price_cents and api_last_price_cents > 0:
                current_price = api_last_price_cents
                bracket.last_price = current_price
            else:
                # Try REST market endpoint directly — it has price data even
                # when positions API and WebSocket don't.
                # Rate limit: only REST-fetch once per 60 seconds per ticker.
                now = asyncio.get_event_loop().time()
                last_rest = getattr(bracket, '_last_rest_price_fetch', 0)
                if now - last_rest >= 60:
                    bracket._last_rest_price_fetch = now
                    ob = await self._fetch_market_price_via_rest(ticker)
                else:
                    ob = None
                if ob is not None:
                    current_price = ob
                    bracket.last_price = current_price
                elif self.cache.get_orderbook(ticker) and self.cache.get_orderbook(ticker).best_ask is not None:
                    current_price = self.cache.get_orderbook(ticker).best_ask
                    bracket.last_price = current_price
                else:
                    current_price = bracket.last_price if bracket.last_price and bracket.last_price > 0 else (bracket.avg_entry or 83)

            bracket.last_price = current_price

            # Log price only when it changes from last logged value
            last_logged = getattr(bracket, '_last_logged_price', None)
            if current_price != last_logged:
                bracket._last_logged_price = current_price
                logger.debug("phase.c.price", ticker=ticker,
                            current_price=current_price,
                            ticker_price=ticker_last_price,
                            api_price=api_last_price_cents,
                            entry=bracket.avg_entry,
                            hedge_trigger=self.config.hedge_trigger_price)

            # Check Hedge trigger FIRST (higher price threshold — triggers before stop loss)
            if (current_price <= self.config.hedge_trigger_price
                    and bracket.phase == Phase.HOLDING
                    and not bracket.hedge_market
                    and current_price > 0):
                # Cooldown: only try hedge once per 60 seconds to prevent spam
                now = asyncio.get_event_loop().time()
                last_attempt = getattr(bracket, '_last_hedge_attempt', 0)
                if now - last_attempt < 60:
                    continue
                bracket._last_hedge_attempt = now
                logger.info("phase.c.hedge_triggered", ticker=ticker,
                            last_price=current_price, hedge_trigger=self.config.hedge_trigger_price)
                await self._execute_hedge(bracket)
                if bracket.phase == Phase.HEDGED:
                    continue

            # Check Stop Loss (lower price threshold — only if hedge already placed or price collapsed)
            if current_price <= self.config.stop_loss_price:
                if bracket.position_quantity <= 0:
                    bracket.phase = Phase.CLOSED
                    self.active_positions.pop(ticker, None)
                    self.brackets.pop(ticker, None)
                    logger.info("phase.c.stop_loss_zero_qty", ticker=ticker)
                    continue
                logger.warning("phase.c.stop_loss_triggered", ticker=ticker,
                               last_price=current_price, stop_loss=self.config.stop_loss_price)
                await self._execute_stop_loss(bracket)
                continue

    async def _execute_hedge(self, bracket: MarketBracket):
        """
        When price drops to HEDGE_TRIGGER_PRICE, buy the next highest bracket
        at the lowest available ask. Calculate quantity to achieve break-even
        if the hedge bracket resolves to Yes, accounting for expected stop loss.

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
            return

        # Find the highest-priced bracket ticker in the same event (most likely to win).
        # Scan ALL brackets in this event (including crossed_buy ones) for the highest ask.
        best_price = -1
        for ticker, b in self.brackets.items():
            if b.event_ticker == bracket.event_ticker and ticker != bracket.market_ticker:
                ob = self.cache.get_orderbook(ticker)
                ask = ob.best_ask if ob and ob.best_ask is not None else None
                if ask is None:
                    # Try REST for this market
                    ask = await self._fetch_market_price_via_rest(ticker)
                if ask is not None and ask > best_price:
                    best_price = ask
                    next_bracket_ticker = ticker

        if best_price <= 0:
            logger.warning("phase.c.hedge_no_priced_bracket", ticker=bracket.market_ticker,
                          event_ticker=bracket.event_ticker)
            return

        hedge_price = best_price

        # CRITICAL: Verify hedge ask price is above stop loss price
        # If the hedge market is already below stop loss level, hedging is pointless
        if hedge_price <= self.config.stop_loss_price:
            logger.warning("phase.c.hedge_price_below_stop_loss",
                          ticker=bracket.market_ticker,
                          hedge_ticker=next_bracket_ticker,
                          hedge_price=hedge_price,
                          stop_loss=self.config.stop_loss_price)
            return


        # Calculate expected loss if price continues to stop loss
        expected_loss = bracket.position_quantity * (bracket.avg_entry - self.config.stop_loss_price)
        
        # Calculate hedge quantity to break even
        hedge_profit_per_contract = 100 - hedge_price
        if hedge_profit_per_contract > 0 and expected_loss > 0:
            hedge_qty = (expected_loss + hedge_profit_per_contract - 1) // hedge_profit_per_contract  # ceiling
        else:
            hedge_qty = bracket.position_quantity  # fallback
        import uuid
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

            # Update positions table
            async with await self.db.get_session() as session:
                result_db = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = result_db.scalar_one_or_none()
                if pos:
                    pos.hedge_market_ticker = next_bracket_ticker
                    pos.hedge_quantity = result.fill_quantity
                    await session.commit()
        else:
            logger.warning("phase.c.hedge_failed", ticker=bracket.market_ticker,
                           notes=result.notes)

    async def _execute_stop_loss(self, bracket: MarketBracket):
        """Execute a stop-loss: sell position at market (1¢) to guarantee fill."""
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

            # Remove from positions table
            await session.execute(
                delete(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
            )
            await session.commit()

        if result.success:
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            logger.info("phase.c.stop_loss_executed", ticker=bracket.market_ticker,
                        price=result.fill_price, proceeds=-result.total_cost_cents)
        else:
            # Mark as CLOSED anyway so we stop spamming stop-loss attempts
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            logger.warning("phase.c.stop_loss_failed_closed", ticker=bracket.market_ticker,
                           notes=result.notes, last_price=bracket.last_price)

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

    async def _fetch_market_price_via_rest(self, ticker: str) -> Optional[int]:
        """
        Fetch current market price via Kalshi REST /markets/{ticker} endpoint.
        Returns price in cents, or None if unavailable.
        Uses last_price_dollars, yes_ask, or no_ask to derive a YES price.
        """
        import httpx
        from app.signing import build_auth_headers
        markets_path = f"/trade-api/v2/markets/{ticker}"
        markets_url = f"{self.config.rest_base_url}{markets_path}"
        try:
            rest_headers = build_auth_headers(self._private_key, self.config.kalshi_api_key, "GET", markets_path)
            async with httpx.AsyncClient() as client:
                resp = await client.get(markets_url, headers=rest_headers)
                if resp.status_code == 200:
                    mkt = resp.json().get("market", {})
                    # Try last_price_dollars first (most reliable for thin markets)
                    lp = mkt.get("last_price_dollars")
                    if lp and float(lp) > 0:
                        return round(float(lp) * 100)
                    # Then try yes_ask (may be empty string, not None)
                    ya = mkt.get("yes_ask")
                    if ya and float(ya) > 0:
                        return round(float(ya) * 100)
                    # Finally derive from no_ask: YES price = 100 - NO_ask
                    na = mkt.get("no_ask")
                    if na and float(na) > 0:
                        return 100 - round(float(na) * 100)
        except Exception:
            pass
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
        """Delete old streaming data every hour to prevent disk bloat."""
        while self._running:
            try:
                async with await self.db.get_session() as session:
                    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
                    
                    # Delete tickers older than 24 hours
                    await session.execute(
                        delete(StreamedTicker).where(StreamedTicker.ticker_ts < cutoff)
                    )
                    
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
