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
from app.signing import build_auth_headers, load_private_key
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
        self._loaded_dates: set[str] = set()

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

        # Get active markets first
        active_markets = await self.executor.get_active_markets()
        # The executor already filters by KXHIGHT/KXLOWT series_tickers
        temp_markets = active_markets
        
        logger.info("strategy.fetching_active",
                     temp_markets_found=len(temp_markets),
                     active_markets_fetched=len(active_markets))
        
        tickers = []
        # Derive today's Kalshi date format (e.g., Jun 12 -> 26JUN12)
        today = datetime.datetime.utcnow()
        today_str = f"{str(today.year)[-2:]}{MONTH_MAP[today.month]}{today.day}"
        
        for m in temp_markets:
            ticker = m.get("ticker", "")
            if ticker:
                # Only today's markets
                if today_str not in ticker:
                    continue
                tickers.append(ticker)
                if ticker not in self.brackets:
                    bracket = MarketBracket(
                        market_ticker=ticker,
                        event_ticker=m.get("event_ticker", ""),
                        series_ticker=m.get("series_ticker", ""),
                        bracket_label=m.get("title", ""),
                        phase=Phase.MONITORING,
                    )
                    self.brackets[ticker] = bracket

        self._current_date_str = today_str
        logger.info("strategy.fetching_active", temp_markets_found=len(tickers))

        # Subscribe to orderbook deltas for price movement tracking
        # Snapshot subscription is needed to initialize the orderbook cache
        await self.ws.subscribe("orderbook_snapshot", tickers)
        await self.ws.subscribe("orderbook_delta", tickers)
        # Also subscribe to market lifecycle for new markets
        await self.ws.subscribe("market_lifecycle_v2")
        # Subscribe to ticker/trade for any sporadic updates
        await self.ws.subscribe("ticker", tickers)
        await self.ws.subscribe("trade", tickers)

        # Start the strategy evaluation loop
        asyncio.create_task(self._strategy_loop())

        # Restore previously-held positions from the database on startup.
        await self._restore_positions()

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
        
        # Only restore positions from today onward — skip settled past dates
        _now = datetime.datetime.utcnow()
        _today_prefix = f"{str(_now.year)[-2:]}{MONTH_MAP[_now.month]}{_now.day}"

        async with await self.db.get_session() as session:
            result = await session.execute(
                select(PositionModel).where(PositionModel.quantity > 0)
            )
            db_positions = result.scalars().all()

        # In LIVE mode, also fetch positions directly from Kalshi API
        if self.config.trading_mode == "LIVE":
            try:
                api_positions = await self.executor.get_positions()
                for ticker, pos_data in api_positions.items():
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
                    qty = int(float(pos_data.get("count", 0)))
                    entry = round(float(pos_data.get("average_fill_cost_dollars", "0")) * 100)
                    bracket.position_quantity = qty
                    bracket.avg_entry = entry
                    bracket.last_price = entry
                    self.active_positions[ticker] = bracket
                    logger.info("strategy.restored_live_position", ticker=ticker,
                                qty=qty, entry=entry)
            except Exception as e:
                logger.error("strategy.restore_positions_error", error=str(e))

        for pos in db_positions:
            ticker = pos.market_ticker
            # Skip settled past dates
            date_match = re.search(r'(\d{2})([A-Z]{3})(\d{2})', ticker)
            if date_match:
                ticker_ymd = f"{date_match.group(1)}{date_match.group(2)}{date_match.group(3)}"
                if ticker_ymd < _today_prefix:
                    continue
            bracket = self.brackets.get(ticker)
            if bracket is None:
                # Market not in current bracket list – create placeholder
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

    async def _get_today_str(self) -> str:
        """Return today's Kalshi date string (e.g. 26JUN14)."""
        today = datetime.datetime.utcnow()
        return f"{str(today.year)[-2:]}{MONTH_MAP[today.month]}{today.day}"

    async def _refresh_todays_markets(self):
        """Periodically check for newly listed markets for today's date."""
        today_str = await self._get_today_str()
        if not hasattr(self, '_current_date_str') or self._current_date_str != today_str:
            return

        logger.info("strategy.refreshing_markets", date=today_str)
        active_markets = await self.executor.get_active_markets()
        new_count = 0
        for m in active_markets:
            ticker = m.get("ticker", "")
            if not ticker or today_str not in ticker:
                continue
            if ticker not in self.brackets:
                self.brackets[ticker] = MarketBracket(
                    market_ticker=ticker,
                    event_ticker=m.get("event_ticker", ""),
                    series_ticker=m.get("series_ticker", ""),
                    bracket_label=m.get("title", ""),
                    phase=Phase.MONITORING,
                )
                new_count += 1
        if new_count > 0:
            logger.info("strategy.new_markets_found", count=new_count, date=today_str)

    async def _check_date_rollover(self):
        """
        Check if the trading date has rolled over (new day's markets available).
        If a new date is detected, re-fetch markets, re-subscribe WebSocket channels.
        This runs periodically in the strategy loop (every ~30 seconds).
        """
        new_date_str = await self._get_today_str()
        if hasattr(self, '_current_date_str') and self._current_date_str == new_date_str:
            return  # same day, nothing to do

        if hasattr(self, '_current_date_str'):
            old_date = self._current_date_str
            logger.info("strategy.date_rollover", old_date=old_date, new_date=new_date_str)

        old_date = self._current_date_str if hasattr(self, '_current_date_str') else None
        self._current_date_str = new_date_str

        if new_date_str in self._loaded_dates:
            return

        logger.info("strategy.loading_new_date_markets", date=new_date_str)
        active_markets = await self.executor.get_active_markets()
        new_tickers = []
        new_brackets = {}
        for m in active_markets:
            ticker = m.get("ticker", "")
            if ticker and new_date_str in ticker:
                new_tickers.append(ticker)
                new_brackets[ticker] = MarketBracket(
                    market_ticker=ticker,
                    event_ticker=m.get("event_ticker", ""),
                    series_ticker=m.get("series_ticker", ""),
                    bracket_label=m.get("title", ""),
                    phase=Phase.MONITORING,
                )
        if not new_tickers:
            logger.info("strategy.no_new_markets_yet", date=new_date_str)
            return
        old_brackets = {t: b for t, b in self.brackets.items() if new_date_str not in t}
        self.brackets = {**old_brackets, **new_brackets}
        self.watchlist = {t: b for t, b in self.watchlist.items() if t in self.brackets}
        await self.ws.subscribe("orderbook_snapshot", new_tickers)
        await self.ws.subscribe("orderbook_delta", new_tickers)
        await self.ws.subscribe("ticker", new_tickers)
        await self.ws.subscribe("trade", new_tickers)
        self._loaded_dates.add(self._current_date_str)
        logger.info("strategy.loaded_new_markets",
                    date=new_date_str, count=len(new_tickers),

                    total_brackets=len(self.brackets))
    async def _handle_ticker(self, msg: dict):
        """Process ticker updates from WebSocket."""
        ticker_data = msg.get("msg", msg)
        market_ticker = ticker_data.get("market_ticker") or ticker_data.get("ticker")
        if not market_ticker:
            return

        last_price = ticker_data.get("last_price")
        yes_bid = ticker_data.get("yes_bid")
        yes_ask = ticker_data.get("yes_ask")

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
            # Check if this is a weather temperature market
            series_ticker = data.get("series_ticker", "")
            title = data.get("title", "")
            if "WEATHER" in series_ticker.upper() or "TEMP" in series_ticker.upper() or "weather" in title.lower():
                market_ticker = data.get("market_ticker")
                if market_ticker and market_ticker not in self.brackets:
                    bracket = MarketBracket(
                        market_ticker=market_ticker,
                        event_ticker=data.get("event_ticker", ""),
                        series_ticker=series_ticker,
                        bracket_label=title,
                        phase=Phase.MONITORING,
                    )
                    self.brackets[market_ticker] = bracket
                    logger.info("strategy.new_weather_market", ticker=market_ticker,
                                label=bracket.bracket_label)

    async def _strategy_loop(self):
        """
        Main strategy evaluation loop runs every ~1 second.
        Evaluates all brackets and transitions phases.
        """
        while self._running:
            try:
                await self._check_date_rollover()      # Date rollover (every ~30s)
                # Periodic market refresh (every 5 min) to catch newly listed markets
                if not hasattr(self, '_market_refresh_count'):
                    self._market_refresh_count = 0
                self._market_refresh_count += 1
                if self._market_refresh_count >= 300:  # 300 cycles * 1 sec = 5 min
                    self._market_refresh_count = 0
                    await self._refresh_todays_markets()
                await self._evaluate_watchlist()       # Phase A -> B
                await self._evaluate_held_positions()  # Phase C
                await self._log_periodic_snapshot()
            except Exception as e:
                logger.error("strategy.loop_error", error=str(e), exc_info=True)
            await asyncio.sleep(1)

    async def _fetch_live_prices(self, tickers: list[str]) -> dict[str, OrderBook]:
        """
        Fetch live OrderBook prices from the WebSocket cache.
        Falls back to REST API if cache is empty for a ticker.
        """
        results = {}
        if not tickers:
            return results

        # First try cache
        cache_miss = []
        for t in tickers:
            ob = self.cache.get_orderbook(t)
            if ob and ob.best_ask is not None and ob.best_bid is not None:
                results[t] = ob
            else:
                cache_miss.append(t)

        # Fallback to REST API for cache misses
        if cache_miss:
            try:
                import httpx
                path = "/trade-api/v2/markets"
                series_groups: dict[str, list[str]] = {}
                for t in cache_miss:
                    prefix = t.split("-")[0] if "-" in t else t
                    series_groups.setdefault(prefix, []).append(t)

                async with httpx.AsyncClient(timeout=15.0) as client:
                    async def _fetch_series(series, group_tickers):
                        headers = build_auth_headers(self._private_key, self.config.kalshi_api_key, "GET", path)
                        try:
                            r = await client.get(
                                f"{self.config.rest_base_url}{path}",
                                headers=headers,
                                params={"series_ticker": series, "limit": 100}
                            )
                            if r.status_code in (200, 201):
                                mkts = r.json().get("markets", [])
                                for m in mkts:
                                    mt = m.get("ticker")
                                    if mt in group_tickers:
                                        yb = m.get("yes_bid")
                                        ya = m.get("yes_ask")
                                        if yb is not None and ya is not None:
                                            ob = OrderBook()
                                            ob.yes_bids = [OrderBookLevel(price=int(float(yb)*100), quantity=0, order_count=0)]
                                            ob.yes_asks = [OrderBookLevel(price=int(float(ya)*100), quantity=0, order_count=0)]
                                            results[mt] = ob
                        except Exception:
                            pass

                    tasks = [_fetch_series(s, g) for s, g in series_groups.items()]
                    await asyncio.gather(*tasks)
            except Exception as e:
                logger.warning("strategy.batch_fetch_fallback_error", error=str(e))
        return results

    async def _evaluate_watchlist(self):
        """
        Simple entry check: every cycle, loop all brackets.
        Uses live Kalshi API prices (NOT websocket cache).
        If price >= buy_trigger and spread <= minimum_spread,
        and we haven't already bought this market -> buy.
        Keep checking until price exceeds spread_monitor.
        """
        # Fetch prices from WebSocket cache (with REST fallback)
        active_tickers = [t for t, b in self.brackets.items() if not b.crossed_buy]
        if not active_tickers:
            return
        live_prices = await self._fetch_live_prices(active_tickers)

        for ticker, bracket in list(self.brackets.items()):
            if bracket.crossed_buy:
                continue

            ob = live_prices.get(ticker)
            if not ob or ob.best_ask is None or ob.best_bid is None:
                continue

            current_price = ob.best_ask
            bracket.last_price = current_price
            spread = ob.spread

            if current_price < self.config.buy_trigger_price:
                continue

            if current_price > self.config.spread_monitor_price:
                bracket.crossed_buy = True
                logger.info("phase.b.missed_entry", ticker=ticker,
                            price=current_price, max_price=self.config.spread_monitor_price)
                continue

            if spread is not None and spread <= self.config.minimum_spread:
                bracket.crossed_buy = True
                logger.info("phase.b.buying", ticker=ticker,
                            label=bracket.bracket_label, price=current_price, spread=spread)
                await self._execute_entry(bracket)
            else:
                if not hasattr(self, '_spread_log_counter'):
                    self._spread_log_counter = 0
                self._spread_log_counter += 1
                if self._spread_log_counter % 60 == 0:
                    logger.info("phase.b.spread_too_wide", ticker=ticker,
                                price=current_price, spread=spread)

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

        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.BUY_YES,
            price=price,
            quantity=self.config.initial_contract_count,
            client_order_id=f"entry_{bracket.market_ticker}_{int(asyncio.get_event_loop().time()*1000)}",
        )

        result = await self.executor.buy_yes(order)

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
        For each held position, monitor for hedge trigger and stop loss.
        """
        # Fetch prices from WebSocket cache (with REST fallback)
        held_tickers = list(self.active_positions.keys())
        live_prices = await self._fetch_live_prices(held_tickers)

        for ticker, bracket in list(self.active_positions.items()):
            # Use live Kalshi API price - NOT websocket cache
            ob = live_prices.get(ticker)
            # Check if market is settled by date (previous trading days)
            # Do this FIRST — before any price check — so old settled markets
            # with no trading activity still get cleaned up.
            date_match = re.search(r'(\d{2})([A-Z]{3})(\d{2})', ticker)
            if date_match:
                ticker_month = date_match.group(2)
                ticker_day = int(date_match.group(3))
                today_date = self._current_date_str if hasattr(self, '_current_date_str') else ''
                ticker_mo_idx = MONTH_ORDINAL.get(ticker_month, -1)
                today_mm = today_date[2:5] if len(today_date) >= 5 else ''
                today_dd = int(today_date[5:]) if len(today_date) >= 7 else 0
                today_mo_idx = MONTH_ORDINAL.get(today_mm, -1)
                if ticker_mo_idx >= 0 and today_mo_idx >= 0:
                    if ticker_mo_idx < today_mo_idx or (ticker_mo_idx == today_mo_idx and ticker_day < today_dd):
                        final_price = bracket.last_price or 0
                        pl_pct = ((final_price - bracket.avg_entry) / bracket.avg_entry * 100) if bracket.avg_entry > 0 else 0
                        logger.info("phase.c.market_expired", ticker=ticker,
                                    entry=bracket.avg_entry,
                                    final_price=final_price,
                                    pl_pct=round(pl_pct, 1),
                                    label=bracket.bracket_label)
                        bracket.phase = Phase.CLOSED
                        del self.active_positions[ticker]
                        self.brackets.pop(ticker, None)
                        continue

            if ob and ob.yes_bids:
                last_price = int(ob.yes_bids[0].price)
            elif ob and ob.yes_asks:
                last_price = int(ob.yes_asks[0].price)
            else:
                # Cannot determine price from live API - skip this cycle
                continue

            bracket.last_price = last_price

            # Check Stop Loss first (most critical)
            if last_price <= self.config.stop_loss_price and bracket.phase != Phase.CLOSED:
                if bracket.position_quantity <= 0:
                    bracket.phase = Phase.CLOSED
                    self.active_positions.pop(ticker, None)
                    self.brackets.pop(ticker, None)
                    logger.info("phase.c.stop_loss_zero_qty", ticker=ticker)
                    continue
                logger.warning("phase.c.stop_loss_triggered", ticker=ticker,
                               last_price=last_price, stop_loss=self.config.stop_loss_price)
                await self._execute_stop_loss(bracket)
                continue

            # Check Hedge trigger
            if (last_price <= self.config.hedge_trigger_price
                    and bracket.phase == Phase.HOLDING
                    and not bracket.hedge_market):
                logger.info("phase.c.hedge_triggered", ticker=ticker,
                            last_price=last_price, hedge_trigger=self.config.hedge_trigger_price)
                await self._execute_hedge(bracket)

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

        # Find the highest-priced bracket ticker in the same event (most likely to win)
        best_price = -1
        for ticker, b in self.brackets.items():
            if (b.event_ticker == bracket.event_ticker
                    and ticker != bracket.market_ticker
                    and not b.crossed_buy):
                ob = self.cache.get_orderbook(ticker)
                if ob and ob.best_ask is not None and ob.best_ask > best_price:
                    best_price = ob.best_ask
                    next_bracket_ticker = ticker

        # Fetch live price for the hedge market
        prices = await self._fetch_live_prices([next_bracket_ticker])
        ob = prices.get(next_bracket_ticker)
        hedge_price = self.config.hedge_trigger_price
        if ob and ob.yes_asks:
            hedge_price = ob.yes_asks[0].price


        # Calculate expected loss if price continues to stop loss
        expected_loss = bracket.position_quantity * (bracket.avg_entry - self.config.stop_loss_price)
        
        # Calculate hedge quantity to break even
        hedge_profit_per_contract = 100 - hedge_price
        if hedge_profit_per_contract > 0 and expected_loss > 0:
            hedge_qty = (expected_loss + hedge_profit_per_contract - 1) // hedge_profit_per_contract  # ceiling
        else:
            hedge_qty = bracket.position_quantity  # fallback
        order = OrderRequest(
            market_ticker=next_bracket_ticker,
            side=OrderSide.BUY_YES,
            price=hedge_price,
            quantity=hedge_qty,
            client_order_id=f"hedge_{next_bracket_ticker}_{int(asyncio.get_event_loop().time()*1000)}",
            is_hedge=True,
        )

        result = await self.executor.buy_yes(order)

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
        """Execute a stop-loss: sell position at best bid price."""
        ob = self.cache.get_orderbook(bracket.market_ticker)
        price = self.config.stop_loss_price
        if ob and ob.yes_bids:
            price = ob.yes_bids[0].price  # best bid

        order = OrderRequest(
            market_ticker=bracket.market_ticker,
            side=OrderSide.SELL_YES,
            price=price,
            quantity=bracket.position_quantity,
            client_order_id=f"stoploss_{bracket.market_ticker}_{int(asyncio.get_event_loop().time()*1000)}",
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
        Find the next highest bracket in the same event/series.
        For temperature markets, brackets are like "96-97", "98-99", etc.
        We need to find the bracket whose lower bound is higher than ours.
        """
        # Parse current bracket label to extract upper bound
        try:
            # Remove degree symbols and split
            clean_label = bracket.bracket_label.replace("°", "").replace("º", "")
            # Try to extract numbers like "96-97" or "HIGH 96-97"
            numbers = re.findall(r'\d+', clean_label)
            if len(numbers) >= 2:
                current_upper = int(numbers[1])
            else:
                logger.warning("strategy.cannot_parse_bracket", label=bracket.bracket_label)
                return None
        except (ValueError, AttributeError):
            logger.warning("strategy.cannot_parse_bracket", label=bracket.bracket_label)
            return None

        next_lower = current_upper + 1

        # Look through known brackets from the same event
        for ticker, b in self.brackets.items():
            if b.event_ticker == bracket.event_ticker and b.market_ticker != bracket.market_ticker:
                try:
                    clean = b.bracket_label.replace("°", "").replace("º", "")
                    nums = re.findall(r'\d+', clean)
                    if len(nums) >= 2:
                        b_lower = int(nums[0])
                        if b_lower == next_lower:
                            return b.market_ticker
                except (ValueError, AttributeError):
                    continue

        logger.warning("strategy.next_bracket_not_found",
                       event=bracket.event_ticker, current=bracket.bracket_label)
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
