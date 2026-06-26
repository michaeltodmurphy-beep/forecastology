# core/state_machine.py
import asyncio
import datetime
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
    Position as PositionModel, PortfolioSnapshot, StopLossLedger,
)
from sqlalchemy import select, delete

logger = structlog.get_logger(__name__)
SERIES_DATE_RE = re.compile(r"^(.+?)-(\d{2}[A-Z]{3}\d{2})-(?:T\d+|B\d+\.?\d*)$")


def parse_series_and_date(market_ticker: str) -> Optional[tuple[str, str]]:
    match = SERIES_DATE_RE.match(market_ticker)
    if not match:
        return None
    return match.group(1), match.group(2)


_MONTH_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_date_prefix(date_prefix: str) -> Optional[datetime.date]:
    """Parse a ticker date prefix like '26JUN25' into a datetime.date, or None on failure."""
    try:
        year = 2000 + int(date_prefix[:2])
        month = _MONTH_NUM.get(date_prefix[2:5])
        day = int(date_prefix[5:])
        if month is None:
            return None
        return datetime.date(year, month, day)
    except (ValueError, IndexError):
        return None


class TemperatureStrategy:
    """
    Core state machine for daily high/low temperature market brackets.

    Phase A: Market Monitoring
    Phase B: Trade Entry (with spread check)
    Phase C: Position Management (Stop Loss)
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

    @staticmethod
    def _first_non_none(*values):
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _to_cents(raw) -> Optional[int]:
        if raw is None or raw == "":
            return None
        try:
            return round(float(raw) * 100)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _market_is_settled(rest_data: Optional[dict]) -> bool:
        if not rest_data:
            return False
        status = str(rest_data.get("status") or "").lower()
        result = str(rest_data.get("result") or "").lower()
        settlement_ts = rest_data.get("settlement_ts")
        is_settled = rest_data.get("is_settled")
        if isinstance(is_settled, str):
            is_settled = is_settled.lower() == "true"
        return bool(
            is_settled
            or settlement_ts
            or status in {"settled", "finalized", "resolved"}
            or result in {"yes", "no"}
        )

    async def _remove_active_position(self, ticker: str, bracket: MarketBracket):
        bracket.phase = Phase.CLOSED
        self.active_positions.pop(ticker, None)
        self.brackets.pop(ticker, None)
        async with await self.db.get_session() as session:
            await session.execute(
                delete(PositionModel).where(PositionModel.market_ticker == ticker)
            )
            await session.commit()

    @staticmethod
    def _avg_buy_fill_price_cents_from_fills(fills: list, ticker: str) -> int:
        total_count = 0.0
        weighted_dollars = 0.0
        for fill in fills or []:
            if fill.get("ticker") != ticker and fill.get("market_ticker") != ticker:
                continue
            if (fill.get("action") or "").lower() != "buy":
                continue
            count = fill.get("count_fp") or fill.get("count") or 0
            price = fill.get("yes_price_dollars") or 0
            try:
                count_f = float(count)
                price_f = float(price)
            except (TypeError, ValueError):
                continue
            if count_f > 0 and price_f > 0:
                total_count += count_f
                weighted_dollars += price_f * count_f
        if total_count > 0:
            return round((weighted_dollars / total_count) * 100)
        return 0

    async def _resolve_entry_cost_basis(self, ticker: str) -> tuple[int, Optional[str]]:
        try:
            positions = await self.executor.get_positions()
            pos_data = positions.get(ticker, {}) if isinstance(positions, dict) else {}
            avg_from_positions = int(pos_data.get("average_fill_cost_cents") or 0)
            if avg_from_positions > 0:
                return avg_from_positions, "positions"
        except Exception as e:
            logger.warning("phase.c.entry_backfill_positions_failed",
                           ticker=ticker, error=str(e))

        if hasattr(self.executor, "get_fills"):
            try:
                fills = await self.executor.get_fills(ticker=ticker)
                avg_fn = getattr(self.executor, "_avg_fill_price_cents_from_fills", None)
                if callable(avg_fn):
                    avg_from_fills = int(avg_fn(fills, ticker) or 0)
                else:
                    avg_from_fills = self._avg_buy_fill_price_cents_from_fills(fills, ticker)
                if avg_from_fills > 0:
                    return avg_from_fills, "fills"
            except Exception as e:
                logger.warning("phase.c.entry_backfill_fills_failed",
                               ticker=ticker, error=str(e))

        return 0, None

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
                     stop_loss=self.config.stop_loss_price,
                     mode=self.config.trading_mode,
                     restored_positions=len(self.active_positions))

        # Start DB cleanup task (runs hourly)
        asyncio.create_task(self._db_cleanup_loop())

    async def _restore_positions(self):
        """
        On startup, re-populate active_positions from the database
        so that position management continues
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

        today_utc = datetime.datetime.utcnow().date()

        # In LIVE mode, also fetch positions directly from Kalshi API
        if self.config.trading_mode == "LIVE":
            try:
                api_positions = await self.executor.get_positions()
                for ticker, pos_data in api_positions.items():
                    # Skip empty/zero-quantity positions
                    qty = int(float(pos_data.get("count", 0)))
                    if qty <= 0:
                        continue
                    # Skip positions whose market date is before today — they have
                    # already settled overnight and no longer exist on the exchange.
                    parsed = parse_series_and_date(ticker)
                    if parsed is not None:
                        _, date_prefix = parsed
                        market_date = _parse_date_prefix(date_prefix)
                        if market_date is not None and market_date < today_utc:
                            logger.info("strategy.skipped_stale_position",
                                        ticker=ticker, date_prefix=date_prefix)
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
            # Skip positions whose market date is before today — they have
            # already settled overnight and no longer exist on the exchange.
            parsed = parse_series_and_date(ticker)
            if parsed is not None:
                _, date_prefix = parsed
                market_date = _parse_date_prefix(date_prefix)
                if market_date is not None and market_date < today_utc:
                    logger.info("strategy.skipped_stale_position",
                                ticker=ticker, date_prefix=date_prefix)
                    continue
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
        yes_bid_raw = self._first_non_none(
            ticker_data.get("yes_bid_dollars"),
            ticker_data.get("yes_bid"),
        )
        yes_ask_raw = self._first_non_none(
            ticker_data.get("yes_ask_dollars"),
            ticker_data.get("yes_ask"),
        )

        # Convert dollars to cents
        last_price = self._to_cents(last_price_raw)
        yes_bid = self._to_cents(yes_bid_raw)
        yes_ask = self._to_cents(yes_ask_raw)

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
            yes_bid = None
            yes_ask = None

            # Primary source: ticker channel quote (yes_ask as price, yes_ask - yes_bid as spread)
            quote = self.cache.get_quote(ticker)
            if quote is not None:
                yes_bid_q, yes_ask_q = quote
                yes_bid = yes_bid_q
                yes_ask = yes_ask_q
                price = yes_ask_q
                spread = yes_ask_q - yes_bid_q

            # Fallback: REST endpoint
            if price is None and rest_calls_this_cycle < max_rest_per_cycle:
                rest_data = await self._fetch_market_data_via_rest(ticker)
                rest_calls_this_cycle += 1
                if rest_data:
                    yes_bid = rest_data.get("yes_bid")
                    yes_ask = rest_data.get("yes_ask")
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

            if (
                yes_bid is not None
                and yes_ask is not None
                and yes_ask >= 99
                and yes_bid <= self.config.eval_price_floor
            ):
                continue

            # Skip near-dead brackets early (quietly) — they will never reach buy_trigger.
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
                count = await self._get_stop_loss_count_for_market(ticker)
                max_doublings = int(self.config.hedge_max_factor)
                if count > max_doublings:
                    logger.info("phase.b.recovery_cap_reached",
                                series_ticker=bracket.series_ticker,
                                count=count,
                                max_doublings=max_doublings)
                    continue

                quantity = self.config.initial_contract_count * (2 ** count)
                if count > 0:
                    logger.info("phase.b.recovery_sized_entry",
                                series_ticker=bracket.series_ticker,
                                count=count,
                                multiplier=2 ** count,
                                quantity=quantity)
                    await self._execute_entry(bracket, quantity=quantity)
                else:
                    await self._execute_entry(bracket)
            else:
                logger.info("phase.b.spread_too_wide", ticker=ticker,
                            price=price, spread=spread)

    async def _execute_entry(self, bracket: MarketBracket, ob: Optional[OrderBook] = None, quantity: Optional[int] = None):
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
            quantity=quantity or self.config.initial_contract_count,
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
            reconciled_fill_price = result.fill_price
            if result.fill_quantity > 0 and result.fill_price <= 0:
                backfilled_cents, source = await self._resolve_entry_cost_basis(bracket.market_ticker)
                if backfilled_cents > 0:
                    reconciled_fill_price = backfilled_cents
                    logger.info("phase.b.entry_cost_reconciled",
                                ticker=bracket.market_ticker,
                                source=source,
                                cents=backfilled_cents)

            bracket.phase = Phase.HOLDING
            bracket.position_quantity = result.fill_quantity
            bracket.avg_entry = reconciled_fill_price
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
                    pos.avg_entry_price = reconciled_fill_price
                    pos.last_price = reconciled_fill_price
                else:
                    # Insert new position
                    pos = PositionModel(
                        market_ticker=bracket.market_ticker,
                        event_ticker=bracket.event_ticker,
                        series_ticker=bracket.series_ticker,
                        side="yes",
                        quantity=result.fill_quantity,
                        avg_entry_price=reconciled_fill_price,
                        last_price=reconciled_fill_price,
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

    async def _get_stop_loss_count_for_market(self, market_ticker: str) -> int:
        parsed = parse_series_and_date(market_ticker)
        if not parsed:
            return 0

        series_ticker, date_prefix = parsed
        async with await self.db.get_session() as session:
            result = await session.execute(
                select(StopLossLedger).where(
                    StopLossLedger.series_ticker == series_ticker,
                    StopLossLedger.date_prefix == date_prefix,
                )
            )
            row = result.scalar_one_or_none()
        return row.stop_loss_count if row else 0

    async def _increment_stop_loss_count_for_market(self, market_ticker: str) -> None:
        parsed = parse_series_and_date(market_ticker)
        if not parsed:
            return

        series_ticker, date_prefix = parsed
        async with await self.db.get_session() as session:
            result = await session.execute(
                select(StopLossLedger).where(
                    StopLossLedger.series_ticker == series_ticker,
                    StopLossLedger.date_prefix == date_prefix,
                )
            )
            row = result.scalar_one_or_none()
            if row is None:
                row = StopLossLedger(
                    series_ticker=series_ticker,
                    date_prefix=date_prefix,
                    stop_loss_count=1,
                    updated_at=datetime.datetime.utcnow(),
                )
                session.add(row)
            else:
                row.stop_loss_count += 1
                row.updated_at = datetime.datetime.utcnow()
            await session.commit()

    async def _decrement_stop_loss_count_for_market(self, market_ticker: str) -> None:
        parsed = parse_series_and_date(market_ticker)
        if not parsed:
            return

        series_ticker, date_prefix = parsed
        async with await self.db.get_session() as session:
            result = await session.execute(
                select(StopLossLedger).where(
                    StopLossLedger.series_ticker == series_ticker,
                    StopLossLedger.date_prefix == date_prefix,
                )
            )
            row = result.scalar_one_or_none()
            if row is not None and row.stop_loss_count > 0:
                row.stop_loss_count -= 1
                row.updated_at = datetime.datetime.utcnow()
                await session.commit()

    async def _evaluate_held_positions(self):
        """Phase C: manage held positions with live sellable-price stop-losses."""
        if not self.active_positions:
            return

        try:
            api_positions = await self.executor.get_positions()
        except Exception as e:
            logger.error("phase.c.get_positions_failed", error=str(e))
            api_positions = {}

        active_tickers = list(self.active_positions.keys())
        absent_tickers = [
            ticker for ticker in active_tickers
            if ticker not in api_positions or api_positions.get(ticker) is None
        ]
        total_active = len(active_tickers)
        mass_absence = (
            total_active >= 2
            and len(absent_tickers) / total_active > 0.5
        )
        if mass_absence:
            now_mass = asyncio.get_event_loop().time()
            last_mass_log = getattr(self, "_last_mass_absence_log", 0)
            if now_mass - last_mass_log >= 60:
                self._last_mass_absence_log = now_mass
                logger.warning(
                    "phase.c.positions_api_mass_absence",
                    count_absent=len(absent_tickers),
                    count_total=total_active,
                )

        for ticker, bracket in list(self.active_positions.items()):
            pos_data = api_positions.get(ticker)
            position_absent = ticker not in api_positions or pos_data is None
            now_ts = asyncio.get_event_loop().time()
            last_seen = getattr(bracket, "_last_seen_in_api", 0)
            if position_absent:
                if last_seen == 0:
                    bracket._last_seen_in_api = now_ts
                    last_seen = now_ts
                seconds_absent = now_ts - last_seen
                grace = 30
                if seconds_absent < grace:
                    logger.debug("phase.c.position_missing_within_grace", ticker=ticker,
                                 seconds_absent=int(seconds_absent))
                elif not mass_absence:
                    last_absent_log = getattr(bracket, "_last_position_absent_log", 0)
                    if now_ts - last_absent_log >= 60:
                        bracket._last_position_absent_log = now_ts
                        logger.warning(
                            "phase.c.position_not_in_api_after_grace",
                            ticker=ticker,
                            qty=bracket.position_quantity,
                            phase=bracket.phase.name,
                            action="retained_pending_settlement_confirmation",
                        )
            else:
                bracket._last_seen_in_api = now_ts
                bracket._last_position_absent_log = 0

                api_count = pos_data.get("count", 1)
                if api_count == 0:
                    logger.info("phase.c.position_settled", ticker=ticker,
                                qty=bracket.position_quantity, api_count=api_count)
                    await self._remove_active_position(ticker, bracket)
                    continue

            if not bracket.avg_entry or bracket.avg_entry <= 0:
                heal_source: Optional[str] = None
                avg_cents = int((pos_data or {}).get("average_fill_cost_cents") or 0)
                if avg_cents > 0:
                    bracket.avg_entry = avg_cents
                    heal_source = "positions_inline"
                else:
                    now_heal = asyncio.get_event_loop().time()
                    last_heal = getattr(bracket, '_last_entry_heal_attempt', 0)
                    if now_heal - last_heal >= 60:
                        bracket._last_entry_heal_attempt = now_heal
                        backfilled_cents, src = await self._resolve_entry_cost_basis(ticker)
                        if backfilled_cents > 0:
                            bracket.avg_entry = backfilled_cents
                            heal_source = src

                if heal_source is not None and bracket.avg_entry > 0:
                    logger.info("phase.c.entry_self_healed",
                                ticker=ticker,
                                source=heal_source,
                                cents=bracket.avg_entry)
                    try:
                        async with await self.db.get_session() as session:
                            existing_pos = await session.execute(
                                select(PositionModel).where(
                                    PositionModel.market_ticker == ticker
                                )
                            )
                            pos_row = existing_pos.scalar_one_or_none()
                            if pos_row:
                                pos_row.avg_entry_price = bracket.avg_entry
                                await session.commit()
                    except Exception:
                        pass

            current_price = None
            yes_ask = None
            rest_data = None
            zero_bid_collapse = False
            blind_below_stop = False
            last_known_price = self.cache.get_last_price(ticker)
            if last_known_price is None:
                last_known_price = bracket.last_price
            quote = self.cache.get_quote(ticker)
            if quote is not None:
                yes_bid, yes_ask = quote
                if yes_bid > 0:
                    current_price = yes_bid
                elif yes_bid == 0:
                    zero_bid_collapse = True

            if current_price is None:
                now_fetch = asyncio.get_event_loop().time()
                last_fetch = getattr(bracket, "_last_rest_price_fetch", 0)
                if now_fetch - last_fetch >= self.config.held_position_price_refresh_seconds:
                    bracket._last_rest_price_fetch = now_fetch
                    rest_data = await self._fetch_market_data_via_rest(ticker)
                    if rest_data:
                        yes_ask = rest_data.get("yes_ask")
                        rest_yes_bid = rest_data.get("yes_bid")
                        if rest_yes_bid is not None and rest_yes_bid > 0:
                            current_price = rest_yes_bid
                            zero_bid_collapse = False
                        elif rest_yes_bid == 0:
                            zero_bid_collapse = True
                        elif rest_data.get("price") is not None:
                            current_price = rest_data["price"]

            if position_absent and not mass_absence and self._market_is_settled(rest_data):
                logger.info(
                    "phase.c.position_settled",
                    ticker=ticker,
                    qty=bracket.position_quantity,
                    source="market_status",
                    market_status=rest_data.get("status") if rest_data else None,
                )
                await self._remove_active_position(ticker, bracket)
                continue

            if current_price is None:
                last_price = self.cache.get_last_price(ticker)
                if (
                    last_price is not None
                    and yes_ask is not None
                    and last_price <= yes_ask
                ):
                    current_price = last_price

            if current_price is None and not zero_bid_collapse:
                bracket._consecutive_no_price_cycles = getattr(
                    bracket, "_consecutive_no_price_cycles", 0
                ) + 1
                if not getattr(bracket, "_no_price_since", 0):
                    bracket._no_price_since = asyncio.get_event_loop().time()
                now_warn = asyncio.get_event_loop().time()
                last_warn = getattr(bracket, "_last_no_price_log", 0)
                if now_warn - last_warn >= 60:
                    bracket._last_no_price_log = now_warn
                    logger.warning("phase.c.no_live_price", ticker=ticker)
                if (
                    bracket.position_quantity > 0
                    and bracket._consecutive_no_price_cycles > self.config.max_no_price_cycles
                ):
                    last_alert = getattr(bracket, "_last_unprotected_log", 0)
                    if now_warn - last_alert >= 60:
                        bracket._last_unprotected_log = now_warn
                        logger.warning(
                            "phase.c.held_position_unprotected",
                            ticker=ticker,
                            qty=bracket.position_quantity,
                            blind_cycles=bracket._consecutive_no_price_cycles,
                            seconds_blind=int(now_warn - bracket._no_price_since),
                            last_known_price=last_known_price,
                        )
                    # When we've gone blind for too long and the last known price
                    # was already below the stop threshold, the safe action is to
                    # attempt protection. If the last known price was still healthy,
                    # alert loudly but do not force an exit on data loss alone.
                    if (
                        last_known_price is not None
                        and last_known_price < self.config.stop_loss_price
                    ):
                        current_price = last_known_price
                        blind_below_stop = True
                if current_price is None:
                    continue

            bracket._consecutive_no_price_cycles = 0
            bracket._no_price_since = 0

            bracket.last_price = current_price
            bypass_spread_guard = zero_bid_collapse or blind_below_stop
            stop_loss_reason = None
            if zero_bid_collapse:
                if bracket.last_price != 0:
                    bracket.last_price = 0
                current_price = 0
                stop_loss_reason = "zero_bid_collapse"
                yes_ask = yes_ask if yes_ask and yes_ask > 0 else None
            elif blind_below_stop:
                stop_loss_reason = "blind_last_known_below_stop"
            elif current_price < self.config.stop_loss_price:
                stop_loss_reason = "price_below_stop"

            if stop_loss_reason is not None:
                if bracket.position_quantity <= 0:
                    bracket.phase = Phase.CLOSED
                    self.active_positions.pop(ticker, None)
                    self.brackets.pop(ticker, None)
                    logger.info("phase.c.stop_loss_zero_qty", ticker=ticker)
                    continue
                if getattr(bracket, "_stop_loss_abandoned", False):
                    now_aband = asyncio.get_event_loop().time()
                    last_aband_log = getattr(bracket, "_last_abandoned_log", 0)
                    if now_aband - last_aband_log >= 60:
                        bracket._last_abandoned_log = now_aband
                        logger.info("phase.c.stop_loss_abandoned_holding", ticker=ticker,
                                    remaining_qty=bracket.position_quantity, last_price=current_price)
                    continue
                # Spread guard: only allow the stop-loss to fire when the YES
                # bid-ask spread is tight, meaning the market agrees the position
                # is a loser. A wide spread means the book is indecisive — the
                # position may recover, so we hold rather than sell into thin air.
                if not bypass_spread_guard and yes_ask is not None and yes_ask > 0:
                    sl_spread = yes_ask - current_price
                    spread_wide = sl_spread > self.config.max_sl_spread
                elif not bypass_spread_guard:
                    # No ask (one-sided book) → treat as wide; PR #29 abandon
                    # logic will take over after enough zero-fill attempts.
                    sl_spread = None
                    spread_wide = True
                else:
                    sl_spread = None
                    spread_wide = False
                if spread_wide:
                    bracket._sl_held_for_spread = True
                    now_spread = asyncio.get_event_loop().time()
                    last_sl_held_log = getattr(bracket, "_last_sl_held_log", 0)
                    if now_spread - last_sl_held_log >= 60:
                        bracket._last_sl_held_log = now_spread
                        logger.info("phase.c.sl_held_for_spread", ticker=ticker,
                                    yes_bid=current_price, yes_ask=yes_ask,
                                    spread=sl_spread, max_spread=self.config.max_sl_spread)
                    continue
                # Spread is tight — reset guard state and fire the stop-loss.
                bracket._sl_held_for_spread = False
                bracket._last_sl_held_log = 0
                logger.warning("phase.c.stop_loss_triggered", ticker=ticker,
                               last_price=current_price, stop_loss=self.config.stop_loss_price,
                               reason=stop_loss_reason)
                if not getattr(bracket, "_stop_loss_counted", False):
                    await self._increment_stop_loss_count_for_market(bracket.market_ticker)
                    bracket._stop_loss_counted = True
                market_gone = await self._execute_stop_loss(bracket)
                if market_gone:
                    # The sell returned market_not_found: the market settled.
                    # Undo the ledger increment since no real stop-loss occurred.
                    await self._decrement_stop_loss_count_for_market(bracket.market_ticker)
                    bracket._stop_loss_counted = False
            else:
                # Bid has recovered above the stop threshold — clear the spread
                # guard so a future re-trigger logs fresh.
                if getattr(bracket, "_sl_held_for_spread", False):
                    bracket._sl_held_for_spread = False
                    bracket._last_sl_held_log = 0

    async def _execute_stop_loss(self, bracket: MarketBracket):
        """Execute a stop-loss: sell position at market (1¢) to guarantee fill.

        Returns True if the sell was rejected with market_not_found (the market
        has settled and is gone — position is cleaned up quietly as confirmed
        settlement).  Returns False in all other cases.
        """
        now = asyncio.get_event_loop().time()
        last_attempt = getattr(bracket, '_last_stop_loss_attempt', 0)
        if now - last_attempt < 60:
            return False
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

        # Detect market_not_found (HTTP 404): the market has already settled and
        # no longer exists on the exchange.  Treat this as confirmed settlement —
        # clean up the position quietly without logging a false stop-loss.
        if not result.success and "market_not_found" in (result.notes or ""):
            logger.info(
                "phase.c.position_settled_market_gone",
                ticker=bracket.market_ticker,
                qty=bracket.position_quantity,
            )
            async with await self.db.get_session() as session:
                await session.execute(
                    delete(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                await session.commit()
            bracket.phase = Phase.CLOSED
            self.active_positions.pop(bracket.market_ticker, None)
            self.brackets.pop(bracket.market_ticker, None)
            return True

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
        else:
            prev_qty = bracket.position_quantity
            bracket.position_quantity = live_count
            if result.fill_quantity > 0 or live_count < prev_qty:
                bracket._consecutive_unfilled_sl = 0
            else:
                bracket._consecutive_unfilled_sl = getattr(bracket, "_consecutive_unfilled_sl", 0) + 1
            logger.warning(
                "phase.c.stop_loss_partial_or_unfilled",
                ticker=bracket.market_ticker,
                attempted_qty=order.quantity,
                filled_qty=result.fill_quantity,
                remaining_count=live_count,
                notes=result.notes,
                last_price=bracket.last_price,
            )
            if bracket._consecutive_unfilled_sl >= self.config.stop_loss_max_unfilled_attempts:
                bracket._stop_loss_abandoned = True
                logger.warning(
                    "phase.c.stop_loss_abandoned_no_liquidity",
                    ticker=bracket.market_ticker,
                    attempts=bracket._consecutive_unfilled_sl,
                    last_price=bracket.last_price,
                    remaining_qty=bracket.position_quantity,
                )
        return False

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

                    ya = self._first_non_none(mkt.get("yes_ask_dollars"), mkt.get("yes_ask"))
                    ya_cents = self._to_cents(ya)
                    if ya_cents is not None:
                        result["yes_ask"] = ya_cents

                    yb = self._first_non_none(mkt.get("yes_bid_dollars"), mkt.get("yes_bid"))
                    yb_cents = self._to_cents(yb)
                    if yb_cents is not None:
                        result["yes_bid"] = yb_cents

                    lp = self._first_non_none(
                        mkt.get("last_price_dollars"),
                        mkt.get("last_price"),
                    )
                    lp_cents = self._to_cents(lp)
                    if lp_cents is not None:
                        result["price"] = lp_cents
                    elif "yes_ask" in result:
                        result["price"] = result["yes_ask"]

                    if "yes_ask" in result and "yes_bid" in result:
                        result["spread"] = result["yes_ask"] - result["yes_bid"]

                    status = self._first_non_none(mkt.get("status"), mkt.get("market_status"))
                    if status is not None:
                        result["status"] = status
                    if mkt.get("result") is not None:
                        result["result"] = mkt.get("result")
                    if mkt.get("settlement_ts") is not None:
                        result["settlement_ts"] = mkt.get("settlement_ts")
                    if mkt.get("is_settled") is not None:
                        result["is_settled"] = mkt.get("is_settled")

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
