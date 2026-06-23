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

MONTH_MAP = {1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"}
MONTH_ORDINAL = {m: i+1 for i, m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}

# Ticker format: <SERIES>-<YYMMMDD>-<T##|B##.#>
_TICKER_RE = re.compile(r'^(.+?)-(\d{2}[A-Z]{3}\d{2})-(?:T\d+|B\d+\.?\d*)$')

logger = structlog.get_logger(__name__)


def parse_series_and_date(market_ticker: str) -> Optional[tuple[str, str]]:
    """Parse (series_ticker, date_prefix) from a Kalshi market ticker.

    Ticker format: <SERIES>-<YYMMMDD>-<T##|B##.#>
    E.g. KXLOWTSATX-26JUN23-T78 -> ('KXLOWTSATX', '26JUN23')
         KXHIGHTPHX-26JUN23-B111.5 -> ('KXHIGHTPHX', '26JUN23')
    Returns None if the ticker does not match.
    """
    m = _TICKER_RE.match(market_ticker)
    if not m:
        return None
    return m.group(1), m.group(2)


class TemperatureStrategy:
    """
    Core state machine for daily high/low temperature market brackets.

    Phase A: Market Monitoring
    Phase B: Trade Entry with martingale sizing based on per-(series,date) stop-loss count
    Phase C: Position Management — dead-simple last-trade stop-loss, no hedging
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
            logger.warning("phase.c.hedge_entry_backfill_positions_failed",
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
                logger.warning("phase.c.hedge_entry_backfill_fills_failed",
                               ticker=ticker, error=str(e))

        return 0, None

    # ------------------------------------------------------------------
    # Stop-loss ledger helpers
    # ------------------------------------------------------------------

    async def _get_stop_loss_count_for_market(self, market_ticker: str) -> int:
        """Return the stop-loss count for (series, date) derived from market_ticker.
        Returns 0 if the ticker is unparseable or no ledger row exists yet.
        """
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
        """Increment (or create) the stop-loss count for (series, date) from market_ticker.
        If the ticker is unparseable, does nothing.
        """
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
            if row:
                row.stop_loss_count = row.stop_loss_count + 1
            else:
                row = StopLossLedger(
                    series_ticker=series_ticker,
                    date_prefix=date_prefix,
                    stop_loss_count=1,
                )
                session.add(row)
            await session.commit()

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

        await self.ws.subscribe("orderbook_snapshot")
        await self.ws.subscribe("orderbook_delta")
        await self.ws.subscribe("market_lifecycle_v2")
        await self.ws.subscribe("ticker")
        await self.ws.subscribe("trade")

        await self._restore_positions()

        asyncio.create_task(self._strategy_loop())

        logger.info("strategy.started",
                     monitor_start=self.config.monitor_start_price,
                     buy_trigger=self.config.buy_trigger_price,
                     minimum_spread=self.config.minimum_spread,
                     spread_monitor=self.config.spread_monitor_price,
                     stop_loss=self.config.stop_loss_price,
                     hedge_max_factor=self.config.hedge_max_factor,
                     mode=self.config.trading_mode,
                     restored_positions=len(self.active_positions))

        asyncio.create_task(self._db_cleanup_loop())

    async def _restore_positions(self):
        """
        On startup, re-populate active_positions from the database
        so that position management (stop-loss) continues across restarts.
        """
        async with await self.db.get_session() as session:
            three_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=3)
            result = await session.execute(
                select(PositionModel).where(
                    PositionModel.quantity > 0,
                    PositionModel.position_ts >= three_days_ago
                )
            )
            db_positions = result.scalars().all()
        db_by_ticker = {pos.market_ticker: pos for pos in db_positions}

        if self.config.trading_mode == "LIVE":
            try:
                api_positions = await self.executor.get_positions()
                for ticker, pos_data in api_positions.items():
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

    async def _ensure_bracket(self, market_ticker: str, event_ticker: str = "", series_ticker: str = "", bracket_label: str = ""):
        """Create a new MarketBracket if the ticker is a temperature market and unknown."""
        if market_ticker in self.brackets:
            return
        today_prefix = get_eastern_today_date_prefix(days_offset=0)
        if today_prefix not in market_ticker:
            return
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

        await self._ensure_bracket(market_ticker)

        last_price_raw = ticker_data.get("last_price")
        yes_bid_raw = ticker_data.get("yes_bid_dollars") or ticker_data.get("yes_bid")
        yes_ask_raw = ticker_data.get("yes_ask_dollars") or ticker_data.get("yes_ask")

        last_price = round(float(last_price_raw) * 100) if last_price_raw is not None else None
        yes_bid = round(float(yes_bid_raw) * 100) if yes_bid_raw is not None else None
        yes_ask = round(float(yes_ask_raw) * 100) if yes_ask_raw is not None else None

        if last_price is not None:
            self.cache.update_last_price(market_ticker, last_price)

        if yes_bid is not None and yes_ask is not None:
            self.cache.update_quote(market_ticker, yes_bid, yes_ask)

        if market_ticker in self.brackets:
            bracket = self.brackets[market_ticker]
            bracket.last_price = last_price

    async def _handle_trade(self, msg: dict):
        """Process trade updates - log to database and update last price."""
        trade_data = msg.get("msg", msg)
        market_ticker = trade_data.get("market_ticker")
        price = trade_data.get("price")
        quantity = trade_data.get("quantity")
        side = trade_data.get("side")
        trade_ts = trade_data.get("ts")

        if not market_ticker or price is None:
            return

        self.cache.update_last_price(market_ticker, price)

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

        await self._ensure_bracket(market_ticker, bracket_label=data.get("title", ""))

        self.cache.update_orderbook_snapshot(market_ticker, data)

        ob = self.cache.get_orderbook(market_ticker)
        if ob and ob.best_ask is not None:
            price = ob.best_ask
            self.cache.update_last_price(market_ticker, price)

            if market_ticker in self.brackets:
                bracket = self.brackets[market_ticker]
                bracket.last_price = price

    async def _handle_orderbook_delta(self, msg: dict):
        """Process orderbook delta - update cached price."""
        data = msg.get("msg", msg)
        market_ticker = data.get("market_ticker")
        if not market_ticker:
            return

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
        """Main strategy evaluation loop runs every ~1 second."""
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
        """Get live prices from WebSocket cache."""
        results = {}
        if not tickers:
            return results

        for t in tickers:
            ob = self.cache.get_orderbook(t)
            if ob and ob.best_ask is not None and ob.best_bid is not None:
                results[t] = ob
            else:
                lp = self.cache.get_last_price(t)
                if lp and lp > 0:
                    from core.types import OrderBookLevel
                    level = OrderBookLevel(price=lp, quantity=1, order_count=0)
                    results[t] = OrderBook(yes_bids=[level], yes_asks=[level])

        return results

    async def _evaluate_watchlist(self):
        """
        Phase B entry check with martingale sizing.
        Every cycle, loop all brackets. When price >= BUY_TRIGGER and spread is tight:
        - Look up stop-loss count for (series, date)
        - If count > hedge_max_factor: skip (cap reached for this series/day)
        - Else compute quantity = initial_contract_count * 2**count and enter
        """
        rest_calls_this_cycle = 0
        max_rest_per_cycle = 5

        for ticker, bracket in list(self.brackets.items()):
            if bracket.crossed_buy or bracket.phase != Phase.MONITORING:
                continue

            price = None
            spread = None

            quote = self.cache.get_quote(ticker)
            if quote is not None:
                yes_bid_q, yes_ask_q = quote
                price = yes_ask_q
                spread = yes_ask_q - yes_bid_q

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

            if price is None or spread is None:
                continue

            bracket.last_price = price

            if price <= self.config.eval_price_floor:
                continue

            if price < self.config.buy_trigger_price:
                logger.debug("phase.b.below_trigger", ticker=ticker, price=price,
                             buy_trigger=self.config.buy_trigger_price)
                continue

            if price > self.config.spread_monitor_price:
                logger.info("phase.b.missed_entry", ticker=ticker,
                            price=price, max_price=self.config.spread_monitor_price)
                continue

            if spread <= self.config.minimum_spread:
                # Martingale sizing: check stop-loss count for this (series, date)
                count = await self._get_stop_loss_count_for_market(ticker)
                max_doublings = int(self.config.hedge_max_factor)

                if count > max_doublings:
                    # Recovery cap reached — no more buys for this series today
                    parsed = parse_series_and_date(ticker)
                    series_ticker = parsed[0] if parsed else ticker
                    logger.info("phase.b.recovery_cap_reached",
                                ticker=ticker,
                                series_ticker=series_ticker,
                                count=count,
                                max_doublings=max_doublings)
                    bracket.crossed_buy = True  # prevent repeated logging every cycle
                    continue

                quantity = self.config.initial_contract_count * (2 ** count)

                spread_note = "crossed" if spread == 0 else "tight" if spread <= 3 else "normal"
                logger.info("phase.b.buying", ticker=ticker,
                            label=bracket.bracket_label, price=price, spread=spread,
                            spread_note=spread_note)

                if count > 0:
                    parsed = parse_series_and_date(ticker)
                    series_ticker = parsed[0] if parsed else ticker
                    logger.info("phase.b.recovery_sized_entry",
                                ticker=ticker,
                                series_ticker=series_ticker,
                                count=count,
                                multiplier=2 ** count,
                                quantity=quantity)

                bracket.crossed_buy = True
                await self._execute_entry(bracket, quantity=quantity)
            else:
                logger.info("phase.b.spread_too_wide", ticker=ticker,
                            price=price, spread=spread)

    async def _execute_entry(self, bracket: MarketBracket, ob: Optional[OrderBook] = None, quantity: Optional[int] = None):
        """
        Execute the initial buy order.
        quantity defaults to initial_contract_count if not provided.
        """
        if quantity is None:
            quantity = self.config.initial_contract_count

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
            quantity=quantity,
        )

        max_price = self.config.spread_monitor_price
        result = await self.executor.buy_yes(order, max_price=max_price)

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

            async with await self.db.get_session() as session:
                existing = await session.execute(
                    select(PositionModel).where(PositionModel.market_ticker == bracket.market_ticker)
                )
                pos = existing.scalar_one_or_none()
                if pos:
                    pos.quantity = pos.quantity + result.fill_quantity
                    pos.avg_entry_price = reconciled_fill_price
                    pos.last_price = reconciled_fill_price
                else:
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

    async def _evaluate_held_positions(self):
        """
        Phase C: Dead-simple stop-loss position management.

        For every held position, every cycle:
        - Read last traded price from cache (set by _handle_trade and _handle_ticker).
        - If last_traded_price < stop_loss_price → SELL the entire holding.
        - If no last-traded price exists → skip this cycle (no invented price).
        """
        if not self.active_positions:
            return

        api_positions: dict = {}
        try:
            api_positions = await self.executor.get_positions()
        except Exception as e:
            logger.error("phase.c.get_positions_failed", error=str(e))
            api_positions = {}

        for ticker, bracket in list(self.active_positions.items()):
            pos_data = api_positions.get(ticker)
            if not pos_data:
                now_ts = asyncio.get_event_loop().time()
                last_seen = getattr(bracket, '_last_seen_in_api', 0)
                if last_seen == 0:
                    bracket._last_seen_in_api = now_ts
                    continue
                grace = 30
                if now_ts - last_seen < grace:
                    logger.debug("phase.c.position_missing_within_grace", ticker=ticker,
                                 seconds_absent=int(now_ts - last_seen))
                    continue
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

            bracket._last_seen_in_api = asyncio.get_event_loop().time()

            api_count = pos_data.get("count", 1)
            if api_count == 0:
                logger.info("phase.c.position_settled", ticker=ticker,
                           qty=bracket.position_quantity, api_count=api_count)
                bracket.phase = Phase.CLOSED
                self.active_positions.pop(ticker, None)
                self.brackets.pop(ticker, None)
                async with await self.db.get_session() as session:
                    await session.execute(
                        delete(PositionModel).where(PositionModel.market_ticker == ticker)
                    )
                    await session.commit()
                continue

            # Self-heal bracket.avg_entry when missing
            if not bracket.avg_entry or bracket.avg_entry <= 0:
                heal_source: Optional[str] = None
                avg_cents = int(pos_data.get("average_fill_cost_cents") or 0)
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

            # Dead-simple stop-loss: use ONLY last traded price from cache.
            # If no last-traded price exists, skip this cycle entirely.
            last_traded_price = self.cache.get_last_price(ticker)

            if last_traded_price is None:
                # No last-traded price yet — skip stop-loss check this cycle.
                logger.debug("phase.c.no_last_price_skip", ticker=ticker)
                continue

            if last_traded_price < self.config.stop_loss_price:
                if bracket.position_quantity <= 0:
                    bracket.phase = Phase.CLOSED
                    self.active_positions.pop(ticker, None)
                    self.brackets.pop(ticker, None)
                    logger.info("phase.c.stop_loss_zero_qty", ticker=ticker)
                    continue

                logger.warning("phase.c.stop_loss_triggered", ticker=ticker,
                               last_price=last_traded_price,
                               stop_loss=self.config.stop_loss_price)

                # Increment the stop-loss ledger once per triggered event
                # (guarded by _stop_loss_counted flag to avoid double-counting on retries)
                if not getattr(bracket, '_stop_loss_counted', False):
                    await self._increment_stop_loss_count_for_market(ticker)
                    bracket._stop_loss_counted = True

                await self._execute_stop_loss(bracket)

    async def _execute_stop_loss(self, bracket: MarketBracket):
        """Execute a stop-loss: sell entire position at 1¢ IOC to guarantee fill."""
        now = asyncio.get_event_loop().time()
        last_attempt = getattr(bracket, '_last_stop_loss_attempt', 0)
        if now - last_attempt < 60:
            return
        bracket._last_stop_loss_attempt = now

        price = 1  # Sell at minimum price to guarantee fill

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

    async def _fetch_market_data_via_rest(self, ticker: str) -> Optional[dict]:
        """Fetch current market data via Kalshi REST /markets/{ticker} endpoint."""
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
        if self._snapshot_counter % 60 != 0:
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
                    await session.execute(
                        delete(StreamedTrade).where(StreamedTrade.trade_ts < cutoff)
                    )
                    await session.commit()
                    logger.info("db.cleanup", hours_retained=24)
            except Exception as e:
                logger.error("db.cleanup_error", error=str(e))

            await asyncio.sleep(3600)

    async def stop(self):
        self._running = False
        logger.info("strategy.stopped")
