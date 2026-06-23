# app/models.py
from sqlalchemy import Column, String, Integer, Float, DateTime, Enum, BigInteger, Text, JSON, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func
import enum


class Base(DeclarativeBase):
    pass


class TradeAction(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    HEDGE = "HEDGE"
    STOP_LOSS = "STOP_LOSS"


class TradeStatus(str, enum.Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class StreamedTrade(Base):
    """Logs every trade/tick received via WebSocket."""
    __tablename__ = "streamed_trades"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    market_ticker = Column(String(200), nullable=False, index=True)
    event_ticker = Column(String(200), nullable=True)
    series_ticker = Column(String(200), nullable=True)
    price = Column(Integer, nullable=False, comment="Price in cents ($0.01 increments)")
    quantity = Column(Integer, nullable=False)
    side = Column(String(10), nullable=True)
    trade_ts = Column(DateTime, nullable=False)
    ingested_at = Column(DateTime, server_default=func.now())


class StreamedTicker(Base):
    """Logs ticker snapshots received via WebSocket ticker channel."""
    __tablename__ = "streamed_tickers"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    market_ticker = Column(String(200), nullable=False, index=True)
    last_price = Column(Integer, nullable=True)
    yes_bid = Column(Integer, nullable=True)
    yes_ask = Column(Integer, nullable=True)
    volume = Column(BigInteger, nullable=True)
    open_interest = Column(BigInteger, nullable=True)
    ticker_ts = Column(DateTime, nullable=False)
    ingested_at = Column(DateTime, server_default=func.now())


class ExecutedTrade(Base):
    """Logs trades this application executed (real or paper)."""
    __tablename__ = "executed_trades"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    market_ticker = Column(String(200), nullable=False, index=True)
    action = Column(Enum(TradeAction), nullable=False)
    side = Column(String(10), nullable=False)           # "yes" or "no"
    price = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    total_cost_cents = Column(Integer, nullable=False)
    trade_mode = Column(String(10), nullable=False)     # "PAPER" or "LIVE"
    status = Column(Enum(TradeStatus), nullable=False)
    kalshi_order_id = Column(String(100), nullable=True)
    executed_at = Column(DateTime, server_default=func.now())
    notes = Column(Text, nullable=True)


class Position(Base):
    """Current open positions (updated on fill/close)."""
    __tablename__ = "positions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    market_ticker = Column(String(200), nullable=False, unique=True, index=True)
    event_ticker = Column(String(200), nullable=True)
    series_ticker = Column(String(200), nullable=True)
    side = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False, default=0)
    avg_entry_price = Column(Integer, nullable=True)
    hedge_market_ticker = Column(String(200), nullable=True)
    hedge_quantity = Column(Integer, nullable=True)
    last_price = Column(Integer, nullable=True)
    unrealized_pnl = Column(Integer, nullable=True)
    # 1 = hedge is armed/deferred for this event; 0 or NULL = not armed.
    # Integer (not Boolean) for broad DB compatibility. Set on the original
    # bracket's row when a hedge is deferred, cleared on fill or close.
    hedge_pending = Column(Integer, nullable=True, default=0)
    position_ts = Column(DateTime, server_default=func.now(), onupdate=func.now())


class PortfolioSnapshot(Base):
    """Periodic portfolio state snapshots."""
    __tablename__ = "portfolio_snapshots"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    cash_balance_cents = Column(BigInteger, nullable=False)
    total_positions = Column(Integer, nullable=False)
    total_risk_cents = Column(BigInteger, nullable=False)
    snapshot_ts = Column(DateTime, server_default=func.now())


class EventWindow(Base):
    """Tracks which events/markets are in which state machine phase."""
    __tablename__ = "event_windows"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    market_ticker = Column(String(200), unique=True, nullable=False, index=True)
    event_ticker = Column(String(200), nullable=True)
    series_ticker = Column(String(200), nullable=True)
    phase = Column(String(50), nullable=False)  # "MONITORING", "WATCHING", "ENTERING", "HOLDING", "HEDGED", "CLOSED"
    bracket_label = Column(String(50), nullable=True)  # e.g. "96-97"
    last_price = Column(Integer, nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class StopLossLedger(Base):
    """Persistent per-(series_ticker, date_prefix) stop-loss counter for martingale sizing."""
    __tablename__ = "stop_loss_ledger"

    id = Column(Integer, primary_key=True, autoincrement=True)
    series_ticker = Column(String(200), nullable=False, index=True)
    date_prefix = Column(String(20), nullable=False)
    stop_loss_count = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("series_ticker", "date_prefix", name="uq_stop_loss_ledger_series_date"),
    )
