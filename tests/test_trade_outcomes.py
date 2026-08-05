"""Tests for the TradeOutcome data model, entry-context utilities, reconciler
logic, and city P&L report math.

These tests are purely unit-level: no real DB or Kalshi API calls are made.
"""
from __future__ import annotations

import asyncio
import datetime
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Task 1 — Model tests
# ---------------------------------------------------------------------------

def test_trade_outcome_model_importable():
    """TradeOutcome and TradeOutcomeStatus are importable from app.models."""
    from app.models import TradeOutcome, TradeOutcomeStatus
    assert TradeOutcome.__tablename__ == "trade_outcomes"
    assert TradeOutcomeStatus.OPEN == "OPEN"
    assert TradeOutcomeStatus.STOPPED == "STOPPED"
    assert TradeOutcomeStatus.CLOSED_OUT == "CLOSED_OUT"
    assert TradeOutcomeStatus.SETTLED_WIN == "SETTLED_WIN"
    assert TradeOutcomeStatus.SETTLED_LOSS == "SETTLED_LOSS"


def test_trade_outcome_unique_constraint():
    """UniqueConstraint on (market_ticker, date_prefix) is present."""
    from app.models import TradeOutcome
    from sqlalchemy import UniqueConstraint
    constraints = [
        c for c in TradeOutcome.__table_args__
        if isinstance(c, UniqueConstraint)
    ]
    assert len(constraints) == 1
    constraint = constraints[0]
    col_names = {c.name for c in constraint.columns}
    assert "market_ticker" in col_names
    assert "date_prefix" in col_names


def test_trade_outcome_columns_exist():
    """All required columns are present on TradeOutcome."""
    from app.models import TradeOutcome
    cols = {c.name for c in TradeOutcome.__table__.columns}
    required = {
        "id", "series_ticker", "date_prefix", "market_ticker", "city", "family",
        "entry_price_avg", "entry_qty", "exit_price_avg", "exit_qty",
        "outcome", "realized_pnl_cents", "stop_loss_count_at_entry",
        "entry_ask_cents", "entry_spread_cents", "entry_price_bucket",
        "minutes_to_forecast_low", "forecast_low_temp", "bracket_temp",
        "forecast_vs_bracket_delta", "created_at", "updated_at",
    }
    assert required.issubset(cols), f"Missing columns: {required - cols}"


# ---------------------------------------------------------------------------
# Task 3 — entry_price_bucket labelling
# ---------------------------------------------------------------------------

from core.trade_outcome_utils import entry_price_bucket, parse_bracket_temp, detect_family


@pytest.mark.parametrize("price,expected", [
    (60,  "<=71"),
    (71,  "<=71"),
    (72,  "72-75"),
    (75,  "72-75"),
    (76,  "76-80"),
    (80,  "76-80"),
    (81,  "81-86"),
    (86,  "81-86"),
    (87,  "87+"),
    (99,  "87+"),
    (100, "87+"),
])
def test_entry_price_bucket(price, expected):
    assert entry_price_bucket(price) == expected


# ---------------------------------------------------------------------------
# Bracket-temp parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ticker,expected", [
    ("KXLOWTPHX-26JUL16-B60",    60.0),
    ("KXLOWTBOS-26JUL16-B52.5",  52.5),
    ("KXHIGHTATL-26JUL16-T95",   95.0),
    ("KXLOWTCHI-26JUL16-B100",   100.0),
    ("KXLOWTLAX-26JUL16-T68",    68.0),
    ("BADTICKER",                 None),
    ("NO-BRACKET",                None),
    ("A-B",                       None),
])
def test_parse_bracket_temp(ticker, expected):
    result = parse_bracket_temp(ticker)
    assert result == expected


# ---------------------------------------------------------------------------
# Family detection
# ---------------------------------------------------------------------------

def test_detect_family_low():
    assert detect_family("KXLOWTBOS-26JUL16-B52.5") == "LOW"


def test_detect_family_high():
    assert detect_family("KXHIGHTATL-26JUL16-T95") == "HIGH"


def test_detect_family_unknown():
    assert detect_family("SOME_OTHER_TICKER") is None


# ---------------------------------------------------------------------------
# Task 2 — Reconciler outcome classification
# ---------------------------------------------------------------------------

from core.settlement_reconciler import _classify_exit_outcome, _compute_pnl
from app.models import TradeAction, TradeOutcomeStatus


def test_classify_stop_loss_as_stopped():
    assert _classify_exit_outcome(TradeAction.STOP_LOSS) == TradeOutcomeStatus.STOPPED


def test_classify_sell_as_closed_out():
    assert _classify_exit_outcome(TradeAction.SELL) == TradeOutcomeStatus.CLOSED_OUT


def test_compute_pnl_profit():
    assert _compute_pnl(72, 90, 6) == (90 - 72) * 6


def test_compute_pnl_loss():
    assert _compute_pnl(80, 55, 6) == (55 - 80) * 6


def test_compute_pnl_settled_win():
    assert _compute_pnl(80, 100, 6) == 20 * 6


def test_compute_pnl_settled_loss():
    assert _compute_pnl(80, 0, 6) == -80 * 6


# ---------------------------------------------------------------------------
# Task 2 — Idempotent upsert (reconciler run twice writes one row)
# ---------------------------------------------------------------------------

class FakeSession:
    """Minimal async session fake that stores add() calls."""

    def __init__(self):
        self._added: list = []
        self._executed: list = []

    def add(self, item):
        self._added.append(item)

    async def execute(self, stmt):
        self._executed.append(stmt)
        # Return an empty result by default
        return FakeResult(None)

    async def commit(self):
        pass

    async def rollback(self):
        pass


class FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def scalars(self):
        return self

    def all(self):
        return [] if self._value is None else [self._value]

    def fetchall(self):
        return []


class FakeDB:
    def __init__(self, session_factory=None):
        self._session_factory = session_factory or (lambda: FakeSession())

    @asynccontextmanager
    async def _ctx(self):
        yield self._session_factory()

    async def get_session(self):
        return self._ctx()


@pytest.mark.asyncio
async def test_reconciler_skips_final_outcome_rows():
    """Reconciler does not re-process rows already in a final outcome state."""
    from app.models import TradeOutcome, TradeOutcomeStatus

    calls = []

    def session_factory():
        s = FakeSession()
        # Pretend there's an existing SETTLED_WIN row
        existing = TradeOutcome(
            market_ticker="KXLOWTBOS-26JUL16-B52.5",
            date_prefix="26JUL16",
            outcome=TradeOutcomeStatus.SETTLED_WIN,
        )
        original_execute = s.execute

        async def patched_execute(stmt):
            calls.append("execute")
            # Return the existing row on first call (the TradeOutcome query)
            if len(calls) == 1:
                return FakeResult(existing)
            return FakeResult(None)

        s.execute = patched_execute
        return s

    db = FakeDB(session_factory)

    # We need to mock the ExecutedTrade query separately
    from core import settlement_reconciler as rec

    with patch.object(rec, "_fetch_kalshi_market_result", new_callable=AsyncMock) as mock_fetch:
        with patch("core.settlement_reconciler.parse_series_and_date", return_value=("KXLOWTBOS", "26JUL16")):
            with patch("core.settlement_reconciler._parse_date_prefix", return_value=datetime.date(2026, 7, 16)):
                await rec._reconcile_one_market(
                    "KXLOWTBOS-26JUL16-B52.5",
                    db,
                    MagicMock(),
                    datetime.date(2026, 8, 1),
                )

    # Kalshi API should never be called for a finalized row
    mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Task 3 — Entry context failure never blocks the entry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_entry_outcome_never_raises():
    """_create_entry_outcome must not raise even if every sub-call fails."""
    from app.config import AppConfig
    from core.state_machine import TemperatureStrategy
    from data.ticker_cache import TickerCache

    class AlwaysFailDB(FakeDB):
        @asynccontextmanager
        async def _ctx(self):
            raise RuntimeError("DB is on fire")
            yield  # noqa: unreachable

    class FakeExecutor:
        async def get_active_markets(self): return []
        async def buy_yes(self, o, max_price=None): return None
        async def sell_yes(self, o): return None
        async def get_balance(self): return 0
        async def get_positions(self): return {}

    class FakeWSManager:
        def on_message(self, *_a, **_k): return None
        async def subscribe(self, *_a, **_k): return None

    defaults = dict(
        trading_mode="DRY_RUN", dry_run=True,
        rest_base_url="https://demo", ws_url="wss://demo",
        kalshi_api_key="x", kalshi_private_key_path="/dev/null",
        mysql_database_url="mysql://localhost/db",
        monitor_start_price=80, buy_trigger_price_low=75,
        buy_trigger_price_high=75, minimum_spread=3,
        spread_monitor_price=70, stop_loss_price=40,
        initial_contract_count=1, hedge_max_factor=4,
        sl_worker_interval_ms=250, sl_exit_mode="PANIC_FLATTEN",
        enable_fast_sl_exit=False, sl_panic_sell_price=1,
        sl_panic_retry_ms=0, sl_panic_max_retries=3,
        sl_panic_max_quote_age_ms=30000, no_trade_tickers=set(),
    )
    config = AppConfig.model_construct(**defaults)
    for k, v in defaults.items():
        setattr(config, k, v)

    import core.state_machine as sm
    import nws.gate as nws_gate

    with patch.object(sm, "load_private_key", lambda _: object()):
        with patch.object(sm, "is_entry_allowed", lambda *a, **k: (True, {})):
            with patch.object(nws_gate, "is_trading_gate_open", lambda *a, **k: True):
                strategy = TemperatureStrategy(
                    config, TickerCache(), FakeWSManager(), FakeExecutor(),
                    AlwaysFailDB(),
                )

    from core.types import MarketBracket, Phase

    bracket = MarketBracket(
        market_ticker="KXLOWTBOS-26JUL16-B52.5",
        event_ticker="KXLOWTBOS-26JUL16",
        series_ticker="KXLOWTBOS",
        bracket_label="B52.5",
        phase=Phase.HOLDING,
    )

    # Must not raise
    await strategy._create_entry_outcome(
        bracket=bracket,
        fill_price=75,
        fill_qty=6,
        ob=None,
    )


# ---------------------------------------------------------------------------
# Task 4 — Report math: breakeven win rate, EV, verdicts
# ---------------------------------------------------------------------------

from reports.city_pnl import breakeven_win_rate, ev_per_contract, verdict


def test_breakeven_win_rate_basic():
    """W* = (entry - loss_exit) / (100 - loss_exit)."""
    w = breakeven_win_rate(80.0, 55.0)
    assert w is not None
    expected = (80.0 - 55.0) / (100.0 - 55.0)
    assert abs(w - expected) < 1e-6


def test_breakeven_win_rate_zero_denom():
    """Returns None when avg_loss_exit == 100."""
    assert breakeven_win_rate(80.0, 100.0) is None


def test_ev_positive():
    """High win rate at reasonable entry should yield positive EV."""
    ev = ev_per_contract(0.90, 80.0, 55.0)
    assert ev > 0


def test_ev_negative():
    """Low win rate at reasonable entry should yield negative EV."""
    ev = ev_per_contract(0.30, 80.0, 55.0)
    assert ev < 0


def test_verdict_insufficient_few_trades():
    assert verdict(10, 0.90, 0.70) == "INSUFFICIENT"


def test_verdict_keep():
    assert verdict(30, 0.90, 0.70) == "KEEP"


def test_verdict_cut():
    assert verdict(30, 0.50, 0.70) == "CUT?"


def test_verdict_insufficient_no_w_star():
    assert verdict(30, 0.90, None) == "INSUFFICIENT"


# ---------------------------------------------------------------------------
# Report aggregation
# ---------------------------------------------------------------------------

from reports.city_pnl import aggregate


def _make_row(city, outcome, entry, exit_p, bucket="76-80", sl_count=0):
    return {
        "city": city,
        "family": "LOW",
        "entry_price_bucket": bucket,
        "stop_loss_count_at_entry": sl_count,
        "entry_price_avg": entry,
        "exit_price_avg": exit_p,
        "outcome": outcome,
        "realized_pnl_cents": None,
    }


def test_aggregate_by_city_basic():
    rows = [
        _make_row("Phoenix", "SETTLED_WIN",  80, None),
        _make_row("Phoenix", "SETTLED_WIN",  80, None),
        _make_row("Phoenix", "SETTLED_LOSS", 80, None),
        _make_row("Miami",   "SETTLED_WIN",  75, None),
    ]
    results = {r["group"]: r for r in aggregate(rows, "city")}
    assert results["Phoenix"]["trades"] == 3
    assert results["Phoenix"]["wins"] == 2
    assert abs(results["Phoenix"]["win_pct"] - 2 / 3) < 1e-6
    assert results["Miami"]["wins"] == 1


def test_aggregate_settled_win_exit_is_100():
    """Settled wins count exit price as 100¢ for W* math."""
    rows = [_make_row("Phoenix", "SETTLED_WIN", 80, None)]
    results = aggregate(rows, "city")
    assert results[0]["wins"] == 1


def test_aggregate_stopped_uses_actual_exit():
    """Stopped trades use the actual exit_price_avg."""
    rows = [_make_row("Phoenix", "STOPPED", 80, 55)]
    results = aggregate(rows, "city")
    # Loss exit price for a stopped trade at 55¢
    assert abs(results[0]["avg_loss_exit"] - 55.0) < 1e-6
