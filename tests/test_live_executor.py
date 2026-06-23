import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import execution.live as live
from core.types import OrderRequest, OrderSide
from execution.live import LiveTradeExecutor


class FakeResponse:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self._data = data

    def json(self):
        return self._data


class FakeClient:
    def __init__(self, responses=None, get_responses=None):
        self.responses = list(responses or [])
        self.get_responses = list(get_responses or [])
        self.post_payloads = []
        self.get_urls = []
        self.get_kwargs = []  # records kwargs (including params) for each GET call

    async def post(self, _url, json=None, headers=None):
        self.post_payloads.append(json)
        return self.responses.pop(0)

    async def get(self, url, headers=None, **kwargs):
        self.get_urls.append(url)
        self.get_kwargs.append(kwargs)
        return self.get_responses.pop(0)

    async def aclose(self):
        return None


def _make_executor(monkeypatch, responses=None, *, get_responses=None, dry_run=False):
    monkeypatch.setattr(live, "load_private_key", lambda _path: object())
    monkeypatch.setattr(live, "build_auth_headers", lambda *_args, **_kwargs: {})
    executor = LiveTradeExecutor("https://example.test", "test-key", "unused.pem", dry_run=dry_run)
    executor._client = FakeClient(responses, get_responses)
    return executor


@pytest.mark.asyncio
async def test_sell_yes_no_fill_returns_failure_and_ioc_payload(monkeypatch):
    executor = _make_executor(monkeypatch, [FakeResponse(200, {"order_id": "o1", "fill": {}})])
    order = OrderRequest("TICKER", OrderSide.SELL_YES, 1, 2)

    result = await executor.sell_yes(order)
    payload = executor._client.post_payloads[0]

    assert payload["time_in_force"] == "immediate_or_cancel"
    assert payload["reduce_only"] is True
    assert payload["post_only"] is False
    assert payload["side"] == "ask"
    assert result.success is False
    assert result.status == "NO_FILL"
    assert result.fill_quantity == 0
    assert result.fill_price == 0
    assert result.side == "yes"


@pytest.mark.asyncio
async def test_sell_yes_partial_fill_uses_actual_fill_fields(monkeypatch):
    executor = _make_executor(
        monkeypatch,
        [FakeResponse(201, {"order_id": "o2", "fill": {"count": 1, "price": 6}})],
    )
    order = OrderRequest("TICKER", OrderSide.SELL_YES, 1, 2)

    result = await executor.sell_yes(order)

    assert result.success is True
    assert result.side == "yes"
    assert result.fill_quantity == 1
    assert result.fill_price == 6
    assert result.total_cost_cents == -6


@pytest.mark.asyncio
async def test_buy_yes_no_fill_returns_failure(monkeypatch):
    executor = _make_executor(monkeypatch, [FakeResponse(200, {"order_id": "o3", "fill": {}})])
    order = OrderRequest("TICKER", OrderSide.BUY_YES, 80, 2)

    result = await executor.buy_yes(order, max_price=90)

    assert result.success is False
    assert result.status == "NO_FILL"
    assert result.fill_quantity == 0
    assert result.fill_price == 0


@pytest.mark.asyncio
async def test_buy_yes_partial_fill_uses_actual_fill_fields(monkeypatch):
    executor = _make_executor(
        monkeypatch,
        [FakeResponse(201, {"order_id": "o4", "fill": {"count": 1, "price": 82}})],
    )
    order = OrderRequest("TICKER", OrderSide.BUY_YES, 80, 2)

    result = await executor.buy_yes(order, max_price=90)

    assert result.success is True
    assert result.fill_quantity == 1
    assert result.fill_price == 82
    assert result.total_cost_cents == 82


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("position_payload", "expected_cost", "expected_source"),
    [
        (
            {
                "ticker": "TICKER",
                "position_fp": "2.00",
                "market_exposure": "164",
                "last_price": "0.35",
            },
            82,
            "market_exposure",
        ),
        (
            {
                "ticker": "TICKER",
                "position_fp": "2.00",
                "average_fill_cost_dollars": "0.8400",
                "last_price": "0.35",
            },
            84,
            "average_fill_cost_dollars",
        ),
        (
            {
                "ticker": "TICKER",
                "position_fp": "2.00",
                "last_price": "0.35",
            },
            0,
            "none",
        ),
    ],
)
async def test_get_positions_cost_basis_fallbacks(monkeypatch, position_payload, expected_cost, expected_source):
    debug_logged = []
    monkeypatch.setattr(live.logger, "debug", lambda event, **kwargs: debug_logged.append((event, kwargs)))
    executor = _make_executor(
        monkeypatch,
        get_responses=[FakeResponse(200, {"market_positions": [position_payload]})],
    )

    positions = await executor.get_positions()

    assert positions["TICKER"]["average_fill_cost_cents"] == expected_cost
    assert positions["TICKER"]["count"] == 2
    assert positions["TICKER"]["last_price_cents"] == 35
    cost_log = next(kwargs for event, kwargs in debug_logged if event == "live.position_cost_basis")
    assert cost_log["source"] == expected_source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "order", "kwargs"),
    [
        ("buy_yes", OrderRequest("TICKER", OrderSide.BUY_YES, 80, 2), {"max_price": 90}),
        ("sell_yes", OrderRequest("TICKER", OrderSide.SELL_YES, 20, 2), {}),
    ],
)
async def test_dry_run_skips_live_orders(monkeypatch, method_name, order, kwargs):
    warning_logged = []
    monkeypatch.setattr(live.logger, "warning", lambda event, **kwargs: warning_logged.append((event, kwargs)))
    executor = _make_executor(monkeypatch, dry_run=True)

    result = await getattr(executor, method_name)(order, **kwargs)

    assert result.success is False
    assert result.status == "DRY_RUN"
    assert result.fill_quantity == 0
    assert executor._client.post_payloads == []
    dry_run_log = next(kwargs for event, kwargs in warning_logged if event == "live.dry_run_skip_order")
    assert dry_run_log["ticker"] == "TICKER"


# ---------------------------------------------------------------------------
# _to_dollars_float robustness
# ---------------------------------------------------------------------------

def test_to_dollars_float_robustness():
    assert live._to_dollars_float("0.8500") == pytest.approx(0.85)
    assert live._to_dollars_float("") == 0.0
    assert live._to_dollars_float(None) == 0.0
    assert live._to_dollars_float("abc") == 0.0


# ---------------------------------------------------------------------------
# _extract_fill — real Kalshi API shapes
# ---------------------------------------------------------------------------

def test_extract_fill_nyc_single_fill():
    """NYC: taker_fill_cost=1.70, fill_count=2 → 85¢ (NOT the limit 90¢)."""
    data = {
        "fill_count_fp": "2.00",
        "taker_fill_cost_dollars": "1.700000",
        "maker_fill_cost_dollars": "0.000000",
        "yes_price_dollars": "0.9000",
        "order_id": "29a4b107-2fbe-48de-b3d4-e317661a895a",
        "status": "executed",
    }
    count, price = live._extract_fill(data)
    assert count == 2
    assert price == 85
    assert price != 90  # must NOT use limit price


def test_extract_fill_miami_multi_lot():
    """Miami: taker_fill_cost=2.02, fill_count=6 → 34¢ (NOT the limit 40¢)."""
    data = {
        "fill_count_fp": "6.00",
        "taker_fill_cost_dollars": "2.020000",
        "maker_fill_cost_dollars": "0.000000",
        "yes_price_dollars": "0.4000",
        "order_id": "e1bdb76c-8366-43b2-b0ff-dac6ac74fae1",
        "status": "executed",
    }
    count, price = live._extract_fill(data)
    assert count == 6
    assert price == 34
    assert price != 40  # must NOT use limit price


def test_extract_fill_zero_fill():
    """Order placed but not yet filled (fill_count_fp=0)."""
    data = {"fill_count_fp": "0.00", "remaining_count_fp": "2.00"}
    count, price = live._extract_fill(data)
    assert count == 0
    assert price == 0


def test_extract_fill_fallback_to_limit_when_no_cost(monkeypatch):
    """When *_fill_cost_dollars are absent, falls back to yes_price_dollars and warns."""
    warnings_logged = []
    monkeypatch.setattr(live.logger, "warning", lambda event, **kw: warnings_logged.append(event))

    data = {"fill_count_fp": "1.00", "yes_price_dollars": "0.5000"}
    count, price = live._extract_fill(data)
    assert count == 1
    assert price == 50
    assert "live.fill_price_fallback_to_limit" in warnings_logged


def test_extract_fill_wrapped_order_key():
    """API response wrapped under {"order": {...}} still returns correct values."""
    data = {
        "order": {
            "fill_count_fp": "2.00",
            "taker_fill_cost_dollars": "1.700000",
            "maker_fill_cost_dollars": "0.000000",
            "yes_price_dollars": "0.9000",
            "order_id": "29a4b107-2fbe-48de-b3d4-e317661a895a",
        }
    }
    count, price = live._extract_fill(data)
    assert count == 2
    assert price == 85


# ---------------------------------------------------------------------------
# _avg_fill_price_cents_from_fills
# ---------------------------------------------------------------------------

def test_avg_fill_price_cents_from_fills_miami():
    """Miami: 1 lot @37¢ + 5 lots @33¢ → weighted avg → 34¢."""
    fills = [
        {
            "ticker": "KXLOWTMIA-26JUN22-B80.5",
            "action": "buy",
            "count_fp": "1.00",
            "yes_price_dollars": "0.3700",
        },
        {
            "ticker": "KXLOWTMIA-26JUN22-B80.5",
            "action": "buy",
            "count_fp": "5.00",
            "yes_price_dollars": "0.3300",
        },
    ]
    result = live._avg_fill_price_cents_from_fills(fills, "KXLOWTMIA-26JUN22-B80.5")
    assert result == 34


def test_avg_fill_price_cents_from_fills_ignores_sells_and_other_tickers():
    """Sell fills and fills for other tickers must be excluded from cost basis."""
    fills = [
        {
            "ticker": "KXLOWTMIA-26JUN22-B80.5",
            "action": "buy",
            "count_fp": "4.00",
            "yes_price_dollars": "0.3500",
        },
        {
            "ticker": "KXLOWTMIA-26JUN22-B80.5",
            "action": "sell",  # should be ignored
            "count_fp": "2.00",
            "yes_price_dollars": "0.9000",
        },
        {
            "ticker": "KXHIGHNY-26JUN22-B72.5",  # different ticker — should be ignored
            "action": "buy",
            "count_fp": "3.00",
            "yes_price_dollars": "0.9000",
        },
    ]
    result = live._avg_fill_price_cents_from_fills(fills, "KXLOWTMIA-26JUN22-B80.5")
    assert result == 35  # only the 4@35¢ buy counts


# ---------------------------------------------------------------------------
# get_positions fills_history fallback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_positions_fills_history_fallback(monkeypatch):
    """When position cost is unknown from positions endpoint, fills_history is used."""
    debug_logged = []
    monkeypatch.setattr(live.logger, "debug", lambda event, **kw: debug_logged.append((event, kw)))

    positions_resp = FakeResponse(200, {
        "market_positions": [{
            "ticker": "KXLOWTNYC-26JUN22-B67.5",
            "position_fp": "2.00",
            "last_price": "0.85",
        }]
    })
    fills_resp = FakeResponse(200, {
        "fills": [
            {
                "ticker": "KXLOWTNYC-26JUN22-B67.5",
                "action": "buy",
                "count_fp": "2.00",
                "yes_price_dollars": "0.8500",
            }
        ]
    })
    executor = _make_executor(monkeypatch, get_responses=[positions_resp, fills_resp])

    positions = await executor.get_positions()

    assert positions["KXLOWTNYC-26JUN22-B67.5"]["average_fill_cost_cents"] == 85
    cost_log = next(kw for event, kw in debug_logged if event == "live.position_cost_basis")
    assert cost_log["source"] == "fills_history"


# ---------------------------------------------------------------------------
# get_fills — per-ticker pagination (new tests)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_fills_pagination_aggregates_pages(monkeypatch):
    """Per-ticker pagination fetches all pages and aggregates fills."""
    ticker = "KXLOWTMIA-26JUN22-B80.5"
    f1 = {"ticker": ticker, "action": "buy", "count_fp": "1.00", "yes_price_dollars": "0.3700"}
    f2 = {"ticker": ticker, "action": "buy", "count_fp": "2.00", "yes_price_dollars": "0.3300"}
    f3 = {"ticker": ticker, "action": "buy", "count_fp": "3.00", "yes_price_dollars": "0.3100"}

    resp1 = FakeResponse(200, {"fills": [f1, f2], "cursor": "abc"})
    resp2 = FakeResponse(200, {"fills": [f3], "cursor": ""})

    executor = _make_executor(monkeypatch, get_responses=[resp1, resp2])

    fills = await executor.get_fills(ticker=ticker)

    assert fills == [f1, f2, f3]
    assert len(executor._client.get_urls) == 2  # stopped because second cursor was empty


@pytest.mark.asyncio
async def test_get_fills_stops_on_empty_page(monkeypatch):
    """Pagination stops immediately when an empty fills page is returned."""
    ticker = "KXLOWTMIA-26JUN22-B80.5"
    f1 = {"ticker": ticker, "action": "buy", "count_fp": "1.00", "yes_price_dollars": "0.3500"}

    resp1 = FakeResponse(200, {"fills": [f1], "cursor": "x"})
    resp2 = FakeResponse(200, {"fills": [], "cursor": "x"})

    executor = _make_executor(monkeypatch, get_responses=[resp1, resp2])

    fills = await executor.get_fills(ticker=ticker)

    assert fills == [f1]
    assert len(executor._client.get_urls) == 2  # stopped because second page was empty


@pytest.mark.asyncio
async def test_get_fills_max_pages_cap(monkeypatch):
    """max_pages limits the number of API calls even when cursor never empties."""
    page = [{"ticker": "T", "action": "buy", "count_fp": "1.00", "yes_price_dollars": "0.5000"}]
    # Provide more responses than max_pages to prove the cap fires
    responses = [FakeResponse(200, {"fills": page, "cursor": "next"}) for _ in range(5)]

    executor = _make_executor(monkeypatch, get_responses=responses)

    fills = await executor.get_fills(ticker="T", max_pages=3)

    assert len(executor._client.get_urls) == 3  # capped at max_pages
    assert len(fills) == 3  # one fill per page × 3 pages


@pytest.mark.asyncio
async def test_get_fills_ticker_param_forwarded(monkeypatch):
    """ticker is included in every request's params; cursor is added on subsequent pages."""
    ticker = "KXLOWTMIA-26JUN22-B80.5"
    f1 = {"ticker": ticker, "action": "buy", "count_fp": "1.00", "yes_price_dollars": "0.3700"}
    f2 = {"ticker": ticker, "action": "buy", "count_fp": "2.00", "yes_price_dollars": "0.3300"}

    resp1 = FakeResponse(200, {"fills": [f1], "cursor": "abc"})
    resp2 = FakeResponse(200, {"fills": [f2], "cursor": ""})

    executor = _make_executor(monkeypatch, get_responses=[resp1, resp2])

    await executor.get_fills(ticker=ticker)

    # First request carries ticker but no cursor
    params1 = executor._client.get_kwargs[0].get("params", {})
    assert params1.get("ticker") == ticker
    assert "cursor" not in params1

    # Second request carries ticker AND the cursor from page 1
    params2 = executor._client.get_kwargs[1].get("params", {})
    assert params2.get("ticker") == ticker
    assert params2.get("cursor") == "abc"


@pytest.mark.asyncio
async def test_get_fills_no_caching(monkeypatch):
    """Calling get_fills twice always hits the API twice — results are never memoized."""
    ticker = "KXLOWTMIA-26JUN22-B80.5"
    f = {"ticker": ticker, "action": "buy", "count_fp": "1.00", "yes_price_dollars": "0.3500"}

    resp1 = FakeResponse(200, {"fills": [f], "cursor": ""})
    resp2 = FakeResponse(200, {"fills": [f], "cursor": ""})

    executor = _make_executor(monkeypatch, get_responses=[resp1, resp2])

    await executor.get_fills(ticker=ticker)
    await executor.get_fills(ticker=ticker)

    assert len(executor._client.get_urls) == 2  # two separate API calls, no caching


# ---------------------------------------------------------------------------
# get_positions — per-ticker guard and no-fills edge case
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_positions_per_ticker_fills_only_for_none(monkeypatch):
    """get_fills is called only for positions that have no resolved cost basis."""
    debug_logged = []
    monkeypatch.setattr(live.logger, "debug", lambda event, **kw: debug_logged.append((event, kw)))

    positions_resp = FakeResponse(200, {
        "market_positions": [
            {
                "ticker": "HEALTHY",
                "position_fp": "2.00",
                "average_fill_cost_dollars": "0.8500",
                "last_price": "0.90",
            },
            {
                "ticker": "MISSING",
                "position_fp": "3.00",
                "last_price": "0.50",
            },
        ]
    })
    fills_resp = FakeResponse(200, {
        "fills": [
            {
                "ticker": "MISSING",
                "action": "buy",
                "count_fp": "3.00",
                "yes_price_dollars": "0.4000",
            }
        ],
        "cursor": "",
    })

    executor = _make_executor(monkeypatch, get_responses=[positions_resp, fills_resp])

    # Wrap get_fills to record which tickers triggered a call
    get_fills_tickers: list = []
    original_get_fills = executor.get_fills

    async def tracked_get_fills(ticker=None, **kwargs):
        get_fills_tickers.append(ticker)
        return await original_get_fills(ticker=ticker, **kwargs)

    executor.get_fills = tracked_get_fills

    positions = await executor.get_positions()

    # get_fills called only for the unresolved ticker
    assert "MISSING" in get_fills_tickers
    assert "HEALTHY" not in get_fills_tickers

    # MISSING resolved via fills history
    missing_log = next(kw for event, kw in debug_logged if event == "live.position_cost_basis" and kw["ticker"] == "MISSING")
    assert missing_log["source"] == "fills_history"
    assert missing_log["cents"] == 40

    # HEALTHY used average_fill_cost_dollars, undisturbed
    healthy_log = next(kw for event, kw in debug_logged if event == "live.position_cost_basis" and kw["ticker"] == "HEALTHY")
    assert healthy_log["source"] == "average_fill_cost_dollars"
    assert healthy_log["cents"] == 85


@pytest.mark.asyncio
async def test_get_positions_no_fills_stays_none(monkeypatch):
    """When get_fills returns [] the position stays source=none with cents=0."""
    debug_logged = []
    monkeypatch.setattr(live.logger, "debug", lambda event, **kw: debug_logged.append((event, kw)))

    positions_resp = FakeResponse(200, {
        "market_positions": [{
            "ticker": "KXLOWTMIA-26JUN22-B80.5",
            "position_fp": "2.00",
            "last_price": "0.50",
        }]
    })
    fills_resp = FakeResponse(200, {"fills": [], "cursor": ""})

    executor = _make_executor(monkeypatch, get_responses=[positions_resp, fills_resp])

    positions = await executor.get_positions()

    assert positions["KXLOWTMIA-26JUN22-B80.5"]["average_fill_cost_cents"] == 0
    cost_log = next(kw for event, kw in debug_logged if event == "live.position_cost_basis")
    assert cost_log["source"] == "none"
