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

    async def post(self, _url, json=None, headers=None):
        self.post_payloads.append(json)
        return self.responses.pop(0)

    async def get(self, url, headers=None):
        self.get_urls.append(url)
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
