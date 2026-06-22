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
    def __init__(self, responses):
        self.responses = list(responses)
        self.post_payloads = []

    async def post(self, _url, json=None, headers=None):
        self.post_payloads.append(json)
        return self.responses.pop(0)

    async def aclose(self):
        return None


def _make_executor(monkeypatch, responses):
    monkeypatch.setattr(live, "load_private_key", lambda _path: object())
    monkeypatch.setattr(live, "build_auth_headers", lambda *_args, **_kwargs: {})
    executor = LiveTradeExecutor("https://example.test", "test-key", "unused.pem")
    executor._client = FakeClient(responses)
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
