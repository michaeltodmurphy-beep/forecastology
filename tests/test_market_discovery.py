import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.constants import SERIES_LIST
from data.ticker_cache import TickerCache
from execution.live import LiveTradeExecutor
from execution.paper import PaperTradeExecutor


class FakeResponse:
    def __init__(self, markets=None):
        self.status_code = 200
        self._markets = markets or []

    def json(self):
        return {"markets": self._markets}


@pytest.mark.asyncio
async def test_live_get_active_markets_queries_today_only(monkeypatch):
    import core.constants
    import execution.live as live

    requested_event_tickers = []

    class FakeClient:
        async def get(self, _url, headers=None, params=None):
            requested_event_tickers.append(params["event_ticker"])
            return FakeResponse()

    monkeypatch.setattr(live, "load_private_key", lambda _path: object())
    monkeypatch.setattr(live, "build_auth_headers", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(core.constants, "get_eastern_today_date_prefix", lambda days_offset=0: "26JUN21")

    executor = LiveTradeExecutor("https://example.test", "test-key", "unused.pem")
    executor._client = FakeClient()

    await executor.get_active_markets()

    assert requested_event_tickers == [f"{series}-26JUN21" for series in SERIES_LIST]


@pytest.mark.asyncio
async def test_paper_get_active_markets_queries_today_only(monkeypatch):
    import app.signing
    import core.constants
    import httpx

    requested_event_tickers = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, _url, headers=None, params=None):
            requested_event_tickers.append(params["event_ticker"])
            return FakeResponse()

    monkeypatch.setattr(app.signing, "load_private_key", lambda _path: object())
    monkeypatch.setattr(app.signing, "build_auth_headers", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(core.constants, "get_eastern_today_date_prefix", lambda days_offset=0: "26JUN21")
    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

    executor = PaperTradeExecutor(TickerCache(), rest_base_url="https://example.test", api_key="test-key", private_key_path="unused.pem")

    await executor.get_active_markets()

    assert requested_event_tickers == [f"{series}-26JUN21" for series in SERIES_LIST]
