"""Tests for core/types.py — especially the sell_yes payload fix."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.types import OrderRequest, OrderSide


class TestOrderRequest:

    def test_buy_yes_payload(self):
        r = OrderRequest(
            market_ticker="KXLOWTAUS-26JUN16-B70.5",
            side=OrderSide.BUY_YES,
            price=85,
            quantity=5,
        )
        p = r.to_kalshi_payload()
        assert p["action"] == "buy", f"Expected action=buy, got {p['action']}"
        assert p["side"] == "yes", f"Expected side=yes, got {p['side']}"
        assert p["ticker"] == "KXLOWTAUS-26JUN16-B70.5"
        assert p["count"] == 5
        assert p["yes_price"] == 85, f"Expected yes_price=85, got {p['yes_price']}"

    def test_sell_yes_payload(self):
        r = OrderRequest(
            market_ticker="KXLOWTAUS-26JUN16-B70.5",
            side=OrderSide.SELL_YES,
            price=35,
            quantity=5,
        )
        p = r.to_kalshi_payload()
        assert p["action"] == "sell", f"Expected action=sell, got {p['action']}"
        assert p["side"] == "yes", f"Expected side=yes, got {p['side']}"
        assert p["count"] == 5
        assert p["yes_price"] == 35, f"Expected yes_price=35, got {p['yes_price']}"

    def test_sell_does_not_use_no_side(self):
        r = OrderRequest(
            market_ticker="KXLOWTAUS-26JUN16-B70.5",
            side=OrderSide.SELL_YES,
            price=25,
            quantity=1,
        )
        p = r.to_kalshi_payload()
        assert p["side"] != "no", "Sell should NOT send side=no"

    def test_old_side_str_removed(self):
        import inspect
        from core import types
        source = inspect.getsource(types.OrderRequest.to_kalshi_payload)
        assert "side_str" not in source, "side_str variable should not exist"
