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
        assert p["side"] == "bid", f"Expected side=bid, got {p['side']}"
        assert p["ticker"] == "KXLOWTAUS-26JUN16-B70.5"
        assert p["count"] == "5.00"
        assert p["price"] == "0.8500", f"Expected price=0.8500, got {p['price']}"
        assert "time_in_force" in p
        assert "self_trade_prevention_type" in p

    def test_sell_yes_payload(self):
        r = OrderRequest(
            market_ticker="KXLOWTAUS-26JUN16-B70.5",
            side=OrderSide.SELL_YES,
            price=35,
            quantity=5,
        )
        p = r.to_kalshi_payload()
        assert p["side"] == "ask", f"Expected side=ask, got {p['side']}"
        assert p["count"] == "5.00"
        assert p["price"] == "0.3500", f"Expected price=0.3500, got {p['price']}"
        assert "time_in_force" in p

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

