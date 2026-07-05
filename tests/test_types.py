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
        assert p["time_in_force"] == "good_till_canceled"
        assert p["post_only"] is False
        assert p["reduce_only"] is False
        assert p["client_order_id"].startswith("APP_")

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
        assert p["time_in_force"] == "good_till_canceled"
        assert p["post_only"] is False
        assert p["reduce_only"] is False
        assert p["client_order_id"].startswith("APP_")

    def test_stop_loss_sell_payload_uses_ioc_and_reduce_only(self):
        r = OrderRequest(
            market_ticker="KXLOWTAUS-26JUN16-B70.5",
            side=OrderSide.SELL_YES,
            price=1,
            quantity=2,
        )
        p = r.to_kalshi_payload(time_in_force="immediate_or_cancel", reduce_only=True)
        assert p["side"] == "ask"
        assert p["time_in_force"] == "immediate_or_cancel"
        assert p["post_only"] is False
        assert p["reduce_only"] is True

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

    def test_client_order_id_prefixed_when_user_supplies_id(self):
        r = OrderRequest(
            market_ticker="KXLOWTAUS-26JUN16-B70.5",
            side=OrderSide.BUY_YES,
            price=85,
            quantity=1,
            client_order_id="manual-id",
        )
        p = r.to_kalshi_payload()
        assert p["client_order_id"] == "APP_manual-id"
