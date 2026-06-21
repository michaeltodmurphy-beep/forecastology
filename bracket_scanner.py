"""
bracket_scanner.py — Standalone WebSocket bracket scanner

Connects directly to Kalshi WebSocket, subscribes to today's temperature
brackets, and prints real-time price updates. NO database writes.

Usage:
    python bracket_scanner.py
    python bracket_scanner.py --min-spread 7 --buy-trigger 85

Press Ctrl+C to stop.
"""

import asyncio
import json
import structlog
import httpx
import argparse
import signal
import sys
from typing import Optional
from datetime import datetime, timezone, timedelta

import websockets
from app.config import AppConfig
from app.signing import load_private_key, build_ws_headers, build_auth_headers

logger = structlog.get_logger(__name__)

MONTHS = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

SERIES_LIST = [
    "KXHIGHTATL", "KXLOWTATL", "KXHIGHAUS", "KXLOWTAUS",
    "KXHIGHTBOS", "KXLOWTBOS", "KXHIGHCHI", "KXLOWTCHI",
    "KXHIGHTDAL", "KXLOWTDAL", "KXHIGHDEN", "KXLOWTDEN",
    "KXHIGHTHOU", "KXLOWTHOU", "KXHIGHTLV", "KXLOWTLV",
    "KXHIGHLAX", "KXLOWTLAX", "KXHIGHMIA", "KXLOWTMIA",
    "KXHIGHTMIN", "KXLOWTMIN", "KXHIGHTNOLA", "KXLOWTNOLA",
    "KXHIGHNY", "KXLOWTNYC", "KXHIGHTOKC", "KXLOWTOKC",
    "KXHIGHPHIL", "KXLOWTPHIL", "KXHIGHTPHX", "KXLOWTPHX",
    "KXHIGHTSATX", "KXLOWTSATX", "KXHIGHTSFO", "KXLOWTSFO",
    "KXHIGHTSEA", "KXLOWTSEA", "KXHIGHTDC", "KXLOWTDC",
]


class BracketScanner:
    """
    Lightweight WebSocket scanner for Kalshi temperature brackets.
    
    Connects directly, subscribes to ticker channel for ALL of today's
    temperature markets, and prints real-time updates to console.
    """
    
    def __init__(self, config: AppConfig, buy_trigger: int = 85, min_spread: int = 7):
        self.config = config
        self.buy_trigger = buy_trigger      # cents
        self.min_spread = min_spread         # cents
        self._private_key = load_private_key(config.kalshi_private_key_path)
        self._running = False
        
        # All today's temperature market tickers
        self.ticker_map: dict[str, dict] = {}  # ticker -> {yes_bid, yes_ask, spread, last_price}
        self.event_map: dict[str, list[str]] = {}  # event_ticker -> [ticker, ...]
        
    async def fetch_today_markets(self) -> list[str]:
        """Fetch today's temperature brackets from REST API."""
        now = datetime.now(timezone.utc) + timedelta(hours=-4)
        today_prefix = f"{now.strftime('%y')}{MONTHS[now.month-1]}{now.strftime('%d')}"
        event_tickers = [f"{s}-{today_prefix}" for s in SERIES_LIST]
        
        all_tickers = []
        path = "/trade-api/v2/markets"
        url = f"{self.config.rest_base_url}{path}"
        
        print(f"\n=== Fetching markets for {today_prefix} ===")
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            for et in event_tickers:
                headers = build_auth_headers(self._private_key, self.config.kalshi_api_key, "GET", path)
                try:
                    resp = await client.get(url, headers=headers,
                                            params={"event_ticker": et, "limit": 100})
                    if resp.status_code in (200, 201):
                        markets = resp.json().get("markets", [])
                        tickers = []
                        for m in markets:
                            t = m.get("ticker")
                            if t:
                                tickers.append(t)
                                self.ticker_map[t] = {}
                        all_tickers.extend(tickers)
                        self.event_map[et] = tickers
                        print(f"  {et}: {len(tickers)} brackets")
                except Exception as e:
                    print(f"  {et}: ERROR - {e}")
        
        print(f"\nTotal temperature markets: {len(all_tickers)}")
        return all_tickers
    
    async def handle_ticker(self, data: dict):
        """Process a ticker message and print if conditions are interesting."""
        inner = data.get("msg", {})
        ticker = inner.get("market_ticker") or inner.get("ticker")
        if not ticker or ticker not in self.ticker_map:
            return
        
        price_dollars = inner.get("price_dollars")
        last_price = round(float(price_dollars) * 100) if price_dollars and float(price_dollars) > 0 else None
        
        yb = inner.get("yes_bid_dollars") or inner.get("yes_bid")
        ya = inner.get("yes_ask_dollars") or inner.get("yes_ask")
        
        if yb is not None and ya is not None:
            bid = round(float(yb) * 100)
            ask = round(float(ya) * 100)
            spread = ask - bid
            
            old = self.ticker_map.get(ticker, {})
            old_bid = old.get("best_bid")
            old_ask = old.get("best_ask")
            
            self.ticker_map[ticker] = {
                "best_bid": bid,
                "best_ask": ask,
                "spread": spread,
                "last_price": last_price or old.get("last_price"),
            }
            
            # Only print if something changed
            if bid != old_bid or ask != old_ask:
                self._print_if_interesting(ticker, bid, ask, spread)
    
    def _print_if_interesting(self, ticker: str, bid: int, ask: int, spread: int):
        """Print bracket info if it meets scan criteria."""
        
        # Check buy condition: ask >= buy_trigger AND spread <= min_spread
        if ask >= self.buy_trigger and spread <= self.min_spread:
            trigger = " 🟢 BUY SIGNAL"
        elif ask >= self.buy_trigger and spread > self.min_spread:
            trigger = " ⏳ WIDE SPREAD"
        elif spread <= self.min_spread and ask < self.buy_trigger:
            trigger = " 🔍 NARROW"
        else:
            return  # Nothing interesting
        
        # Extract readable info
        parts = ticker.split('-')
        series = parts[0] if parts else ticker
        bracket = '-'.join(parts[2:]) if len(parts) > 2 else ""
        price_str = f"${ask/100:.2f}" if ask > 0 else "N/A"
        spread_str = f"${spread/100:.2f}"
        
        print(f"{trigger}  {series:15s} {bracket:10s}  Ask: {price_str:6s}  Spread: {spread_str:5s}  Bid: ${bid/100:.2f}")
    
    async def run(self):
        """Main loop: fetch markets, connect WS, scan."""
        self._running = True
        
        # First fetch today's markets
        await self.fetch_today_markets()
        
        print(f"\n=== Scanning for buy signals ===")
        print(f"Buy trigger: ≥ ${self.buy_trigger/100:.2f}")
        print(f"Max spread:  ≤ ${self.min_spread/100:.2f}")
        print(f"Press Ctrl+C to stop\n")
        
        while self._running:
            try:
                ws_headers = build_ws_headers(self._private_key, self.config.kalshi_api_key)
                async with websockets.connect(
                    self.config.ws_url,
                    additional_headers=ws_headers,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    print("✅ WebSocket connected\n")
                    
                    # Subscribe to ticker channel (all markets)
                    subscribe_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {"channels": ["ticker"]}
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    print("📡 Subscribed to ticker channel (all markets)\n")
                    
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            msg_type = msg.get("type")
                            if msg_type == "ticker":
                                await self.handle_ticker(msg)
                        except json.JSONDecodeError:
                            pass
                        except Exception as e:
                            print(f"⚠️ Handler error: {e}")
                            
            except websockets.exceptions.ConnectionClosed as e:
                print(f"⚠️ Connection closed: {e}")
            except Exception as e:
                print(f"⚠️ Error: {e}")
            
            if self._running:
                wait = 5
                print(f"🔄 Reconnecting in {wait}s...")
                await asyncio.sleep(wait)
        
        print("Scanner stopped.")
    
    def stop(self):
        self._running = False


def main():
    parser = argparse.ArgumentParser(description="Bracket Scanner - Real-time Kalshi temperature bracket watcher")
    parser.add_argument("--buy-trigger", type=int, default=85, help="Minimum ask price in cents to trigger (default: 85 = $0.85)")
    parser.add_argument("--min-spread", type=int, default=7, help="Maximum spread in cents (default: 7 = $0.07)")
    args = parser.parse_args()
    
    config = AppConfig.from_env()
    
    print(f"\n{'='*60}")
    print(f"  BRACKET SCANNER")
    print(f"{'='*60}")
    print(f"  Mode:      {config.trading_mode}")
    print(f"  Buy at:    ≥ ${args.buy_trigger/100:.2f}")
    print(f"  Spread:    ≤ ${args.min_spread/100:.2f}")
    print(f"{'='*60}\n")
    
    scanner = BracketScanner(config, buy_trigger=args.buy_trigger, min_spread=args.min_spread)
    
    def handle_sig(sig, frame):
        print("\n\nShutting down...")
        scanner.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)
    
    asyncio.run(scanner.run())


if __name__ == "__main__":
    main()
