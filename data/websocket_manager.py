import asyncio
import json
from typing import Callable, Optional
import websockets
from websockets.exceptions import ConnectionClosed
import structlog
from app.signing import load_private_key, build_ws_headers

logger = structlog.get_logger(__name__)

MessageHandler = Callable[[dict], None]


class WebSocketManager:

    def __init__(self, ws_url, api_key, private_key_path, max_retries=5, base_delay=1.0):
        self.ws_url = ws_url
        self.api_key = api_key
        self.private_key_path = private_key_path
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.websocket = None
        self._running = False
        self._message_id = 0
        self._subscriptions = {}
        self.handlers = {}
        self._private_key = load_private_key(private_key_path)

    def on_message(self, channel, handler):
        self.handlers[channel] = handler

    async def connect(self):
        from app.signing import build_auth_headers
        headers = build_auth_headers(self._private_key, self.api_key, "GET", "/trade-api/ws/v2")
        retries = 0
        delay = self.base_delay
        while retries < self.max_retries:
            try:
                logger.info("ws.connecting", url=self.ws_url, attempt=retries + 1)
                self.websocket = await websockets.connect(self.ws_url, additional_headers=headers, ping_interval=None, ping_timeout=None)
                logger.info("ws.connected")
                self._running = True
                return
            except (OSError, ConnectionRefusedError) as e:
                retries += 1
                logger.warning("ws.connect_failed", error=str(e), retry=retries, max=self.max_retries)
                if retries < self.max_retries:
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    raise RuntimeError(f"Failed to connect after {self.max_retries} attempts") from e

    async def subscribe(self, channel, market_tickers=None):
        self._message_id += 1
        params = {"channels": [channel]}
        # Do not include 'market_tickers' in the params at all if we want everything!
        if market_tickers is not None and len(market_tickers) > 0:
            params["market_tickers"] = market_tickers

        msg = {"id": self._message_id, "cmd": "subscribe", "params": params}
        if self.websocket:
            await self.websocket.send(json.dumps(msg))
            if channel not in self._subscriptions:
                self._subscriptions[channel] = []
            if market_tickers:
                self._subscriptions[channel].extend(market_tickers)
            logger.info("ws.subscribed", channel=channel, num_markets=len(market_tickers) if market_tickers else "ALL")
        else:
            raise RuntimeError("WebSocket not connected")

    async def _send_subscribe(self, channel, market_tickers=None):
        pass  # Removing this helper since we're reverting chunking

    async def listen(self):
        if not self.websocket:
            raise RuntimeError("WebSocket not connected")
            
        async def keepalive():
            """Send periodic ping requests manually to satisfy Kalshi"""
            while self._running and self.websocket:
                try:
                    await self.websocket.ping()
                    await asyncio.sleep(20) # Kalshi requires < 30 seconds
                except Exception as e:
                    logger.debug("ws.keepalive_failed", error=str(e))
                    break
                    
        asyncio.create_task(keepalive())
        
        while self._running:
            try:
                async for raw_message in self.websocket:
                    try:
                        data = json.loads(raw_message)
                        msg_type = data.get("type", "")
                        
                        # Print raw message so we know it's arriving
                        # if msg_type != "pong":
                        #     print(f"WS MSG RECVD: {raw_message[:150]}")
                            
                        handler = self.handlers.get(msg_type)
                        if handler:
                            await handler(data)
                        msg_content = data.get("msg", {})
                        internal_type = msg_content.get("type", "")
                        if internal_type and internal_type in self.handlers:
                            await self.handlers[internal_type](data)
                    except json.JSONDecodeError:
                        logger.warning("ws.decode_error", raw=raw_message[:200])
                    except Exception as e:
                        logger.error("ws.handler_error", error=str(e))
                logger.warning("ws.disconnected")
                if self._running:
                    await asyncio.sleep(self.base_delay)
                    await self.reconnect()
            except ConnectionClosed as e:
                logger.warning("ws.connection_closed", error=str(e))
                if self._running:
                    await asyncio.sleep(self.base_delay)
                    await self.reconnect()
            except Exception as e:
                logger.error("ws.listen_error", error=str(e))
                if self._running:
                    await asyncio.sleep(self.base_delay)
                    await self.reconnect()

    async def reconnect(self):
        self._running = False
        if self.websocket:
            await self.websocket.close()
        await self.connect()
        for channel, markets in self._subscriptions.items():
            await self.subscribe(channel, markets)

    async def close(self):
        self._running = False
        if self.websocket:
            await self.websocket.close()
        logger.info("ws.closed")
