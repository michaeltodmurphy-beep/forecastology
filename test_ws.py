import asyncio
import pytest
import json
pytest.importorskip("websockets")
import websockets
from app.signing import load_private_key, build_ws_headers
from app.config import AppConfig

@pytest.mark.asyncio
async def test_websocket_connection():
    c = AppConfig.from_env()
    key = load_private_key("kalshi_private_key.pem")
    headers = build_ws_headers(key, c.kalshi_api_key)
    print("Connecting to", c.ws_url)
    print("Auth key:", c.kalshi_api_key)
    try:
        async with websockets.connect(c.ws_url, additional_headers=headers, ping_interval=10, ping_timeout=5) as ws:
            print("Connected!")
            msg = {"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"]}}
            await ws.send(json.dumps(msg))
            resp = await asyncio.wait_for(ws.recv(), timeout=10)
            print("Response:", resp)
    except Exception as e:
        print("Failed:", type(e).__name__, str(e))


