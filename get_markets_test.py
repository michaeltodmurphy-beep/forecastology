import asyncio
import httpx
from app.signing import load_private_key, build_auth_headers
from app.config import AppConfig

async def fetch_markets():
    config = AppConfig.from_env()
    key = load_private_key(config.kalshi_private_key_path)
    
    # We want active markets
    path = "/trade-api/v2/markets"
    headers = build_auth_headers(key, config.kalshi_api_key, "GET", path)
    
    async with httpx.AsyncClient() as client:
        # Fetch high/low temperature active markets
        url = f"{config.rest_base_url}{path}"
        params = {"status": "active", "limit": 100}
        
        print(f"Fetching from {url}...")
        resp = await client.get(url, headers=headers, params=params)
        
        if resp.status_code == 200:
            data = resp.json()
            markets = data.get("markets", [])
            print(f"Got {len(markets)} markets.")
            # Filter for weather/temp markets
            temp_markets = [
                m["ticker"] for m in markets 
                if "HIGH" in m.get("ticker", "") or "LOW" in m.get("ticker", "") or "WEATHER" in m.get("ticker", "") or "TEMP" in m.get("ticker", "")
            ]
            print(f"Found {len(temp_markets)} temperature/weather markets:")
            print(temp_markets[:10])  # print first 10
        else:
            print("Error fetching markets:", resp.status_code, resp.text)

if __name__ == "__main__":
    asyncio.run(fetch_markets())
