# core/constants.py

import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

# All 40 temperature series (20 cities × high/low) with their Kalshi series tickers
SERIES_LIST = [
    "KXHIGHTATL", "KXLOWTATL",
    "KXHIGHAUS", "KXLOWTAUS",
    "KXHIGHTBOS", "KXLOWTBOS",
    "KXHIGHCHI", "KXLOWTCHI",
    "KXHIGHTDAL", "KXLOWTDAL",
    "KXHIGHDEN", "KXLOWTDEN",
    "KXHIGHTHOU", "KXLOWTHOU",
    "KXHIGHTLV", "KXLOWTLV",
    "KXHIGHLAX", "KXLOWTLAX",
    "KXHIGHMIA", "KXLOWTMIA",
    "KXHIGHTMIN", "KXLOWTMIN",
    "KXHIGHTNOLA", "KXLOWTNOLA",
    "KXHIGHNY", "KXLOWTNYC",
    "KXHIGHTOKC", "KXLOWTOKC",
    "KXHIGHPHIL", "KXLOWTPHIL",
    "KXHIGHTPHX", "KXLOWTPHX",
    "KXHIGHTSATX", "KXLOWTSATX",
    "KXHIGHTSFO", "KXLOWTSFO",
    "KXHIGHTSEA", "KXLOWTSEA",
    "KXHIGHTDC", "KXLOWTDC",
]

def get_eastern_today_date_prefix(days_offset: int = 0) -> str:
    """Return today's (or offset day) date in US Eastern time formatted as YYMMMDD,
    e.g., 250305 for March 5, 2025."""
    eastern = ZoneInfo("America/New_York")
    now = datetime.datetime.now(eastern) + datetime.timedelta(days=days_offset)
    months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    return f"{now.strftime('%y')}{months[now.month-1]}{now.strftime('%d')}"

# WebSocket channels
CHANNEL_TICKER = "ticker"
CHANNEL_TRADE = "trade"
CHANNEL_ORDERBOOK_DELTA = "orderbook_delta"
CHANNEL_MARKET_POSITIONS = "market_positions"
CHANNEL_FILL = "fill"
CHANNEL_MARKET_LIFECYCLE = "market_lifecycle_v2"

# Kalshi REST API paths (full paths including /trade-api/v2)
REST_PORTFOLIO_BALANCE = "/trade-api/v2/portfolio/balance"
REST_PORTFOLIO_ORDERS = "/trade-api/v2/portfolio/events/orders"
REST_PORTFOLIO_POSITIONS = "/trade-api/v2/portfolio/positions"
REST_MARKET = "/trade-api/v2/markets/{ticker}"
REST_MARKETS = "/trade-api/v2/markets"
REST_ORDERBOOK = "/trade-api/v2/markets/{ticker}/orderbook"
REST_SERIES = "/trade-api/v2/series/{series_ticker}"
REST_EVENTS = "/trade-api/v2/events"

# Weather category filter
WEATHER_CATEGORY = "Weather"

