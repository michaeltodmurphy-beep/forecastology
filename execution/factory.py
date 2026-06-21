# execution/factory.py
from execution.base import BaseExecutor
from execution.paper import PaperTradeExecutor
from execution.live import LiveTradeExecutor
from data.ticker_cache import TickerCache


def create_executor(
    trading_mode: str,
    ticker_cache: TickerCache,
    rest_base_url: str,
    api_key: str,
    private_key_path: str,
) -> BaseExecutor:
    """Factory that returns the appropriate executor based on trading mode."""
    if trading_mode.upper() == "PAPER":
        return PaperTradeExecutor(
            ticker_cache,
            rest_base_url=rest_base_url,
            api_key=api_key,
            private_key_path=private_key_path,
        )
    elif trading_mode.upper() == "LIVE":
        return LiveTradeExecutor(rest_base_url, api_key, private_key_path)
    else:
        raise ValueError(f"Unknown trading mode: {trading_mode}")
