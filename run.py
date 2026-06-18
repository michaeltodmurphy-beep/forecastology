import asyncio
import structlog
from app.config import AppConfig
from app.database import DatabaseManager
from data.ticker_cache import TickerCache
from data.websocket_manager import WebSocketManager
from execution.factory import create_executor
from core.state_machine import TemperatureStrategy

logger = structlog.get_logger(__name__)

async def main():
    config = AppConfig.from_env()
    logger.info("app.config_loaded", mode=config.trading_mode)
    if config.trading_mode == "LIVE":
        if "demo" in config.rest_base_url.lower() or "demo" in config.ws_url.lower():
            raise RuntimeError("CRITICAL: LIVE mode must use Kalshi PRODUCTION URLs.")
        if not config.kalshi_api_key:
            raise RuntimeError("LIVE mode requires KALSHI_API_KEY in .env")
        logger.warning("app.live_mode", message="REAL MONEY TRADING ENABLED")
    db = DatabaseManager(config.mysql_database_url)
    await db.initialize()
    cache = TickerCache()
    ws_manager = WebSocketManager(ws_url=config.ws_url, api_key=config.kalshi_api_key, private_key_path=config.kalshi_private_key_path)
    executor = create_executor(trading_mode=config.trading_mode, ticker_cache=cache, rest_base_url=config.rest_base_url, api_key=config.kalshi_api_key, private_key_path=config.kalshi_private_key_path)
    strategy = TemperatureStrategy(config=config, cache=cache, ws_manager=ws_manager, executor=executor, db=db)
    await ws_manager.connect()
    await strategy.start()
    try:
        await ws_manager.listen()
    except KeyboardInterrupt:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        await strategy.stop()
        await ws_manager.close()
        if hasattr(executor, "close"):
            await executor.close()
        await db.dispose()

if __name__ == "__main__":
    asyncio.run(main())
