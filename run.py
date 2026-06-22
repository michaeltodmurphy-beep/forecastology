import asyncio
import fcntl
import os
import structlog
import sys
from app.config import AppConfig
from app.database import DatabaseManager
from data.ticker_cache import TickerCache
from data.websocket_manager import WebSocketManager
from execution.factory import create_executor
from core.state_machine import TemperatureStrategy

logger = structlog.get_logger(__name__)


def _acquire_lock():
    lockfile_path = os.getenv("FORECASTOLOGY_LOCKFILE", "/tmp/forecastology.lock")
    lock_handle = open(lockfile_path, "w")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.error("app.already_running", lockfile=lockfile_path)
        lock_handle.close()
        sys.exit(1)
    return lock_handle


async def main():
    lock_handle = _acquire_lock()
    db = None
    ws_manager = None
    executor = None
    strategy = None
    try:
        config = AppConfig.from_env()
        logger.info("app.config_loaded", mode=config.trading_mode)
        if config.trading_mode == "LIVE":
            if "demo" in config.rest_base_url.lower() or "demo" in config.ws_url.lower():
                raise RuntimeError("CRITICAL: LIVE mode must use Kalshi PRODUCTION URLs.")
            if not config.kalshi_api_key:
                raise RuntimeError("LIVE mode requires KALSHI_API_KEY in .env")
            logger.warning("app.live_mode", message="REAL MONEY TRADING ENABLED")
        if config.dry_run:
            logger.warning("app.dry_run", message="DRY RUN — no live orders will be placed")
        db = DatabaseManager(config.mysql_database_url)
        await db.initialize()
        cache = TickerCache()
        ws_manager = WebSocketManager(ws_url=config.ws_url, api_key=config.kalshi_api_key, private_key_path=config.kalshi_private_key_path)
        executor = create_executor(
            trading_mode=config.trading_mode,
            ticker_cache=cache,
            rest_base_url=config.rest_base_url,
            api_key=config.kalshi_api_key,
            private_key_path=config.kalshi_private_key_path,
            dry_run=config.dry_run,
        )
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
        if strategy is not None:
            await strategy.stop()
        if ws_manager is not None:
            await ws_manager.close()
        if executor is not None and hasattr(executor, "close"):
            await executor.close()
        if db is not None:
            await db.dispose()
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_handle.close()

if __name__ == "__main__":
    asyncio.run(main())
