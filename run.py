import asyncio
import os
import hashlib
import atexit
import signal
import structlog
import sys
from app.config import AppConfig
from app.runtime_safety import LockConflict, InstanceLock, configure_logging, maybe_acquire_instance_lock
from app.database import DatabaseManager
from data.ticker_cache import TickerCache
from data.websocket_manager import WebSocketManager
from execution.factory import create_executor
from execution.sl_watcher import StopLossWatcher
from core.state_machine import TemperatureStrategy
from nws.scheduler import bootstrap as nws_bootstrap
from nws.scheduler import shutdown as shutdown_nws_scheduler

logger = structlog.get_logger(__name__)
_active_instance_lock: InstanceLock | None = None


def _account_id_hash(config: AppConfig) -> str:
    identity = (
        config.instance_id.strip()
        or os.getenv("KALSHI_API_KEY_ID", "").strip()
        or "default"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]


def _release_instance_lock() -> None:
    global _active_instance_lock
    if _active_instance_lock is None:
        return
    _active_instance_lock.release()
    logger.info(
        "instance.lock_released",
        lock_file=_active_instance_lock.lock_file,
        account_id_hash=_active_instance_lock.account_id_hash,
    )
    _active_instance_lock = None


def _install_signal_handlers() -> None:
    def _shutdown(signum, _frame):
        logger.info("instance.shutdown_signal_received", signal=signal.Signals(signum).name)
        _release_instance_lock()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)


def _acquire_startup_lock(config: AppConfig, account_id_hash: str) -> InstanceLock | None:
    lock_result = maybe_acquire_instance_lock(
        enabled=config.instance_lock_enabled,
        base_lock_file=config.instance_lock_file,
        account_id_hash=account_id_hash,
    )
    if isinstance(lock_result, LockConflict):
        logger.critical(
            "instance.lock_conflict",
            lock_file=lock_result.lock_file,
            holder_pid=lock_result.holder_pid,
            account_id_hash=lock_result.account_id_hash,
        )
        print(
            "Another Forecastology instance is already running against this account; refusing to start.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    if lock_result is not None:
        logger.info(
            "instance.lock_acquired",
            lock_file=lock_result.lock_file,
            pid=os.getpid(),
            account_id_hash=account_id_hash,
        )
        atexit.register(_release_instance_lock)
        _install_signal_handlers()
        return lock_result
    logger.warning(
        "instance.lock_disabled",
        account_id_hash=account_id_hash,
        message="INSTANCE_LOCK_ENABLED=false; startup single-instance guard is disabled.",
    )
    return None


async def _start_nws_backend() -> None:
    try:
        await asyncio.to_thread(nws_bootstrap)
        logger.info("nws.bootstrap.started")
    except Exception:
        logger.exception(
            "nws.bootstrap_failed",
            message="NWS backend failed to start; continuing trading loop without NWS forecasts",
        )


def _shutdown_nws_backend() -> None:
    try:
        shutdown_nws_scheduler()
    except Exception:
        logger.exception("nws.shutdown_failed")


async def main():
    global _active_instance_lock
    db = None
    ws_manager = None
    executor = None
    strategy = None
    stop_loss_watcher = None
    stop_loss_task = None
    try:
        config = AppConfig.from_env()
        configure_logging(
            log_file=config.log_file,
            log_max_bytes=config.log_max_bytes,
            log_backup_count=config.log_backup_count,
        )
        logger.info(
            "app.logging_configured",
            log_file=config.log_file,
            log_max_bytes=config.log_max_bytes,
            log_backup_count=config.log_backup_count,
        )

        account_id_hash = _account_id_hash(config)
        _active_instance_lock = _acquire_startup_lock(config, account_id_hash)
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
        await _start_nws_backend()
        cache = TickerCache()
        ws_manager = WebSocketManager(ws_url=config.ws_url, api_key=config.kalshi_api_key, private_key_path=config.kalshi_private_key_path)
        executor = create_executor(
            trading_mode=config.trading_mode,
            ticker_cache=cache,
            rest_base_url=config.rest_base_url,
            api_key=config.kalshi_api_key,
            private_key_path=config.kalshi_private_key_path,
            dry_run=config.dry_run,
            max_buy_qty=config.initial_contract_count * (2 ** (config.hedge_max_factor - 1)),
        )
        strategy = TemperatureStrategy(config=config, cache=cache, ws_manager=ws_manager, executor=executor, db=db)
        stop_loss_watcher = StopLossWatcher(
            strategy._execute_stop_loss_from_watcher,
            poll_interval_ms=config.sl_worker_interval_ms,
        )
        strategy.stop_loss_watcher = stop_loss_watcher
        await ws_manager.connect()
        stop_loss_task = asyncio.create_task(stop_loss_watcher.run())
        await strategy.start()
        try:
            await ws_manager.listen()
        except KeyboardInterrupt:
            pass
        except asyncio.CancelledError:
            pass
    finally:
        _shutdown_nws_backend()
        if stop_loss_watcher is not None:
            await stop_loss_watcher.stop()
        if stop_loss_task is not None:
            try:
                await stop_loss_task
            except asyncio.CancelledError:
                pass
        if strategy is not None:
            await strategy.stop()
        if ws_manager is not None:
            await ws_manager.close()
        if executor is not None and hasattr(executor, "close"):
            await executor.close()
        if db is not None:
            await db.dispose()
        _release_instance_lock()

if __name__ == "__main__":
    asyncio.run(main())
