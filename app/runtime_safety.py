import datetime as dt
import fcntl
import json
import logging
import os
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog


def account_lock_file(base_lock_file: str, account_id_hash: str) -> str:
    path = Path(base_lock_file)
    if path.suffix:
        return str(path.with_name(f"{path.stem}.{account_id_hash}{path.suffix}"))
    return str(path.with_name(f"{path.name}.{account_id_hash}.lock"))


@dataclass
class LockConflict:
    lock_file: str
    holder_pid: int | None
    account_id_hash: str


@dataclass
class InstanceLock:
    file_handle: object
    lock_file: str
    account_id_hash: str

    def release(self) -> None:
        try:
            fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            self.file_handle.close()
        except OSError:
            pass


def acquire_instance_lock(*, base_lock_file: str, account_id_hash: str) -> InstanceLock | LockConflict:
    lock_file = account_lock_file(base_lock_file, account_id_hash)
    lock_dir = os.path.dirname(lock_file)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)
    lock_handle = open(lock_file, "a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_handle.seek(0)
        holder_pid = None
        try:
            payload = json.loads(lock_handle.read() or "{}")
            if isinstance(payload, dict):
                holder_pid = payload.get("pid")
        except json.JSONDecodeError:
            holder_pid = None
        lock_handle.close()
        return LockConflict(lock_file=lock_file, holder_pid=holder_pid, account_id_hash=account_id_hash)

    payload = {
        "pid": os.getpid(),
        "account_id_hash": account_id_hash,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(json.dumps(payload))
    lock_handle.write("\n")
    lock_handle.flush()
    return InstanceLock(file_handle=lock_handle, lock_file=lock_file, account_id_hash=account_id_hash)


def maybe_acquire_instance_lock(
    *, enabled: bool, base_lock_file: str, account_id_hash: str
) -> InstanceLock | LockConflict | None:
    if not enabled:
        return None
    return acquire_instance_lock(base_lock_file=base_lock_file, account_id_hash=account_id_hash)



def configure_logging(
    *,
    log_file: str,
    log_max_bytes: int,
    log_backup_count: int,
    log_to_console: bool = True,
    log_to_file: bool = True,
) -> RotatingFileHandler | None:
    """Configure the root logger with console and/or rotating-file sinks.

    The console and file sinks are independent: each renders every event
    exactly once.  When the process's stdout and the log file are collected
    into the same view (a terminal whose stdout *is* the redirected log, or a
    tool that concatenates console and file output), every line therefore
    appears TWICE.  ``log_to_console`` / ``log_to_file`` let an operator select
    a single sink when that duplicate is not wanted; both default to True so
    historical behavior is preserved.

    This function is idempotent: it removes only the handlers it previously
    installed (tagged ``_forecastology_managed``) before adding new ones, so
    calling it more than once never accumulates duplicate handlers.

    Returns the rotating file handler when a file sink is installed, else None.
    """
    log_path = Path(log_file)
    if log_path.parent and str(log_path.parent) != ".":
        log_path.parent.mkdir(parents=True, exist_ok=True)

    pre_chain = [
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.stdlib.add_log_level,
    ]

    # Colored renderer for the live terminal stream.
    stream_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=pre_chain,
        processor=structlog.dev.ConsoleRenderer(),
    )

    # Plain (uncolored) renderer for the log file so it contains no ANSI
    # escape sequences.  ConsoleRenderer emits color codes even when output
    # is redirected to a file, which corrupts grep/awk pipelines over the log.
    file_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=pre_chain,
        processor=structlog.dev.ConsoleRenderer(colors=False),
    )

    root_logger = logging.getLogger()
    # Remove ONLY the handlers we previously installed.  A blanket
    # ``handlers.clear()`` also dropped a *previous* configure_logging()'s
    # handlers without closing them, so a second call could leave stale
    # handlers alive (double emission) or close a file another sink still used.
    for existing in list(root_logger.handlers):
        if getattr(existing, "_forecastology_managed", False):
            root_logger.removeHandler(existing)
            try:
                existing.close()
            except Exception:
                pass

    rotating_file_handler: RotatingFileHandler | None = None

    if log_to_console:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(stream_formatter)
        stream_handler._forecastology_managed = True  # type: ignore[attr-defined]
        root_logger.addHandler(stream_handler)

    if log_to_file:
        rotating_file_handler = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
        )
        rotating_file_handler.setFormatter(file_formatter)
        rotating_file_handler._forecastology_managed = True  # type: ignore[attr-defined]
        root_logger.addHandler(rotating_file_handler)

    root_logger.setLevel(logging.INFO)

    # httpx logs every outbound HTTP request at INFO, which floods the log.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return rotating_file_handler

