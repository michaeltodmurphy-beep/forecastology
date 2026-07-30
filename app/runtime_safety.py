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


def configure_logging(*, log_file: str, log_max_bytes: int, log_backup_count: int) -> RotatingFileHandler:
    log_path = Path(log_file)
    if log_path.parent and str(log_path.parent) != ".":
        log_path.parent.mkdir(parents=True, exist_ok=True)

    processor_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.stdlib.add_log_level,
        ],
        processor=structlog.dev.ConsoleRenderer(),
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(processor_formatter)

    rotating_file_handler = RotatingFileHandler(
        filename=str(log_path),
        maxBytes=log_max_bytes,
        backupCount=log_backup_count,
    )
    rotating_file_handler.setFormatter(processor_formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(stream_handler)
    root_logger.addHandler(rotating_file_handler)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return rotating_file_handler
