import logging

import structlog

from app.runtime_safety import LockConflict, acquire_instance_lock, configure_logging, maybe_acquire_instance_lock


def test_instance_lock_acquire_conflict_release_reacquire(tmp_path):
    base_lock = str(tmp_path / "forecastology.lock")
    account_id_hash = "abc123hash"

    first = acquire_instance_lock(base_lock_file=base_lock, account_id_hash=account_id_hash)
    assert not isinstance(first, LockConflict)

    second = acquire_instance_lock(base_lock_file=base_lock, account_id_hash=account_id_hash)
    assert isinstance(second, LockConflict)
    assert second.holder_pid is not None

    first.release()
    third = acquire_instance_lock(base_lock_file=base_lock, account_id_hash=account_id_hash)
    assert not isinstance(third, LockConflict)
    third.release()


def test_instance_lock_is_account_scoped(tmp_path):
    base_lock = str(tmp_path / "forecastology.lock")
    first = acquire_instance_lock(base_lock_file=base_lock, account_id_hash="account_a")
    second = acquire_instance_lock(base_lock_file=base_lock, account_id_hash="account_b")
    assert not isinstance(first, LockConflict)
    assert not isinstance(second, LockConflict)
    first.release()
    second.release()


def test_instance_lock_disabled_bypass(tmp_path):
    base_lock = str(tmp_path / "forecastology.lock")
    account_id_hash = "abc123hash"
    first = acquire_instance_lock(base_lock_file=base_lock, account_id_hash=account_id_hash)
    assert not isinstance(first, LockConflict)
    disabled_guard_result = maybe_acquire_instance_lock(
        enabled=False,
        base_lock_file=base_lock,
        account_id_hash=account_id_hash,
    )
    assert disabled_guard_result is None
    first.release()


def test_rotating_file_handler_rollover(tmp_path):
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    log_file = tmp_path / "run.log"
    try:
        rotating_handler = configure_logging(
            log_file=str(log_file),
            log_max_bytes=512,
            log_backup_count=2,
        )
        assert rotating_handler.maxBytes == 512
        assert rotating_handler.backupCount == 2

        logger = structlog.get_logger("tests.runtime_safety")
        for _ in range(40):
            logger.info("test.rotation", payload="x" * 64)

        for handler in logging.getLogger().handlers:
            handler.flush()

        assert log_file.exists()
        assert (tmp_path / "run.log.1").exists()
    finally:
        for handler in list(root_logger.handlers):
            handler.close()
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)
