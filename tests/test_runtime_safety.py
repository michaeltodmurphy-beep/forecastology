import logging
import os

import structlog
import pytest

from app.runtime_safety import (
    LockConflict,
    _stdout_targets_same_file,
    acquire_instance_lock,
    configure_logging,
    maybe_acquire_instance_lock,
)


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


def test_stdout_same_file_missing_file_is_false(tmp_path):
    """A not-yet-created log file cannot be compared -> False (no suppression)."""
    assert _stdout_targets_same_file(tmp_path / "does_not_exist.log") is False


def test_stdout_same_file_terminal_is_false(tmp_path):
    """When stdout is the terminal (pytest capture), it is never the log file."""
    log_file = tmp_path / "run.log"
    log_file.write_text("")
    assert _stdout_targets_same_file(log_file) is False


@pytest.mark.skipif(os.name != "posix", reason="fd/inode comparison is POSIX-only")
def test_stdout_same_file_detects_redirect(tmp_path):
    """stdout dup'd onto the log file's fd must be detected as a merge."""
    log_file = tmp_path / "run.log"
    log_file.write_text("")
    saved = os.dup(1)
    fd = os.open(str(log_file), os.O_WRONLY)
    try:
        os.dup2(fd, 1)
        os.close(fd)
        assert _stdout_targets_same_file(log_file) is True
    finally:
        os.dup2(saved, 1)
        os.close(saved)


@pytest.mark.skipif(os.name != "posix", reason="fd/inode comparison is POSIX-only")
def test_configure_logging_suppresses_console_on_merge(tmp_path):
    """With stdout merged into the log file, only ONE sink is attached."""
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    log_file = tmp_path / "run.log"
    log_file.write_text("")
    saved = os.dup(1)
    fd = os.open(str(log_file), os.O_WRONLY)
    try:
        os.dup2(fd, 1)
        os.close(fd)
        configure_logging(
            log_file=str(log_file),
            log_max_bytes=512,
            log_backup_count=2,
            log_to_console=True,
            log_to_file=True,
        )
        managed = [
            h for h in root_logger.handlers
            if getattr(h, "_forecastology_managed", False)
        ]
        # Console sink suppressed for the merge -> exactly the file handler.
        assert len(managed) == 1
        assert managed[0].__class__.__name__ == "RotatingFileHandler"
    finally:
        os.dup2(saved, 1)
        os.close(saved)
        for handler in list(root_logger.handlers):
            handler.close()
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)

