"""
Unit tests for scanner.py critical-fix changes:
  - _daemon_is_running() correctly detects whether run.py holds the lockfile
  - main() exits immediately when the daemon lockfile is held (Critical #1 guard)
"""

import asyncio
import fcntl
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import scanner as scanner_module
from scanner import _daemon_is_running


# ---------------------------------------------------------------------------
# _daemon_is_running() tests
# ---------------------------------------------------------------------------

def test_daemon_not_running_when_no_lockfile(tmp_path):
    """If the lockfile does not exist, _daemon_is_running() returns False."""
    non_existent = str(tmp_path / "missing.lock")
    with patch.object(scanner_module, "DAEMON_LOCKFILE", non_existent):
        assert _daemon_is_running() is False


def test_daemon_not_running_when_lock_available(tmp_path):
    """If the lockfile exists but no process holds it, _daemon_is_running() returns False."""
    lockfile = str(tmp_path / "test.lock")
    with open(lockfile, "w") as f:
        f.write("")
    with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
        assert _daemon_is_running() is False


def test_daemon_is_running_when_lock_held(tmp_path):
    """If another fd holds an exclusive lock on the lockfile, _daemon_is_running() returns True."""
    lockfile = str(tmp_path / "held.lock")
    # Hold an exclusive lock in this process to simulate run.py
    holder = open(lockfile, "w")
    try:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
            assert _daemon_is_running() is True
    finally:
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        holder.close()


# ---------------------------------------------------------------------------
# main() skips scan when daemon is running (Critical #1 guard)
# ---------------------------------------------------------------------------

def test_main_exits_early_when_daemon_running(tmp_path, capfd):
    """main() must return without running the scan cycle when the daemon lock is held."""
    lockfile = str(tmp_path / "daemon.lock")
    holder = open(lockfile, "w")
    try:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with patch.object(scanner_module, "DAEMON_LOCKFILE", lockfile):
            with patch.object(scanner_module, "run_scan_cycle", new_callable=AsyncMock) as mock_scan:
                scanner_module.main()
                mock_scan.assert_not_awaited()
    finally:
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        holder.close()
