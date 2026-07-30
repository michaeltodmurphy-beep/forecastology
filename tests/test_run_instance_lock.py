from types import SimpleNamespace

import pytest

import run
from app.runtime_safety import LockConflict


class _StubLogger:
    def __init__(self):
        self.events = []

    def critical(self, event, **kwargs):
        self.events.append(("critical", event, kwargs))

    def info(self, event, **kwargs):
        self.events.append(("info", event, kwargs))

    def warning(self, event, **kwargs):
        self.events.append(("warning", event, kwargs))


def test_acquire_startup_lock_conflict_logs_and_exits(monkeypatch, capsys):
    stub_logger = _StubLogger()
    monkeypatch.setattr(run, "logger", stub_logger)
    monkeypatch.setattr(
        run,
        "maybe_acquire_instance_lock",
        lambda **_kwargs: LockConflict(
            lock_file="/tmp/forecastology.abc123.lock",
            holder_pid=4242,
            account_id_hash="abc123",
        ),
    )
    config = SimpleNamespace(instance_lock_enabled=True, instance_lock_file="/tmp/forecastology.lock")

    with pytest.raises(SystemExit) as exc:
        run._acquire_startup_lock(config, "abc123")
    assert exc.value.code == 1
    assert ("critical", "instance.lock_conflict", {"lock_file": "/tmp/forecastology.abc123.lock", "holder_pid": 4242, "account_id_hash": "abc123"}) in stub_logger.events
    assert "Another Forecastology instance is already running against this account; refusing to start." in capsys.readouterr().err


def test_acquire_startup_lock_disabled_logs_warning(monkeypatch):
    stub_logger = _StubLogger()
    monkeypatch.setattr(run, "logger", stub_logger)
    monkeypatch.setattr(run, "maybe_acquire_instance_lock", lambda **_kwargs: None)
    config = SimpleNamespace(instance_lock_enabled=False, instance_lock_file="/tmp/forecastology.lock")

    lock = run._acquire_startup_lock(config, "abc123")
    assert lock is None
    assert any(
        level == "warning" and event == "instance.lock_disabled"
        for level, event, _kwargs in stub_logger.events
    )
