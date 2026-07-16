import asyncio

import run


def test_start_nws_backend_calls_bootstrap(monkeypatch):
    called = {"value": False}

    def fake_bootstrap():
        called["value"] = True

    monkeypatch.setattr(run, "nws_bootstrap", fake_bootstrap)
    asyncio.run(run._start_nws_backend())

    assert called["value"] is True


def test_start_nws_backend_logs_and_continues_on_failure(monkeypatch):
    class FakeLogger:
        def __init__(self):
            self.events = []

        def exception(self, event, **kwargs):
            self.events.append((event, kwargs))

        def info(self, event, **kwargs):
            self.events.append((event, kwargs))

    fake_logger = FakeLogger()

    def bad_bootstrap():
        raise RuntimeError("boom")

    monkeypatch.setattr(run, "nws_bootstrap", bad_bootstrap)
    monkeypatch.setattr(run, "logger", fake_logger)

    asyncio.run(run._start_nws_backend())

    assert any(event == "nws.bootstrap_failed" for event, _ in fake_logger.events)


def test_shutdown_nws_backend_swallows_exceptions(monkeypatch):
    class FakeLogger:
        def __init__(self):
            self.events = []

        def exception(self, event, **kwargs):
            self.events.append((event, kwargs))

    fake_logger = FakeLogger()

    def bad_shutdown():
        raise RuntimeError("shutdown error")

    monkeypatch.setattr(run, "shutdown_nws_scheduler", bad_shutdown)
    monkeypatch.setattr(run, "logger", fake_logger)

    run._shutdown_nws_backend()

    assert any(event == "nws.shutdown_failed" for event, _ in fake_logger.events)
