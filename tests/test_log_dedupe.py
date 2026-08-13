from core.log_dedupe import DedupeLogger


class _CaptureLogger:
    def __init__(self):
        self.events = []

    def info(self, event, **kwargs):
        self.events.append(("info", event, kwargs))

    def debug(self, event, **kwargs):
        self.events.append(("debug", event, kwargs))


def test_dedupe_logger_suppresses_identical_repeats():
    logger = _CaptureLogger()
    dedupe = DedupeLogger(summary_interval_seconds=300, monotonic_fn=lambda: 0.0)

    dedupe.log(logger, "info", "phase.b.missed_entry", "KXLOWTATL-26AUG13-T76", day="26AUG13", ticker="KXLOWTATL-26AUG13-T76", price=95)
    dedupe.log(logger, "info", "phase.b.missed_entry", "KXLOWTATL-26AUG13-T76", day="26AUG13", ticker="KXLOWTATL-26AUG13-T76", price=95)

    assert logger.events == [
        ("info", "phase.b.missed_entry", {"ticker": "KXLOWTATL-26AUG13-T76", "price": 95})
    ]


def test_dedupe_logger_logs_again_when_fields_change():
    logger = _CaptureLogger()
    dedupe = DedupeLogger(summary_interval_seconds=300, monotonic_fn=lambda: 0.0)

    dedupe.log(logger, "info", "phase.b.spread_too_wide", "KXLOWTATL-26AUG13-T76", day="26AUG13", ticker="KXLOWTATL-26AUG13-T76", spread=8, price=80)
    dedupe.log(logger, "info", "phase.b.spread_too_wide", "KXLOWTATL-26AUG13-T76", day="26AUG13", ticker="KXLOWTATL-26AUG13-T76", spread=9, price=80)

    assert [event for _, event, _ in logger.events] == [
        "phase.b.spread_too_wide",
        "phase.b.spread_too_wide",
    ]


def test_dedupe_logger_emits_periodic_repeat_summary():
    logger = _CaptureLogger()
    ticks = iter([0.0, 10.0, 320.0])
    dedupe = DedupeLogger(summary_interval_seconds=300, monotonic_fn=lambda: next(ticks))

    dedupe.log(logger, "info", "sunrise.gate_blocked", "KXLOWTATL-26AUG13-T76", day="2026-08-13", ticker="KXLOWTATL-26AUG13-T76")
    dedupe.log(logger, "info", "sunrise.gate_blocked", "KXLOWTATL-26AUG13-T76", day="2026-08-13", ticker="KXLOWTATL-26AUG13-T76")
    dedupe.log(logger, "info", "sunrise.gate_blocked", "KXLOWTATL-26AUG13-T76", day="2026-08-13", ticker="KXLOWTATL-26AUG13-T76")

    assert logger.events[0][1] == "sunrise.gate_blocked"
    assert logger.events[1][1] == "sunrise.gate_blocked.repeated"
    assert logger.events[1][2]["count"] == 2
