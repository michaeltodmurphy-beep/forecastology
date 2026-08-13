from __future__ import annotations

import datetime
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class _DedupeState:
    fingerprint: tuple
    last_fields: dict
    suppressed_count: int
    last_summary_monotonic: float


class DedupeLogger:
    """Suppress repeated identical logs and emit periodic repeat summaries."""

    def __init__(
        self,
        summary_interval_seconds: int = 300,
        monotonic_fn: Optional[Callable[[], float]] = None,
    ) -> None:
        self._summary_interval_seconds = int(summary_interval_seconds)
        self._monotonic_fn = monotonic_fn or time.monotonic
        self._states: dict[tuple[str, str, str], _DedupeState] = {}

    @staticmethod
    def _normalize_value(value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        if isinstance(value, dict):
            return tuple(sorted((str(k), DedupeLogger._normalize_value(v)) for k, v in value.items()))
        if isinstance(value, (list, tuple)):
            return tuple(DedupeLogger._normalize_value(v) for v in value)
        if isinstance(value, set):
            return tuple(sorted(DedupeLogger._normalize_value(v) for v in value))
        return value

    @classmethod
    def _fingerprint(cls, fields: dict) -> tuple:
        return tuple(sorted((str(k), cls._normalize_value(v)) for k, v in fields.items()))

    @staticmethod
    def _scope_day(day: Optional[datetime.date | str]) -> str:
        if day is None:
            return "global"
        if isinstance(day, datetime.date):
            return day.isoformat()
        return str(day)

    @staticmethod
    def _emit(logger_obj, level: str, event: str, fields: dict) -> None:
        getattr(logger_obj, level)(event, **fields)

    def log(
        self,
        logger_obj,
        level: str,
        event: str,
        key: str,
        *,
        day: Optional[datetime.date | str] = None,
        **fields,
    ) -> None:
        now_mono = self._monotonic_fn()
        scope = (event, str(key), self._scope_day(day))
        fingerprint = self._fingerprint(fields)
        state = self._states.get(scope)

        if state is None:
            self._emit(logger_obj, level, event, fields)
            self._states[scope] = _DedupeState(
                fingerprint=fingerprint,
                last_fields=dict(fields),
                suppressed_count=0,
                last_summary_monotonic=now_mono,
            )
            return

        if state.fingerprint != fingerprint:
            if state.suppressed_count > 0:
                self._emit(
                    logger_obj,
                    "info",
                    f"{event}.repeated",
                    {"count": state.suppressed_count, **state.last_fields},
                )
                state.suppressed_count = 0
            self._emit(logger_obj, level, event, fields)
            state.fingerprint = fingerprint
            state.last_fields = dict(fields)
            state.last_summary_monotonic = now_mono
            return

        state.suppressed_count += 1
        if now_mono - state.last_summary_monotonic < self._summary_interval_seconds:
            return

        self._emit(
            logger_obj,
            "info",
            f"{event}.repeated",
            {"count": state.suppressed_count, **state.last_fields},
        )
        state.suppressed_count = 0
        state.last_summary_monotonic = now_mono
