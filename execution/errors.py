"""
execution/errors.py

Typed error hierarchy for the execution layer.

Use these exceptions to distinguish failure classes so callers can apply
appropriate retry / backoff / fail-closed policies.

    TransientExecutionError  – network / timeout / server-unavailable errors.
                               Safe to retry with backoff.  Does NOT indicate
                               that the order was definitely NOT submitted.

    PermanentExecutionError  – validation / permanent rejection errors.
                               Do NOT retry without operator review.  The
                               exchange will refuse this order regardless of
                               how many times it is resubmitted.
"""


class ExecutionError(Exception):
    """Base class for all execution-layer errors."""

    def __init__(self, message: str, error_class: str = "unknown"):
        super().__init__(message)
        self.error_class = error_class


class TransientExecutionError(ExecutionError):
    """
    Network / transient failure – safe to retry with exponential back-off.

    Examples: connection timeout, DNS failure, HTTP 5xx server error,
    TCP reset while waiting for response.
    """

    def __init__(self, message: str):
        super().__init__(message, error_class="transient")


class PermanentExecutionError(ExecutionError):
    """
    Validation / permanent rejection – do NOT retry automatically.

    Examples: invalid ticker, insufficient balance, order already cancelled,
    exchange returned HTTP 400/422 with a non-retryable error code.
    """

    def __init__(self, message: str):
        super().__init__(message, error_class="permanent")
