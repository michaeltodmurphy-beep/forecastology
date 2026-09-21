# core/trade_outcome_utils.py
"""Utility functions shared between entry-context capture and the reconciler.

All helpers are pure (no DB access) and importable from any context.
"""
from __future__ import annotations

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Price bucket labelling
# ---------------------------------------------------------------------------

_BUCKET_THRESHOLDS = [
    (71, "<=71"),
    (75, "72-75"),
    (80, "76-80"),
    (86, "81-86"),
]
_BUCKET_HIGH = "87+"


def entry_price_bucket(price_cents: int) -> str:
    """Return the entry-price bucket label for *price_cents*.

    Buckets:
        <=71  → "<=71"
        72–75 → "72-75"
        76–80 → "76-80"
        81–86 → "81-86"
        87+   → "87+"
    """
    for threshold, label in _BUCKET_THRESHOLDS:
        if price_cents <= threshold:
            return label
    return _BUCKET_HIGH


# ---------------------------------------------------------------------------
# Bracket temperature parsing
# ---------------------------------------------------------------------------

# Matches the bracket segment of a Kalshi ticker, e.g.:
#   "B52.5" → 52.5   (below-bracket)
#   "T68"   → 68.0   (target-bracket)
#   "B100"  → 100.0
_BRACKET_RE = re.compile(r"^[BT](\d+\.?\d*)$", re.IGNORECASE)


def parse_bracket_temp(market_ticker: str) -> Optional[float]:
    """Extract the numeric temperature from the bracket segment of *market_ticker*.

    The Kalshi format is ``{SERIES}-{YYMMMDD}-{BRACKET}``.  The bracket segment
    starts with 'B' (below) or 'T' (target/above) followed by a number, e.g.
    ``B52.5`` or ``T68``.

    Returns the float value, or ``None`` if the segment cannot be parsed.
    """
    parts = market_ticker.split("-")
    if len(parts) < 3:
        return None
    bracket_seg = parts[-1]
    m = _BRACKET_RE.match(bracket_seg)
    if m is None:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def parse_bracket_kind(market_ticker: str) -> Optional[str]:
    """Return the bracket inequality kind for *market_ticker*'s last segment.

    Kalshi encodes the settlement inequality in the bracket prefix letter:

      - ``T`` → **strictly greater than** the line (e.g. ``T75`` = "low > 75").
        A market settles YES only when the value is strictly above the line, so
        an observed value of *exactly* the line is a LOSS.
      - ``B`` → **greater-than-or-equal-to** the line (e.g. ``B75`` = "low >= 75";
        Kalshi's "below range" brackets are written as ``B`` on the lower edge).
        An observed value *equal* to the line is a WIN.

    Returns ``"T"``, ``"B"``, or ``None`` when the segment cannot be parsed.

    This exists because the entry gates must treat the boundary differently for
    the two kinds: a ``T`` line is exclusive (``> X``), a ``B`` line is
    inclusive (``>= X``).  Collapsing both to a single number loses the
    strict-inequality boundary and lets a ``T`` entry through when the observed
    (or forecast) minimum merely *touches* the line.
    """
    parts = market_ticker.split("-")
    if len(parts) < 3:
        return None
    bracket_seg = parts[-1]
    m = _BRACKET_RE.match(bracket_seg)
    if m is None:
        return None
    return bracket_seg[0].upper()


# ---------------------------------------------------------------------------
# Bracket reachability range
# ---------------------------------------------------------------------------

# KXLOW bracket encoding, VERIFIED against the Kalshi markets API (real titles):
#
#   ticker segment   title ("sub_title")     meaning            reachability
#   --------------   --------------------    ----------------   ------------------
#   B57.5            "57° to 58°"            window {57, 58}     low in [57, 58]
#   B55.5            "55° to 56°"            window {55, 56}     low in [55, 56]
#   B51.5            "51° to 52°"            window {51, 52}     low in [51, 52]
#   T58              "59° or above"          low > 58            EXEMPT (top)
#   T51              "50° or below"          low < 51            low reached 50
#
# So a ``B<line>`` window covers the two whole degrees ``floor(line)`` and
# ``floor(line)+1`` (B57.5 -> 57,58).  The OPEN-ENDED ends are both encoded as
# ``T`` tickers: the event's SMALLEST ``T`` is the bottom ("X or below") and the
# LARGEST ``T`` is the top ("X or above").  Which is which therefore depends on
# the sibling lines in the same event, which the caller passes in.
#
# Whole-degree values are what the observed 5-min low is compared against; the
# half-integer line is only Kalshi's split point between adjacent degrees.


def bracket_reachability_range(
    market_ticker: str,
    bracket_temp_f: float,
    sibling_lines: Optional[list[float]] = None,
) -> Optional[tuple[str, float, float]]:
    """Return ``(kind, lo, hi)`` for the whole-degree range a KXLOW bracket can
    win, or ``None`` when it cannot be determined.

    ``kind`` is one of:

      - ``"below"``  -- bottom open-ended ("X or below").  *lo* is ``-inf`` and
        *hi* is the inclusive ceiling; the observed low must have reached
        at/under *hi* (i.e. ``day_min <= hi``).
      - ``"hard"``   -- a bounded 2-degree window ``[lo, hi]`` (e.g. 57 to 58);
        the observed low must fall within it.
            - ``"above"``  -- top open-ended ("X or above").  EXEMPT from
        reachability (a colder-than-range morning does not disqualify it).

    ``sibling_lines`` MUST be the numeric lines of the ``T`` tickers in the
    same event (not the ``B`` lines) -- it is used to tell the bottom ``T``
    ("X or below") apart from the top ``T`` ("X or above"): the smallest ``T``
    line is the bottom, the largest is the top.  When only one ``T`` line is
    supplied it is treated as the top (the common case once the event has fully
    populated), which is the safe default because the top is exempt, so this
    never wrongly blocks.
    """
    import math

    parts = market_ticker.split("-")
    if len(parts) < 3:
        return None
    bracket_seg = parts[-1]
    if _BRACKET_RE.match(bracket_seg) is None:
        return None
    letter = bracket_seg[0].upper()
    line = float(bracket_temp_f)

    if letter == "B":
        # Hard window spanning the two whole degrees floor(line) and floor(line)+1.
        lo = float(math.floor(line))
        hi = float(math.floor(line) + 1)
        return "hard", lo, hi

    # A "T" ticker is one of the two open ends.  The smallest T line in the
    # event is the bottom ("X or below"); every other T (in practice the largest)
    # is the top ("X or above").
    t_lines = []
    if sibling_lines:
        t_lines = [ln for ln in sibling_lines if ln is not None]
    if t_lines and line <= min(t_lines):
        # Bottom open end: low < n settles it, and n is exclusive on the warm
        # side, so the bracket covers every whole degree <= floor(n) - 1.
        # "50 or below" is encoded as T51 -> ceiling floor(51) - 1 = 50.
        return "below", float("-inf"), float(math.floor(line) - 1)

    # Top open end: low > n settles it (n exclusive on the cold side), so the
    # bracket covers every whole degree >= floor(n) + 1.  Exempt from
    # reachability regardless.
    return "above", float(math.floor(line) + 1), float("inf")


# ---------------------------------------------------------------------------
# Family detection
# ---------------------------------------------------------------------------

def detect_family(market_ticker: str) -> Optional[str]:
    """Return "LOW", "HIGH", or None based on the ticker series prefix."""
    upper = market_ticker.upper()
    if "KXLOW" in upper:
        return "LOW"
    if "KXHIGH" in upper:
        return "HIGH"
    return None
