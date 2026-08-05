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
