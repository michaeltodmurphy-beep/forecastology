# nws/config.py
"""NWS forecast backend configuration loaded from environment variables."""
import os
import re

from dotenv import load_dotenv

load_dotenv()

# Required: NWS API custom User-Agent string
NWS_USER_AGENT: str = os.getenv("NWS_USER_AGENT", "").strip()

# How often (minutes) to refresh NWS forecast data in the background
HIGH_LOW_UPDATE: int = int(os.getenv("HIGH_LOW_UPDATE", "60"))

# Trading gate window offsets (minutes) around the forecasted low/high times
GATE_LOW_BEFORE: int = int(os.getenv("GATE_LOW_BEFORE", "120"))
GATE_LOW_AFTER: int = int(os.getenv("GATE_LOW_AFTER", "45"))
GATE_HIGH_BEFORE: int = int(os.getenv("GATE_HIGH_BEFORE", "60"))
GATE_HIGH_AFTER: int = int(os.getenv("GATE_HIGH_AFTER", "30"))

# Database URL for the sync SQLAlchemy engine used by the NWS scheduler.
# Uses MYSQL_URL if set; falls back to MYSQL_DATABASE_URL (async URL) after
# converting the driver to the sync pymysql variant.
def _to_sync_mysql_url(raw_url: str) -> str:
    raw_url = raw_url.strip()
    if not raw_url:
        return ""

    if "://" not in raw_url:
        return re.sub(r"aiomysql", "pymysql", raw_url, count=1, flags=re.IGNORECASE)

    scheme, rest = raw_url.split("://", 1)
    scheme = re.sub(r"aiomysql", "pymysql", scheme, count=1, flags=re.IGNORECASE)
    return f"{scheme}://{rest}"


_raw_url: str = (os.getenv("MYSQL_URL") or os.getenv("MYSQL_DATABASE_URL", "")).strip()
MYSQL_URL: str = _to_sync_mysql_url(_raw_url)
