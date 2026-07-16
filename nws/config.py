# nws/config.py
"""NWS forecast backend configuration loaded from environment variables."""
import os

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
_raw_url: str = (os.getenv("MYSQL_URL") or os.getenv("MYSQL_DATABASE_URL", "")).strip()
MYSQL_URL: str = _raw_url.replace("mysql+aiomysql://", "mysql+pymysql://")
