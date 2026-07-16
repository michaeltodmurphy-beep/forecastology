# nws/stations.py
"""NWS ICAO station codes for the 20 monitored US cities."""
from typing import Dict

STATIONS: Dict[str, str] = {
    "Atlanta": "KATL",
    "Austin": "KAUS",
    "Boston": "KBOS",
    "Chicago": "KMDW",
    "Dallas": "KDFW",
    "Denver": "KDEN",
    "Houston": "KHOU",
    "Las Vegas": "KLAS",
    "Los Angeles": "KLAX",
    "Miami": "KMIA",
    "Minneapolis": "KMSP",
    "New Orleans": "KMSY",
    "New York City": "KNYC",
    "Oklahoma City": "KOKC",
    "Philadelphia": "KPHL",
    "Phoenix": "KPHX",
    "San Antonio": "KSAT",
    "San Francisco": "KSFO",
    "Seattle": "KSEA",
    "Washington DC": "KDCA",
}
