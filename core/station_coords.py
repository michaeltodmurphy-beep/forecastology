"""Settlement station coordinates for KXLOW temperature series."""

from __future__ import annotations

from typing import Dict, Tuple

# series_prefix -> (station_id, latitude, longitude)
SERIES_STATION_COORDS: Dict[str, Tuple[str, float, float]] = {
    "KXLOWTATL": ("KATL", 33.6367, -84.4281),
    "KXLOWTAUS": ("KAUS", 30.1945, -97.6699),
    "KXLOWTBOS": ("KBOS", 42.3656, -71.0096),
    "KXLOWTCHI": ("KMDW", 41.7868, -87.7522),
    "KXLOWTDAL": ("KDFW", 32.8998, -97.0403),
    "KXLOWTDC": ("KDCA", 38.8512, -77.0402),
    "KXLOWTDEN": ("KDEN", 39.8561, -104.6737),
    "KXLOWTHOU": ("KHOU", 29.6454, -95.2789),
    "KXLOWTLAX": ("KLAX", 33.9425, -118.4081),
    "KXLOWTLV": ("KLAS", 36.0801, -115.1522),
    "KXLOWTMIA": ("KMIA", 25.7959, -80.2870),
    "KXLOWTMIN": ("KMSP", 44.8848, -93.2223),
    "KXLOWTNOLA": ("KMSY", 29.9934, -90.2580),
    "KXLOWTNYC": ("KNYC", 40.7789, -73.9692),
    "KXLOWTOKC": ("KOKC", 35.3931, -97.6007),
    "KXLOWTPHIL": ("KPHL", 39.8729, -75.2437),
    "KXLOWTPHX": ("KPHX", 33.4342, -112.0116),
    "KXLOWTSATX": ("KSAT", 29.5337, -98.4698),
    "KXLOWTSEA": ("KSEA", 47.4489, -122.3094),
    "KXLOWTSFO": ("KSFO", 37.6188, -122.3750),
}

