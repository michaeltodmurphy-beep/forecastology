"""
Read-only data harness for the KXLOW overnight-low (21:00-01:00 local) gate.

Pulls the LIVE NWS hourly forecast for each monitored city and prints the
21:00-01:00 LOCAL hourly periods plus the computed window-min temperature,
so you can eyeball real data before enabling any gating behavior.

No trading action, no DB writes, no state changes.  Uses the same api.weather
.gov forecastHourly feed the bot already uses.

Usage:
    python inspect_night_min.py            # all monitored stations
    python inspect_night_min.py --station KNYC     # one station
    python inspect_night_min.py --city "New York City"

Requires NWS_USER_AGENT in the environment / .env (as the bot already does).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone


def _resolve_stations(all_stations: dict, city: str | None, station: str | None):
    """Return ordered (city, station) pairs to inspect."""
    if city:
        city2 = city.strip().lower()
        match = {
            c: s for c, s in all_stations.items() if c.strip().lower() == city2
        }
        if not match:
            sys.exit(f"Unknown city: {city}")
        return list(match.items())
    if station:
        code = station.strip().upper()
        match = {c: s for c, s in all_stations.items() if s == code}
        if not match:
            sys.exit(f"Unknown station: {station}")
        return list(match.items())
    return list(all_stations.items())


def _fmt_period(nws_client, period: dict, tz_name: str) -> str:
    """Return 'HH:00  TdegF' string for a raw NWS hourly period."""
    try:
        from zoneinfo import ZoneInfo
        dt = nws_client._parse_iso_dt(period["startTime"]).astimezone(
            ZoneInfo(tz_name)
        )
        hour = dt.strftime("%H:%M")
    except Exception:  # noqa: BLE001
        hour = period.get("startTime", "?")
    temp = period.get("temperature")
    return f"{hour}  {temp}F"


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect NWS 21:00-01:00 window min.")
    parser.add_argument("--station", type=str, default=None)
    parser.add_argument("--city", type=str, default=None)
    args = parser.parse_args()

    # Ensure env is loaded exactly like the bot.
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:  # noqa: BLE001
        pass

    from nws.client import NWSClient
    from nws.config import NWS_USER_AGENT
    from nws.stations import STATIONS
    from nws.night_min import fetch_night_window_min

    if not NWS_USER_AGENT:
        print("ERROR: NWS_USER_AGENT is not set.  Set it in your .env file.")
        return 1

    pairs = _resolve_stations(STATIONS, args.city, args.station)
    client = NWSClient(user_agent=NWS_USER_AGENT)

    print("")
    print("Live NWS hourly forecast - 21:00 to 01:00 LOCAL window")
    print(f"UTC now: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    for city, station_code in pairs:
        try:
            min_f, base_date, window_periods, tz_name = fetch_night_window_min(
                client, station_code
            )
        except Exception as exc:  # noqa: BLE001
            print(f"\n[{city} / {station_code}] ERROR: {exc}")
            continue

        print("")
        print(f"[{city}] station={station_code} tz={tz_name or '?'} base_local_date={base_date}")
        if window_periods:
            for p in sorted(
                window_periods,
                key=lambda x: str(x.get("startTime", "")),
            ):
                print("    " + _fmt_period(client, p, tz_name))
            if min_f is not None:
                print(f"    --> 21:00-01:00 window min = {min_f}F")
            else:
                print("    --> no matching hourly periods in window")
        else:
            print("    (no hourly periods in the 21:00-01:00 window)")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())