# nws/scheduler.py
"""APScheduler-based background updater for NWS temperature forecasts.

``bootstrap()`` should be called once at application startup.  It:

1. Initialises the database (creates ``station_forecasts`` table if absent).
2. Runs an immediate forecast update for all monitored stations.
3. Starts a ``BackgroundScheduler`` that repeats the update every
   ``HIGH_LOW_UPDATE`` minutes without blocking the main trading thread.

``shutdown()`` should be called on clean exit to stop the scheduler.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.exc import SQLAlchemyError

from app.models import StationForecast
from nws.client import NWSClient
from nws.config import HIGH_LOW_UPDATE, NWS_USER_AGENT
from nws.db import get_session, init_nws_db
from nws.stations import STATIONS

logger = logging.getLogger("forecastology.nws.scheduler")

_scheduler: Optional[BackgroundScheduler] = None


# ---------------------------------------------------------------------------
# Upsert helper
# ---------------------------------------------------------------------------

def _upsert_forecast(
    session,
    station_code: str,
    forecast_date_utc: datetime,
    high_time_utc: Optional[datetime],
    low_time_utc: Optional[datetime],
) -> None:
    """Insert or update a ``StationForecast`` row (station + date)."""
    row: Optional[StationForecast] = (
        session.query(StationForecast)
        .filter(
            StationForecast.station_code == station_code,
            StationForecast.forecast_date_utc == forecast_date_utc,
        )
        .one_or_none()
    )

    now_utc = datetime.now(timezone.utc)

    if row is None:
        row = StationForecast(
            station_code=station_code,
            forecast_date_utc=forecast_date_utc,
            high_time_utc=high_time_utc,
            low_time_utc=low_time_utc,
            updated_at=now_utc,
        )
        session.add(row)
    else:
        row.high_time_utc = high_time_utc
        row.low_time_utc = low_time_utc
        row.updated_at = now_utc


# ---------------------------------------------------------------------------
# Update job (runs in background thread)
# ---------------------------------------------------------------------------

def run_forecast_update_job() -> None:
    """Fetch NWS hourly forecasts for all stations and persist to the DB.

    Each station's high/low is derived from its LOCAL calendar day, so
    UTC day boundaries never skew the selection.  The ``forecast_date_utc``
    row key is UTC midnight of the station's local today (which may differ
    by one day from the UTC date when the updater runs near midnight UTC).

    Errors for individual stations are logged but do not abort the batch;
    a DB transaction failure rolls back the entire batch.
    """
    logger.info("nws.update_job.start")

    if not NWS_USER_AGENT:
        logger.error(
            "nws.update_job.skipped NWS_USER_AGENT is not set — "
            "set it in your .env file"
        )
        return

    client = NWSClient(user_agent=NWS_USER_AGENT)
    now_utc = datetime.now(timezone.utc)

    try:
        with get_session() as session:
            for city, station_code in STATIONS.items():
                try:
                    high_time, low_time, forecast_date_utc = (
                        client.fetch_high_low_for_date(station_code, now_utc)
                    )
                    _upsert_forecast(
                        session,
                        station_code,
                        forecast_date_utc,
                        high_time,
                        low_time,
                    )
                    logger.info(
                        "nws.updated city=%s station=%s trading_date_utc=%s high_utc=%s low_utc=%s",
                        city,
                        station_code,
                        forecast_date_utc.isoformat(),
                        high_time,
                        low_time,
                    )
                except Exception:
                    logger.exception(
                        "nws.update_error city=%s station=%s", city, station_code
                    )
    except SQLAlchemyError:
        logger.exception("nws.update_job.db_error — batch rolled back")

    logger.info("nws.update_job.done")


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler() -> None:
    """Start the APScheduler ``BackgroundScheduler`` for periodic NWS updates.

    No-op if the scheduler is already running.

    This scheduler is exclusively used for NWS weather forecast data refresh.
    It contains ZERO sell, exit, or ask-price evaluation logic.  The only
    registered job is ``nws_high_low_updater`` (forecast data fetch).
    Any accidental introduction of sell/exit jobs will be caught by the
    ``_ALLOWED_JOB_IDS`` assertion below.
    """
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return

    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(
        run_forecast_update_job,
        trigger="interval",
        minutes=HIGH_LOW_UPDATE,
        id="nws_high_low_updater",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    # Whitelist guard: assert only the forecast-update job is registered.
    # This prevents accidental introduction of sell/exit scheduler jobs —
    # especially any periodic ask-lowering sell trigger, which must not exist.
    _ALLOWED_JOB_IDS = {"nws_high_low_updater"}
    registered_ids = {j.id for j in _scheduler.get_jobs()}
    unexpected = registered_ids - _ALLOWED_JOB_IDS
    if unexpected:  # pragma: no cover
        raise RuntimeError(
            f"NWS scheduler has unexpected jobs: {unexpected}. "
            "Only forecast-update jobs are permitted; sell/exit jobs must NOT "
            "be registered in this scheduler."
        )

    _scheduler.start()
    logger.info(
        "nws.scheduler.started interval_minutes=%s", HIGH_LOW_UPDATE
    )


def shutdown() -> None:
    """Stop the background scheduler gracefully."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("nws.scheduler.stopped")


# ---------------------------------------------------------------------------
# Bootstrap (call once at application startup)
# ---------------------------------------------------------------------------

def bootstrap() -> None:
    """Initialise DB, run an immediate update, then start the scheduler.

    Intended to be called from the application entry point (e.g. ``run.py``)
    before the main trading loop begins, so that gate data is available from
    the first second of operation.
    """
    init_nws_db()
    run_forecast_update_job()
    start_scheduler()
