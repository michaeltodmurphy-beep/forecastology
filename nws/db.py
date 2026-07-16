# nws/db.py
"""Synchronous SQLAlchemy engine and session factory for the NWS module.

The NWS scheduler runs in a background thread (APScheduler
``BackgroundScheduler``), which is incompatible with the async engine used by
the main trading loop.  This module creates a separate *synchronous* engine
backed by ``pymysql`` that is used exclusively by the NWS forecast updater and
gate logic.

The ``StationForecast`` model is defined in ``app.models`` (shared ``Base``),
so ``init_nws_db()`` creates the table via the same metadata object that the
async engine already manages — the operation is idempotent.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.models import Base
from nws.config import MYSQL_URL

logger = logging.getLogger("forecastology.nws.db")

_engine = None
_SessionLocal: sessionmaker | None = None


def _get_engine():
    """Return the sync SQLAlchemy engine, creating it on first call."""
    global _engine
    if _engine is None:
        if not MYSQL_URL:
            raise RuntimeError(
                "MYSQL_URL (or MYSQL_DATABASE_URL) is not configured. "
                "Set it in your .env file before starting the NWS scheduler."
            )
        _engine = create_engine(
            MYSQL_URL,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    return _engine


def _get_session_factory() -> sessionmaker:
    """Return the session factory, creating it on first call."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=_get_engine(),
            autoflush=False,
            autocommit=False,
        )
    return _SessionLocal


def init_nws_db() -> None:
    """Create the ``station_forecasts`` table (and any other missing tables).

    Safe to call multiple times; SQLAlchemy only issues CREATE TABLE IF NOT
    EXISTS-equivalent DDL.
    """
    Base.metadata.create_all(bind=_get_engine())
    logger.info("nws.db.initialized")


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a synchronous database session and handle commit/rollback."""
    session: Session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
