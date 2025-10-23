"""
Code for DB session management.

Notes: August 27th 2025.

We have been trying to trace segfault issues for months. They're
our white whale. We identified a hypothesis: the sqlalchemy engine
and session were initialised on module import, before uvicorn
spawned the worker processes. This meant that the engine and session
were shared across all workers. Ruh roh. SQLALCHEMY ISNT THREAD SAFE.

Update: October 2025.
Stray connection leaks are being caused by services calling get_db()
without closing sessions, particularly via the defensive programming
pattern we were using in the admin service where cleanup
wasn't implemented properly.
"""

import logging

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import SQLALCHEMY_DATABASE_URI, STATEMENT_TIMEOUT

_LOGGER = logging.getLogger(__name__)

# Engine with connection pooling to prevent connection leaks
# Lazy initialisation - created once per worker
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_timeout=30,  # Wait up to 30s for a connection before error
    connect_args={"options": f"-c statement_timeout={STATEMENT_TIMEOUT}"},
)


# OpenTelemetry instrumentation
SQLAlchemyInstrumentor().instrument(engine=_engine)

# Session factory, exported callable for tests
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


def get_db():
    """Get the database session.

    Tries to get a database session. If there is no session, it will
    create one AFTER the uvicorn stuff has started.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
