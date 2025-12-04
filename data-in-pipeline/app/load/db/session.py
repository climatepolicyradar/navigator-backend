"""
Code for DB session management.

Stray connections can be caused by services calling get_db() without
closing sessions.
"""

import logging
import os

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_LOGGER = logging.getLogger(__name__)

SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "")
if not SQLALCHEMY_DATABASE_URI:
    raise RuntimeError("'{DATABASE_URL}' environment variable must be set")

STATEMENT_TIMEOUT = int(os.getenv("STATEMENT_TIMEOUT", "10000"))  # ms

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
