"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import logging
import os
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.exc import DisconnectionError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

_LOGGER = logging.getLogger(__name__)

# Connection parameter validation
STATEMENT_TIMEOUT = os.getenv("STATEMENT_TIMEOUT", "10000")  # ms
DB_USERNAME = os.getenv("DB_MASTER_USERNAME")
DB_PASSWORD = os.getenv("MANAGED_DB_PASSWORD")
CLUSTER_URL = os.getenv("LOAD_DATABASE_URL")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Validate required connection parameters
_required_params = {
    "DB_MASTER_USERNAME": DB_USERNAME,
    "MANAGED_DB_PASSWORD": DB_PASSWORD,
    "LOAD_DATABASE_URL": CLUSTER_URL,
    "DB_NAME": DB_NAME,
}
_missing_params = [name for name, value in _required_params.items() if not value]
if _missing_params:
    raise RuntimeError(
        f"Missing required database environment variables: "
        f"{', '.join(_missing_params)}"
    )

SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{CLUSTER_URL}:"
    f"{DB_PORT}/{DB_NAME}?sslmode=no-verify"
)

_LOGGER.debug(f"Initialising database engine for {CLUSTER_URL}:{DB_PORT}/{DB_NAME}")

# Engine with connection pooling to prevent connection leaks
# Lazy initialisation - created once per worker
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_timeout=30,  # Wait up to 30s for a connection before error
    isolation_level="READ COMMITTED",  # PostgreSQL default, explicit
    connect_args={"options": f"-c statement_timeout={STATEMENT_TIMEOUT}"},
    echo=False,  # Set to True for SQL query logging in debug
)

# Session factory, exported callable for tests
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """Context manager for database session lifecycle.

    Use this for all database operations. Ensures proper cleanup
    even if operations are retried or fail. Automatically commits
    on success and rolls back on error.

    :yields: Database session
    :rtype: Generator[Session, None, None]
    """
    db = SessionLocal()
    try:
        _LOGGER.debug("Database session created (context manager)")
        yield db
        db.commit()
        _LOGGER.debug("Database session committed")
    except Exception:
        db.rollback()
        _LOGGER.exception("Database session rolled back")
        raise
    finally:
        db.close()
        _LOGGER.debug("Database session closed (context manager)")


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency wrapper for get_db_context().

    This function provides FastAPI-compatible dependency injection
    using the context manager internally. Use get_db_context()
    directly for non-FastAPI code.

    :yields: Database session
    :rtype: Generator[Session, None, None]
    """
    with get_db_context() as db:
        yield db


def check_db_health() -> bool:
    """Check database connection health.

    Performs a simple query to verify the database is accessible
    and responsive.

    :return: True if database is healthy, False otherwise
    :rtype: bool
    """
    try:
        with _engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return result.one_or_none() is not None
    except (OperationalError, DisconnectionError):
        _LOGGER.exception("Database health check failed")
    except Exception:
        _LOGGER.exception("Unexpected error during health check")
    return False
