"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine

from app.aws import get_aws_session, get_ssm_parameter
from app.settings import settings

_LOGGER = logging.getLogger(__name__)


username = get_ssm_parameter("data-in-pipeline-aurora-write-replica-db-username")

session = get_aws_session()
client = session.client("rds")

ENDPOINT = settings.load_database_url.get_secret_value()
PORT = settings.db_port
REGION = settings.aws_region

token = client.generate_db_auth_token(
    DBHostname=ENDPOINT, Port=PORT, DBUsername=username, Region=REGION
)


SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{username}:"
    f"{token}@"
    f"{ENDPOINT}:"
    f"{PORT}/{settings.db_name}?sslmode={settings.db_sslmode}"
)

_LOGGER.info(
    f"🔌 Initialising database engine for "
    f"{settings.load_database_url.get_secret_value()}:"
    f"{settings.db_port}/{settings.db_name}"
)

# Engine with connection pooling to prevent connection leaks
# Lazy initialisation - created once per worker
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=840,  # Recycle every 14 min to avoid expired IAM auth tokens (15 min lifetime). https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html
    pool_timeout=30,  # Wait up to 30s for a connection before error
    isolation_level="READ COMMITTED",  # PostgreSQL default, explicit
    connect_args={"options": f"-c statement_timeout={settings.statement_timeout}"},
    echo=False,  # Set to True for SQL query logging in debug
)


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """Context manager for database session lifecycle.

    Use this for all database operations. Ensures proper cleanup
    even if operations are retried or fail. Automatically commits
    on success and rolls back on error.

    :yields: Database session
    :rtype: Generator[Session, None, None]
    """
    db = Session(_engine)
    try:
        _LOGGER.debug("Database session created (context manager)")
        yield db
        _LOGGER.debug("Safely exiting database session (context manager)")
    except Exception:
        db.rollback()
        _LOGGER.exception("Database session rolled back (context manager)")
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


def get_engine() -> Engine:
    """Get the database engine instance.

    Exposed for testing and advanced use cases. Generally prefer
    get_db_context() for normal operations.

    :return: SQLModel engine instance
    :rtype: Engine
    """
    return _engine
