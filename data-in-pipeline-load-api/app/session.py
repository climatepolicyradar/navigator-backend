"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import json
import logging
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.settings import settings

_LOGGER = logging.getLogger(__name__)


def _build_database_uri() -> str:
    """Build the database URI based on authentication method.

    :return: SQLAlchemy database URI
    :rtype: str
    """
    hostname = settings.load_database_url.get_secret_value()
    port = settings.db_port
    username = settings.db_master_username
    database = settings.db_name

    password = None
    if settings.db_use_iam_auth:
        # For IAM auth, we use a custom creator, so password is placeholder
        # The actual token is generated in _create_connection_with_iam()
        # trunk-ignore(bandit/B105)
        password = "placeholder"

    elif settings.managed_db_password:
        # Try to parse as JSON first, fall back to plain string
        _LOGGER.info("🔑 Retrieving database password from Secrets Manager")
        password_raw = settings.managed_db_password.get_secret_value()
        try:
            password_dict = json.loads(password_raw)
            password = password_dict.get("password", password_raw)
            _LOGGER.debug("🔑 Extracted password from JSON secret format")
        except (json.JSONDecodeError, TypeError, AttributeError):
            password = password_raw
            _LOGGER.debug("🔑 Using plain string password format")

    else:
        raise ValueError(
            "🔒 managed_db_password is required when db_use_iam_auth=False"
        )

    return f"postgresql://{username}:{password}@" f"{hostname}:{port}/{database}"


# Connection parameters from pydantic settings (validated on import)
SQLALCHEMY_DATABASE_URI = _build_database_uri()

auth_method = "IAM" if settings.db_use_iam_auth else "password"
_LOGGER.info(
    f"🔌 Initialising database engine for "
    f"{settings.load_database_url.get_secret_value()}:"
    f"{settings.db_port}/{settings.db_name} (auth: {auth_method})"
)


if settings.db_use_iam_auth:
    _LOGGER.info("Attempting to connect to database using IAM authentication")

# Engine with connection pooling to prevent connection leaks
# Lazy initialisation - created once per worker
connect_args = {
    "options": f"-c statement_timeout={settings.statement_timeout}",
    "sslmode": settings.db_sslmode,
}
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_timeout=30,  # Wait up to 30s for a connection before error
    isolation_level="READ COMMITTED",  # PostgreSQL default, explicit
    connect_args=connect_args,
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

    :return: SQLAlchemy engine instance
    :rtype: Engine
    """
    return _engine
