"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import logging
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any

import psycopg2
from aws import generate_rds_iam_token
from settings import settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

_LOGGER = logging.getLogger(__name__)

# IAM token cache (token, generation_timestamp)
# Tokens expire after 15 minutes, refresh at 14 minutes to be safe
_iam_token_cache: tuple[str, float] | None = None
_IAM_TOKEN_LIFETIME_SECONDS = 15 * 60  # RDS IAM tokens expire after 15 min
_IAM_TOKEN_REFRESH_SECONDS = 14 * 60  # Refresh 1 minute before expiry


def _get_iam_token() -> str:
    """Get a cached IAM token or generate a new one.

    Tokens are cached and refreshed when they approach expiry (14 minutes).

    :return: IAM authentication token
    :rtype: str
    """
    # trunk-ignore(ruff/PLW0603)
    global _iam_token_cache
    current_time = time.time()

    if (
        _iam_token_cache is None
        or current_time >= _iam_token_cache[1] + _IAM_TOKEN_REFRESH_SECONDS
    ):
        hostname = settings.load_database_url.get_secret_value()
        port = int(settings.db_port)
        username = settings.db_master_username
        _LOGGER.info(f"🔐 Generating fresh IAM auth token for {username}@{hostname}")
        token = generate_rds_iam_token(hostname, port, username)
        # Store token with generation timestamp
        _iam_token_cache = (token, current_time)
        return token

    return _iam_token_cache[0]


def _create_connection_with_iam() -> psycopg2.extensions.connection:
    """Create a database connection using IAM authentication.

    This function is used as a custom connection creator for SQLAlchemy
    when IAM authentication is enabled. It generates a fresh IAM token
    for each connection attempt.

    :return: psycopg2 connection object
    :rtype: psycopg2.extensions.connection
    """
    token = _get_iam_token()
    hostname = settings.load_database_url.get_secret_value()
    port = int(settings.db_port)
    username = settings.db_master_username
    database = settings.db_name

    conn = psycopg2.connect(
        host=hostname,
        port=port,
        user=username,
        password=token,
        database=database,
        sslmode=settings.db_sslmode,
        connect_timeout=10,
    )
    return conn


def _build_database_uri() -> str:
    """Build the database URI based on authentication method.

    :return: SQLAlchemy database URI
    :rtype: str
    """
    hostname = settings.load_database_url.get_secret_value()
    port = settings.db_port
    username = settings.db_master_username
    database = settings.db_name
    sslmode = settings.db_sslmode

    if settings.db_use_iam_auth:
        # For IAM auth, we use a custom creator, so password is placeholder
        # The actual token is generated in _create_connection_with_iam()
        # trunk-ignore(bandit/B105)
        password = "placeholder"
    else:
        if settings.managed_db_password is None:
            raise ValueError(
                "🔒 managed_db_password is required when db_use_iam_auth=False"
            )
        password = settings.managed_db_password.get_secret_value()

    return (
        f"postgresql://{username}:{password}@"
        f"{hostname}:{port}/{database}?sslmode={sslmode}"
    )


# Connection parameters from pydantic settings (validated on import)
SQLALCHEMY_DATABASE_URI = _build_database_uri()

auth_method = "IAM" if settings.db_use_iam_auth else "password"
_LOGGER.info(
    f"🔌 Initialising database engine for "
    f"{settings.load_database_url.get_secret_value()}:"
    f"{settings.db_port}/{settings.db_name} (auth: {auth_method})"
)

# Configure connection creator for IAM auth
connect_args: dict[str, Any] = {
    "options": f"-c statement_timeout={settings.statement_timeout}"
}
creator: Callable[[], psycopg2.extensions.connection] | None = None

if settings.db_use_iam_auth:
    creator = _create_connection_with_iam
    _LOGGER.info("✅ Using IAM authentication for database connections")

# Engine with connection pooling to prevent connection leaks
# Lazy initialisation - created once per worker
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    creator=creator,  # Custom creator for IAM auth
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
