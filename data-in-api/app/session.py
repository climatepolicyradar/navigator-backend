"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.engine.interfaces import Dialect
from sqlmodel import Session, create_engine

from app.aws import get_aws_session
from app.settings import settings

_LOGGER = logging.getLogger(__name__)

db_url = settings.db_url.get_secret_value()
db_port = settings.db_port
aws_region = settings.aws_region
db_username = settings.db_username.get_secret_value()

SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{db_username}@{db_url}:{db_port}/{settings.db_name}"
    f"?sslmode={settings.db_sslmode}"
)

_LOGGER.info(
    f"🔌 Initialising database engine for "
    f"{settings.db_url.get_secret_value()}:"
    f"{settings.db_port}/{settings.db_name}"
)

# Engine with connection pooling to prevent connection leaks
# Lazy initialisation - created once per worker
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=600,  # Recycle every 10 min to avoid expired IAM auth tokens (15 min lifetime). https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html
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


def _generate_token() -> str:
    """Generates a short-lived IAM authentication token for Aurora.

    Calls the AWS RDS API to obtain a signed token that can be used as the
    password when connecting to Aurora. The token is valid for 15
    minutes and is scoped to the configured user, host, port, and region.

    Intended for use inside the ``do_connect`` event listener so each new
    DB connection authenticates with a fresh, non-expired token.

    :return: A signed IAM auth token suitable for use as a Postgres password.
    :rtype: str
    """
    aws_session = get_aws_session()
    rds_client = aws_session.client("rds")
    return rds_client.generate_db_auth_token(
        DBHostname=db_url,
        Port=db_port,
        DBUsername=db_username,
        Region=aws_region,
    )


# @see:  https://docs.sqlalchemy.org/en/20/core/engines.html#generating-dynamic-authentication-tokens
@event.listens_for(_engine, "do_connect")
def provide_token(
    dialect: Dialect,
    conn_rec: Any,
    cargs: tuple[Any, ...],
    cparams: dict[str, Any],
):
    """
    This function generates an IAM token to be used to connect to Aurora DB.
    It is triggered on every connection to the database based on an emitted "do_connect" event.
    This is the SQAlchemy recommended approach:
    https://docs.sqlalchemy.org/en/20/core/engines.html#generating-dynamic-authentication-tokens

    :param dialect: The SQLAlchemy dialect handling the connection (psycopg2 here).
    :param conn_rec: The pool's bookkeeping record for the connection being created.
    :param cargs: Positional args that will be passed to the DBAPI ``connect()`` call.
    :param cparams: Keyword args for the DBAPI ``connect()`` call; mutated in place
        to inject the IAM token as the password.
    :return: None. The function mutates ``cparams`` in place; SQLAlchemy then
        uses the updated params to open the connection.
    """
    _LOGGER.info("Generating fresh IAM auth token for new connection")
    cparams["password"] = _generate_token()


def get_engine() -> Engine:
    """Get the database engine instance.
    :return: SQLModel engine instance
    :rtype: Engine
    """
    return _engine
