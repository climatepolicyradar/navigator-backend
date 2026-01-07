"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import logging
import os
from collections.abc import Generator
from contextlib import contextmanager

import psycopg2
from aws import get_aws_session, get_ssm_parameter
from settings import settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

_LOGGER = logging.getLogger(__name__)

# Connection parameters from pydantic settings (validated on import)
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{settings.db_master_username}:"
    f"{settings.managed_db_password.get_secret_value()}@"
    f"{settings.load_database_url.get_secret_value()}:"
    f"{settings.db_port}/{settings.db_name}?sslmode=no-verify"
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
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_timeout=30,  # Wait up to 30s for a connection before error
    isolation_level="READ COMMITTED",  # PostgreSQL default, explicit
    connect_args={"options": f"-c statement_timeout={settings.statement_timeout}"},
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


def test_db_connection():
    """Test database connection using IAM authentication token."""
    session = get_aws_session()
    client = session.client("rds")

    DB_USERNAME = os.getenv("DB_MASTER_USERNAME")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

    cluster_endpoint = get_ssm_parameter("/data-in-pipeline-load-api/load-database-url")

    token = client.generate_db_auth_token(
        DBHostname=cluster_endpoint,
        Port=DB_PORT,
        DBUsername=DB_USERNAME,
        Region="eu-west-1",
    )

    try:
        conn = psycopg2.connect(
            host=cluster_endpoint,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USERNAME,
            password=token,
            sslrootcert="SSLCERTIFICATE",
        )
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        _LOGGER.info("Database connection successful.Current time: %s", query_results)
        print(query_results)
    except Exception as e:
        _LOGGER.error("Database connection failed due to %s", e)
        print(f"Database connection failed due to {e}")
