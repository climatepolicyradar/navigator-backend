"""
Code for DB session management.

Stray connections can be caused by services not properly closing
sessions. Use get_db_context() for all database operations.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager

from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.run_migrations.aws import get_secret

_LOGGER = logging.getLogger(__name__)


class LoadDBCredentials(BaseModel):
    username: str
    password: str
    url: str
    port: str
    db_name: str
    sslmode: str


def get_load_db_credentials():
    """Get the load database credentials from the AWS Secrets Manager."""
    return LoadDBCredentials(
        username=get_secret("load-db-password", parse_json=True).get("username"),
        password=get_secret("load-db-password", parse_json=True).get("password"),
        url="cluster-write",
        port="5432",
        db_name="data-in-pipeline-load-api",
        sslmode="require",
    )


def create_database_uri(credentials: LoadDBCredentials) -> str:
    """Create the database URI for the load database."""
    return f"postgresql://{credentials.username}:{credentials.password}@{credentials.url}:{credentials.port}/{credentials.db_name}?sslmode={credentials.sslmode}"


def connect_to_db():
    """Create the database engine for the load database.


    Returns the database engine.
    """
    credentials = get_load_db_credentials()
    SQLALCHEMY_DATABASE_URI = create_database_uri(credentials)

    _LOGGER.info(
        f"🔌 Initialising database engine for "
        f"{credentials.url}:{credentials.port}/{credentials.db_name}"
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
    return _engine


def create_session_factory(engine: Engine) -> sessionmaker:
    """Create the session factory for the load database."""
    # Session factory, exported callable for tests
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """Context manager for database session lifecycle.

    Use this for all database operations. Ensures proper cleanup
    even if operations are retried or fail. Automatically commits
    on success and rolls back on error.

    :yields: Database session
    :rtype: Generator[Session, None, None]
    """
    engine = connect_to_db()
    db = create_session_factory(engine)
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


def run_migrations():
    print("Inside run_migrations function")
    # # For now we just want to connect to the database and check that we have access.
    # with get_db_context() as db:
    #     is_connected = db.execute(text("SELECT 1")) is not None
    #     print(is_connected)
