import os

import pytest
from pytest_alembic.config import Config as PytestAlembicConfig
from sqlmodel import Session, SQLModel

from app.session import get_engine


@pytest.fixture
def alembic_config():
    """Override this fixture to configure the exact alembic context setup required."""
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    alembic_dir = os.path.join(root_dir, "app", "alembic")

    return PytestAlembicConfig(
        config_options={
            "file": os.path.join(alembic_dir, "alembic.ini"),
            "script_location": os.path.join(alembic_dir, "migrations"),
            "include_schemas": True,
        }
    )


@pytest.fixture(scope="session")
def engine():
    """Reuse the production engine, but manage schema lifecycle for tests."""
    engine = get_engine()
    SQLModel.metadata.create_all(engine)
    yield engine
    SQLModel.metadata.drop_all(engine)


@pytest.fixture
def alembic_engine(engine):
    """Create an engine specifically for alembic testing.
    Resets the whole schema so that the alembic revision state is clean for each test.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
    return engine


@pytest.fixture(scope="function")
def blank_engine():
    """Create engine with no tables for migration testing.

    Drops all tables before yielding to ensure migrations can run
    cleanly. Used specifically for testing migration endpoints.
    """
    engine = get_engine()

    # Drop all tables to ensure clean state for migrations
    SQLModel.metadata.drop_all(engine)

    yield engine

    # Clean up after test
    SQLModel.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def session(engine):
    """Create a database session with transaction rollback.

    Each test gets a fresh session. All changes are rolled back
    after the test, providing isolation without dropping tables.
    Faster than drop_all/create_all pattern.

    :yields: Database session
    :rtype: Generator[Session, None, None]
    """
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()
