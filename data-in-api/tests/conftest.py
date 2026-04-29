import os

import pytest
import time_machine
from pytest_alembic.config import Config as PytestAlembicConfig
from sqlmodel import Session, SQLModel

# Freeze before any other imports — critical that this runs before
# anything pulls in app.db.session, which generates IAM tokens.
_freezer = time_machine.travel("2026-01-01 00:00:00", tick=False)
_freezer.start()

# The freeze must happen before other imports so it's active when those modules read time
# during their own initialisation.
from app.session import get_engine  # noqa: E402


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
