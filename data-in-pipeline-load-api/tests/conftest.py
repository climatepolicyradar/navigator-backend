import json
import os

import pytest
from pytest_alembic.config import Config as PytestAlembicConfig
from sqlmodel import Session, SQLModel, create_engine


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
    """Create engine using test-db service from docker-compose.

    Session-scoped fixture that creates tables once for all tests.
    Uses transaction rollback in session fixture for test isolation.
    """
    # These are provided by the test-db service from docker-compose.
    db_host = os.getenv("load_database_url")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")
    db_user = os.getenv("db_master_username")
    db_password = os.getenv("managed_db_password")

    # Parse password from JSON if needed
    if db_password is not None and db_password.startswith("{"):
        db_password = json.loads(db_password)["password"]

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(db_url)
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
