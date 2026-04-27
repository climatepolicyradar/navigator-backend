import os

import pytest
from pydantic import BaseModel
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


class DBSecrets(BaseModel):
    username: str
    password: str


@pytest.fixture(scope="function")
def engine():
    """Create engine using test-db service from docker-compose.

    Session-scoped fixture that creates tables once for all tests.
    Uses transaction rollback in session fixture for test isolation.
    """

    # These are provided by the test-db service from docker-compose.
    db_host = os.getenv("DB_URL")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_MASTER_PASSWORD")

    db_url = f"postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
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
