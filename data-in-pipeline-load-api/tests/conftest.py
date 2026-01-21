import os

import pytest
from pytest_alembic.config import Config as PytestAlembicConfig
from sqlmodel import SQLModel, create_engine
from testcontainers.postgres import PostgresContainer


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
def postgres_container():
    with PostgresContainer("postgres:17") as postgres:
        yield postgres


@pytest.fixture
def engine(postgres_container):
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    yield engine
    SQLModel.metadata.drop_all(engine)


# # Declarative base object
# Base = declarative_base()
# SQLModel.metadata = Base.metadata


# SQLModel.metadata.create_all(engine)
