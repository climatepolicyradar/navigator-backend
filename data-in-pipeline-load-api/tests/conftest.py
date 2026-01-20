import pytest
from sqlmodel import SQLModel, create_engine
from testcontainers.postgres import PostgresContainer


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
