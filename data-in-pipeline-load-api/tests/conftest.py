import pytest
from pytest_mock_resources import PostgresConfig


# Configuration of pytest mock postgres fixtures
@pytest.fixture(scope="session")
def pmr_postgres_config():
    return PostgresConfig(image="postgres:17")
