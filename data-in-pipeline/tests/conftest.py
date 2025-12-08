import os

import pytest
from opentelemetry import trace
from opentelemetry._logs import get_logger_provider
from prefect.testing.utilities import prefect_test_harness
from pytest_alembic.config import Config
from pytest_mock_resources import PostgresConfig, create_postgres_fixture
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from app.load.db.base import get_library_path
from app.load.db.run_migrations import run_migrations

# Set environment variables before any imports that might initialise telemetry
os.environ.setdefault("DISABLE_OTEL_LOGGING", "true")
os.environ.setdefault("OTEL_TRACES_EXPORTER", "none")
os.environ.setdefault("OTEL_LOGS_EXPORTER", "none")
os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")


@pytest.fixture
def alembic_config():
    """Override this fixture to configure the exact alembic context setup required."""
    root_dir = get_library_path()
    alembic_ini_path = os.path.join(root_dir, "alembic.ini")
    alembic_scripts_path = os.path.join(root_dir, "migrations")
    return Config(
        config_options={
            "file": alembic_ini_path,
            "script_location": alembic_scripts_path,
            "include_schemas": "db_client",
        }
    )


# Configuration of pytest mock postgres fixtures
@pytest.fixture(scope="session")
def pmr_postgres_config():
    return PostgresConfig(image="postgres:14")


# Engine Postgres fixture for alembic tests
alembic_engine = create_postgres_fixture()

# Engine Postgres fixture for our custom tests
test_engine_fixture = create_postgres_fixture()

template_engine_fixture = create_postgres_fixture(scope="session")


@pytest.fixture(scope="session")
def template_db_engine(template_engine_fixture):
    """Create a template database with migrations applied to be used by test_db."""
    db_url = template_engine_fixture.url

    # Drop template if it exists
    if database_exists(db_url):
        drop_database(db_url)

    # Create fresh template database
    create_database(db_url)

    # Apply migrations to template
    engine = None
    connection = None
    try:
        engine = create_engine(db_url)
        connection = engine.connect()
        run_migrations(engine)
    finally:
        if connection is not None:
            connection.close()
        if engine is not None:
            engine.dispose()

    yield engine

    # Cleanup
    if database_exists(db_url):
        drop_database(db_url)


@pytest.fixture(scope="function")
def test_db(template_db_engine, test_engine_fixture):
    """Create a fresh test database for each test by cloning the template database."""
    db_url = test_engine_fixture.url

    # Drop existing test database if it exists
    if database_exists(db_url):
        drop_database(db_url)

    # Create new test database using template database
    create_database(db_url, template=template_db_engine.url.database)

    test_session = None
    connection = None
    try:
        test_engine = create_engine(db_url)
        connection = test_engine.connect()

        test_session_maker = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=test_engine,
        )
        test_session = test_session_maker()

        yield test_session
    finally:
        if test_session is not None:
            test_session.close()

        if connection is not None:
            connection.close()

        # Drop the test database
        drop_database(db_url)


@pytest.fixture(autouse=True, scope="session")
def disable_telemetry():
    """Disable OpenTelemetry logging during tests to prevent export errors.

    :return: The function does not return anything.
    :rtype: None
    """
    # Set before any imports that might initialise telemetry
    os.environ["DISABLE_OTEL_LOGGING"] = "true"
    yield
    # Cleanup: ensure telemetry is shut down if it was initialised
    try:
        from app.bootstrap_telemetry import telemetry

        if telemetry:
            telemetry.shutdown()
    except Exception:
        pass

    # Force shutdown all global OpenTelemetry providers
    try:
        # Shutdown logger provider
        logger_provider = get_logger_provider()
        if logger_provider and hasattr(logger_provider, "shutdown"):
            logger_provider.shutdown()  # type: ignore[attr-defined]

        # Shutdown tracer provider
        tracer_provider = trace.get_tracer_provider()
        if tracer_provider and hasattr(tracer_provider, "shutdown"):
            tracer_provider.shutdown()  # type: ignore[attr-defined]
    except Exception:
        # Silently ignore shutdown errors in test cleanup
        pass


@pytest.fixture(autouse=True, scope="session")
def prefect_backend():
    """
    Use a temporary local Prefect backend for the entire test session.
    Faster than per-test DB creation while keeping tests isolated.
    """
    with prefect_test_harness():
        yield
