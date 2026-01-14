import os

import pytest
from opentelemetry import trace
from opentelemetry._logs import get_logger_provider
from prefect.testing.utilities import prefect_test_harness

# Set environment variables before any imports that might initialise telemetry
# or settings that read from environment variables
os.environ.setdefault("DISABLE_OTEL_LOGGING", "true")
os.environ.setdefault("OTEL_TRACES_EXPORTER", "none")
os.environ.setdefault("OTEL_LOGS_EXPORTER", "none")
os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")

# Set database environment variables before settings module is imported
os.environ.setdefault("DB_MASTER_USERNAME", "test_username")
os.environ.setdefault("MANAGED_DB_PASSWORD", "test_password")
os.environ.setdefault("AURORA_WRITER_ENDPOINT", "test-db-endpoint")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault("AWS_REGION", "eu-west-1")


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
