import os
import typing

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config):
    """Prepare telemetry and cache configuration before tests run.

    :param config: Pytest configuration instance.
    :type config: pytest.Config
    :return: None.
    :rtype: None
    """
    os.environ.setdefault("OTEL_SDK_DISABLED", "true")
    os.environ.setdefault("OTEL_LOGS_EXPORTER", "none")
    os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")
    os.environ.setdefault("OTEL_TRACES_EXPORTER", "none")
    os.environ.setdefault(
        "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED",
        "false",
    )
    os.environ.setdefault("PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY", "False")
    os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")
    os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_TASK", "ignore")
    os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_WORKER", "ignore")
    os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
    yield


@pytest.fixture(autouse=True, scope="session")
def prefect_backend() -> typing.Iterator[None]:
    """Run tests against an isolated Prefect backend session.

    :return: Iterator yielding control to tests.
    :rtype: typing.Iterator[None]
    """
    with prefect_test_harness():
        yield
