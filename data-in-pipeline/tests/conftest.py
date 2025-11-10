import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_backend():
    """
    Use a temporary local Prefect backend for the entire test session.
    Faster than per-test DB creation while keeping tests isolated.
    """
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True, scope="function")
def disable_otel_logging(monkeypatch: pytest.MonkeyPatch):
    """
    Disable OTEL log exporting during tests.

    :param monkeypatch: Pytest monkeypatch helper.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Yields control back to the test.
    :rtype: typing.Iterator[None]
    """
    monkeypatch.setenv("OTEL_LOGS_EXPORTER", "none")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")

    monkeypatch.setenv("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED", "false")
    monkeypatch.setenv("PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY", "False")
    yield
