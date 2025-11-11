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
