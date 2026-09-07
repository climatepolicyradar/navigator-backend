import os
from unittest.mock import Mock

import pytest
from fastapi import status

HEALTH_ENDPOINT = "/health"
DEEP_HEALTH_ENDPOINT = "/health/deep"


def test_read_main(test_client):
    response = test_client.get("/api/v1")
    assert response.status_code == 200
    assert response.json() == {"message": "CPR API v1"}


def test_read_docs(test_client):
    docs_enabled_env = os.getenv("ENABLE_API_DOCS", "")
    docs_response = test_client.get("/api/docs")
    openapi_response = test_client.get("/api")

    if docs_enabled_env.lower() == "true":
        assert docs_response.status_code == 200
        assert openapi_response.status_code == 200
    else:
        assert docs_response.status_code == 404
        assert openapi_response.status_code == 404


@pytest.mark.parametrize(
    "rds_online,vespa_online",
    [
        (True, True),
        (False, True),
        (True, False),
        (False, False),
    ],
)
def test_health_endpoint_is_shallow(test_client, monkeypatch, rds_online, vespa_online):
    """The shallow health endpoint returns 200 whatever the dependencies do."""
    # Mock the database health check functions
    mock_rds = Mock(return_value=rds_online)
    mock_vespa = Mock(return_value=vespa_online)

    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds)
    monkeypatch.setattr("app.service.health.is_vespa_online", mock_vespa)

    response = test_client.get(HEALTH_ENDPOINT)
    assert response.status_code == status.HTTP_200_OK


def test_health_endpoint_does_not_check_dependencies(test_client, monkeypatch):
    """The shallow health endpoint must not touch Postgres or Vespa."""
    mock_rds = Mock(return_value=True)
    mock_vespa = Mock(return_value=True)

    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds)
    monkeypatch.setattr("app.service.health.is_vespa_online", mock_vespa)

    response = test_client.get(HEALTH_ENDPOINT)

    assert response.status_code == status.HTTP_200_OK
    mock_rds.assert_not_called()
    mock_vespa.assert_not_called()


@pytest.mark.parametrize(
    "rds_online,vespa_online,expected_status",
    [
        (True, True, status.HTTP_200_OK),
        (False, True, status.HTTP_503_SERVICE_UNAVAILABLE),
        (True, False, status.HTTP_503_SERVICE_UNAVAILABLE),
        (False, False, status.HTTP_503_SERVICE_UNAVAILABLE),
    ],
)
def test_deep_health_endpoint_returns_correct_status(
    test_client, monkeypatch, rds_online, vespa_online, expected_status
):
    """Test deep health endpoint returns correct status based on service health."""
    # Mock the database health check functions
    mock_rds = Mock(return_value=rds_online)
    mock_vespa = Mock(return_value=vespa_online)

    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds)
    monkeypatch.setattr("app.service.health.is_vespa_online", mock_vespa)

    response = test_client.get(DEEP_HEALTH_ENDPOINT)
    assert response.status_code == expected_status
