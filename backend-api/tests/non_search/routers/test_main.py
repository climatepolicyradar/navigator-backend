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


@pytest.mark.parametrize("rds_online", [True, False])
def test_health_endpoint_is_shallow(test_client, monkeypatch, rds_online):
    """The shallow health endpoint returns 200 whatever the dependencies do."""
    mock_rds = Mock(return_value=rds_online)
    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds)

    response = test_client.get(HEALTH_ENDPOINT)
    assert response.status_code == status.HTTP_200_OK


def test_health_endpoint_does_not_check_dependencies(test_client, monkeypatch):
    """The shallow health endpoint must not touch Postgres."""
    mock_rds = Mock(return_value=True)
    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds)

    response = test_client.get(HEALTH_ENDPOINT)

    assert response.status_code == status.HTTP_200_OK
    mock_rds.assert_not_called()


@pytest.mark.parametrize(
    "rds_online,expected_status",
    [
        (True, status.HTTP_200_OK),
        (False, status.HTTP_503_SERVICE_UNAVAILABLE),
    ],
)
def test_deep_health_endpoint_returns_correct_status(
    test_client, monkeypatch, rds_online, expected_status
):
    """Test deep health endpoint returns correct status based on RDS health."""
    mock_rds = Mock(return_value=rds_online)
    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds)

    response = test_client.get(DEEP_HEALTH_ENDPOINT)
    assert response.status_code == expected_status
