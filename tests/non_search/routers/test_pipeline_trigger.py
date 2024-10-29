START_INGEST_ENDPOINT = "/api/v1/admin/start-ingest"


def test_unauthorized_start_ingest(test_client):
    response = test_client.post(START_INGEST_ENDPOINT)
    assert response.status_code == 401


def test_start_ingest(test_client, superuser_token_headers, mocker):
    mock_start_import = mocker.patch(
        "app.api.api_v1.routers.pipeline_trigger._start_ingest"
    )

    response = test_client.post(
        START_INGEST_ENDPOINT,
        headers=superuser_token_headers,
    )
    assert response.status_code == 202
    response_json = response.json()
    assert response_json["detail"] is None  # Not yet implemented

    mock_start_import.assert_called_once()
