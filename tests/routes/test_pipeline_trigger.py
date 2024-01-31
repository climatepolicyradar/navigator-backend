from app.data_migrations import (
    populate_document_role,
    populate_document_type,
    populate_document_variant,
    populate_geography,
    populate_taxonomy,
)


START_INGEST_ENDPOINT = "/api/v1/admin/start-ingest"


def test_unauthorized_start_ingest(client):
    response = client.post(START_INGEST_ENDPOINT)
    assert response.status_code == 401


def test_start_ingest(
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch(
        "app.api.api_v1.routers.pipeline_trigger._start_ingest"
    )

    populate_geography(test_db)
    populate_taxonomy(test_db)
    populate_document_type(test_db)
    populate_document_role(test_db)
    populate_document_variant(test_db)
    test_db.commit()

    response = client.post(
        START_INGEST_ENDPOINT,
        headers=superuser_token_headers,
    )
    assert response.status_code == 202
    response_json = response.json()
    assert response_json["detail"] is None  # Not yet implemented

    mock_start_import.assert_called_once()
