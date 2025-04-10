from unittest.mock import patch

import jwt
import pytest
from fastapi import status

from app.service import search
from tests.search.vespa.setup_search_tests import _populate_db_families

ALL_DATA_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-all-data"


@pytest.mark.search
@pytest.mark.parametrize(
    "side_effect",
    [
        jwt.exceptions.InvalidAudienceError,
        jwt.exceptions.ExpiredSignatureError,
        jwt.exceptions.InvalidTokenError,
    ],
)
def test_whole_database_download_fails_when_decoding_token_raises_PyJWTError(
    side_effect, data_client, data_db, valid_token, monkeypatch, test_vespa
):
    """
    GIVEN a request to download the whole database
    WHEN the decode() function call raises a PyJWTError
    THEN raise a 400 HTTP error
    """
    _populate_db_families(data_db)

    with patch(
        "app.service.custom_app.jwt.decode",
        side_effect=side_effect,
    ), patch(
        "app.api.api_v1.routers.search.PIPELINE_BUCKET", "test_pipeline_bucket"
    ), patch(
        "app.api.api_v1.routers.search.DOCUMENT_CACHE_BUCKET", "test_cdn_bucket"
    ), patch(
        "app.clients.aws.client.S3Client.is_connected", return_value=True
    ):
        response = data_client.get(
            ALL_DATA_DOWNLOAD_ENDPOINT,
            headers={"app-token": valid_token},
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert response.json()["detail"] == "Could not decode configuration token"


@pytest.mark.search
def test_whole_database_download_fails_when_corpus_ids_in_token_not_in_db(
    data_client, data_db, monkeypatch, test_vespa
):
    """
    GIVEN a list of corpora IDs decoded from an app config token
    WHEN one or more of those corpora IDs are not in our database
    THEN raise a 400 HTTP error
    """
    _populate_db_families(data_db)

    with patch(
        "app.api.api_v1.routers.search.AppTokenFactory.decode",
        return_value=True,
    ), patch(
        "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
        return_value=False,
    ), patch(
        "app.clients.aws.client.S3Client.is_connected", return_value=True
    ), patch(
        "app.api.api_v1.routers.search.PIPELINE_BUCKET", "test_pipeline_bucket"
    ), patch(
        "app.api.api_v1.routers.search.DOCUMENT_CACHE_BUCKET", "test_cdn_bucket"
    ):
        response = data_client.get(
            ALL_DATA_DOWNLOAD_ENDPOINT,
            headers={"app-token": "some_token"},
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert response.json()["detail"] == "Error verifying corpora IDs."


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_all_data_download(mock_corpora_exist_in_db, data_db, data_client, valid_token):
    _populate_db_families(data_db)

    headers = {"app-token": valid_token}

    with (
        patch("app.api.api_v1.routers.search.PIPELINE_BUCKET", "test_pipeline_bucket"),
        patch("app.api.api_v1.routers.search.DOCUMENT_CACHE_BUCKET", "test_cdn_bucket"),
        patch("app.clients.aws.client.S3Client.is_connected", return_value=True),
    ):
        data_client.follow_redirects = False
        download_response = data_client.get(ALL_DATA_DOWNLOAD_ENDPOINT, headers=headers)

    # Redirects to cdn
    assert download_response.status_code == status.HTTP_303_SEE_OTHER
    assert download_response.headers["location"] == (
        "https://cdn.climatepolicyradar.org/"
        "navigator/dumps/CCLW-whole_data_dump-2024-03-22.zip"
    )

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_all_data_download_fails_when_s3_upload_failed(
    mock_corpora_exist_in_db, data_db, data_client, valid_token
):
    _populate_db_families(data_db)

    headers = {"app-token": valid_token}

    with (
        patch("app.api.api_v1.routers.search.PIPELINE_BUCKET", "test_pipeline_bucket"),
        patch("app.api.api_v1.routers.search.DOCUMENT_CACHE_BUCKET", "test_cdn_bucket"),
        patch("app.clients.aws.client.S3Client.is_connected", return_value=True),
        patch(
            "app.api.api_v1.routers.search.get_s3_doc_url_from_cdn", return_value=None
        ),
    ):
        data_client.follow_redirects = False
        download_response = data_client.get(ALL_DATA_DOWNLOAD_ENDPOINT, headers=headers)

    # Redirects to cdn
    assert download_response.status_code == status.HTTP_404_NOT_FOUND
    assert mock_corpora_exist_in_db.assert_called
