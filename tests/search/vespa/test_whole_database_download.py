from unittest.mock import patch

import pytest

from tests.search.vespa.setup_search_tests import _populate_db_families

ALL_DATA_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-all-data"


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
        patch("app.api.api_v1.routers.search.DOC_CACHE_BUCKET", "test_cdn_bucket"),
        patch("app.core.aws.S3Client.is_connected", return_value=True),
    ):
        data_client.follow_redirects = False
        download_response = data_client.get(ALL_DATA_DOWNLOAD_ENDPOINT, headers=headers)

    # Redirects to cdn
    assert download_response.status_code == 303
    assert download_response.headers["location"] == (
        "https://cdn.climatepolicyradar.org/"
        "navigator/dumps/CCLW-whole_data_dump-2024-03-22.zip"
    )

    assert mock_corpora_exist_in_db.assert_called
