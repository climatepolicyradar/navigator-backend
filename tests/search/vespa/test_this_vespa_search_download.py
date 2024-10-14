import csv
from io import StringIO
from typing import Any, Mapping
from unittest.mock import patch

import jwt
import pytest
from fastapi import status

from app.api.api_v1.routers import search
from tests.search.vespa.setup_search_tests import (
    _make_search_request,
    _populate_db_families,
)

SEARCH_ENDPOINT = "/api/v1/searches"
CSV_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-csv"

_CSV_SEARCH_RESPONSE_COLUMNS = [
    "Collection Name",
    "Collection Summary",
    "Family Name",
    "Family Summary",
    "Family URL",
    "Family Publication Date",
    "Geographies",
    "Document Title",
    "Document URL",
    "Document Content URL",
    "Document Type",
    "Document Content Matches Search Phrase",
    "Category",
    "Languages",
    "Source",
]


def _make_download_request(
    client,
    token,
    params: Mapping[str, Any],
    expected_status_code: int = status.HTTP_200_OK,
):
    headers = {"app-token": token}

    response = client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json=params,
        headers=headers,
    )
    assert response is not None
    assert response.status_code == expected_status_code, response.text
    return response


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("exact_match", [True, False])
@pytest.mark.parametrize("query_string", ["", "local"])
def test_csv_content(
    mock_corpora_exist_in_db,
    exact_match,
    query_string,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):
    """Make sure that downloaded CSV content matches a given search"""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)
    params = {
        "exact_match": exact_match,
        "query_string": query_string,
    }
    body = _make_search_request(data_client, valid_token, params)
    families = body["families"]
    assert len(families) > 0

    csv_response = _make_download_request(
        data_client,
        valid_token,
        params={
            "exact_match": exact_match,
            "query_string": query_string,
        },
    )

    csv_content = csv.DictReader(StringIO(csv_response.text))
    for row, family in zip(csv_content, families):
        assert all(col in row.keys() for col in _CSV_SEARCH_RESPONSE_COLUMNS)

        assert row["Family Name"] == family["family_name"]
        assert row["Family Summary"] == family["family_description"]
        assert row["Family Publication Date"] == family["family_date"]
        assert row["Category"] == family["family_category"]

        assert isinstance(row["Geographies"], str)
        if len(family["family_geographies"]) > 1:
            assert (
                row["Geographies"].count(";") == len(family["family_geographies"]) - 1
            )

        # TODO: Add collections to test db setup to provide document level coverage

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
@pytest.mark.parametrize("limit", [100, 250, 500])
def test_csv_download_search_variable_limit(
    mock_corpora_exist_in_db,
    label,
    query,
    limit,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    mocker,
    valid_token,
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")

    params = {
        "query_string": query,
        "limit": limit,
        "page_size": 100,
        "offset": 0,
    }

    _make_download_request(data_client, valid_token, params=params)

    actual_params = query_spy.call_args.kwargs["parameters"].model_dump()

    # Check requested params are not changed
    for key, value in params.items():
        assert actual_params[key] == value

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_csv_download__ignore_extra_fields(
    mock_corpora_exist_in_db, test_vespa, data_db, monkeypatch, data_client, valid_token
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": "winter",
    }

    # Ensure extra, unspecified fields don't cause an error
    fields = []
    with patch("app.core.search._CSV_SEARCH_RESPONSE_COLUMNS", fields):
        _make_download_request(data_client, valid_token, params=params)

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@pytest.mark.parametrize(
    "side_effect",
    [
        jwt.exceptions.InvalidAudienceError,
        jwt.exceptions.ExpiredSignatureError,
        jwt.exceptions.InvalidTokenError,
    ],
)
def test_csv_download_fails_when_decoding_token_raises_PyJWTError(
    side_effect, data_client, data_db, valid_token, monkeypatch, test_vespa
):
    """
    GIVEN a request to download the whole database
    WHEN the decode() function call raises a PyJWTError
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": "winter",
    }

    with patch("app.core.custom_app.jwt.decode", side_effect=side_effect):
        response = _make_download_request(
            data_client,
            valid_token,
            params=params,
            expected_status_code=status.HTTP_400_BAD_REQUEST,
        )
        assert response.json()["detail"] == "Could not decode configuration token"


@pytest.mark.search
def test_csv_download_fails_when_corpus_ids_in_token_not_in_db(
    data_client, data_db, monkeypatch, test_vespa
):
    """
    GIVEN a list of corpora IDs decoded from an app config token
    WHEN one or more of those corpora IDs are not in our database
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": "winter",
    }

    with patch(
        "app.api.api_v1.routers.search.AppTokenFactory.decode",
        return_value=True,
    ), patch(
        "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
        return_value=False,
    ):
        response = _make_download_request(
            data_client,
            "some_token",
            params=params,
            expected_status_code=status.HTTP_400_BAD_REQUEST,
        )
        assert response.json()["detail"] == "Error verifying corpora IDs."
