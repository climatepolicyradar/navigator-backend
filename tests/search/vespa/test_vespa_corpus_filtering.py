from typing import Any
from unittest.mock import patch

import jwt
import pytest
from fastapi import status

from app.api.api_v1.routers import search
from tests.search.vespa.setup_search_tests import (
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.search
@pytest.mark.parametrize(
    ("corpus_import_id", "corpus_type_name", "expected_hits"),
    [
        ("CCLW.corpus.1.0", "UNFCCC Submissions", 1),
        ("CCLW.corpus.1.0", None, 1),
        (None, "UNFCCC Submissions", 1),
        (None, None, 5),
        (None, "Laws and Policies", 4),
        ("CCLW.corpus.2.0", None, 4),
    ],
)
def test_corpus_filtering(
    test_vespa,
    monkeypatch,
    data_client,
    data_db,
    corpus_import_id: str,
    corpus_type_name: str,
    expected_hits: int,
    valid_token,
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params: dict[str, Any] = {"query_string": "and"}
    if corpus_import_id:
        params["corpus_import_ids"] = [corpus_import_id]
    if corpus_type_name:
        params["corpus_type_names"] = [corpus_type_name]

    with patch(
        "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
        return_value=True,
    ):
        response = _make_search_request(
            data_client,
            token=valid_token,
            params=params,
        )

    assert len(response["families"]) > 0
    assert len(response["families"]) == expected_hits
    for family in response["families"]:
        if corpus_import_id:
            assert family["corpus_import_id"] == corpus_import_id
        if corpus_type_name:
            assert family["corpus_type_name"] == corpus_type_name


@pytest.mark.search
def test_search_with_corpus_ids_in_token_not_in_db(
    data_client, data_db, monkeypatch, test_vespa
):
    """
    GIVEN a list of corpora IDs decoded from an app config token
    WHEN one or more of those corpora IDs are not in our database
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    with patch(
        "app.service.custom_app.AppTokenFactory.decode", return_value=True
    ), patch(
        "app.service.custom_app.AppTokenFactory.verify_corpora_in_db",
        return_value=False,
    ):
        response = _make_search_request(
            data_client,
            "test_token",
            params={"query_string": ""},
            expected_status_code=status.HTTP_400_BAD_REQUEST,
        )

        assert response["detail"] == "Error verifying corpora IDs."


@pytest.mark.search
@pytest.mark.parametrize(
    "side_effect",
    [
        jwt.exceptions.InvalidAudienceError,
        jwt.exceptions.ExpiredSignatureError,
        jwt.exceptions.InvalidTokenError,
    ],
)
def test_search_decoding_token_raises_PyJWTError(
    side_effect,
    data_client,
    data_db,
    valid_token,
    monkeypatch,
    test_vespa,
):
    """
    GIVEN a request to the search endpoint
    WHEN the decode() function call raises a PyJWTError
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    with patch("jwt.decode", side_effect=side_effect):
        response = _make_search_request(
            data_client,
            valid_token,
            params={"query_string": ""},
            expected_status_code=status.HTTP_400_BAD_REQUEST,
        )

        assert response["detail"] == "Could not decode configuration token"


@pytest.mark.skip("Re-implement this as part of PDCT-1509")
@pytest.mark.search
def test_search_decoding_token_with_none_origin_passed_to_audience(
    data_client,
    data_db,
    valid_token,
    monkeypatch,
    test_vespa,
):
    """
    GIVEN a request to the search endpoint
    WHEN the decode_config_token() function is passed a None origin
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    response = _make_search_request(
        data_client,
        valid_token,
        params={"query_string": ""},
        origin=None,
        expected_status_code=status.HTTP_400_BAD_REQUEST,
    )

    assert response["detail"] == "Could not decode configuration token"


@pytest.mark.search
def test_search_with_invalid_corpus_id_in_search_request_params(
    data_client, data_db, valid_token, monkeypatch, test_vespa
):
    """
    GIVEN a list of corpora IDs from the search request body params
    WHEN those corpora IDs are not a subset of the app token corpora IDs
    THEN raise a 403 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    with patch(
        "app.service.custom_app.AppTokenFactory.validate_corpora_ids",
        return_value=False,
    ):
        response = _make_search_request(
            data_client,
            valid_token,
            params={"query_string": ""},
            expected_status_code=status.HTTP_403_FORBIDDEN,
        )

        assert response["detail"] == "Error validating corpora IDs."
