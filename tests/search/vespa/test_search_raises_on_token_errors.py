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
def test_search_with_invalid_corpus_id_in_token(
    data_client, data_db, valid_token, monkeypatch, test_vespa
):
    """
    GIVEN a list of corpora IDs decoded from an app config token
    WHEN one or more of those corpora IDs are not in our database
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    with patch(
        "app.api.api_v1.routers.search.verify_any_corpora_ids_in_db", return_value=False
    ):
        response = _make_search_request(
            data_client,
            valid_token,
            params={"query_string": ""},
            expected_status_code=status.HTTP_400_BAD_REQUEST,
        )

        assert response["detail"] == "Error verifying corpora IDs."


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
        "app.api.api_v1.routers.search.validate_corpora_ids", return_value=True
    ), patch(
        "app.api.api_v1.routers.search.verify_any_corpora_ids_in_db", return_value=False
    ):
        response = _make_search_request(
            data_client,
            valid_token,
            params={"query_string": ""},
            expected_status_code=status.HTTP_403_FORBIDDEN,
        )

        assert response["detail"] == "Error validating corpora IDs."


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
    WHEN the decode_config_token() function call raises a PyJWTError
    THEN raise a 400 HTTP error
    """
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    with patch(
        "app.api.api_v1.routers.search.decode_config_token", side_effect=side_effect
    ):
        response = _make_search_request(
            data_client,
            valid_token,
            params={"query_string": ""},
            expected_status_code=status.HTTP_400_BAD_REQUEST,
        )

        assert response["detail"] == "Could not decode configuration token"
