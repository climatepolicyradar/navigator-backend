from unittest.mock import patch

import pytest

from app.service import search
from tests.search.vespa.setup_search_tests import (
    VESPA_FIXTURE_COUNT,
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_simple_pagination_families(
    mock_corpora_exist_in_db, test_vespa, data_client, data_db, monkeypatch, valid_token
):
    _populate_db_families(data_db)

    PAGE_SIZE = 2

    # Query one
    params = {
        "query_string": "and",
        "page_size": PAGE_SIZE,
        "offset": 0,
    }
    body_one = _make_search_request(data_client, valid_token, params)
    assert body_one["hits"] == VESPA_FIXTURE_COUNT
    assert len(body_one["families"]) == PAGE_SIZE
    query_one_family_slugs = set([f["family_slug"] for f in body_one["families"]])

    # Query two
    params = {
        "query_string": "and",
        "page_size": PAGE_SIZE,
        "offset": 2,
    }
    body_two = _make_search_request(data_client, valid_token, params)
    assert body_two["hits"] == VESPA_FIXTURE_COUNT
    assert len(body_two["families"]) == PAGE_SIZE
    query_two_family_slugs = set([f["family_slug"] for f in body_two["families"]])

    assert query_one_family_slugs.isdisjoint(query_two_family_slugs)

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_continuation_token__families(
    mock_corpora_exist_in_db, test_vespa, data_db, monkeypatch, data_client, valid_token
):

    _populate_db_families(data_db)

    params = {"query_string": "the", "limit": 2, "page_size": 1}
    response = _make_search_request(data_client, valid_token, params)
    continuation = response["continuation_token"]
    first_family_ids = [f["family_slug"] for f in response["families"]]

    # Confirm we have grabbed a subset of all results
    assert len(response["families"]) < response["total_family_hits"]

    # Get next results set
    params = {"query_string": "the", "continuation_tokens": [continuation]}
    response = _make_search_request(data_client, valid_token, params)
    second_family_ids = [f["family_slug"] for f in response["families"]]

    # Confirm we actually got different results
    assert sorted(first_family_ids) != sorted(second_family_ids)

    # Go back to prev and confirm its what we had initially
    params = {
        "query_string": "the",
        "continuation_tokens": [response["prev_continuation_token"]],
        "limit": 2,
        "page_size": 1,
    }
    response = _make_search_request(data_client, valid_token, params)
    prev_family_ids = [f["family_slug"] for f in response["families"]]

    assert sorted(first_family_ids) == sorted(prev_family_ids)

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_continuation_token__passages(
    mock_corpora_exist_in_db, test_vespa, data_db, monkeypatch, data_client, valid_token
):

    _populate_db_families(data_db)

    # Get second set of families
    params = {
        "query_string": "climate",
        "document_ids": ["CCLW.executive.10246.4861", "CCLW.executive.4934.1571"],
        "limit": 1,
        "page_size": 1,
    }
    first_family = _make_search_request(data_client, valid_token, params)
    params["continuation_tokens"] = [first_family["continuation_token"]]
    second_family_first_passages = _make_search_request(
        data_client, valid_token, params
    )
    second_family_first_passages_ids = [
        h["text_block_id"]
        for h in second_family_first_passages["families"][0]["family_documents"][0][
            "document_passage_matches"
        ]
    ]

    # Get next set of passages
    this_family_continuation = second_family_first_passages["this_continuation_token"]
    next_passages_continuation = second_family_first_passages["families"][0][
        "continuation_token"
    ]
    params["continuation_tokens"] = [
        this_family_continuation,
        next_passages_continuation,
    ]
    second_family_second_passages = _make_search_request(
        data_client, valid_token, params
    )
    second_family_second_passages_ids = [
        h["text_block_id"]
        for h in second_family_second_passages["families"][0]["family_documents"][0][
            "document_passage_matches"
        ]
    ]

    # Confirm we actually got different results
    assert sorted(second_family_first_passages_ids) != sorted(
        second_family_second_passages_ids
    )

    # Go to previous set and confirm its the same
    prev_passages_continuation = second_family_second_passages["families"][0][
        "prev_continuation_token"
    ]

    params["continuation_tokens"] = [
        this_family_continuation,
        prev_passages_continuation,
    ]
    response = _make_search_request(data_client, valid_token, params)
    second_family_prev_passages_ids = [
        h["text_block_id"]
        for h in response["families"][0]["family_documents"][0][
            "document_passage_matches"
        ]
    ]

    assert sorted(second_family_second_passages_ids) != sorted(
        second_family_prev_passages_ids
    )

    assert mock_corpora_exist_in_db.assert_called
