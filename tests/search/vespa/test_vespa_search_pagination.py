from typing import Mapping

import pytest

from app.api.api_v1.routers import search
from tests.search.vespa.setup_search_tests import (
    VESPA_FIXTURE_COUNT,
    _populate_db_families,
)

SEARCH_ENDPOINT = "/api/v1/searches"


def _make_search_request(client, params: Mapping[str, str]):
    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.search
def test_simple_pagination_families(test_vespa, data_client, data_db, monkeypatch):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    PAGE_SIZE = 2

    # Query one
    params = {
        "query_string": "and",
        "page_size": PAGE_SIZE,
        "offset": 0,
    }
    body_one = _make_search_request(data_client, params)
    assert body_one["hits"] == VESPA_FIXTURE_COUNT
    assert len(body_one["families"]) == PAGE_SIZE
    assert (
        body_one["families"][0]["family_slug"]
        == "agriculture-sector-plan-2015-2019_7999"
    )
    assert (
        body_one["families"][1]["family_slug"]
        == "national-environment-policy-of-guinea_f0df"
    )

    # Query two
    params = {
        "query_string": "and",
        "page_size": PAGE_SIZE,
        "offset": 2,
    }
    body_two = _make_search_request(data_client, params)
    assert body_two["hits"] == VESPA_FIXTURE_COUNT
    assert len(body_two["families"]) == PAGE_SIZE
    assert (
        body_two["families"][0]["family_slug"]
        == "national-energy-policy-and-energy-action-plan_9262"
    )
    assert (
        body_two["families"][1]["family_slug"]
        == "submission-to-the-unfccc-ahead-of-the-first-technical-dialogue_e760"
    )


@pytest.mark.search
def test_continuation_token__families(test_vespa, data_db, monkeypatch, data_client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    _populate_db_families(data_db)

    params = {"query_string": "the", "limit": 2, "page_size": 1}
    response = _make_search_request(data_client, params)
    continuation = response["continuation_token"]
    first_family_ids = [f["family_slug"] for f in response["families"]]

    # Confirm we have grabbed a subset of all results
    assert len(response["families"]) < response["total_family_hits"]

    # Get next results set
    params = {"query_string": "the", "continuation_tokens": [continuation]}
    response = _make_search_request(data_client, params)
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
    response = _make_search_request(data_client, params)
    prev_family_ids = [f["family_slug"] for f in response["families"]]

    assert sorted(first_family_ids) == sorted(prev_family_ids)


@pytest.mark.search
def test_continuation_token__passages(test_vespa, data_db, monkeypatch, data_client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    _populate_db_families(data_db)

    # Get second set of families
    params = {
        "query_string": "the",
        "document_ids": ["CCLW.executive.10246.4861", "CCLW.executive.4934.1571"],
        "limit": 1,
        "page_size": 1,
    }
    first_family = _make_search_request(data_client, params)
    params["continuation_tokens"] = [first_family["continuation_token"]]
    second_family_first_passages = _make_search_request(data_client, params)
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
    second_family_second_passages = _make_search_request(data_client, params)
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
    response = _make_search_request(data_client, params)
    second_family_prev_passages_ids = [
        h["text_block_id"]
        for h in response["families"][0]["family_documents"][0][
            "document_passage_matches"
        ]
    ]

    assert sorted(second_family_second_passages_ids) != sorted(
        second_family_prev_passages_ids
    )
