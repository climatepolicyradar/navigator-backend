import time
from typing import Mapping

import pytest
from db_client.models.dfce import Geography
from db_client.models.dfce.family import FamilyDocument
from sqlalchemy import update

from app.api.api_v1.routers import search
from app.core.lookups import get_country_slug_from_country_code
from tests.search.vespa.setup_search_tests import (
    VESPA_FIXTURE_COUNT,
    _create_document,
    _create_family,
    _create_family_event,
    _create_family_metadata,
    _populate_db_families,
)

SEARCH_ENDPOINT = "/api/v1/searches"


def _make_search_request(client, params: Mapping[str, str]):
    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.search
def test_empty_search_term_performs_browse(
    test_vespa, data_client, data_db, mocker, monkeypatch
):
    """Make sure that empty search term returns results in browse mode."""
    _populate_db_families(data_db)
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")
    body = _make_search_request(data_client, {"query_string": ""})

    assert body["hits"] > 0
    assert len(body["families"]) > 0

    # Should automatically use vespa `all_results` parameter for browse requests
    assert query_spy.call_args.kwargs["parameters"].all_results
    query_spy.assert_called_once()


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
@pytest.mark.parametrize("exact_match", [True, False])
def test_search_body_valid(exact_match, test_vespa, data_client, data_db, monkeypatch):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    body = _make_search_request(
        data_client,
        params={
            "query_string": "and",
            "exact_match": exact_match,
        },
    )

    fields = sorted(body.keys())
    assert fields == [
        "continuation_token",
        "families",
        "hits",
        "prev_continuation_token",
        "query_time_ms",
        "this_continuation_token",
        "total_family_hits",
        "total_time_ms",
    ]
    assert isinstance(body["families"], list)


@pytest.mark.search
def test_no_doc_if_in_postgres_but_not_vespa(
    test_vespa, data_client, data_db, monkeypatch
):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    # Add an extra postgres family that won't be in vespa
    EXTRA_TEST_FAMILY = "Extra Test Family"
    new_family = {
        "id": "id:doc_search:family_document::CCLW.executive.111.222",
        "fields": {
            "family_source": "CCLW",
            "family_name": EXTRA_TEST_FAMILY,
            "family_slug": "extra-test-family",
            "family_category": "Executive",
            "document_languages": ["French"],
            "document_import_id": "CCLW.executive.111.222",
            "document_slug": "aslug",
            "family_description": "",
            "family_geography": "CAN",
            "family_publication_ts": "2011-08-01T00:00:00+00:00",
            "family_import_id": "CCLW.family.111.0",
        },
    }
    new_doc = {
        "id": "id:doc_search:document_passage::CCLW.executive.111.222.333",
        "fields": {},
    }
    _create_family(data_db, new_family)
    _create_family_event(data_db, new_family)
    _create_family_metadata(data_db, new_family)
    _create_document(data_db, new_doc, new_family)

    # This will also not be present in browse
    body = _make_search_request(data_client, params={"query_string": ""})
    browse_families = [f["family_name"] for f in body["families"]]
    assert EXTRA_TEST_FAMILY not in browse_families

    # But it won't break when running a search
    body = _make_search_request(
        data_client,
        params={
            "query_string": EXTRA_TEST_FAMILY,
            "exact_match": "true",
        },
    )

    assert len(body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_benchmark_families_search(
    label, query, test_vespa, monkeypatch, data_client, data_db
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    # This is high as it's meant as a last resort for catching new perfomance problems
    REASONABLE_LATENCY_MS = 50

    times = []
    for _ in range(1, 10):
        params = {
            "query_string": query,
            "exact_match": True,
        }
        body = _make_search_request(data_client, params)

        time_taken = body["total_time_ms"]
        times.append(time_taken)

    average = sum(times) / len(times)
    assert average < REASONABLE_LATENCY_MS


@pytest.mark.search
def test_specific_doc_returned(test_vespa, monkeypatch, data_client, data_db):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    family_name_query = "Agriculture Sector Plan 2015-2019"
    params = {
        "query_string": family_name_query,
        "exact_match": True,
        "page_size": 1,
    }
    body = _make_search_request(data_client, params)

    families = [f for f in body["families"]]
    assert body["hits"] == len(families) == 1
    family_name = families[0]["family_name"]
    assert family_name == family_name_query


@pytest.mark.parametrize(
    ("extra_params", "invalid_field"),
    [
        ({"page_size": 20, "limit": 10}, "page_size"),
        ({"offset": 20, "limit": 10}, "offset"),
        ({"limit": 501}, "limit"),
        ({"max_hits_per_family": 501}, "max_hits_per_family"),
    ],
)
@pytest.mark.search
def test_search_params_backend_limits(
    test_vespa, monkeypatch, data_client, data_db, extra_params, invalid_field
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {"query_string": "the", **extra_params}
    response = data_client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 422, response.text
    for error in response.json()["detail"]:
        assert "body" in error["loc"], error
        assert invalid_field in error["loc"], error


@pytest.mark.search
def test_search_with_deleted_docs(test_vespa, monkeypatch, data_client, data_db):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    start_body = _make_search_request(data_client, params={"query_string": "and"})

    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.10246.4861")
        .values(document_status="Deleted")
    )
    one_deleted_body = _make_search_request(data_client, params={"query_string": "and"})

    data_db.execute(update(FamilyDocument).values(document_status="Deleted"))
    all_deleted_body = _make_search_request(data_client, params={"query_string": "and"})

    start_family_count = len(start_body["families"])
    one_deleted_count = len(one_deleted_body["families"])
    all_deleted_count = len(all_deleted_body["families"])
    assert start_family_count > one_deleted_count > all_deleted_count
    assert len(all_deleted_body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_country_filters(
    label, query, test_vespa, data_client, data_db, monkeypatch
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)
    base_params = {"query_string": query}

    # Get all documents and iterate over their country codes to confirm that each are
    # the specific one that is returned in the query (as they each have a unique
    # country code)
    all_body = _make_search_request(data_client, params=base_params)
    families = [f for f in all_body["families"]]
    assert len(families) == VESPA_FIXTURE_COUNT

    for family in families:
        country_code = family["family_geography"]

        country_slug = get_country_slug_from_country_code(data_db, country_code)

        params = {**base_params, **{"keyword_filters": {"countries": [country_slug]}}}
        body_with_filters = _make_search_request(data_client, params=params)
        filtered_family_slugs = [
            f["family_slug"] for f in body_with_filters["families"]
        ]
        assert len(filtered_family_slugs) == 1
        assert family["family_slug"] in filtered_family_slugs


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_region_filters(
    label, query, test_vespa, data_client, data_db, monkeypatch
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)
    base_params = {"query_string": query}

    # Get regions of all documents and iterate over them
    # to confirm the originals are returned when filtered on
    all_body = _make_search_request(data_client, params=base_params)
    families = [f for f in all_body["families"]]
    assert len(families) == VESPA_FIXTURE_COUNT

    for family in families:
        country_code = family["family_geography"]

        # Fixture for UNFCCC.non-party.1267.0 has a non geography (XAA)
        if country_code == "Other":
            return

        parent_id = (
            data_db.query(Geography)
            .filter(Geography.value == country_code)
            .first()
            .parent_id
        )
        region = data_db.query(Geography).filter(Geography.id == parent_id).first()

        params = {**base_params, **{"keyword_filters": {"regions": [region.slug]}}}
        body_with_filters = _make_search_request(data_client, params=params)
        filtered_family_slugs = [
            f["family_slug"] for f in body_with_filters["families"]
        ]
        assert family["family_slug"] in filtered_family_slugs


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_region_and_country_filters(
    label, query, test_vespa, data_client, data_db, monkeypatch
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    # Filtering on one region and one country should return the one match
    base_params = {
        "query_string": query,
        "keyword_filters": {
            "regions": ["europe-central-asia"],
            "countries": ["ITA"],
        },
    }

    body = _make_search_request(data_client, params=base_params)

    assert len(body["families"]) == 1
    assert body["families"][0]["family_name"] == "National Energy Strategy"


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_invalid_keyword_filters(
    label, query, test_vespa, data_db, monkeypatch, data_client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    response = data_client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": query,
            "keyword_filters": {
                "geographies": ["kenya"],
                "unknown_filter_no1": ["BOOM"],
            },
        },
    )
    assert response.status_code == 422


@pytest.mark.search
@pytest.mark.parametrize(
    "year_range", [(None, None), (1900, None), (None, 2020), (1900, 2020)]
)
def test_year_range_filterered_in(
    year_range, test_vespa, data_db, monkeypatch, data_client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    # Search
    params = {"query_string": "and", "year_range": year_range}
    body = _make_search_request(data_client, params=params)
    assert len(body["families"]) > 0

    # Browse
    params = {"query_string": "", "year_range": year_range}
    body = _make_search_request(data_client, params=params)
    assert len(body["families"]) > 0


@pytest.mark.search
@pytest.mark.parametrize("year_range", [(None, 2010), (2024, None)])
def test_year_range_filterered_out(
    year_range, test_vespa, data_db, monkeypatch, data_client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    # Search
    params = {"query_string": "and", "year_range": year_range}
    body = _make_search_request(data_client, params=params)
    assert len(body["families"]) == 0

    # Browse
    params = {"query_string": "", "year_range": year_range}
    body = _make_search_request(data_client, params=params)
    assert len(body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_multiple_filters(label, query, test_vespa, data_db, monkeypatch, data_client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": query,
        "keyword_filters": {
            "countries": ["south-korea"],
            "sources": ["CCLW"],
            "categories": ["Legislative"],
        },
        "year_range": (1900, 2020),
    }

    _ = _make_search_request(data_client, params)


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_result_order_score(
    label, query, test_vespa, data_db, monkeypatch, data_client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": query,
        "sort_field": "date",
        "sort_order": "asc",
    }
    asc_date_body = _make_search_request(data_client, params)
    asc_dates = [f["family_date"] for f in asc_date_body["families"]]

    params["sort_order"] = "desc"
    desc_date_body = _make_search_request(data_client, params)
    desc_dates = [f["family_date"] for f in desc_date_body["families"]]

    assert VESPA_FIXTURE_COUNT == len(asc_dates) == len(desc_dates)
    assert asc_dates == list(reversed(desc_dates))
    assert asc_dates[0] < desc_dates[0]
    assert asc_dates[-1] > desc_dates[-1]


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_result_order_title(
    label, query, test_vespa, data_db, monkeypatch, data_client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": query,
        "sort_field": "title",
        "sort_order": "asc",
    }

    # Scope of test is to confirm this does not cause a failure
    _ = _make_search_request(data_client, params)


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


@pytest.mark.search
def test_case_insensitivity(test_vespa, data_db, monkeypatch, data_client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    lower_body = _make_search_request(data_client, {"query_string": "the"})
    upper_body = _make_search_request(data_client, {"query_string": "THE"})

    assert lower_body["families"] == upper_body["families"]


@pytest.mark.search
def test_punctuation_ignored(test_vespa, data_db, monkeypatch, data_client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    regular_body = _make_search_request(data_client, {"query_string": "the"})
    punc_body = _make_search_request(data_client, {"query_string": ", the."})
    accent_body = _make_search_request(data_client, {"query_string": "thë"})

    assert (
        sorted([f["family_slug"] for f in punc_body["families"]])
        == sorted([f["family_slug"] for f in regular_body["families"]])
        == sorted([f["family_slug"] for f in accent_body["families"]])
    )


@pytest.mark.search
def test_accents_ignored(
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    start = time.time()
    body = _make_search_request(data_client, {"query_string": "the"})
    end = time.time()

    request_time_ms = 1000 * (end - start)
    assert 0 < body["query_time_ms"] < body["total_time_ms"] < request_time_ms