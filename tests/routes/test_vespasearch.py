import csv
import time
from io import StringIO
from typing import Mapping

import pytest
from sqlalchemy import update

from tests.routes.setup_search_tests import (
    _create_family,
    _create_family_event,
    _create_family_metadata,
    _create_document,
    _populate_db_families,
)

from cpr_data_access.models.search import SearchParameters

from app.api.api_v1.routers import search
from app.core.lookups import get_country_slug_from_country_code

from app.db.models.law_policy import Geography
from app.db.models.law_policy.family import FamilyDocument


SEARCH_ENDPOINT = "/api/v1/searches"
CSV_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-csv"


def _make_search_request(client, params: Mapping[str, str]):
    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 200
    return response.json()


@pytest.mark.search
def test_empty_search_term_performs_browse(client, test_db, mocker):
    """Make sure that empty search term returns results in browse mode."""
    _populate_db_families(test_db)
    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")

    body = _make_search_request(client, {"query_string": ""})

    assert body["hits"] > 0
    assert len(body["families"]) > 0
    query_spy.assert_not_called()


@pytest.mark.search
def test_simple_pagination_families(test_vespa, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    FIXTURE_COUNT = 4
    LIMIT = 2

    # Query one
    params = {
        "query_string": "and",
        "limit": LIMIT,
        "offset": 0,
    }
    body_one = _make_search_request(client, params)
    assert body_one["hits"] == FIXTURE_COUNT
    assert len(body_one["families"]) == LIMIT
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
        "limit": LIMIT,
        "offset": 2,
    }
    body_two = _make_search_request(client, params)
    assert body_two["hits"] == FIXTURE_COUNT
    assert len(body_two["families"]) == LIMIT
    assert (
        body_two["families"][0]["family_slug"]
        == "submission-to-the-unfccc-ahead-of-the-first-technical-dialogue-of-the-global-stocktake-formally-submitted-by-observer-organization-climateworks-foundation-on-behalf-of-the-igst-consortium_e760"
    )
    assert body_two["families"][1]["family_slug"] == "national-energy-strategy_980b"


@pytest.mark.search
@pytest.mark.parametrize("exact_match", [True, False])
def test_search_body_valid(exact_match, test_vespa, client, test_db, monkeypatch):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    body = _make_search_request(
        client,
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
        "query_time_ms",
        "total_time_ms",
    ]
    assert isinstance(body["families"], list)


@pytest.mark.search
def test_no_doc_if_in_postgres_but_not_vespa(test_vespa, client, test_db, monkeypatch):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

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
            "family_description": "",
            "family_publication_ts": "2011-08-01T00:00:00+00:00",
            "family_import_id": "CCLW.family.111.0",
        },
    }
    new_doc = {
        "id": "id:doc_search:document_passage::CCLW.executive.111.222.333",
        "fields": {},
    }
    _create_family(test_db, new_family)
    _create_family_event(test_db, new_family)
    _create_family_metadata(test_db, new_family)
    _create_document(test_db, new_doc, new_family)

    # This will be present in browse, which is fine
    body = _make_search_request(client, params={"query_string": ""})
    browse_families = [f["family_name"] for f in body["families"]]
    assert EXTRA_TEST_FAMILY in browse_families

    # But it won't break when running a search
    body = _make_search_request(
        client,
        params={
            "query_string": EXTRA_TEST_FAMILY,
            "exact_match": "true",
        },
    )

    assert len(body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_benchmark_families_search(
    label, query, test_vespa, monkeypatch, client, test_db
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    # This is high as it's meant as a last resort for catching new perfomance problems
    REASONABLE_LATENCY_MS = 25

    times = []
    for _ in range(1, 10):
        params = {
            "query_string": query,
            "exact_match": True,
        }
        body = _make_search_request(client, params)

        time_taken = body["total_time_ms"]
        times.append(time_taken)

    average = sum(times) / len(times)
    assert average < REASONABLE_LATENCY_MS

    with open(f"/data/benchmark_{label}_vespa.txt", "w") as out_file:
        out_file.write("\n".join([str(t) for t in times]))


@pytest.mark.search
def test_specific_doc_returned(test_vespa, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    family_name_query = "Agriculture Sector Plan 2015-2019"
    params = {
        "query_string": family_name_query,
        "exact_match": True,
        "limit": 1,
    }
    body = _make_search_request(client, params)

    families = [f for f in body["families"]]
    assert body["hits"] == len(families) == 1
    family_name = families[0]["family_name"]
    assert family_name == family_name_query


@pytest.mark.search
@pytest.mark.parametrize(
    "params",
    [
        SearchParameters(query_string="climate"),
        SearchParameters(query_string="climate", exact_match=True),
        SearchParameters(
            query_string="climate",
            exact_match=True,
            limit=1,
            max_hits_per_family=10,
        ),
    ],
)
def test_search_params_contract(
    params, test_vespa, monkeypatch, client, test_db, mocker
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)
    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")

    _make_search_request(
        client,
        params={
            "query_string": params.query_string,
            "exact_match": params.exact_match,
            "limit": params.limit,
            "max_hits_per_family": params.max_hits_per_family,
        },
    )

    expected_params = params
    expected_params.limit = 150
    query_spy.assert_called_once_with(parameters=expected_params)


@pytest.mark.search
def test_search_with_deleted_docs(test_vespa, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    start_body = _make_search_request(client, params={"query_string": "and"})

    test_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.10246.4861")
        .values(document_status="Deleted")
    )
    one_deleted_body = _make_search_request(client, params={"query_string": "and"})

    test_db.execute(update(FamilyDocument).values(document_status="Deleted"))
    all_deleted_body = _make_search_request(client, params={"query_string": "and"})

    start_family_count = len(start_body["families"])
    one_deleted_count = len(one_deleted_body["families"])
    all_deleted_count = len(all_deleted_body["families"])
    assert start_family_count > one_deleted_count > all_deleted_count
    assert len(all_deleted_body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_country_filters(
    label, query, test_vespa, client, test_db, monkeypatch
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)
    base_params = {"query_string": query}

    # Get all documents and iterate over there country codes
    # to confirm they are returned when filtered on
    all_body = _make_search_request(client, params=base_params)
    families = [f for f in all_body["families"]]
    assert len(families) >= 4

    for family in families:
        country_code = family["family_geography"]

        # Fixture for UNFCCC.non-party.1267.0 has a non geography (XAA)
        if country_code == "Other":
            return

        country_slug = get_country_slug_from_country_code(test_db, country_code)

        params = {**base_params, **{"keyword_filters": {"countries": [country_slug]}}}
        body_with_filters = _make_search_request(client, params=params)
        filtered_family_slugs = [
            f["family_slug"] for f in body_with_filters["families"]
        ]

        assert family["family_slug"] in filtered_family_slugs


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_region_filters(label, query, test_vespa, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)
    base_params = {"query_string": query}

    # Get regions of all documents and iterate over them
    # to confirm the originals are returned when filtered on
    all_body = _make_search_request(client, params=base_params)
    families = [f for f in all_body["families"]]
    assert len(families) >= 4

    for family in families:
        country_code = family["family_geography"]

        # Fixture for UNFCCC.non-party.1267.0 has a non geography (XAA)
        if country_code == "Other":
            return

        parent_id = (
            test_db.query(Geography)
            .filter(Geography.value == country_code)
            .first()
            .parent_id
        )
        region = test_db.query(Geography).filter(Geography.id == parent_id).first()

        params = {**base_params, **{"keyword_filters": {"regions": [region.slug]}}}
        body_with_filters = _make_search_request(client, params=params)
        filtered_family_slugs = [
            f["family_slug"] for f in body_with_filters["families"]
        ]
        assert family["family_slug"] in filtered_family_slugs


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_invalid_keyword_filters(
    label, query, test_vespa, test_db, monkeypatch, client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    response = client.post(
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
def test_year_range_filterered_in(year_range, test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    # Search
    params = {"query_string": "and", "year_range": year_range}
    body = _make_search_request(client, params=params)
    assert len(body["families"]) > 0

    # Browse
    params = {"query_string": "", "year_range": year_range}
    body = _make_search_request(client, params=params)
    assert len(body["families"]) > 0


@pytest.mark.search
@pytest.mark.parametrize("year_range", [(None, 2010), (2024, None)])
def test_year_range_filterered_out(
    year_range, test_vespa, test_db, monkeypatch, client
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    # Search
    params = {"query_string": "and", "year_range": year_range}
    body = _make_search_request(client, params=params)
    assert len(body["families"]) == 0

    # Browse
    params = {"query_string": "", "year_range": year_range}
    body = _make_search_request(client, params=params)
    assert len(body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_multiple_filters(label, query, test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    params = {
        "query_string": query,
        "keyword_filters": {
            "countries": ["south-korea"],
            "sources": ["CCLW"],
            "categories": ["Legislative"],
        },
        "year_range": (1900, 2020),
    }

    _ = _make_search_request(client, params)


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_result_order_score(label, query, test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    params = {
        "query_string": query,
        "sort_field": "date",
        "sort_order": "asc",
    }
    asc_date_body = _make_search_request(client, params)
    asc_dates = [f["family_date"] for f in asc_date_body["families"]]

    params["sort_order"] = "desc"
    desc_date_body = _make_search_request(client, params)
    desc_dates = [f["family_date"] for f in desc_date_body["families"]]

    assert 4 == len(asc_dates) == len(desc_dates)
    assert asc_dates == list(reversed(desc_dates))
    assert asc_dates[0] < desc_dates[0]
    assert asc_dates[-1] > desc_dates[-1]


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_result_order_title(label, query, test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    params = {
        "query_string": query,
        "sort_field": "title",
        "sort_order": "asc",
    }

    # Scope of test is to confirm this does not cause a failure
    _ = _make_search_request(client, params)


@pytest.mark.search
@pytest.mark.parametrize(
    "params",
    [
        {"exact_match": False},
        {},
    ],
)
def test_invalid_requests(params, test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 422


@pytest.mark.search
def test_case_insensitivity(test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    lower_body = _make_search_request(client, {"query_string": "the"})
    upper_body = _make_search_request(client, {"query_string": "THE"})

    assert lower_body["families"] == upper_body["families"]


@pytest.mark.search
def test_punctuation_ignored(test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    regular_body = _make_search_request(client, {"query_string": "the"})
    punc_body = _make_search_request(client, {"query_string": ", the."})
    accent_body = _make_search_request(client, {"query_string": "thë"})

    assert punc_body["families"] == regular_body["families"] == accent_body["families"]


@pytest.mark.search
def test_accents_ignored(
    test_vespa,
    test_db,
    monkeypatch,
    client,
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    start = time.time()
    body = _make_search_request(client, {"query_string": "the"})
    end = time.time()

    request_time_ms = 1000 * (end - start)
    assert 0 < body["query_time_ms"] < body["total_time_ms"] < request_time_ms


@pytest.mark.search
@pytest.mark.parametrize("exact_match", [True, False])
@pytest.mark.parametrize("query_string", ["", "local"])
def test_csv_content(
    exact_match, query_string, test_vespa, test_db, monkeypatch, client
):
    """Make sure that downloaded CSV content matches a given search"""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)
    params = {
        "exact_match": exact_match,
        "query_string": query_string,
    }
    body = _make_search_request(client, params)
    families = body["families"]
    assert len(families) > 0

    csv_response = client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json={
            "exact_match": exact_match,
            "query_string": query_string,
        },
    )
    assert csv_response.status_code == 200

    csv_content = csv.DictReader(StringIO(csv_response.text))
    for row, family in zip(csv_content, families):
        assert row["Family Name"] == family["family_name"]
        assert row["Family Summary"] == family["family_description"]
        assert row["Family Publication Date"] == family["family_date"]
        assert row["Category"] == family["family_category"]
        assert row["Geography"] == family["family_geography"]

        # TODO: Add collections to test db setup to provide document level coverage


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
@pytest.mark.parametrize("limit", [10, 50, 500])
def test_csv_download_search_no_limit(
    label, query, limit, test_vespa, test_db, monkeypatch, client, mocker
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(test_db)

    if label == "search":
        query_spy = mocker.spy(search._VESPA_CONNECTION, "search")
    elif label == "browse":
        query_spy = mocker.spy(search, "browse_rds_families")
    else:
        raise ValueError("unexpected label parameter")

    download_response = client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json={
            "query_string": query,
            "limit": limit,
        },
    )
    assert download_response.status_code == 200

    if label == "search":
        actual_search_req = query_spy.mock_calls[0].kwargs["parameters"]
    elif label == "browse":
        actual_search_req = query_spy.mock_calls[0].kwargs["req"]
    else:
        raise ValueError("unexpected label parameter")

    # Make sure we overrode the search request content to produce the CSV download
    assert 100 <= actual_search_req.limit
