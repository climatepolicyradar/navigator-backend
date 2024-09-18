import pytest
from db_client.models.dfce.family import FamilyDocument
from sqlalchemy import update

from app.api.api_v1.routers import search
from tests.search.vespa.setup_search_tests import (
    SEARCH_ENDPOINT,
    _create_document,
    _create_family,
    _create_family_event,
    _create_family_metadata,
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.search
def test_empty_search_term_performs_browse(
    test_vespa, data_client, data_db, mocker, monkeypatch, valid_token
):
    """Make sure that empty search term returns results in browse mode."""
    _populate_db_families(data_db)
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")
    body = _make_search_request(data_client, valid_token, {"query_string": ""})

    assert body["hits"] > 0
    assert len(body["families"]) > 0

    # Should automatically use vespa `all_results` parameter for browse requests
    assert query_spy.call_args.kwargs["parameters"].all_results
    query_spy.assert_called_once()


@pytest.mark.search
@pytest.mark.parametrize("exact_match", [True, False])
def test_search_body_valid(
    exact_match, test_vespa, data_client, data_db, monkeypatch, valid_token
):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    body = _make_search_request(
        data_client,
        valid_token,
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
    test_vespa, data_client, data_db, monkeypatch, valid_token
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
            "family_geographies": ["CAN"],
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
    body = _make_search_request(data_client, valid_token, params={"query_string": ""})
    browse_families = [f["family_name"] for f in body["families"]]
    assert EXTRA_TEST_FAMILY not in browse_families

    # But it won't break when running a search
    body = _make_search_request(
        data_client,
        valid_token,
        params={
            "query_string": EXTRA_TEST_FAMILY,
            "exact_match": "true",
        },
    )

    assert len(body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_benchmark_families_search(
    label, query, test_vespa, monkeypatch, data_client, data_db, valid_token
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
        body = _make_search_request(data_client, valid_token, params)

        time_taken = body["total_time_ms"]
        times.append(time_taken)

    average = sum(times) / len(times)
    assert average < REASONABLE_LATENCY_MS


@pytest.mark.search
def test_specific_doc_returned(
    test_vespa, monkeypatch, data_client, data_db, valid_token
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    family_name_query = "Agriculture Sector Plan 2015-2019"
    params = {
        "query_string": family_name_query,
        "exact_match": True,
        "page_size": 1,
    }
    body = _make_search_request(data_client, valid_token, params)

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
    test_vespa,
    monkeypatch,
    data_client,
    data_db,
    extra_params,
    invalid_field,
    valid_token,
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
def test_search_with_deleted_docs(
    test_vespa, monkeypatch, data_client, data_db, valid_token
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    start_body = _make_search_request(
        data_client, valid_token, params={"query_string": "and"}
    )

    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.10246.4861")
        .values(document_status="Deleted")
    )
    one_deleted_body = _make_search_request(
        data_client, valid_token, params={"query_string": "and"}
    )

    data_db.execute(update(FamilyDocument).values(document_status="Deleted"))
    all_deleted_body = _make_search_request(
        data_client, valid_token, params={"query_string": "and"}
    )

    start_family_count = len(start_body["families"])
    one_deleted_count = len(one_deleted_body["families"])
    all_deleted_count = len(all_deleted_body["families"])
    assert start_family_count > one_deleted_count > all_deleted_count
    assert len(all_deleted_body["families"]) == 0


@pytest.mark.search
@pytest.mark.parametrize(
    "label,query,metadata_filters",
    [
        ("search", "the", [{"name": "sector", "value": "Price"}]),
        (
            "browse",
            "",
            [
                {"name": "topic", "value": "Mitigation"},
                {"name": "instrument", "value": "Capacity building"},
            ],
        ),
    ],
)
def test_metadata_filter(
    label,
    query,
    metadata_filters,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    _populate_db_families(data_db, deterministic_metadata=True)

    response = data_client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": query,
            "metadata": metadata_filters,
        },
    )
    assert response.status_code == 200
    assert len(response.json()["families"]) > 0

    for metadata_filter in metadata_filters:
        for f in response.json()["families"]:
            assert metadata_filter["name"] in f["family_metadata"]
            assert (
                metadata_filter["value"]
                in f["family_metadata"][metadata_filter["name"]]
            )
