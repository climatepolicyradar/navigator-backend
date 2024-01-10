import csv
from datetime import datetime
import json
import random
import time
from io import StringIO
from pathlib import Path
from typing import Mapping, Optional, Sequence

import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session

from cpr_data_access.models.search import SearchParameters

from app.api.api_v1.routers import search

from app.core.lookups import get_country_slug_from_country_code

from app.data_migrations.taxonomy_cclw import get_cclw_taxonomy
from app.data_migrations.taxonomy_unf3c import get_unf3c_taxonomy

from app.db.models.app import Organisation
from app.db.models.law_policy import Geography
from app.db.models.law_policy.family import (
    DocumentStatus,
    EventStatus,
    FamilyCategory,
    Family,
    FamilyDocument,
    FamilyDocumentType,
    FamilyEvent,
    FamilyOrganisation,
    Slug,
    Variant,
)
from app.db.models.law_policy.metadata import (
    FamilyMetadata,
)
from app.db.models.document.physical_document import (
    LanguageSource,
    PhysicalDocument,
    PhysicalDocumentLanguage,
)
from app.initial_data import run_data_migrations

SEARCH_ENDPOINT = "/api/v1/searches"
CSV_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-csv"
FIXTURE_DIR = Path(__file__).parents[1] / "search_fixtures"
VESPA_FAMILY_PATH = FIXTURE_DIR / "vespa_family_document.json"
VESPA_DOCUMENT_PATH = FIXTURE_DIR / "vespa_document_passage.json"


def _parse_id(schema, convert_to: Optional[str] = None) -> str:
    schema_id = schema["id"].split("::")[-1]
    if convert_to is None:
        return schema_id
    else:
        id_parts = schema_id.split(".")
    return f"{id_parts[0]}.{convert_to}.{id_parts[2]}.{id_parts[3]}"


def _get_family_fixture(doc):
    with open(VESPA_FAMILY_PATH, "r") as vf:
        for family in json.load(vf):
            if family["id"] == doc["fields"]["family_document_ref"]:
                return family


def _fixture_docs():
    with open(VESPA_DOCUMENT_PATH, "r") as vd:
        documents = json.load(vd)

    for doc in documents:
        family = _get_family_fixture(doc)
        yield doc, family


def _populate_search_db_families(db: Session) -> None:
    run_data_migrations(db)
    _create_organisation(db)

    seen_family_ids = []
    for doc, family in _fixture_docs():
        if doc["fields"]["family_document_ref"] not in seen_family_ids:
            _create_family(db, family)
            _create_family_event(db, family)
            _create_family_metadata(db, family)
            seen_family_ids.append(doc["fields"]["family_document_ref"])
        _create_document(db, doc, family)


def _create_organisation(db):
    for org in [
        Organisation(
            id=0, name="CCLW", description="CCLW", organisation_type="CCLW Type"
        ),
        Organisation(
            id=1, name="UNFCCC", description="UNFCCC", organisation_type="UNFCCC Type"
        ),
    ]:
        db.merge(org)
        db.commit()


def _create_family(db, family):
    family_id = _parse_id(family)
    family_import_id = family["fields"]["family_import_id"]

    family_object = Family(
        title=family["fields"]["family_name"],
        import_id=family_import_id,
        description=family["fields"]["family_description"],
        geography_id=1,
        family_category=FamilyCategory(family["fields"]["family_category"]),
    )
    db.add(family_object)
    db.commit()

    family_slug = Slug(
        name=family_id,
        family_import_id=family_import_id,
        family_document_import_id=None,
    )

    if family["fields"]["family_source"] == "CCLW":
        orgid = 0
    elif family["fields"]["family_source"] == "UNFCCC":
        orgid = 1
    else:
        raise ValueError(f"Unexpected value in: {family['fields']['family_source']}")

    family_organisation = FamilyOrganisation(
        family_import_id=family_import_id,
        organisation_id=orgid,
    )

    db.add(family_slug)
    db.commit()
    db.add(family_organisation)
    db.commit()
    db.refresh(family_object)


def _create_family_event(db, family):
    event_id = _parse_id(family, convert_to="event")
    family_id = _parse_id(family)

    family_import_id = family["fields"]["family_import_id"]
    family_event = FamilyEvent(
        import_id=event_id,
        title=f"{family_id} Event",
        date=datetime.fromisoformat(family["fields"]["family_publication_ts"]),
        event_type_name="Passed/Approved",
        family_import_id=family_import_id,
        family_document_import_id=None,
        status=EventStatus.OK,
    )
    db.add(family_event)
    db.commit()


def _generate_synthetic_metadata(
    taxonomy: Mapping[str, dict]
) -> Mapping[str, Sequence[str]]:
    meta_value = {}
    for k in taxonomy:
        allowed_values = taxonomy[k]["allowed_values"]
        element_count = random.randint(0, len(allowed_values))
        meta_value[k] = random.sample(allowed_values, element_count)
    return meta_value


def _create_family_metadata(db, family):
    if family["fields"]["family_source"] == "UNFCCC":
        taxonomy = get_unf3c_taxonomy()
    elif family["fields"]["family_source"] == "CCLW":
        taxonomy = get_cclw_taxonomy()
    else:
        raise ValueError(
            f"Could not get taxonomy for: {family['fields']['family_source']}"
        )
    metadata_value = _generate_synthetic_metadata(taxonomy)

    family_import_id = family["fields"]["family_import_id"]
    family_metadata = FamilyMetadata(
        family_import_id=family_import_id,
        taxonomy_id=1,
        value=metadata_value,
    )
    db.add(family_metadata)
    db.commit()


def _create_document(db, doc, family):
    physical_document = PhysicalDocument(
        title="doc name",
        cdn_object="cdn_object",
        md5_sum="md5_sum",
        source_url="source_url",
        content_type="content_type",
    )

    db.add(physical_document)
    db.commit()
    db.refresh(physical_document)
    physical_document_language = PhysicalDocumentLanguage(
        language_id=1826,  # English
        document_id=physical_document.id,
        source=LanguageSource.USER,
        visible=True,
    )
    db.add(physical_document_language)
    db.commit()
    db.refresh(physical_document_language)
    db.refresh(physical_document)

    if len(family["fields"]["document_languages"]) > 0:
        variant = Variant(variant_name="Official Translation", description="")
    else:
        variant = Variant(variant_name="Original Language", description="")
    db.merge(variant)
    db.commit()

    doc_type = FamilyDocumentType(
        name=family["fields"]["family_category"], description=""
    )
    db.merge(doc_type)
    db.commit()

    family_import_id = family["fields"]["family_import_id"]
    doc_import_id = family["fields"]["document_import_id"]

    family_document = FamilyDocument(
        family_import_id=family_import_id,
        physical_document_id=physical_document.id,
        import_id=doc_import_id,
        variant_name=variant.variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=doc_type.name,
    )

    family_document_slug = Slug(
        name=f"fd_{_parse_id(doc)}",
        family_import_id=None,
        family_document_import_id=doc_import_id,
    )

    db.add(family_document)
    db.commit()
    db.add(family_document_slug)
    db.commit()
    db.refresh(family_document)


def _make_search_request(client, params):
    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 200
    return response.json()


@pytest.mark.search
def test_empty_search_term_performs_browse(client, test_db, mocker):
    """Make sure that empty search term returns results in browse mode."""
    _populate_search_db_families(test_db)
    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")

    body = _make_search_request(client, {"query_string": ""})

    assert body["hits"] > 0
    assert len(body["families"]) > 0
    query_spy.assert_not_called()


@pytest.mark.search
def test_simple_pagination_families(test_vespa, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

    doc_slugs = []
    for offset in range(3):
        params = {
            "query_string": "and",
            "limit": 1,
            "offset": offset,
        }
        body = _make_search_request(client, params)

        for f in body["families"]:
            for d in f["family_documents"]:
                doc_slugs.append(d["document_slug"])

    assert len(set(doc_slugs)) == len(doc_slugs)


@pytest.mark.search
@pytest.mark.parametrize("exact_match", [True, False])
def test_search_body_valid(exact_match, test_vespa, client, test_db, monkeypatch):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

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
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_benchmark_families_search(
    label, query, test_vespa, monkeypatch, client, test_db
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

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
@pytest.mark.parametrize("exact_match", [True, False])
def test_specific_doc_returned(exact_match, test_vespa, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

    family_name_query = "Agriculture Sector Plan 2015-2019"
    params = {
        "query_string": family_name_query,
        "exact_match": exact_match,
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
    _populate_search_db_families(test_db)
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

    query_spy.assert_called_once_with(parameters=params)


@pytest.mark.search
def test_search_with_deleted_docs(test_vespa, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)
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
    _populate_search_db_families(test_db)
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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 422


@pytest.mark.search
def test_case_insensitivity(test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

    lower_body = _make_search_request(client, {"query_string": "the"})
    upper_body = _make_search_request(client, {"query_string": "THE"})

    assert lower_body["families"] == upper_body["families"]


@pytest.mark.search
def test_punctuation_ignored(test_vespa, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)

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
    _populate_search_db_families(test_db)
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
    _populate_search_db_families(test_db)

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
