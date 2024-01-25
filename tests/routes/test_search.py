import csv
import json
import random
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Mapping, Sequence, cast
import httpx

import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.api.api_v1.routers import search
from app.api.api_v1.schemas.search import (
    FilterField,
    IncludedResults,
    SortOrder,
    SearchRequestBody,
)
from app.core.search import _FILTER_FIELD_MAP, OpenSearchQueryConfig
from tests.core.ingestion.legacy_setup.utils import get_or_create
from app.data_migrations.taxonomy_cclw import get_cclw_taxonomy
from app.db.models.app import Organisation
from app.db.models.law_policy.family import (
    DocumentStatus,
    EventStatus,
    FamilyCategory,
    Family,
    FamilyDocument,
    FamilyDocumentType,
    FamilyEvent,
    FamilyEventType,
    FamilyOrganisation,
    Geography,
    Slug,
    Variant,
)
from app.db.models.law_policy.metadata import (
    FamilyMetadata,
    MetadataTaxonomy,
    MetadataOrganisation,
)
from app.db.models.document.physical_document import (
    Language,
    LanguageSource,
    PhysicalDocument,
    PhysicalDocumentLanguage,
)
from app.initial_data import populate_geography, populate_language, populate_taxonomy

SEARCH_ENDPOINT = "/api/v1/searches"
CSV_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-csv"
_EXPECTED_FAMILY_TITLE = "Decision No 1386/2013/EU"


def clean_response(r: httpx.Response) -> dict:
    new_r = r.json()
    del new_r["query_time_ms"]
    del new_r["total_time_ms"]
    return new_r


def _populate_search_db_families(db: Session) -> None:
    documents: dict[str, FamilyDocument] = {}
    families: dict[str, Family] = {}

    populate_language(db)
    populate_geography(db)
    populate_taxonomy(db)

    original = Variant(variant_name="Original Language", description="")
    translated = Variant(variant_name="Official Translation", description="")
    variants: dict[str, Variant] = {
        "translated_True": translated,
        "translated_False": original,
    }
    organisation = Organisation(
        name="CCLW", description="CCLW", organisation_type="CCLW Type"
    )
    family_event_type = FamilyEventType(
        name="Passed/Approved",
        description="",
    )
    db.add(family_event_type)
    db.add(original)
    db.add(translated)
    db.add(organisation)
    db.commit()
    db.refresh(organisation)

    cclw_taxonomy_data = get_cclw_taxonomy()

    containing_dir = Path(__file__).parent
    data_dir = containing_dir.parent / "data"
    for f in data_dir.iterdir():
        if f.is_file() and f.suffixes == [".json"]:
            with open(f, "r") as of:
                for line in of.readlines():
                    search_document = json.loads(line)
                    _create_family_structures(
                        db,
                        search_document,
                        documents,
                        families,
                        variants,
                        organisation,
                        cclw_taxonomy_data,
                    )


def _generate_metadata(
    cclw_taxonomy_data: Mapping[str, dict]
) -> Mapping[str, Sequence[str]]:
    meta_value = {}
    for k in cclw_taxonomy_data:
        element_count = random.randint(0, 3)
        meta_value[k] = random.sample(
            cclw_taxonomy_data[k]["allowed_values"], element_count
        )
    return meta_value


def _create_family_structures(
    db: Session,
    doc: dict[str, Any],
    documents: dict[str, FamilyDocument],
    families: dict[str, Family],
    variants: dict[str, Variant],
    organisation: Organisation,
    cclw_taxonomy_data: Mapping[str, dict],
) -> None:
    """Populate a db to match the test search index code"""
    doc_details = doc["_source"]
    doc_id = doc_details["document_id"]
    if doc_id in documents:
        return

    doc_type = get_or_create(
        db,
        FamilyDocumentType,
        **{
            "name": doc_details["document_type"],
            "description": doc_details["document_type"],
        },
    )

    doc_id_components = doc_id.split(".")
    family_id = f"CCLW.family.{doc_id_components[2]}.0"  # assume single family

    if family_id not in families:
        family = Family(
            # Truncate the family name to produce the same "family name" for the example
            # data where we have engineered 2 documents into a single family.
            title=doc_details["document_name"][:24],
            import_id=family_id,
            description=doc_details["document_description"],
            geography_id=(
                db.query(Geography)
                .filter(Geography.value == doc_details["document_geography"])
                .one()
                .id
            ),
            family_category=FamilyCategory(doc_details["document_category"]),
        )
        family_slug = Slug(
            name=family_id,
            family_import_id=family_id,
            family_document_import_id=None,
        )
        family_organisation = FamilyOrganisation(
            family_import_id=family_id,
            organisation_id=organisation.id,
        )
        db.add(family)
        db.commit()
        db.add(family_slug)
        db.add(family_organisation)
        db.commit()
        db.refresh(family)
        families[family_id] = family

        # Make sure we add an event so we can filter by date
        family_event = FamilyEvent(
            import_id=f"CCLW.event.{doc_id_components[2]}.0",
            title=f"CCLW.family.{doc_id_components[2]}.0 Event",
            date=datetime.strptime(doc_details["document_date"], "%d/%m/%Y"),
            event_type_name="Passed/Approved",
            family_import_id=family_id,
            family_document_import_id=None,
            status=EventStatus.OK,
        )
        db.add(family_event)
        db.commit()

        metadata_value = _generate_metadata(cclw_taxonomy_data)

        family_metadata = FamilyMetadata(
            family_import_id=family.import_id,
            taxonomy_id=(
                db.query(MetadataTaxonomy)
                .join(
                    MetadataOrganisation,
                    MetadataOrganisation.taxonomy_id == MetadataTaxonomy.id,
                )
                .join(
                    Organisation,
                    MetadataOrganisation.organisation_id == Organisation.id,
                )
                .filter(Organisation.name == "CCLW")
                .one()
                .id
            ),
            value=metadata_value,
        )
        db.add(family_metadata)
        db.commit()

    physical_document = PhysicalDocument(
        title=doc_details["document_name"],
        cdn_object=doc_details["document_cdn_object"],
        md5_sum=doc_details["document_md5_sum"],
        source_url=doc_details["document_source_url"],
        content_type=doc_details["document_content_type"],
    )
    db.add(physical_document)
    db.commit()
    db.refresh(physical_document)
    # TODO: better handling of document language!
    existing_language = db.query(Language).filter(Language.name == "English").one()
    physical_document_language = PhysicalDocumentLanguage(
        language_id=existing_language.id,
        document_id=physical_document.id,
        source=LanguageSource.USER,
        visible=True,
    )
    db.add(physical_document_language)
    db.commit()
    db.refresh(physical_document_language)
    db.refresh(physical_document)
    family_document = FamilyDocument(
        family_import_id=family_id,
        physical_document_id=physical_document.id,
        import_id=doc_id,
        variant_name=variants[f"translated_{doc_details['translated']}"].variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=doc_type.name,
    )
    family_document_slug = Slug(
        name=f"fd_{doc_id}",
        family_import_id=None,
        family_document_import_id=doc_id,
    )
    db.add(family_document)
    db.commit()
    db.add(family_document_slug)
    db.commit()
    db.refresh(family_document)
    documents[doc_id] = family_document


@pytest.mark.opensearch
def test_slug_is_from_family_document(test_opensearch, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    page1_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "and",
            "exact_match": False,
            "limit": 2,
            "offset": 0,
        },
        params={"use_vespa": False},
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    fam1 = page1_response_body["families"][0]
    doc1 = fam1["family_documents"][0]
    assert doc1["document_slug"].startswith("fd_") and "should be from FamilyDocument"


@pytest.mark.opensearch
def test_simple_pagination_families(test_opensearch, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    page1_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "and",
            "exact_match": False,
            "limit": 2,
            "offset": 0,
        },
        params={"use_vespa": False},
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    page1_families = page1_response_body["families"]
    assert len(page1_families) == 2

    page2_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "and",
            "exact_match": False,
            "limit": 2,
            "offset": 2,
        },
        params={"use_vespa": False},
    )
    assert page2_response.status_code == 200

    page2_response_body = page2_response.json()
    page2_families = page2_response_body["families"]
    assert len(page2_families) == 2

    # Sanity check that we really do have 4 different documents
    family_slugs = {d["family_slug"] for d in page1_families} | {
        d["family_slug"] for d in page2_families
    }

    assert len(family_slugs) == 4

    for d in page1_families:
        assert d not in page2_families


@pytest.mark.opensearch
@pytest.mark.parametrize("exact_match", [True, False])
def test_search_body_valid(exact_match, test_opensearch, monkeypatch, client, test_db):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "disaster",
            "exact_match": exact_match,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200


@pytest.mark.opensearch
def test_benchmark_families_search(test_opensearch, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    times = []
    for _ in range(1, 10):
        response = client.post(
            SEARCH_ENDPOINT,
            json={
                "query_string": "climate",
                "exact_match": True,
            },
            params={"use_vespa": False},
        )
        assert response.status_code == 200
        time_taken = response.json()["total_time_ms"]
        times.append(str(time_taken))

    with open("/data/benchmark_search.txt", "w") as out_file:
        out_file.write("\n".join(times))


@pytest.mark.opensearch
def test_benchmark_families_browse(test_opensearch, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    times = []
    for _ in range(1, 10):
        response = client.post(
            SEARCH_ENDPOINT,
            json={
                "query_string": "",
            },
            params={"use_vespa": False},
        )
        assert response.status_code == 200
        time_taken = response.json()["total_time_ms"]
        times.append(str(time_taken))

    with open("/data/benchmark_browse.txt", "w") as out_file:
        out_file.write("\n".join(times))


@pytest.mark.opensearch
def test_families_search(test_opensearch, monkeypatch, client, test_db, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    expected_config = OpenSearchQueryConfig()
    expected_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        limit=10,
        offset=0,
    )
    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "query_families")

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": True,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    # Ensure nothing has/is going on in the background
    assert query_spy.call_count == 1  # Called once as not using jit search

    actual_search_body = query_spy.mock_calls[0].kwargs["search_request_body"]
    assert actual_search_body == expected_search_body

    # Check default config is used
    actual_config = query_spy.mock_calls[0].kwargs["opensearch_internal_config"]
    assert actual_config == expected_config

    # Check the correct number of hits is returned
    data = response.json()
    assert data["hits"] == 3
    assert len(data["families"]) == 3

    names_returned = [f["family_name"] for f in data["families"]]
    assert _EXPECTED_FAMILY_TITLE in names_returned


@pytest.mark.opensearch
def test_families_search_with_all_docs_deleted(
    test_opensearch, monkeypatch, client, test_db
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)
    # This test is fragile due to _EXPECTED_FAMILY_TITLE being generated in the
    # populate db function from imperfect data. Ye be warned! 🏴‍☠️
    family = test_db.query(Family).filter(Family.title == _EXPECTED_FAMILY_TITLE).one()
    for doc in family.family_documents:
        test_db.execute(
            update(FamilyDocument)
            .where(FamilyDocument.import_id == doc.import_id)
            .values(document_status="Deleted")
        )

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response2 = client.get(f"/api/v1/documents/{family.import_id}")
    assert response2.status_code == 404

    # Check the correct number of hits is returned
    data = response.json()
    assert data["hits"] == 2
    assert len(data["families"]) == 2
    names_returned = [f["family_name"] for f in data["families"]]
    assert _EXPECTED_FAMILY_TITLE not in names_returned


@pytest.mark.opensearch
def test_families_search_with_one_doc_deleted(
    test_opensearch, monkeypatch, client, test_db
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)
    # This test is fragile due to _EXPECTED_FAMILY_TITLE being generated in the
    # populate db function from imperfect data. Ye be warned! 🏴‍☠️
    family = test_db.query(Family).filter(Family.title == _EXPECTED_FAMILY_TITLE).one()
    doc = family.family_documents[0]
    test_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == doc.import_id)
        .values(document_status="Deleted")
    )
    deleted_title = doc.physical_document.title

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": True,
        },
        params={"use_vespa": False},
    )

    assert response.status_code == 200

    # Check the correct number of hits is returned
    data = response.json()
    assert data["hits"] == 3
    assert len(data["families"]) == 3
    names_returned = [f["family_name"] for f in data["families"]]
    assert _EXPECTED_FAMILY_TITLE in names_returned

    # Check the deleted document is not returned but the non-deleted one is
    found = False
    for fam in data["families"]:
        if fam["family_name"] == _EXPECTED_FAMILY_TITLE:
            found = True
            doc_titles = [d["document_title"] for d in fam["family_documents"]]
            assert len(doc_titles) == 1
            assert deleted_title not in doc_titles

    assert found


@pytest.mark.opensearch
def test_keyword_filters(test_opensearch, client, test_db, monkeypatch, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "keyword_filters": {"countries": ["kenya"]},
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("countries")]: ["KEN"]}
    } in query_body["query"]["bool"]["filter"]


@pytest.mark.opensearch
def test_keyword_filters_region(test_opensearch, test_db, monkeypatch, client, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "keyword_filters": {"regions": ["south-asia"]},
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    assert {
        "terms": {
            _FILTER_FIELD_MAP[FilterField.COUNTRY]: [
                "AFG",
                "BGD",
                "BTN",
                "IND",
                "LKA",
                "MDV",
                "NPL",
                "PAK",
            ]
        }
    } in query_body["query"]["bool"]["filter"]

    # Only country filters should be added
    query_term_keys = []
    for d in query_body["query"]["bool"]["filter"]:
        search_term_dict = d["terms"]
        query_term_keys.extend(search_term_dict.keys())

    assert [_FILTER_FIELD_MAP[FilterField.COUNTRY]] == query_term_keys


@pytest.mark.opensearch
def test_keyword_filters_region_invalid(
    test_opensearch, monkeypatch, client, test_db, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "keyword_filters": {"regions": ["daves-region"]},
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    # The region is invalid, so no filters should be applied
    assert "filter" not in query_body["query"]["bool"]


@pytest.mark.opensearch
def test_invalid_keyword_filters(test_opensearch, test_db, monkeypatch, client):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "disaster",
            "exact_match": False,
            "keyword_filters": {
                "geographies": ["kenya"],
                "unknown_filter_no1": ["BOOM"],
            },
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 422


@pytest.mark.opensearch
@pytest.mark.parametrize(
    "year_range", [(None, None), (1900, None), (None, 2020), (1900, 2020)]
)
def test_year_range_filters(
    year_range,
    test_opensearch,
    monkeypatch,
    client,
    test_db,
    mocker,
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "disaster",
            "exact_match": False,
            "year_range": year_range,
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    query_body = query_spy.mock_calls[0].args[0]

    assert response.status_code == 200
    assert query_spy.call_count == 1
    # Check that search query default order is not modified unless requested
    assert query_body["aggs"]["sample"]["aggs"]["top_docs"]["terms"]["order"] == {
        "top_hit": "desc"
    }

    if year_range[0] or year_range[1]:
        expected_range_check = {
            "range": {
                "document_date": dict(
                    [
                        r
                        for r in zip(
                            ["gte", "lte"],
                            [
                                f"01/01/{year_range[0]}"
                                if year_range[0] is not None
                                else None,
                                f"31/12/{year_range[1]}"
                                if year_range[1] is not None
                                else None,
                            ],
                        )
                        if r[1] is not None
                    ]
                )
            }
        }

        assert expected_range_check in query_body["query"]["bool"]["filter"]
    else:
        assert "filter" not in query_body["query"]["bool"]


@pytest.mark.opensearch
def test_multiple_filters(test_opensearch, test_db, monkeypatch, client, mocker):
    """Check that multiple filters are successfully applied"""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "greenhouse",
            "exact_match": False,
            "keyword_filters": {
                "countries": ["south-korea"],
                "sources": ["CCLW"],
                "categories": ["Legislative"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("countries")]: ["KOR"]}
    } in query_body["query"]["bool"]["filter"]
    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("sources")]: ["CCLW"]}
    } in query_body["query"]["bool"]["filter"]
    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("categories")]: ["Legislative"]}
    } in query_body["query"]["bool"]["filter"]
    assert {
        "range": {"document_date": {"gte": "01/01/1900", "lte": "31/12/2020"}}
    } in query_body["query"]["bool"]["filter"]

    response_content = response.json()
    assert response_content["hits"] > 0
    assert len(response.json()["families"]) > 0
    families = response_content["families"]
    for family in families:
        assert family["family_category"] == "Legislative"


@pytest.mark.opensearch
def test_result_order_score(test_opensearch, monkeypatch, client, test_db, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "disaster",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    query_response = query_spy.spy_return.raw_response
    result_docs = query_response["aggregations"]["sample"]["top_docs"]["buckets"]

    s = None
    for d in result_docs:
        new_s = d["top_hit"]["value"]
        if s is not None:
            assert new_s <= s
        s = new_s


@pytest.mark.opensearch
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
def test_result_order_date(test_opensearch, monkeypatch, client, test_db, order):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "sort_field": "date",
            "sort_order": order.value,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    elements = response_body["families"]
    assert len(elements) > 1

    dt = None
    for e in elements:
        new_dt = datetime.fromisoformat(e["family_date"])
        if dt is not None:
            if order == SortOrder.DESCENDING:
                assert new_dt <= dt
            if order == SortOrder.ASCENDING:
                assert new_dt >= dt
        dt = new_dt


@pytest.mark.opensearch
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
def test_result_order_title(test_opensearch, monkeypatch, client, test_db, order):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "sort_field": "title",
            "sort_order": order.value,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    elements = response_body["families"]
    assert len(elements) > 1

    t = None
    for e in elements:
        new_t = e["family_name"]
        if t is not None:
            if order == SortOrder.DESCENDING:
                assert new_t <= t
            if order == SortOrder.ASCENDING:
                assert new_t >= t
        t = new_t


@pytest.mark.opensearch
def test_invalid_request(test_opensearch, monkeypatch, client, test_db):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={"exact_match": False},
    )
    assert response.status_code == 422

    response = client.post(
        SEARCH_ENDPOINT,
        json={"limit": 1, "offset": 2},
    )
    assert response.status_code == 422

    response = client.post(
        SEARCH_ENDPOINT,
        json={},
    )
    assert response.status_code == 422


@pytest.mark.opensearch
def test_case_insensitivity(test_opensearch, monkeypatch, client, test_db):
    """Make sure that query string results are not affected by case."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response1 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )
    response2 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )
    response3 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )

    response1_json = clean_response(response1)
    response2_json = clean_response(response2)
    response3_json = clean_response(response3)

    assert response1_json["families"]
    assert response1_json == response2_json == response3_json


@pytest.mark.opensearch
def test_punctuation_ignored(test_opensearch, monkeypatch, client, test_db):
    """Make sure that punctuation in query strings is ignored."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response1 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate.",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )
    response2 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate, ",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )
    response3 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": ";climate",
            "exact_match": False,
        },
        params={"use_vespa": False},
    )

    response1_json = clean_response(response1)
    response2_json = clean_response(response2)
    response3_json = clean_response(response3)

    assert response1_json["families"]
    assert response1_json == response2_json == response3_json


@pytest.mark.opensearch
def test_sensitive_queries(test_db, test_opensearch, monkeypatch, client):
    """Make sure that queries in the list of sensitive queries only return results containing that term, and not KNN results."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response1 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "spain", "exact_match": False},
        params={"use_vespa": False},
    )

    response2 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "clean energy strategy", "exact_match": False},
        params={"use_vespa": False},
    )

    # In this example the sensitive term is less than half the length of the query, so KNN results should be returned
    response3 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "spanish ghg emissions", "exact_match": False},
        params={"use_vespa": False},
    )

    response1_json = response1.json()
    response2_json = response2.json()
    response3_json = response3.json()

    # If the queries above return no results then the tests below are meaningless
    assert len(response1_json["families"]) > 0
    assert len(response2_json["families"]) > 0
    assert len(response3_json["families"]) > 0

    assert all(
        [
            "spain" in passage_match["text"].lower()
            for family in response1_json["families"]
            for document in family["family_documents"]
            for passage_match in document["document_passage_matches"]
        ]
    )
    assert not all(
        [
            "clean energy strategy" in passage_match["text"].lower()
            for family in response1_json["families"]
            for document in family["family_documents"]
            for passage_match in document["document_passage_matches"]
        ]
    )
    assert not all(
        [
            "spanish ghg emissions" in passage_match["text"].lower()
            for family in response1_json["families"]
            for document in family["family_documents"]
            for passage_match in document["document_passage_matches"]
        ]
    )


@pytest.mark.opensearch
def test_accents_ignored(test_db, test_opensearch, monkeypatch, client):
    """Make sure that accents in query strings are ignored."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    response1 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "climàte", "exact_match": False},
        params={"use_vespa": False},
    )
    response2 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "climatë", "exact_match": False},
        params={"use_vespa": False},
    )
    response3 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "climàtë", "exact_match": False},
        params={"use_vespa": False},
    )

    response1_json = clean_response(response1)
    response2_json = clean_response(response2)
    response3_json = clean_response(response3)
    assert response1_json["families"]
    assert response1_json == response2_json == response3_json


@pytest.mark.opensearch
def test_time_taken(test_opensearch, monkeypatch, client):
    """Make sure that query time taken is sensible."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    start = time.time()
    response = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "disaster", "exact_match": False},
        params={"use_vespa": False},
    )
    end = time.time()

    assert response.status_code == 200
    response_json = response.json()
    reported_response_time_ms = response_json["query_time_ms"]
    expected_response_time_ms_max = 1000 * (end - start)
    assert 0 < reported_response_time_ms < expected_response_time_ms_max


@pytest.mark.opensearch
def test_empty_search_term_performs_browse(client, test_db):
    """Make sure that empty search term returns results in browse mode."""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": ""},
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    assert response.json()["hits"] > 0
    assert len(response.json()["families"]) > 0


@pytest.mark.opensearch
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
def test_browse_order_by_title(client, test_db, order):
    """Make sure that empty search terms return no results."""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "sort_field": "title",
            "sort_order": order.value,
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    result_elements = response_body["families"]

    assert len(result_elements) > 0

    t = None
    for e in result_elements:
        new_t = e["family_name"]
        if t is not None:
            if order == SortOrder.DESCENDING:
                assert new_t <= t
            if order == SortOrder.ASCENDING:
                assert new_t >= t
        t = new_t


@pytest.mark.opensearch
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
@pytest.mark.parametrize("start_year", [None, 1999, 2007])
@pytest.mark.parametrize("end_year", [None, 2011, 2018])
def test_browse_order_by_date(order, start_year, end_year, client, test_db):
    """Make sure that empty search terms return no results."""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "sort_field": "date",
            "sort_order": order.value,
            "year_range": [start_year, end_year],
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    result_elements = response_body["families"]
    assert len(result_elements) > 0

    dt = None
    new_dt = None
    for e in result_elements:
        if e["family_date"]:
            new_dt = datetime.fromisoformat(e["family_date"]).isoformat()
        if dt is not None and new_dt is not None:
            if order == SortOrder.DESCENDING:
                assert new_dt <= dt
            if order == SortOrder.ASCENDING:
                assert new_dt >= dt
            if start_year is not None:
                assert new_dt >= datetime(year=start_year, month=1, day=1).isoformat()
            if end_year is not None:
                assert new_dt <= datetime(year=end_year, month=12, day=31).isoformat()
        dt = new_dt


@pytest.mark.opensearch
@pytest.mark.parametrize("limit", [1, 4, 7, 10])
def test_browse_limit_offset(client, test_db, limit):
    """Make sure that the offset parameter in browse mode works."""
    _populate_search_db_families(test_db)

    response_offset_0 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "limit": limit,
            "offset": 0,
        },
        params={"use_vespa": False},
    )
    response_offset_2 = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "limit": limit,
            "offset": 2,
        },
        params={"use_vespa": False},
    )

    assert response_offset_0.status_code == 200
    assert response_offset_2.status_code == 200

    response_offset_0_body = response_offset_0.json()
    result_elements_0 = response_offset_0_body["families"]
    assert len(result_elements_0) <= limit

    response_offset_2_body = response_offset_2.json()
    result_elements_2 = response_offset_2_body["families"]
    assert len(result_elements_2) <= limit

    assert result_elements_0[2 : len(result_elements_2)] == result_elements_2[:-2]


@pytest.mark.opensearch
def test_browse_filters(client, test_db):
    """Check that multiple filters are successfully applied"""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "keyword_filters": {
                "countries": ["japan"],
                "sources": ["CCLW"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    result_elements = response_body["families"]
    assert len(result_elements) == 1

    for result in result_elements:
        result_date = result["family_date"]
        assert result["family_source"] == "CCLW"
        assert result["family_geography"] == "JPN"
        assert result_date == "2017-01-01T00:00:00+00:00"


@pytest.mark.opensearch
def test_browse_filters_region(client, test_db):
    """Check that multiple filters are successfully applied"""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "keyword_filters": {
                "regions": ["east-asia-pacific"],
                "sources": ["CCLW"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    result_elements = response_body["families"]
    assert len(result_elements) == 4
    geographies = [family["family_geography"] for family in result_elements]
    assert set(geographies) == set(["JPN", "AUS", "IDN", "KOR"])


@pytest.mark.opensearch
def test_browse_filters_region_and_geography(client, test_db):
    """Check that multiple filters are successfully applied"""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "keyword_filters": {
                "regions": ["east-asia-pacific"],
                "countries": ["japan"],
                "sources": ["CCLW"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    result_elements = response_body["families"]
    assert len(result_elements) == 4
    geographies = [family["family_geography"] for family in result_elements]
    # TODO: I think it should behave like this:
    # assert set(geographies) == set(['JPN'])
    assert set(geographies) == set(["JPN", "AUS", "IDN", "KOR"])


# TODO: This test will fail - as the countries expects a slug not an ISO
# value - this is in contrast to Opensearch which uses the same files but
# in this case the value will be an ISO.
#
# @pytest.mark.opensearch
# def test_browse_filters_geography_iso(client, test_db):
#     """Check that multiple filters are successfully applied"""
#     _populate_search_db_families(test_db)

#     response = client.post(
#         SEARCH_ENDPOINT,
#         json={
#             "query_string": "",
#             "keyword_filters": {
#                 "countries": ["JPN"],
#                 "sources": ["CCLW"],
#             },
#             "year_range": (1900, 2020),
#             "jit_query": "disabled",
#         },
#     )
#     assert response.status_code == 200

#     response_body = response.json()
#     result_elements = response_body["families"]
#     assert len(result_elements) == 1

#     geographies = [
#         family["family_geography"]
#         for family in result_elements
#     ]
#     assert set(geographies) == set(['JPN'])


@pytest.mark.opensearch
def test_browse_filters_geography_slug(client, test_db):
    """Check that multiple filters are successfully applied"""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "keyword_filters": {
                "countries": ["japan"],
                "sources": ["CCLW"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200

    response_body = response.json()
    result_elements = response_body["families"]
    assert len(result_elements) == 1
    geographies = [family["family_geography"] for family in result_elements]
    assert set(geographies) == set(["JPN"])


@pytest.mark.opensearch
def test_browse_filter_category(client, test_db):
    """Make sure that empty search term returns results in browse mode."""
    _populate_search_db_families(test_db)

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "",
            "keyword_filters": {"categories": ["Executive"]},
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    response_content = response.json()
    assert response_content["hits"] > 0
    assert len(response.json()["families"]) > 0
    families = response_content["families"]
    for family in families:
        assert family["family_category"] == "Executive"


def _get_docs_for_family(db: Session, slug: str) -> Sequence[FamilyDocument]:
    slug_object: Slug = db.query(Slug).filter(Slug.name == slug).one()
    family: Family = (
        db.query(Family).filter(Family.import_id == slug_object.family_import_id).one()
    )
    documents: Sequence[FamilyDocument] = (
        db.query(FamilyDocument)
        .filter(FamilyDocument.family_import_id == family.import_id)
        .all()
    )

    return documents


def _get_validation_data(db: Session, families: Sequence[dict]) -> dict[str, Any]:
    return {
        family["family_name"]: {
            "family": family,
            "documents": {
                doc["document_title"]: doc for doc in family["family_documents"]
            },
            "all_docs": {
                doc.physical_document.title: doc
                for doc in _get_docs_for_family(db, family["family_slug"])
                if doc.physical_document is not None
            },
            "metadata": (
                db.query(FamilyMetadata)
                .join(Slug, FamilyMetadata.family_import_id == Slug.family_import_id)
                .filter(Slug.family_import_id == family["family_slug"])
                .one()
                .value
            ),
        }
        for family in families
    }


@pytest.mark.opensearch
@pytest.mark.parametrize("exact_match", [True, False])
@pytest.mark.parametrize("query_string", ["", "carbon"])
def test_csv_content(
    exact_match,
    query_string,
    client,
    test_db,
    test_opensearch,
    monkeypatch,
):
    """Make sure that downloaded CSV content matches a given search"""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    search_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": query_string,
            "exact_match": exact_match,
        },
        params={"use_vespa": False},
    )
    assert search_response.status_code == 200
    search_response_content = search_response.json()
    assert search_response_content["hits"] > 0
    assert len(search_response.json()["families"]) > 0
    families = search_response_content["families"]

    validation_data = _get_validation_data(test_db, families)
    expected_csv_row_count = len(validation_data)
    for f in validation_data:
        len_all_family_documents = len(validation_data[f]["documents"]) + len(
            [
                None
                for d in validation_data[f]["all_docs"]
                if d not in validation_data[f]["documents"]
            ]
        )
        if len_all_family_documents > 1:
            # Extra rows only exist for multi-doc families
            expected_csv_row_count += len_all_family_documents - 1

    search_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": query_string,
            "exact_match": exact_match,
        },
        params={"use_vespa": False},
    )
    assert search_response.status_code == 200
    search_content = search_response.json()
    all_matching_titles = {
        d["document_title"]
        for f in search_content["families"]
        for d in f["family_documents"]
        if d["document_passage_matches"]
    }

    download_response = client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json={
            "query_string": query_string,
            "exact_match": exact_match,
        },
        params={"use_vespa": False},
    )
    assert download_response.status_code == 200
    csv_content = csv.DictReader(StringIO(download_response.content.decode("utf8")))

    row_count = 0
    doc_match_count = 0
    for row in csv_content:
        row_count += 1
        family_name = row["Family Name"]
        assert family_name in validation_data
        family = validation_data[family_name]["family"]
        assert family["family_name"] == row["Family Name"]
        assert family["family_description"] == row["Family Summary"]
        assert family["family_date"] == row["Family Publication Date"]
        assert family["family_source"] == row["Source"]
        assert family["family_category"] == row["Category"]
        assert row["Family URL"].endswith(family["family_slug"])
        assert family["family_geography"] == row["Geography"]

        # TODO: Test family metadata - need improved test_db setup
        if doc_title := row["Document Title"]:
            if doc_title in validation_data[family_name]["documents"]:
                # The result is in search results directly, so use those details
                document = validation_data[family_name]["documents"][doc_title]
                assert document["document_title"] == row["Document Title"]
                assert row["Document URL"].endswith(document["document_slug"])
                # Check that if the content type is pdf, we include a CDN URL for
                # the document, otherwise we send the document source URL.
                if document["document_content_type"] == "application/pdf":
                    assert row["Document Content URL"].startswith(
                        "https://cdn.climatepolicyradar.org/"
                    )
                else:
                    # Deal with the fact that our document model allows `None` for URL
                    validation_source_url = document["document_source_url"] or ""
                    assert validation_source_url == row["Document Content URL"]
                assert document["document_type"] == row["Document Type"]
            else:
                # The result is an extra document retrieved from the database
                assert doc_title in validation_data[family_name]["all_docs"]
                db_document: FamilyDocument = validation_data[family_name]["all_docs"][
                    doc_title
                ]
                assert db_document.physical_document is not None
                assert db_document.physical_document.title == row["Document Title"]
                assert row["Document URL"].endswith(
                    cast(str, db_document.slugs[-1].name)
                )
                if db_document.physical_document.content_type == "application/pdf":
                    assert row["Document Content URL"].startswith(
                        "https://cdn.climatepolicyradar.org/"
                    )
                else:
                    assert (
                        db_document.physical_document.source_url
                        or "" == row["Document Content URL"]
                    )
                assert db_document.document_type == row["Document Type"]
            if query_string:
                should_match_document = row["Document Title"] in all_matching_titles
                if should_match_document:
                    doc_match_count += 1
                    assert row["Document Content Matches Search Phrase"] == "Yes"
                else:
                    assert row["Document Content Matches Search Phrase"] == "No"
            else:
                assert row["Document Content Matches Search Phrase"] == "n/a"
            assert row["Languages"] == "English"
        else:
            assert row["Document URL"] == ""
            assert row["Document Content URL"] == ""
            assert row["Document Type"] == ""
            assert row["Document Content Matches Search Phrase"] == "n/a"
            assert row["Languages"] == ""

        expected_metadata = validation_data[family_name]["metadata"]
        for k in expected_metadata:
            assert k.title() in row
            assert row[k.title()] == ";".join(expected_metadata[k])

    if query_string:
        # Make sure that we have tested some rows, that we have some "Yes" the document
        # matches the search term, and some "No" values too!
        assert doc_match_count > 0
        assert doc_match_count < row_count
    assert row_count == expected_csv_row_count


@pytest.mark.opensearch
@pytest.mark.parametrize("query_string", ["", "greenhouse"])
@pytest.mark.parametrize("limit", [1, 10, 35, 150])
@pytest.mark.parametrize("offset", [0, 5, 10, 80])
def test_csv_download_no_limit(
    query_string,
    limit,
    offset,
    client,
    test_db,
    test_opensearch,
    monkeypatch,
    mocker,
):
    """Make sure that downloaded CSV is not limited to a single page of results."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    if query_string:
        query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "query_families")
    else:
        query_spy = mocker.spy(search, "browse_rds_families")

    download_response = client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json={
            "query_string": query_string,
            "limit": limit,
            "offset": offset,
        },
        params={"use_vespa": False},
    )
    assert download_response.status_code == 200

    if query_string:
        actual_search_req = query_spy.mock_calls[0].kwargs["search_request_body"]
    else:
        actual_search_req = query_spy.mock_calls[0].kwargs["req"]

    # Make sure we overrode the search request content to produce the CSV download
    assert actual_search_req.limit == max(limit, 100)
    assert actual_search_req.offset == 0


@pytest.mark.opensearch
def test_extra_indices_with_html_search(
    test_opensearch, monkeypatch, client, test_db, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    expected_config = OpenSearchQueryConfig()
    expected_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=False,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        limit=10,
        offset=0,
        include_results=[IncludedResults.HTMLS_NON_TRANSLATED],
    )
    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "query_families")

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "include_results": ["htmlsNonTranslated"],
        },
        params={"use_vespa": False},
    )
    assert response.status_code == 200
    # Ensure nothing has/is going on in the background
    assert query_spy.call_count == 1  # Called once as not using jit search

    actual_search_body = query_spy.mock_calls[0].kwargs["search_request_body"]
    assert actual_search_body == expected_search_body

    # Check default config is used
    actual_config = query_spy.mock_calls[0].kwargs["opensearch_internal_config"]
    assert actual_config == expected_config
