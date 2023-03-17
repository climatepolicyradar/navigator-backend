import dataclasses
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import fastapi
import pytest
from sqlalchemy.orm import Session

import app.core
import app.core.jit_query_wrapper
from app.api.api_v1.routers import search
from app.api.api_v1.schemas.search import (
    FilterField,
    JitQuery,
    SortOrder,
    SearchRequestBody,
)
from app.core.search import _FILTER_FIELD_MAP, OpenSearchQueryConfig
from app.db.models.app import Organisation
from app.db.models.deprecated import Category, Document, Source, DocumentType
from app.db.models.law_policy.family import (
    DocumentStatus,
    FamilyCategory,
    FamilyStatus,
    Family,
    FamilyDocument,
    FamilyDocumentType,
    FamilyOrganisation,
    Geography,
    Slug,
    Variant,
)
from app.db.models.document import PhysicalDocument
from app.initial_data import populate_geography
from tests.routes.test_documents_deprecated import create_4_documents

SEARCH_ENDPOINT = "/api/v1/searches"


def _populate_search_db_documents(db: Session) -> None:
    documents: dict[str, Document] = {}
    document_types: dict[str, DocumentType] = {}
    categories: dict[str, Category] = {}

    populate_geography(db)
    source = Source(name="CCLW")
    db.add(source)
    db.commit()
    db.refresh(source)

    containing_dir = Path(__file__).parent
    data_dir = containing_dir.parent / "data"
    for f in data_dir.iterdir():
        if f.is_file() and f.suffixes == [".json"]:
            with open(f, "r") as of:
                for line in of.readlines():
                    search_document = json.loads(line)
                    doc_details = search_document["_source"]

                    doc_id = doc_details["document_id"]
                    if doc_id in documents:
                        continue

                    if doc_details["document_category"] not in categories:
                        doc_category = Category(
                            name=doc_details["document_category"],
                            description="Really doesn't matter",
                        )
                        db.add(doc_category)
                        db.commit()
                        db.refresh(doc_category)
                        categories[doc_details["document_category"]] = doc_category

                    if doc_details["document_type"] not in document_types:
                        doc_type = DocumentType(
                            name=doc_details["document_type"],
                            description="Doesn't matter",
                        )
                        db.add(doc_type)
                        db.commit()
                        db.refresh(doc_type)
                        document_types[doc_details["document_type"]] = doc_type

                    geography_id = (
                        db.query(Geography)
                        .filter(Geography.value == doc_details["document_geography"])
                        .one()
                        .id
                    )
                    doc = Document(
                        publication_ts=datetime.strptime(
                            doc_details["document_date"], "%d/%m/%Y"
                        ),
                        name=doc_details["document_name"],
                        description=doc_details["document_description"],
                        source_url=doc_details["document_source_url"],
                        source_id=source.id,
                        url=doc_details["document_cdn_object"],
                        md5_sum=doc_details["document_md5_sum"],
                        slug=doc_details["document_slug"],
                        import_id=doc_details["document_id"],
                        geography_id=geography_id,
                        type_id=document_types[doc_details["document_type"]].id,
                        category_id=categories[doc_details["document_category"]].id,
                    )
                    db.add(doc)
                    db.commit()
                    db.refresh(doc)
                    documents[doc_id] = doc


def _populate_search_db_families(db: Session) -> None:
    documents: dict[str, FamilyDocument] = {}
    families: dict[str, Family] = {}

    populate_geography(db)

    original = Variant(variant_name="Original Language", description="")
    translated = Variant(variant_name="Official Translation", description="")
    variants: dict[str, Variant] = {
        "translated_True": translated,
        "translated_False": original,
    }
    document_type = FamilyDocumentType(
        name="Strategy",
        description="",
    )
    organisation = Organisation(
        name="CCLW", description="CCLW", organisation_type="CCLW Type"
    )
    db.add(original)
    db.add(translated)
    db.add(document_type)
    db.add(organisation)
    db.commit()
    db.refresh(organisation)

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
                        document_type,
                        organisation,
                    )


def _doc_str_to_category(doc_category: str) -> FamilyCategory:
    if doc_category.lower() == "law":
        return FamilyCategory.LEGISLATIVE
    if doc_category.lower() == "policy":
        return FamilyCategory.EXECUTIVE
    raise RuntimeError(f"Unknown category string: '{doc_category}'")


def _create_family_structures(
    db: Session,
    doc: dict[str, Any],
    documents: dict[str, FamilyDocument],
    families: dict[str, Family],
    variants: dict[str, Variant],
    document_type: FamilyDocumentType,
    organisation: Organisation,
) -> None:
    """Populate a db to match the test search index code"""

    doc_details = doc["_source"]
    doc_id = doc_details["document_id"]
    if doc_id in documents:
        return

    doc_id_components = doc_id.split(".")
    family_id = f"CCLW.family.{doc_id_components[2]}.0"  # assume single family

    if family_id not in families:
        family = Family(
            title=doc_details["document_name"],
            import_id=family_id,
            description=doc_details["document_description"],
            geography_id=(
                db.query(Geography)
                .filter(Geography.value == doc_details["document_geography"])
                .one()
                .id
            ),
            family_status=FamilyStatus.PUBLISHED,
            family_category=_doc_str_to_category(doc_details["document_category"]),
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

    physical_document = PhysicalDocument(
        title=doc_details["document_name"],
        cdn_object=None,
        md5_sum=None,
        source_url=None,
        content_type=None,
    )
    db.add(physical_document)
    db.commit()
    db.refresh(physical_document)
    family_document = FamilyDocument(
        family_import_id=family_id,
        physical_document_id=physical_document.id,
        import_id=doc_id,
        variant_name=variants[f"translated_{doc_details['translated']}"].variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=document_type.name,
    )
    family_document_slug = Slug(
        name=doc_id,
        family_import_id=None,
        family_document_import_id=doc_id,
    )
    db.add(family_document)
    db.commit()
    db.add(family_document_slug)
    db.commit()
    db.refresh(family_document)
    documents[doc_id] = family_document


@pytest.mark.search
def test_simple_pagination_families(test_opensearch, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)
    search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"

    page1_response = client.post(
        search_endpoint,
        json={
            "query_string": "and",
            "exact_match": False,
            "limit": 2,
            "offset": 0,
        },
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    page1_families = page1_response_body["families"]
    assert len(page1_families) == 2

    page2_response = client.post(
        search_endpoint,
        json={
            "query_string": "and",
            "exact_match": False,
            "limit": 2,
            "offset": 2,
        },
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


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
@pytest.mark.parametrize("exact_match", [True, False])
def test_search_body_valid(
    group_documents, exact_match, test_opensearch, monkeypatch, client, test_db
):
    """Test a simple known valid search responds with success."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={
            "query_string": "disaster",
            "exact_match": exact_match,
        },
    )
    assert response.status_code == 200


@pytest.mark.search
def test_jit_query_families_is_default(
    test_opensearch, monkeypatch, client, test_db, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)
    search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"

    jit_query_spy = mocker.spy(app.core.jit_query_wrapper, "jit_query_families")  # noqa
    background_task_spy = mocker.spy(fastapi.BackgroundTasks, "add_task")

    response = client.post(
        search_endpoint,
        json={"query_string": "climate", "exact_match": True},
    )
    assert response.status_code == 200

    # Check the jit query called by checking the background task has been added
    assert jit_query_spy.call_count == 1 or jit_query_spy.call_count == 2
    assert background_task_spy.call_count == 1


@pytest.mark.search
def test_families_with_jit(test_opensearch, monkeypatch, client, test_db, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)
    search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"

    jit_query_spy = mocker.spy(app.core.jit_query_wrapper, "jit_query_families")
    background_task_spy = mocker.spy(fastapi.BackgroundTasks, "add_task")

    response = client.post(
        search_endpoint,
        json={"query_string": "climate", "exact_match": True},
    )

    assert response.status_code == 200

    # Check the jit query call
    assert jit_query_spy.call_count == 1 or jit_query_spy.call_count == 2
    actual_search_body = jit_query_spy.mock_calls[0].args[1]
    actual_config = jit_query_spy.mock_calls[0].args[2]

    expected_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        jit_query=JitQuery.ENABLED,
        limit=10,
        offset=0,
    )
    assert actual_search_body == expected_search_body

    # Check the first call has overriden the default config
    overrides = {
        "max_doc_count": 20,
    }
    expected_config = dataclasses.replace(OpenSearchQueryConfig(), **overrides)
    assert actual_config == expected_config

    # Check the background query call
    assert background_task_spy.call_count == 1
    actual_bkg_search_body = background_task_spy.mock_calls[0].args[3]

    expected_bkg_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        jit_query=JitQuery.ENABLED,
        limit=10,
        offset=0,
    )
    assert actual_bkg_search_body == expected_bkg_search_body

    # Check the background call is run with default config
    actual_bkg_config = background_task_spy.mock_calls[0].args[4]
    assert actual_bkg_config == OpenSearchQueryConfig()


@pytest.mark.search
def test_families_without_jit(test_opensearch, monkeypatch, client, test_db, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "query_families")
    background_task_spy = mocker.spy(fastapi.BackgroundTasks, "add_task")

    response = client.post(
        f"{SEARCH_ENDPOINT}?group_documents=True",
        json={
            "query_string": "climate",
            "exact_match": True,
            "jit_query": "disabled",
        },
    )
    assert response.status_code == 200
    # Ensure nothing has/is going on in the background
    assert background_task_spy.call_count == 0
    assert query_spy.call_count == 1  # Called once as not using jit search

    actual_search_body = query_spy.mock_calls[0].args[0]

    expected_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        jit_query=JitQuery.DISABLED,
        limit=10,
        offset=0,
    )
    assert actual_search_body == expected_search_body

    # Check default config is used
    actual_config = query_spy.mock_calls[0].args[1]
    expected_config = OpenSearchQueryConfig()
    assert actual_config == expected_config


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_keyword_filters(
    group_documents, test_opensearch, client, test_db, monkeypatch, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        search_endpoint = SEARCH_ENDPOINT

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
            "keyword_filters": {"countries": ["kenya"]},
            "jit_query": "disabled",
        },
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("countries")]: ["KEN"]}
    } in query_body["query"]["bool"]["filter"]


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_keyword_filters_region(
    group_documents, test_opensearch, test_db, monkeypatch, client, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        search_endpoint = SEARCH_ENDPOINT

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
            "keyword_filters": {"regions": ["south-asia"]},
            "jit_query": "disabled",
        },
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


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_keyword_filters_region_invalid(
    group_documents, test_opensearch, monkeypatch, client, test_db, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
            "keyword_filters": {"regions": ["daves-region"]},
            "jit_query": "disabled",
        },
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    # The region is invalid, so no filters should be applied
    assert "filter" not in query_body["query"]["bool"]


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_invalid_keyword_filters(
    group_documents, test_opensearch, test_db, monkeypatch, client
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={
            "query_string": "disaster",
            "exact_match": False,
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
@pytest.mark.parametrize("group_documents", [True, False])
def test_year_range_filters(
    year_range,
    group_documents,
    test_opensearch,
    monkeypatch,
    client,
    test_db,
    mocker,
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "disaster",
            "exact_match": False,
            "year_range": year_range,
            "jit_query": "disabled",
        },
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


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_multiple_filters(
    group_documents, test_opensearch, test_db, monkeypatch, client, mocker
):
    """Check that multiple filters are successfully applied"""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        search_endpoint = SEARCH_ENDPOINT

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "disaster",
            "exact_match": False,
            "keyword_filters": {
                "countries": ["kenya"],
                "sources": ["CCLW"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
    )
    assert response.status_code == 200
    assert query_spy.call_count == 1
    query_body = query_spy.mock_calls[0].args[0]

    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("countries")]: ["KEN"]}
    } in query_body["query"]["bool"]["filter"]
    assert {
        "terms": {_FILTER_FIELD_MAP[FilterField("sources")]: ["CCLW"]}
    } in query_body["query"]["bool"]["filter"]
    assert {
        "range": {"document_date": {"gte": "01/01/1900", "lte": "31/12/2020"}}
    } in query_body["query"]["bool"]["filter"]


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_result_order_score(
    group_documents, test_opensearch, monkeypatch, client, test_db, mocker
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "disaster",
            "exact_match": False,
        },
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


@pytest.mark.search
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
@pytest.mark.parametrize("group_documents", [False])  # FIXME: add family ordering
def test_result_order_date(
    group_documents, test_opensearch, monkeypatch, client, test_db, order
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
            "sort_field": "date",
            "sort_order": order.value,
        },
    )
    assert response.status_code == 200

    response_body = response.json()
    if group_documents:
        elements = response_body["families"]
    else:
        elements = response_body["documents"]
    assert len(elements) > 1

    dt = None
    for e in elements:
        if group_documents:
            new_dt = datetime.strptime(e["family_date"], "%d/%m/%Y")
        else:
            new_dt = datetime.strptime(e["document_date"], "%d/%m/%Y")
        if dt is not None:
            if order == SortOrder.DESCENDING:
                assert new_dt <= dt
            if order == SortOrder.ASCENDING:
                assert new_dt >= dt
        dt = new_dt


@pytest.mark.search
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
@pytest.mark.parametrize("group_documents", [True, False])
def test_result_order_title(
    group_documents, test_opensearch, monkeypatch, client, test_db, order
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
            "sort_field": "title",
            "sort_order": order.value,
        },
    )
    assert response.status_code == 200

    response_body = response.json()
    if group_documents:
        elements = response_body["families"]
    else:
        elements = response_body["documents"]
    assert len(elements) > 1

    t = None
    for e in elements:
        if group_documents:
            new_t = e["family_name"]
        else:
            new_t = e["document_name"]
        if t is not None:
            if order == SortOrder.DESCENDING:
                assert new_t <= t
            if order == SortOrder.ASCENDING:
                assert new_t >= t
        t = new_t


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_invalid_request(
    group_documents, test_opensearch, monkeypatch, client, test_db
):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={"exact_match": False},
    )
    assert response.status_code == 422

    response = client.post(
        search_endpoint,
        json={"limit": 1, "offset": 2},
    )
    assert response.status_code == 422

    response = client.post(
        search_endpoint,
        json={},
    )
    assert response.status_code == 422


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_case_insensitivity(
    group_documents, test_opensearch, monkeypatch, client, test_db
):
    """Make sure that query string results are not affected by case."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    response1 = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
    )
    response2 = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
    )
    response3 = client.post(
        search_endpoint,
        json={
            "query_string": "climate",
            "exact_match": False,
        },
    )

    response1_json = response1.json()
    del response1_json["query_time_ms"]
    response2_json = response2.json()
    del response2_json["query_time_ms"]
    response3_json = response3.json()
    del response3_json["query_time_ms"]

    if group_documents:
        assert response1_json["families"]
    else:
        assert response1_json["documents"]
    assert response1_json == response2_json == response3_json


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_punctuation_ignored(
    group_documents, test_opensearch, monkeypatch, client, test_db
):
    """Make sure that punctuation in query strings is ignored."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        search_endpoint = SEARCH_ENDPOINT

    response1 = client.post(
        search_endpoint,
        json={
            "query_string": "climate.",
            "exact_match": False,
        },
    )
    response2 = client.post(
        search_endpoint,
        json={
            "query_string": "climate, ",
            "exact_match": False,
        },
    )
    response3 = client.post(
        search_endpoint,
        json={
            "query_string": ";climate",
            "exact_match": False,
        },
    )

    response1_json = response1.json()
    del response1_json["query_time_ms"]
    response2_json = response2.json()
    del response2_json["query_time_ms"]
    response3_json = response3.json()
    del response3_json["query_time_ms"]

    if group_documents:
        assert response1_json["families"]
    else:
        assert response1_json["documents"]
    assert response1_json == response2_json == response3_json


@pytest.mark.search
def test_sensitive_queries(test_opensearch, monkeypatch, client):
    """Make sure that queries in the list of sensitive queries only return results containing that term, and not KNN results."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    response1 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "spain", "exact_match": False},
    )

    response2 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "clean energy strategy", "exact_match": False},
    )

    # In this example the sensitive term is less than half the length of the query, so KNN results should be returned
    response3 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "spanish ghg emissions", "exact_match": False},
    )

    response1_json = response1.json()
    response2_json = response2.json()
    response3_json = response3.json()

    # If the queries above return no results then the tests below are meaningless
    assert len(response1_json["documents"]) > 0
    assert len(response2_json["documents"]) > 0
    assert len(response3_json["documents"]) > 0

    assert all(
        [
            "spain" in passage_match["text"].lower()
            for document in response1_json["documents"]
            for passage_match in document["document_passage_matches"]
        ]
    )
    assert not all(
        [
            "clean energy strategy" in passage_match["text"].lower()
            for document in response2_json["documents"]
            for passage_match in document["document_passage_matches"]
        ]
    )
    assert not all(
        [
            "spanish ghg emissions" in passage_match["text"].lower()
            for document in response3_json["documents"]
            for passage_match in document["document_passage_matches"]
        ]
    )


@pytest.mark.search
def test_accents_ignored(test_opensearch, monkeypatch, client):
    """Make sure that accents in query strings are ignored."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    response1 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "climàte", "exact_match": False},
    )
    response2 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "climatë", "exact_match": False},
    )
    response3 = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "climàtë", "exact_match": False},
    )

    response1_json = response1.json()
    del response1_json["query_time_ms"]
    response2_json = response2.json()
    del response2_json["query_time_ms"]
    response3_json = response3.json()
    del response3_json["query_time_ms"]

    assert response1_json["documents"]
    assert response1_json == response2_json == response3_json


@pytest.mark.search
def test_time_taken(test_opensearch, monkeypatch, client):
    """Make sure that query time taken is sensible."""
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    start = time.time()
    response = client.post(
        SEARCH_ENDPOINT,
        json={"query_string": "disaster", "exact_match": False},
    )
    end = time.time()

    assert response.status_code == 200
    response_json = response.json()
    reported_response_time_ms = response_json["query_time_ms"]
    expected_response_time_ms_max = 1000 * (end - start)
    assert 0 < reported_response_time_ms < expected_response_time_ms_max


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_empty_search_term_performs_browse(
    group_documents,
    client,
    test_db,
):
    """Make sure that empty search term returns results in browse mode."""
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        create_4_documents(test_db)
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={"query_string": ""},
    )
    assert response.status_code == 200
    assert response.json()["hits"] > 0
    if group_documents:
        assert len(response.json()["families"]) > 0
    else:
        assert len(response.json()["documents"]) > 0


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
def test_browse_order_by_title(
    group_documents,
    client,
    test_db,
    order,
):
    """Make sure that empty search terms return no results."""
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        create_4_documents(test_db)
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={
            "query_string": "",
            "sort_field": "title",
            "sort_order": order.value,
        },
    )
    assert response.status_code == 200

    response_body = response.json()
    if group_documents:
        result_elements = response_body["families"]
    else:
        result_elements = response_body["documents"]

    assert len(result_elements) > 0

    t = None
    for e in result_elements:
        if group_documents:
            new_t = e["family_name"]
        else:
            new_t = e["document_name"]
        if t is not None:
            if order == SortOrder.DESCENDING:
                assert new_t <= t
            if order == SortOrder.ASCENDING:
                assert new_t >= t
        t = new_t


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [False])  # FIXME: add ', True'
@pytest.mark.parametrize("order", [SortOrder.ASCENDING, SortOrder.DESCENDING])
def test_browse_order_by_date(
    group_documents,
    client,
    test_db,
    order,
):
    """Make sure that empty search terms return no results."""
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        create_4_documents(test_db)
        search_endpoint = SEARCH_ENDPOINT

    response = client.post(
        search_endpoint,
        json={
            "query_string": "",
            "sort_field": "date",
            "sort_order": order.value,
        },
    )
    assert response.status_code == 200

    response_body = response.json()
    if group_documents:
        result_elements = response_body["families"]
    else:
        result_elements = response_body["documents"]
    assert len(result_elements) > 0

    dt = None
    for e in result_elements:
        if group_documents:
            new_dt = datetime.fromisoformat(e["family_date"])
        else:
            new_dt = datetime.fromisoformat(e["document_date"])
        if dt is not None:
            if order == SortOrder.DESCENDING:
                assert new_dt <= dt
            if order == SortOrder.ASCENDING:
                assert new_dt >= dt
        dt = new_dt


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
@pytest.mark.parametrize("limit", [1, 4, 7, 10])
def test_browse_limit_offset(
    group_documents,
    # test_opensearch,
    # monkeypatch,
    client,
    test_db,
    limit,
):
    """Make sure that the offset parameter in browse mode works."""
    # monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        search_endpoint = SEARCH_ENDPOINT

    response_offset_0 = client.post(
        search_endpoint,
        json={
            "query_string": "",
            "limit": limit,
            "offset": 0,
        },
    )
    response_offset_2 = client.post(
        search_endpoint,
        json={
            "query_string": "",
            "limit": limit,
            "offset": 2,
        },
    )

    assert response_offset_0.status_code == 200
    assert response_offset_2.status_code == 200

    response_offset_0_body = response_offset_0.json()
    if group_documents:
        result_elements_0 = response_offset_0_body["families"]
    else:
        result_elements_0 = response_offset_0_body["documents"]
    assert len(result_elements_0) <= limit

    response_offset_2_body = response_offset_2.json()
    if group_documents:
        result_elements_2 = response_offset_2_body["families"]
    else:
        result_elements_2 = response_offset_2_body["documents"]
    assert len(result_elements_2) <= limit

    assert result_elements_0[2 : len(result_elements_2)] == result_elements_2[:-2]


@pytest.mark.search
@pytest.mark.parametrize("group_documents", [True, False])
def test_browse_filters(group_documents, client, test_db):
    """Check that multiple filters are successfully applied"""
    if group_documents:
        _populate_search_db_families(test_db)
        search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"
    else:
        populate_geography(test_db)
        search_endpoint = SEARCH_ENDPOINT

    # query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "raw_query")
    response = client.post(
        search_endpoint,
        json={
            "query_string": "",
            "keyword_filters": {
                "countries": ["kenya"],
                "sources": ["CCLW"],
            },
            "year_range": (1900, 2020),
            "jit_query": "disabled",
        },
    )
    assert response.status_code == 200

    # FIXME: Check that filters are applied
    # assert query_spy.call_count == 1
    # query_body = query_spy.mock_calls[0].args[0]

    # assert {
    #     "terms": {_FILTER_FIELD_MAP[FilterField("countries")]: ["KEN"]}
    # } in query_body["query"]["bool"]["filter"]
    # assert {
    #     "terms": {_FILTER_FIELD_MAP[FilterField("sources")]: ["CCLW"]}
    # } in query_body["query"]["bool"]["filter"]
    # assert {
    #     "range": {"document_date": {"gte": "01/01/1900", "lte": "31/12/2020"}}
    # } in query_body["query"]["bool"]["filter"]

    response_body = response.json()
    if group_documents:
        result_elements = response_body["families"]
    else:
        result_elements = response_body["documents"]
    assert len(result_elements) == 0


##########################################
############# DEPRECATED #################
##########################################
@pytest.mark.search
def test_simple_pagination(test_opensearch, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    page1_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "limit": 2,
            "offset": 0,
        },
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    page1_documents = page1_response_body["documents"]
    assert len(page1_documents) == 2

    page2_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "limit": 2,
            "offset": 2,
        },
    )
    assert page2_response.status_code == 200

    page2_response_body = page2_response.json()
    page2_documents = page2_response_body["documents"]
    assert len(page2_documents) == 2

    # Sanity check that we really do have 4 different documents
    document_slugs = {d["document_slug"] for d in page1_documents} | {
        d["document_slug"] for d in page2_documents
    }
    assert len(document_slugs) == 4

    for d in page1_documents:
        assert d not in page2_documents


@pytest.mark.search
def test_search_result_schema(caplog, test_opensearch, monkeypatch, client):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    expected_search_result_schema = sorted(
        [
            "document_name",
            "document_postfix",
            "document_geography",
            "document_source",
            "document_sectors",
            "document_date",
            "document_id",
            "document_slug",
            "document_description",
            "document_type",
            "document_category",
            "document_source_url",
            "document_url",
            "document_content_type",
            "document_title_match",
            "document_description_match",
            "document_passage_matches",
        ]
    )
    page1_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "limit": 100,
            "offset": 0,
        },
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    page1_documents = page1_response_body["documents"]
    assert len(page1_documents) > 0

    for d in page1_documents:
        assert sorted(list(d.keys())) == expected_search_result_schema

    assert "Document ids missing" in caplog.text


@pytest.mark.search
def test_pagination_overlap(test_opensearch, monkeypatch, client):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)

    page1_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "limit": 2,
            "offset": 0,
        },
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    page1_documents = page1_response_body["documents"]
    assert len(page1_documents) > 1

    page2_response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": False,
            "limit": 2,
            "offset": 1,
        },
    )
    assert page2_response.status_code == 200

    page2_response_body = page2_response.json()
    page2_documents = page2_response_body["documents"]
    assert len(page2_documents) > 0

    # Check that page 2 documents are different to page 1 documents
    assert len(
        {d["document_slug"] for d in page1_documents}
        | {d["document_slug"] for d in page2_documents}
    ) > len({d["document_slug"] for d in page1_documents})

    assert page1_documents[-1] == page2_documents[0]


@pytest.mark.search
def test_jit_query_is_default(test_opensearch, monkeypatch, client, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    jit_query_spy = mocker.spy(app.core.jit_query_wrapper, "jit_query")  # noqa
    background_task_spy = mocker.spy(fastapi.BackgroundTasks, "add_task")

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": True,
        },
    )
    assert response.status_code == 200

    # Check the jit query called by checking the background task has been added
    assert jit_query_spy.call_count == 1 or jit_query_spy.call_count == 2
    assert background_task_spy.call_count == 1


@pytest.mark.search
def test_with_jit(test_opensearch, monkeypatch, client, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    jit_query_spy = mocker.spy(app.core.jit_query_wrapper, "jit_query")
    background_task_spy = mocker.spy(fastapi.BackgroundTasks, "add_task")

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": True,
        },
    )

    assert response.status_code == 200

    # Check the jit query call
    assert jit_query_spy.call_count == 1 or jit_query_spy.call_count == 2
    actual_search_body = jit_query_spy.mock_calls[0].args[1]
    actual_config = jit_query_spy.mock_calls[0].args[2]

    expected_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        jit_query=JitQuery.ENABLED,
        limit=10,
        offset=0,
    )
    assert actual_search_body == expected_search_body

    # Check the first call has overriden the default config
    overrides = {
        "max_doc_count": 20,
    }
    expected_config = dataclasses.replace(OpenSearchQueryConfig(), **overrides)
    assert actual_config == expected_config

    # Check the background query call
    assert background_task_spy.call_count == 1
    actual_bkg_search_body = background_task_spy.mock_calls[0].args[3]

    expected_bkg_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        jit_query=JitQuery.ENABLED,
        limit=10,
        offset=0,
    )
    assert actual_bkg_search_body == expected_bkg_search_body

    # Check the background call is run with default config
    actual_bkg_config = background_task_spy.mock_calls[0].args[4]
    assert actual_bkg_config == OpenSearchQueryConfig()


@pytest.mark.search
def test_without_jit(test_opensearch, monkeypatch, client, mocker):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    query_spy = mocker.spy(search._OPENSEARCH_CONNECTION, "query")
    background_task_spy = mocker.spy(fastapi.BackgroundTasks, "add_task")

    response = client.post(
        SEARCH_ENDPOINT,
        json={
            "query_string": "climate",
            "exact_match": True,
            "jit_query": "disabled",
        },
    )
    assert response.status_code == 200
    # Ensure nothing has/is going on in the background
    assert background_task_spy.call_count == 0
    assert query_spy.call_count == 1  # Called once as not using jit search

    actual_search_body = query_spy.mock_calls[0].args[0]

    expected_search_body = SearchRequestBody(
        query_string="climate",
        exact_match=True,
        max_passages_per_doc=10,
        keyword_filters=None,
        year_range=None,
        sort_field=None,
        sort_order=SortOrder.DESCENDING,
        jit_query=JitQuery.DISABLED,
        limit=10,
        offset=0,
    )
    assert actual_search_body == expected_search_body

    # Check default config is used
    actual_config = query_spy.mock_calls[0].args[1]
    expected_config = OpenSearchQueryConfig()
    assert actual_config == expected_config


@pytest.mark.search
def test_search_response_family(test_opensearch, client, test_db, monkeypatch):
    monkeypatch.setattr(search, "_OPENSEARCH_CONNECTION", test_opensearch)
    _populate_search_db_families(test_db)
    search_endpoint = f"{SEARCH_ENDPOINT}?group_documents=True"

    page1_response = client.post(
        search_endpoint,
        json={
            "query_string": "and",
            "exact_match": False,
            "limit": 2,
            "offset": 0,
        },
    )
    assert page1_response.status_code == 200

    page1_response_body = page1_response.json()
    fam1 = page1_response_body["families"][0]
    doc1 = fam1["family_documents"][0]
    slug = doc1["document_slug"]
    print()
    print(">>>> " + slug)
    print()
    assert doc1["document_slug"].startswith("fd_") and "should be from FamilyDocument"
