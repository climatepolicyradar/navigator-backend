from unittest.mock import patch

import pytest
from db_client.models.dfce import Slug
from db_client.models.dfce.family import FamilyDocument
from sqlalchemy.orm import Session

from app.service import search
from tests.search.vespa.setup_search_tests import (
    _make_search_request,
    _populate_db_families,
)


def _doc_ids_from_response(test_db: Session, response: dict) -> list[str]:
    """The response doesnt know about ids, so we look them up using the slug"""
    document_ids = []
    for fam in response["families"]:
        for doc in fam["family_documents"]:
            family_document = (
                test_db.query(FamilyDocument)
                .join(Slug, Slug.family_document_import_id == FamilyDocument.import_id)
                .filter(Slug.name == doc["document_slug"])
                .one()
            )
            document_ids.append(family_document.import_id)

    return document_ids


def _fam_ids_from_response(test_db, response) -> list[str]:
    """The response doesnt know about ids, so we look them up using the slug"""
    family_ids = []
    for fam in response["families"]:
        family_document = (
            test_db.query(FamilyDocument)
            .join(Slug, Slug.family_import_id == FamilyDocument.family_import_id)
            .filter(Slug.name == fam["family_slug"])
            .one()
        )
        family_ids.append(family_document.family_import_id)
    return family_ids


@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize(
    "family_ids",
    [
        ["CCLW.family.1385.0"],
        ["CCLW.family.10246.0", "CCLW.family.8633.0"],
        ["CCLW.family.10246.0", "CCLW.family.8633.0", "UNFCCC.family.1267.0"],
    ],
)
@pytest.mark.search
def test_family_ids_search(
    mock_corpora_exist_in_db,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    family_ids,
    valid_token,
):

    _populate_db_families(data_db)

    params = {
        "query_string": "the",
        "family_ids": family_ids,
    }

    response = _make_search_request(data_client, valid_token, params)

    got_family_ids = _fam_ids_from_response(data_db, response)
    assert sorted(got_family_ids) == sorted(family_ids)
    assert mock_corpora_exist_in_db.assert_called


@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize(
    "document_ids",
    [
        ["CCLW.executive.1385.5336"],
        ["CCLW.executive.10246.4861", "UNFCCC.non-party.1267.0"],
        [
            "CCLW.executive.8633.3052",
            "UNFCCC.non-party.1267.0",
            "CCLW.executive.10246.4861",
        ],
    ],
)
@pytest.mark.search
def test_document_ids_search(
    mock_corpora_exist_in_db,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    document_ids,
    valid_token,
):

    _populate_db_families(data_db)

    params = {
        "query_string": "the",
        "document_ids": document_ids,
    }
    response = _make_search_request(data_client, valid_token, params)

    got_document_ids = _doc_ids_from_response(data_db, response)
    assert sorted(got_document_ids) == sorted(document_ids)
    assert mock_corpora_exist_in_db.assert_called


@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.search
def test_document_ids_and_family_ids_search(
    mock_corpora_exist_in_db, test_vespa, data_db, monkeypatch, data_client, valid_token
):

    _populate_db_families(data_db)

    # The doc doesnt belong to the family, so we should get no results
    family_ids = ["UNFCCC.family.1267.0"]
    document_ids = ["CCLW.executive.10246.4861"]
    params = {
        "query_string": "the",
        "family_ids": family_ids,
        "document_ids": document_ids,
    }

    response = _make_search_request(data_client, valid_token, params)
    assert len(response["families"]) == 0
    assert mock_corpora_exist_in_db.assert_called


@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.search
def test_empty_ids_dont_limit_result(
    mock_corpora_exist_in_db, test_vespa, data_db, monkeypatch, data_client, valid_token
):

    _populate_db_families(data_db)

    # We'd expect this to be interpreted as 'unlimited'
    params = {
        "query_string": "the",
        "family_ids": [],
        "document_ids": [],
    }

    response = _make_search_request(data_client, valid_token, params)

    got_document_ids = _doc_ids_from_response(data_db, response)
    got_family_ids = _fam_ids_from_response(data_db, response)

    assert len(got_family_ids) > 1
    assert len(got_document_ids) > 1
    assert mock_corpora_exist_in_db.assert_called

