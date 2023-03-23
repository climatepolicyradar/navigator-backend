from typing import Callable, Generator

import pytest
from sqlalchemy.orm import Session

from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from app.db.models.document.physical_document import Language, PhysicalDocument
from app.db.models.law_policy.family import Family, FamilyEvent
from tests.routes.document_helpers import (
    TWO_DFC_ROW_ONE_LANGUAGE,
    populate_languages,
    setup_with_docs,
    setup_with_two_docs,
)

N_METADATA_KEYS = 6
N_FAMILY_KEYS = 14


def test_documents_family_slug_returns_not_found(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_docs(test_db, mocker)
    assert test_db.query(Family).count() == 1
    assert test_db.query(FamilyEvent).count() == 1

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug100?group_documents=True",
    )
    assert response.status_code == 404


def test_documents_family_slug_returns_correct_family(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug1?group_documents=True",
    )

    json_response = response.json()
    assert response.status_code == 200
    assert json_response["import_id"] == "CCLW.family.1001.0"

    # Ensure a different family is returned
    response = client.get(
        "/api/v1/documents/FamSlug2?group_documents=True",
    )

    json_response = response.json()
    assert response.status_code == 200
    assert json_response["import_id"] == "CCLW.family.2002.0"


def test_documents_family_slug_returns_correct_json(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug1?group_documents=True",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response) == N_FAMILY_KEYS
    assert json_response["organisation"] == "CCLW"
    assert json_response["import_id"] == "CCLW.family.1001.0"
    assert json_response["title"] == "Fam1"
    assert json_response["summary"] == "Summary1"
    assert json_response["geography"] == "GEO"
    assert json_response["category"] == "Executive"
    assert json_response["status"] == "Published"
    assert json_response["published_date"] == "2019-12-25T00:00:00+00:00"
    assert json_response["last_updated_date"] == "2019-12-25T00:00:00+00:00"

    assert len(json_response["metadata"]) == N_METADATA_KEYS
    assert json_response["metadata"]["keyword"] == ["Energy Supply"]

    assert len(json_response["slugs"]) == 1
    assert json_response["slugs"][0] == "FamSlug1"

    assert len(json_response["events"]) == 1
    assert json_response["events"][0]["title"] == "Published"

    assert len(json_response["documents"]) == 1
    assert json_response["documents"][0]["title"] == "Title1"
    assert json_response["documents"][0]["slugs"] == ["DocSlug1"]
    assert json_response["documents"][0]["import_id"] == "CCLW.executive.1.2"

    assert len(json_response["collections"]) == 1
    assert json_response["collections"][0]["title"] == "Collection1"

    assert json_response["collections"][0]["families"] == [
        {"title": "Fam1", "slug": "FamSlug1", "description": "Summary1"},
        {"title": "Fam2", "slug": "FamSlug2", "description": "Summary2"},
    ]


def test_documents_doc_slug_returns_not_found(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_docs(test_db, mocker)
    assert test_db.query(Family).count() == 1
    assert test_db.query(FamilyEvent).count() == 1

    # Test associations
    response = client.get(
        "/api/v1/documents/DocSlug100?group_documents=True",
    )
    assert response.status_code == 404


def test_documents_doc_slug_preexisting_objects(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)
    assert test_db.query(PhysicalDocument).count() == 2
    assert test_db.query(Family).count() == 2
    assert test_db.query(FamilyEvent).count() == 2

    # Test associations
    response = client.get(
        "/api/v1/documents/DocSlug2?group_documents=True",
    )
    json_response = response.json()
    assert response.status_code == 200
    assert len(json_response) == 2

    family = json_response["family"]
    assert family
    assert len(family.keys()) == 7
    assert family["title"] == "Fam2"
    assert family["import_id"] == "CCLW.family.2002.0"
    assert family["geography"] == "GEO"
    assert family["category"] == "Executive"
    assert len(family["slugs"]) == 1
    assert family["slugs"][0] == "FamSlug2"
    assert family["published_date"] == "2019-12-25T00:00:00+00:00"
    assert family["last_updated_date"] == "2019-12-25T00:00:00+00:00"

    doc = json_response["document"]
    assert doc
    assert len(doc) == 11
    assert doc["import_id"] == "CCLW.executive.2.2"
    assert doc["variant"] is None
    assert doc["slugs"] == ["DocSlug2"]
    assert doc["title"] == "Title2"
    assert doc["md5_sum"] is None
    assert doc["cdn_object"] == "https://cdn.climatepolicyradar.org/"
    assert doc["source_url"] == "http://another_somewhere"
    assert doc["language"] == ""
    assert doc["document_type"] == "Order"
    assert doc["document_role"] == "MAIN"


@pytest.mark.languages
def test_physical_doc_languages(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    populate_languages(test_db)
    setup_with_two_docs(test_db, mocker, doc_data=TWO_DFC_ROW_ONE_LANGUAGE)

    assert test_db.query(Language).count() == 3
    assert test_db.query(PhysicalDocument).count() == 2
    assert test_db.query(Family).count() == 2
    assert test_db.query(FamilyEvent).count() == 2

    response = client.get(
        "/api/v1/documents/DocSlug1?group_documents=True",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert document["language"] == "eng"

    response = client.get(
        "/api/v1/documents/DocSlug2?group_documents=True",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    assert document["language"] == ""
