import pytest
from typing import Callable, Generator
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from app.db.models.document.physical_document import PhysicalDocumentLanguage
from app.db.models.law_policy.family import Family, FamilyDocument, FamilyEvent
from tests.routes.document_helpers import (
    ONE_DFC_ROW_TWO_LANGUAGES,
    ONE_EVENT_ROW,
    TWO_DFC_ROW_DIFFERENT_ORG,
    TWO_DFC_ROW_ONE_LANGUAGE,
    TWO_DFC_ROW_NON_MATCHING_IDS,
    TWO_EVENT_ROWS,
    populate_languages,
    setup_with_docs,
    setup_with_multiple_docs,
    setup_with_two_docs,
)

N_METADATA_KEYS = 6
N_FAMILY_KEYS = 14
N_FAMILY_OVERVIEW_KEYS = 7
N_DOCUMENT_KEYS = 12


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
        "/api/v1/documents/FamSlug100",
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
        "/api/v1/documents/FamSlug1",
    )

    json_response = response.json()
    assert response.status_code == 200
    assert json_response["import_id"] == "CCLW.family.1001.0"

    # Ensure a different family is returned
    response = client.get(
        "/api/v1/documents/FamSlug2",
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
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response) == N_FAMILY_KEYS
    assert json_response["organisation"] == "CCLW"
    assert json_response["import_id"] == "CCLW.family.1001.0"
    assert json_response["title"] == "Fam1"
    assert json_response["summary"] == "Summary1"
    assert json_response["geography"] == "GBR"
    assert json_response["category"] == "Executive"
    assert json_response["status"] == "Published"
    assert json_response["published_date"] == "2019-12-25T00:00:00+00:00"
    assert json_response["last_updated_date"] == "2019-12-25T00:00:00+00:00"

    assert len(json_response["metadata"]) == N_METADATA_KEYS
    assert json_response["metadata"]["keyword"] == ["Energy Supply"]

    assert json_response["slug"] == "FamSlug1"

    assert len(json_response["events"]) == 1
    assert json_response["events"][0]["title"] == "Published"

    assert len(json_response["documents"]) == 1
    assert json_response["documents"][0]["title"] == "Title1"
    assert json_response["documents"][0]["slug"] == "DocSlug1"
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
        "/api/v1/documents/DocSlug100",
    )
    assert response.status_code == 404


def test_documents_doc_slug_preexisting_objects(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    # Test associations
    response = client.get(
        "/api/v1/documents/DocSlug2",
    )
    json_response = response.json()
    assert response.status_code == 200
    assert len(json_response) == 2

    family = json_response["family"]
    assert family
    assert len(family.keys()) == N_FAMILY_OVERVIEW_KEYS
    assert family["title"] == "Fam2"
    assert family["import_id"] == "CCLW.family.2002.0"
    assert family["geography"] == "GBR"
    assert family["category"] == "Executive"
    assert family["slug"] == "FamSlug2"
    assert family["published_date"] == "2019-12-25T00:00:00+00:00"
    assert family["last_updated_date"] == "2019-12-25T00:00:00+00:00"

    doc = json_response["document"]
    assert doc
    assert len(doc) == N_DOCUMENT_KEYS
    assert doc["import_id"] == "CCLW.executive.2.2"
    assert doc["variant"] is None
    assert doc["slug"] == "DocSlug2"
    assert doc["title"] == "Title2"
    assert doc["md5_sum"] is None
    assert doc["cdn_object"] is None
    assert doc["content_type"] is None
    assert doc["source_url"] == "http://another_somewhere"
    assert doc["language"] == ""
    assert doc["languages"] == []
    assert doc["document_type"] == "Order"
    assert doc["document_role"] == "MAIN"


def test_physical_doc_languages(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    populate_languages(test_db)
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_ONE_LANGUAGE, event_data=TWO_EVENT_ROWS
    )

    response = client.get(
        "/api/v1/documents/DocSlug1",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert document["languages"] == ["eng"]

    response = client.get(
        "/api/v1/documents/DocSlug2",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    assert document["languages"] == []


def test_physical_doc_multiple_languages(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    populate_languages(test_db)
    setup_with_multiple_docs(
        test_db, mocker, doc_data=ONE_DFC_ROW_TWO_LANGUAGES, event_data=ONE_EVENT_ROW
    )

    response = client.get(
        "/api/v1/documents/DocSlug1",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert set(document["languages"]) == set(["fra", "eng"])


def test_update_document__is_secure(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    import_id = "CCLW.executive.1.2"
    payload = {
        "md5sum": "abc123",
        "content_type": "content_type",
        "source_url": "source_url",
    }

    response = client.put(f"/api/v1/admin/documents/{import_id}", json=payload)

    assert response.status_code == 401


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.12",
        "UNFCCC.s.ill.y.2.2",
    ],
)
def test_update_document__fails_on_non_matching_import_id(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    setup_with_multiple_docs(
        test_db,
        mocker,
        doc_data=TWO_DFC_ROW_NON_MATCHING_IDS,
        event_data=TWO_EVENT_ROWS,
    )
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_import_id(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"

    # Now Check the db
    doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_new_language(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    populate_languages(test_db)
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["eng"],
    }

    response = client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"
    assert {language['language_code'] for language in json_object["languages"]} == {"eng"}

    # Now Check the db
    doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .all()
    )
    assert len(languages) == 1
    assert set([l.language_id for l in languages]) == {2}

    # NOW ADD A NEW LANGUAGE TO CHECK THAT THE UPDATE IS ADDITIVE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["fra"],
    }

    response = client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"
    assert {language['language_code'] for language in json_object["languages"]} == {"eng", "fra"}

    # Now Check the db
    doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .all()
    )
    assert len(languages) == 2
    assert set(l.language_id for l in languages) == {2, 1}


def test_update_document__idempotent(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    import_id = "CCLW.executive.1.2"
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=superuser_token_headers,
        json=payload,
    )
    assert response.status_code == 200

    response = client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=superuser_token_headers,
        json=payload,
    )
    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"

    # Now Check the db
    doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"


def test_update_document__works_on_slug(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    slug = "DocSlug1"
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = client.put(
        f"/api/v1/admin/documents/{slug}",
        headers=superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"

    # Now Check the db
    import_id = "CCLW.executive.1.2"
    doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"


def test_update_document__status_422_when_not_found(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = client.put(
        "/api/v1/admin/documents/nothing",
        headers=superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 422
