import pytest
from typing import Callable, Generator, Optional
from sqlalchemy import update
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from app.db.models.document.physical_document import (
    Language,
    LanguageSource,
    PhysicalDocumentLanguage,
)
from app.db.models.law_policy.family import Family, FamilyDocument, FamilyEvent
from tests.routes.document_helpers import (
    ONE_DFC_ROW_TWO_LANGUAGES,
    ONE_EVENT_ROW,
    TWO_DFC_ROW_DIFFERENT_ORG,
    TWO_DFC_ROW_ONE_LANGUAGE,
    TWO_DFC_ROW_NON_MATCHING_IDS,
    TWO_EVENT_ROWS,
    setup_with_docs,
    setup_with_multiple_docs,
    setup_with_two_docs,
    setup_with_two_docs_one_family,
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
    assert response.json()["detail"] == "Nothing found for FamSlug100"


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


def test_documents_family_slug_returns_multiple_docs(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs_one_family(test_db, mocker)

    response = client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response["documents"]) == 2


def test_documents_family_slug_returns_only_published_docs(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs_one_family(test_db, mocker)
    test_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.1.2")
        .values(document_status="Deleted")
    )

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response["documents"]) == 1


def test_documents_family_slug_returns_404_when_all_docs_deleted(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs_one_family(test_db, mocker)
    test_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.1.2")
        .values(document_status="Deleted")
    )
    test_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.2.2")
        .values(document_status="Deleted")
    )

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 404
    assert json_response["detail"] == "Family CCLW.family.1001.0 is not published"


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
    assert response.json()["detail"] == "Nothing found for DocSlug100"


def test_documents_doc_slug_preexisting_objects(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)

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


def test_documents_doc_slug_when_deleted(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)
    test_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.2.2")
        .values(document_status="Deleted")
    )
    response = client.get(
        "/api/v1/documents/DocSlug2",
    )
    json_response = response.json()
    assert response.status_code == 404
    assert json_response["detail"] == "The document CCLW.executive.2.2 is not published"


def test_physical_doc_languages(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
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


def test_physical_doc_languages_not_visible(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_ONE_LANGUAGE, event_data=TWO_EVENT_ROWS
    )
    test_db.execute(
        update(PhysicalDocumentLanguage)
        .where(PhysicalDocumentLanguage.document_id == 1)
        .values(visible=False)
    )

    response = client.get(
        "/api/v1/documents/DocSlug1",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert document["languages"] == []


def test_physical_doc_multiple_languages(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
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
    """Send two payloads in series to assert that languages are additive and we don't remove existing languages."""
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
    assert json_object["languages"] == ["eng"]

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
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = test_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "eng"

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
    expected_languages = ["eng", "fra"]
    assert json_object["languages"] == expected_languages

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

    doc_languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 2
    for doc_lang in doc_languages:
        lang = test_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_new_iso_639_1_language(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    """Send two payloads in series to assert that languages are additive and we don't remove existing languages."""
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bo"],
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
    assert json_object["languages"] == ["bod"]

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
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = test_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "bod"

    # NOW ADD A NEW LANGUAGE TO CHECK THAT THE UPDATE IS ADDITIVE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["el"],
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
    expected_languages = ["ell", "bod"]
    assert json_object["languages"] == expected_languages

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

    doc_languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 2
    for doc_lang in doc_languages:
        lang = test_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


# TODO Parametrize this test with the two languages as parameters
@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_existing_iso_639_1_language(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    """
    Assert that we can skip over existing languages for documents when using two letter iso codes.

    Send two payloads in series to assert that if we add a 639 two letter iso code where there is already a
    language entry for that physical document we don't throw an error. This proves that we can detect that the
    two-letter iso code language already exists.
    """
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bod"],
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
    assert json_object["languages"] == ["bod"]

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
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = test_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "bod"

    # NOW ADD THE SAME LANGUAGE AGAIN TO CHECK THAT THE UPDATE IS ADDITIVE AND WE SKIP OVER EXISTING LANGUAGES
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bo"],
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
    expected_languages = ["bod"]
    assert json_object["languages"] == expected_languages

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

    doc_languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 1
    for doc_lang in doc_languages:
        lang = test_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_existing_iso_639_3_language(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    """
    Assert that we can skip over existing languages for documents when using three letter iso codes.

    Send two payloads in series to assert that if we add a 639 three letter iso code where there is already a
    language entry for that physical document we don't throw an error. This proves that we can detect that the
    three-letter iso code language already exists.
    """
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bo"],
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
    assert json_object["languages"] == ["bod"]

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
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = test_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "bod"

    # NOW ADD THE SAME LANGUAGE AGAIN TO CHECK THAT THE UPDATE IS ADDITIVE AND WE SKIP OVER EXISTING LANGUAGES
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bod"],
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
    expected_languages = ["bod"]
    assert json_object["languages"] == expected_languages

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

    doc_languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 1
    for doc_lang in doc_languages:
        lang = test_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__logs_warning_on_four_letter_language(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
):
    """Send a payload to assert that languages that are too long aren't added and a warning is logged."""
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    # Payload with a four letter language code that won't exist in the db
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["boda"],
    }

    from app.api.api_v1.routers.admin import _LOGGER

    log_spy = mocker.spy(_LOGGER, "warning")

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
    assert json_object["languages"] == []

    assert (
        log_spy.call_args_list[0].args[0]
        == "Retrieved no language from database for meta_data object "
        "language"
    )
    assert len(log_spy.call_args_list) == 1

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
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 0


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
@pytest.mark.parametrize(
    "languages",
    [[], None],
)
def test_update_document__works_with_no_language(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
    languages: Optional[list[str]],
):
    """Test that we can send a payload to the backend with no languages to assert that none are added."""
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": languages,
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
    assert json_object["languages"] == []

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

    db_languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(db_languages) == 0
    assert set([lang.language_id for lang in db_languages]) == set()


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
@pytest.mark.parametrize(
    "existing_languages",
    [[], ["eng"], ["aaa"], ["aaa", "aab"]],
)
def test_update_document__works_existing_languages(
    client: TestClient,
    superuser_token_headers: dict[str, str],
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
    import_id: str,
    existing_languages: list[str],
):
    """Test that we can send a payload to the backend with multiple languages to assert that both are added."""
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW_DIFFERENT_ORG, event_data=TWO_EVENT_ROWS
    )

    for lang_code in existing_languages:
        existing_doc = (
            test_db.query(FamilyDocument)
            .filter(FamilyDocument.import_id == import_id)
            .one()
            .physical_document
        )
        existing_lang = (
            test_db.query(Language).filter(Language.language_code == lang_code).one()
        )
        existing_doc_lang = PhysicalDocumentLanguage(
            language_id=existing_lang.id,
            document_id=existing_doc.id,
            source=LanguageSource.MODEL,
        )
        test_db.add(existing_doc_lang)
        test_db.flush()
        test_db.commit()

    languages_to_add = ["eng", "fra"]
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": languages_to_add,
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

    expected_languages = set(languages_to_add + existing_languages)
    assert set(json_object["languages"]) == expected_languages

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

    doc_languages = (
        test_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == len(expected_languages)
    for doc_lang in doc_languages:
        lang = test_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


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
