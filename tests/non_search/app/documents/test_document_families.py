import logging
from typing import Optional

import pytest
from db_client.models.dfce.family import FamilyDocument
from db_client.models.document.physical_document import (
    Language,
    LanguageSource,
    PhysicalDocumentLanguage,
)
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import update
from sqlalchemy.orm import Session

from tests.non_search.setup_helpers import (
    setup_docs_with_two_orgs,
    setup_docs_with_two_orgs_no_langs,
    setup_with_two_docs,
    setup_with_two_docs_bad_ids,
    setup_with_two_docs_multiple_languages,
    setup_with_two_unpublished_docs,
)


def test_physical_doc_languages(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)

    response = data_client.get(
        "/api/v1/documents/DocSlug1",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert document["languages"] == ["eng"]

    response = data_client.get(
        "/api/v1/documents/DocSlug2",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    assert document["languages"] == []


def test_physical_doc_languages_not_visible(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)
    data_db.execute(update(PhysicalDocumentLanguage).values(visible=False))

    response = data_client.get(
        "/api/v1/documents/DocSlug1",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert document["languages"] == []


def test_physical_doc_multiple_languages(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs_multiple_languages(data_db)

    response = data_client.get(
        "/api/v1/documents/DocSlug1",
    )
    json_response = response.json()
    document = json_response["document"]

    assert response.status_code == 200
    print(json_response)
    assert set(document["languages"]) == set(["fra", "eng"])


def test_update_document_status__is_secure(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)

    import_id = "CCLW.executive.1.2"
    response = data_client.post(f"/api/v1/admin/documents/{import_id}/processed")
    assert response.status_code == 401


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.12",
        "UNFCCC.s.ill.y.2.2",
    ],
)
def test_update_document_status__fails_on_non_matching_import_id(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    setup_with_two_docs_bad_ids(data_db)

    response = data_client.post(
        f"/api/v1/admin/documents/{import_id}/processed",
        headers=data_superuser_token_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_update_document_status__publishes_document(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
):
    """Test that we can send a payload to the backend to update family_document.document_status"""
    setup_with_two_unpublished_docs(data_db)
    UPDATE_IMPORT_ID = "CCLW.executive.1.2"
    UNCHANGED_IMPORT_ID = "CCLW.executive.2.2"

    # State of db beforehand
    pre_family_status = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == UPDATE_IMPORT_ID)
        .one()
        .document_status
    )

    response = data_client.post(
        f"/api/v1/admin/documents/{UPDATE_IMPORT_ID}/processed",
        headers=data_superuser_token_headers,
    )

    assert response.status_code == 200
    json_object = response.json()

    assert json_object["import_id"] == UPDATE_IMPORT_ID
    assert json_object["document_status"] == "Published"

    # Now Check the db
    updated_family = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == UPDATE_IMPORT_ID)
        .one()
    )
    assert updated_family.document_status == "Published"
    assert updated_family.document_status != pre_family_status

    unchanged_family = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == UNCHANGED_IMPORT_ID)
        .one()
    )
    assert unchanged_family.document_status == "Deleted"


def test_update_document__is_secure(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)

    import_id = "CCLW.executive.1.2"
    payload = {
        "md5sum": "abc123",
        "content_type": "content_type",
        "source_url": "source_url",
    }

    response = data_client.put(f"/api/v1/admin/documents/{import_id}", json=payload)

    assert response.status_code == 401


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.12",
        "UNFCCC.s.ill.y.2.2",
    ],
)
def test_update_document__fails_on_non_matching_import_id(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    setup_with_two_docs_bad_ids(data_db)
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
        json=payload,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_import_id(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    setup_docs_with_two_orgs(data_db)

    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"

    # Now Check the db
    doc = (
        data_db.query(FamilyDocument)
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
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    """Send two payloads in series to assert that languages are additive and we don't remove existing languages."""
    setup_docs_with_two_orgs_no_langs(data_db)

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["eng"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = data_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "eng"

    # NOW ADD A NEW LANGUAGE TO CHECK THAT THE UPDATE IS ADDITIVE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["fra"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    doc_languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 2
    for doc_lang in doc_languages:
        lang = data_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_new_iso_639_1_language(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    """Send two payloads in series to assert that languages are additive and we don't remove existing languages."""
    setup_docs_with_two_orgs_no_langs(data_db)

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bo"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = data_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "bod"

    # NOW ADD A NEW LANGUAGE TO CHECK THAT THE UPDATE IS ADDITIVE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["el"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
        json=payload,
    )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"
    expected_languages = set(["ell", "bod"])
    assert set(json_object["languages"]) == expected_languages

    # Now Check the db
    doc = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    doc_languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 2
    for doc_lang in doc_languages:
        lang = data_db.query(Language).filter(Language.id == doc_lang.language_id).one()
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
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    """
    Assert that we can skip over existing languages for documents when using two letter iso codes.

    Send two payloads in series to assert that if we add a 639 two letter iso code where there is already a
    language entry for that physical document we don't throw an error. This proves that we can detect that the
    two-letter iso code language already exists.
    """
    setup_docs_with_two_orgs_no_langs(data_db)

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bod"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = data_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "bod"

    # NOW ADD THE SAME LANGUAGE AGAIN TO CHECK THAT THE UPDATE IS ADDITIVE AND WE SKIP OVER EXISTING LANGUAGES
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bo"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    doc_languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 1
    for doc_lang in doc_languages:
        lang = data_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__works_on_existing_iso_639_3_language(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
):
    """
    Assert that we can skip over existing languages for documents when using three letter iso codes.

    Send two payloads in series to assert that if we add a 639 three letter iso code where there is already a
    language entry for that physical document we don't throw an error. This proves that we can detect that the
    three-letter iso code language already exists.
    """
    setup_docs_with_two_orgs_no_langs(data_db)

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bo"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(languages) == 1
    lang = data_db.query(Language).filter(Language.id == languages[0].language_id).one()
    assert lang.language_code == "bod"

    # NOW ADD THE SAME LANGUAGE AGAIN TO CHECK THAT THE UPDATE IS ADDITIVE AND WE SKIP OVER EXISTING LANGUAGES
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["bod"],
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    doc_languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == 1
    for doc_lang in doc_languages:
        lang = data_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


@pytest.mark.parametrize(
    "import_id",
    [
        "CCLW.executive.1.2",
        "UNFCCC.non-party.2.2",
    ],
)
def test_update_document__logs_warning_on_four_letter_language(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
    caplog,
):
    """Send a payload to assert that languages that are too long aren't added and a warning is logged."""
    setup_docs_with_two_orgs_no_langs(data_db)

    # Payload with a four letter language code that won't exist in the db
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": ["boda"],
    }

    with caplog.at_level(logging.WARNING):
        response = data_client.put(
            f"/api/v1/admin/documents/{import_id}",
            headers=data_superuser_token_headers,
            json=payload,
        )

    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"
    assert json_object["languages"] == []

    assert (
        "Retrieved no language from database for meta_data object language"
        in caplog.text
    )

    # Now Check the db
    doc = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    languages = (
        data_db.query(PhysicalDocumentLanguage)
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
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
    languages: Optional[list[str]],
):
    """Test that we can send a payload to the backend with no languages to assert that none are added."""
    setup_docs_with_two_orgs_no_langs(data_db)

    # ADD THE FIRST LANGUAGE
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": languages,
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    db_languages = (
        data_db.query(PhysicalDocumentLanguage)
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
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
    import_id: str,
    existing_languages: list[str],
):
    """Test that we can send a payload to the backend with multiple languages to assert that both are added."""
    setup_docs_with_two_orgs_no_langs(data_db)

    for lang_code in existing_languages:
        existing_doc = (
            data_db.query(FamilyDocument)
            .filter(FamilyDocument.import_id == import_id)
            .one()
            .physical_document
        )
        existing_lang = (
            data_db.query(Language).filter(Language.language_code == lang_code).one()
        )
        existing_doc_lang = PhysicalDocumentLanguage(
            language_id=existing_lang.id,
            document_id=existing_doc.id,
            source=LanguageSource.MODEL,
        )
        data_db.add(existing_doc_lang)
        data_db.flush()
        data_db.commit()

    languages_to_add = ["eng", "fra"]
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
        "languages": languages_to_add,
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"

    doc_languages = (
        data_db.query(PhysicalDocumentLanguage)
        .filter(PhysicalDocumentLanguage.document_id == doc.id)
        .filter(PhysicalDocumentLanguage.source == LanguageSource.MODEL)
        .all()
    )
    assert len(doc_languages) == len(expected_languages)
    for doc_lang in doc_languages:
        lang = data_db.query(Language).filter(Language.id == doc_lang.language_id).one()
        assert lang.language_code in expected_languages


def test_update_document__idempotent(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
):
    setup_with_two_docs(data_db)

    import_id = "CCLW.executive.1.2"
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
        json=payload,
    )
    assert response.status_code == 200

    response = data_client.put(
        f"/api/v1/admin/documents/{import_id}",
        headers=data_superuser_token_headers,
        json=payload,
    )
    assert response.status_code == 200
    json_object = response.json()
    assert json_object["md5_sum"] == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert json_object["content_type"] == "updated/content_type"
    assert json_object["cdn_object"] == "folder/file"

    # Now Check the db
    doc = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"


def test_update_document__works_on_slug(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
):
    setup_with_two_docs(data_db)

    slug = "DocSlug1"
    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = data_client.put(
        f"/api/v1/admin/documents/{slug}",
        headers=data_superuser_token_headers,
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
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == import_id)
        .one()
        .physical_document
    )
    assert doc.md5_sum == "c184214e-4870-48e0-adab-3e064b1b0e76"
    assert doc.content_type == "updated/content_type"
    assert doc.cdn_object == "folder/file"


def test_update_document__status_422_when_not_found(
    data_client: TestClient,
    data_superuser_token_headers: dict[str, str],
    data_db: Session,
):
    setup_with_two_docs(data_db)

    payload = {
        "md5_sum": "c184214e-4870-48e0-adab-3e064b1b0e76",
        "content_type": "updated/content_type",
        "cdn_object": "folder/file",
    }

    response = data_client.put(
        "/api/v1/admin/documents/nothing",
        headers=data_superuser_token_headers,
        json=payload,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
