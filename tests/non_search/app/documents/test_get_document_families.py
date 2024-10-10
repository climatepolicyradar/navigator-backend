from db_client.models.dfce.family import Family, FamilyDocument, FamilyEvent
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import update
from sqlalchemy.orm import Session

from tests.non_search.setup_helpers import (
    setup_with_docs,
    setup_with_two_docs,
    setup_with_two_docs_one_family,
)

N_FAMILY_KEYS = 15
N_FAMILY_OVERVIEW_KEYS = 8
N_DOCUMENT_KEYS = 12

DOCUMENTS_ENDPOINT = "/api/v1/documents"


def _make_get_family_or_doc_via_slug_request(
    client,
    token,
    family_slug: str,
    expected_status_code: int = status.HTTP_200_OK,
):
    headers = {"app-token": token}

    response = client.get(
        f"{DOCUMENTS_ENDPOINT}/{family_slug}",
        headers=headers,
    )
    assert response is not None
    assert response.status_code == expected_status_code, response.text
    return response.json()


def test_documents_family_slug_returns_not_found(
    data_db: Session, data_client: TestClient, valid_token
):
    setup_with_docs(data_db)
    assert data_db.query(Family).count() == 1
    assert data_db.query(FamilyEvent).count() == 1

    # Test by slug
    response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug100", status.HTTP_404_NOT_FOUND
    )
    assert response["detail"] == "Nothing found for FamSlug100"


def test_documents_family_slug_returns_correct_family(
    data_db: Session, data_client: TestClient, valid_token
):
    setup_with_two_docs(data_db)

    # Test by slug
    response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug1", status.HTTP_200_OK
    )
    assert response["import_id"] == "CCLW.family.1001.0"

    # Ensure a different family is returned
    response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug2", status.HTTP_200_OK
    )
    assert response["import_id"] == "CCLW.family.2002.0"


def test_documents_family_slug_returns_correct_json(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs(data_db)

    # Test associations
    json_response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug1", status.HTTP_200_OK
    )
    assert len(json_response) == N_FAMILY_KEYS
    assert json_response["organisation"] == "CCLW"
    assert json_response["import_id"] == "CCLW.family.1001.0"
    assert json_response["title"] == "Fam1"
    assert json_response["summary"] == "Summary1"
    assert json_response["geography"] == "South Asia"
    assert json_response["category"] == "Executive"
    assert json_response["status"] == "Published"
    assert json_response["corpus_id"] == "CCLW.corpus.i00000001.n0000"
    assert json_response["published_date"] == "2019-12-25T00:00:00Z"
    assert json_response["last_updated_date"] == "2019-12-25T00:00:00Z"

    # TODO: https://linear.app/climate-policy-radar/issue/PDCT-1017
    assert len(json_response["metadata"]) == 2
    assert json_response["metadata"]["size"] == "big"

    assert json_response["slug"] == "FamSlug1"

    assert len(json_response["events"]) == 1
    assert json_response["events"][0]["title"] == "Published"

    assert len(json_response["documents"]) == 1
    assert json_response["documents"][0]["title"] == "Document1"
    assert json_response["documents"][0]["slug"] == "DocSlug1"
    assert json_response["documents"][0]["import_id"] == "CCLW.executive.1.2"

    assert len(json_response["collections"]) == 1
    assert json_response["collections"][0]["title"] == "Collection1"

    assert json_response["collections"][0]["families"] == [
        {"title": "Fam1", "slug": "FamSlug1", "description": "Summary1"},
        {"title": "Fam2", "slug": "FamSlug2", "description": "Summary2"},
    ]


def test_documents_family_slug_returns_multiple_docs(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs_one_family(data_db)

    json_response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug1", status.HTTP_200_OK
    )
    assert len(json_response["documents"]) == 2


def test_documents_family_slug_returns_only_published_docs(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs_one_family(data_db)
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.1.2")
        .values(document_status="Deleted")
    )

    # Test associations
    json_response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug1", status.HTTP_200_OK
    )
    assert len(json_response["documents"]) == 1


def test_documents_family_slug_returns_404_when_all_docs_deleted(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs_one_family(data_db)
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.1.2")
        .values(document_status="Deleted")
    )
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.2.2")
        .values(document_status="Deleted")
    )

    # Test associations
    json_response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "FamSlug1", status.HTTP_404_NOT_FOUND
    )
    assert json_response["detail"] == "Family CCLW.family.1001.0 is not published"


def test_documents_doc_slug_returns_not_found(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_docs(data_db)
    assert data_db.query(Family).count() == 1
    assert data_db.query(FamilyEvent).count() == 1

    # Test associations
    response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "DocSlug100", status.HTTP_404_NOT_FOUND
    )
    assert response["detail"] == "Nothing found for DocSlug100"


def test_documents_doc_slug_preexisting_objects(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs(data_db)

    json_response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "DocSlug2", status.HTTP_200_OK
    )
    assert len(json_response) == 2

    family = json_response["family"]
    assert family
    assert len(family.keys()) == N_FAMILY_OVERVIEW_KEYS
    assert family["title"] == "Fam2"
    assert family["import_id"] == "CCLW.family.2002.0"
    assert family["geography"] == "AFG"
    assert family["category"] == "Executive"
    assert family["slug"] == "FamSlug2"
    assert family["corpus_id"] == "CCLW.corpus.i00000001.n0000"
    assert family["published_date"] == "2019-12-25T00:00:00Z"
    assert family["last_updated_date"] == "2019-12-25T00:00:00Z"

    doc = json_response["document"]
    assert doc
    assert len(doc) == N_DOCUMENT_KEYS
    assert doc["import_id"] == "CCLW.executive.2.2"
    assert doc["variant"] is None
    assert doc["slug"] == "DocSlug2"
    assert doc["title"] == "Document2"
    assert doc["md5_sum"] is None
    assert doc["cdn_object"] is None
    assert doc["content_type"] is None
    assert doc["source_url"] == "http://another_somewhere"
    assert doc["language"] == ""
    assert doc["languages"] == []
    assert doc["document_type"] == "Order"
    assert doc["document_role"] == "MAIN"


def test_documents_doc_slug_when_deleted(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs(data_db)
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.2.2")
        .values(document_status="Deleted")
    )
    json_response = _make_get_family_or_doc_via_slug_request(
        data_client, valid_token, "DocSlug2", status.HTTP_404_NOT_FOUND
    )
    assert json_response["detail"] == "The document CCLW.executive.2.2 is not published"
