from db_client.models.dfce.family import Family, FamilyDocument, FamilyEvent
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


def test_documents_family_slug_returns_not_found(
    data_db: Session,
    data_client: TestClient,
):
    setup_with_docs(data_db)
    assert data_db.query(Family).count() == 1
    assert data_db.query(FamilyEvent).count() == 1

    # Test by slug
    response = data_client.get(
        "/api/v1/documents/FamSlug100",
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Nothing found for FamSlug100"


def test_documents_family_slug_returns_correct_family(
    data_db: Session,
    data_client: TestClient,
):
    setup_with_two_docs(data_db)

    # Test by slug
    response = data_client.get(
        "/api/v1/documents/FamSlug1",
    )

    json_response = response.json()
    assert response.status_code == 200
    assert json_response["import_id"] == "CCLW.family.1001.0"

    # Ensure a different family is returned
    response = data_client.get(
        "/api/v1/documents/FamSlug2",
    )

    json_response = response.json()
    assert response.status_code == 200
    assert json_response["import_id"] == "CCLW.family.2002.0"


def test_documents_family_slug_returns_correct_json(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)

    # Test associations
    response = data_client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response) == N_FAMILY_KEYS
    assert json_response["organisation"] == "CCLW"
    assert json_response["import_id"] == "CCLW.family.1001.0"
    assert json_response["title"] == "Fam1"
    assert json_response["summary"] == "Summary1"
    assert json_response["geography"] == "Other"
    assert json_response["category"] == "Executive"
    assert json_response["status"] == "Published"
    assert json_response["corpus"] == "CCLW.corpus.i00000001.n0000"
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
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs_one_family(data_db)

    response = data_client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response["documents"]) == 2


def test_documents_family_slug_returns_only_published_docs(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs_one_family(data_db)
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.1.2")
        .values(document_status="Deleted")
    )

    # Test associations
    response = data_client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response["documents"]) == 1


def test_documents_family_slug_returns_404_when_all_docs_deleted(
    data_client: TestClient,
    data_db: Session,
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
    response = data_client.get(
        "/api/v1/documents/FamSlug1",
    )
    json_response = response.json()

    assert response.status_code == 404
    assert json_response["detail"] == "Family CCLW.family.1001.0 is not published"


def test_documents_doc_slug_returns_not_found(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_docs(data_db)
    assert data_db.query(Family).count() == 1
    assert data_db.query(FamilyEvent).count() == 1

    # Test associations
    response = data_client.get(
        "/api/v1/documents/DocSlug100",
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Nothing found for DocSlug100"


def test_documents_doc_slug_preexisting_objects(
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)

    response = data_client.get(
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
    assert family["geography"] == "Other"
    assert family["category"] == "Executive"
    assert family["slug"] == "FamSlug2"
    assert family["corpus"] == "CCLW.corpus.i00000001.n0000"
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
    data_client: TestClient,
    data_db: Session,
):
    setup_with_two_docs(data_db)
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.2.2")
        .values(document_status="Deleted")
    )
    response = data_client.get(
        "/api/v1/documents/DocSlug2",
    )
    json_response = response.json()
    assert response.status_code == 404
    assert json_response["detail"] == "The document CCLW.executive.2.2 is not published"
