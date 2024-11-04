import pytest
from db_client.models.dfce.family import Family, FamilyDocument, FamilyEvent
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import update
from sqlalchemy.orm import Session

from tests.non_search.routers.documents.setup_doc_fam_lookup import (
    _make_doc_fam_lookup_request,
)
from tests.non_search.setup_helpers import (
    setup_with_docs,
    setup_with_two_docs,
    setup_with_two_docs_one_family,
)

N_FAMILY_KEYS = 15
N_FAMILY_OVERVIEW_KEYS = 8
N_DOCUMENT_KEYS = 12


def test_documents_family_slug_returns_not_found(
    data_db: Session, data_client: TestClient, valid_token
):
    setup_with_docs(data_db)
    assert data_db.query(Family).count() == 1
    assert data_db.query(FamilyEvent).count() == 1

    # Test by slug
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug100",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "Nothing found for FamSlug100"


def test_documents_family_slug_returns_correct_family(
    data_db: Session, data_client: TestClient, valid_token
):
    setup_with_two_docs(data_db)

    # Test by slug
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug1",
    )
    assert json_response["import_id"] == "CCLW.family.1001.0"

    # Ensure a different family is returned
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug2",
    )
    assert json_response["import_id"] == "CCLW.family.2002.0"


@pytest.mark.parametrize(
    ("slug", "expected_fam", "expected_doc"),
    [
        (
            "FamSlug1",
            {
                "title": "Fam1",
                "import_id": "CCLW.family.1001.0",
                "geographies": ["South Asia"],
                "category": "Executive",
                "slug": "FamSlug1",
                "corpus_id": "CCLW.corpus.i00000001.n0000",
                "published_date": "2019-12-25T00:00:00Z",
                "last_updated_date": "2019-12-25T00:00:00Z",
                "metadata": {"color": "pink", "size": "big"},
                "organisation": "CCLW",
                "status": "Published",
                "summary": "Summary1",
            },
            {
                "import_id": "CCLW.executive.1.2",
                "variant": "Original Language",
                "slug": "DocSlug1",
                "title": "Document1",
                "md5_sum": "111",
                "cdn_object": None,
                "content_type": "application/pdf",
                "source_url": "http://somewhere1",
                "language": "eng",
                "languages": ["eng"],
                "document_type": "Plan",
                "document_role": "MAIN",
            },
        ),
        (
            "FamSlug2",
            {
                "title": "Fam2",
                "import_id": "CCLW.family.2002.0",
                "geographies": ["AFG", "IND"],
                "category": "Executive",
                "slug": "FamSlug2",
                "corpus_id": "CCLW.corpus.i00000001.n0000",
                "published_date": "2019-12-25T00:00:00Z",
                "last_updated_date": "2019-12-25T00:00:00Z",
                "metadata": {"color": "blue", "size": "small"},
                "organisation": "CCLW",
                "status": "Published",
                "summary": "Summary2",
            },
            {
                "import_id": "CCLW.executive.2.2",
                "variant": None,
                "slug": "DocSlug2",
                "title": "Document2",
                "md5_sum": None,
                "cdn_object": None,
                "content_type": None,
                "source_url": "http://another_somewhere",
                "language": "",
                "languages": [],
                "document_type": "Order",
                "document_role": "MAIN",
            },
        ),
    ],
)
def test_documents_family_slug_returns_correct_json(
    data_client: TestClient,
    data_db: Session,
    slug,
    expected_fam,
    expected_doc,
    valid_token,
):
    setup_with_two_docs(data_db)

    # Test associations
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        slug,
    )

    # Verify family data correct.
    assert len(json_response) == N_FAMILY_KEYS
    actual_family_data = {
        k: v
        for k, v in json_response.items()
        if k not in ["events", "documents", "collections"]
    }
    assert actual_family_data == expected_fam

    # Verify events data correct.
    events = json_response["events"]
    assert len(json_response["events"]) == 1
    event = events[0]
    assert event["title"] == "Published"

    # Verify documents data correct.
    docs = json_response["documents"]
    assert len(docs) == 1
    doc = docs[0]
    assert len(doc.keys()) == N_DOCUMENT_KEYS
    assert doc == expected_doc

    # Verify collections data correct.
    collections = json_response["collections"]
    assert len(json_response["collections"]) == 1
    collection = collections[0]
    assert collection["title"] == "Collection1"
    assert collection["families"] == [
        {"title": "Fam1", "slug": "FamSlug1", "description": "Summary1"},
        {"title": "Fam2", "slug": "FamSlug2", "description": "Summary2"},
    ]


def test_documents_family_slug_returns_multiple_docs(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs_one_family(data_db)

    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug1",
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
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug1",
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
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug1",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "Family CCLW.family.1001.0 is not published"


def test_documents_doc_slug_returns_not_found(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_docs(data_db)
    assert data_db.query(Family).count() == 1
    assert data_db.query(FamilyEvent).count() == 1

    # Test associations
    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "DocSlug100",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "Nothing found for DocSlug100"


@pytest.mark.parametrize(
    ("slug", "expected_fam", "expected_doc"),
    [
        (
            "DocSlug1",
            {
                "title": "Fam1",
                "import_id": "CCLW.family.1001.0",
                "geographies": ["South Asia"],
                "category": "Executive",
                "slug": "FamSlug1",
                "corpus_id": "CCLW.corpus.i00000001.n0000",
                "published_date": "2019-12-25T00:00:00Z",
                "last_updated_date": "2019-12-25T00:00:00Z",
            },
            {
                "import_id": "CCLW.executive.1.2",
                "variant": "Original Language",
                "slug": "DocSlug1",
                "title": "Document1",
                "md5_sum": "111",
                "cdn_object": None,
                "content_type": "application/pdf",
                "source_url": "http://somewhere1",
                "language": "eng",
                "languages": ["eng"],
                "document_type": "Plan",
                "document_role": "MAIN",
            },
        ),
        (
            "DocSlug2",
            {
                "title": "Fam2",
                "import_id": "CCLW.family.2002.0",
                "geographies": ["AFG", "IND"],
                "category": "Executive",
                "slug": "FamSlug2",
                "corpus_id": "CCLW.corpus.i00000001.n0000",
                "published_date": "2019-12-25T00:00:00Z",
                "last_updated_date": "2019-12-25T00:00:00Z",
            },
            {
                "import_id": "CCLW.executive.2.2",
                "variant": None,
                "slug": "DocSlug2",
                "title": "Document2",
                "md5_sum": None,
                "cdn_object": None,
                "content_type": None,
                "source_url": "http://another_somewhere",
                "language": "",
                "languages": [],
                "document_type": "Order",
                "document_role": "MAIN",
            },
        ),
    ],
)
def test_documents_doc_slug_preexisting_objects(
    data_client: TestClient,
    data_db: Session,
    slug,
    expected_fam,
    expected_doc,
    valid_token,
):
    setup_with_two_docs(data_db)

    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        slug,
    )
    assert len(json_response) == 2

    family = json_response["family"]
    assert family
    assert len(family.keys()) == N_FAMILY_OVERVIEW_KEYS
    assert family == expected_fam

    doc = json_response["document"]
    assert doc
    assert len(doc) == N_DOCUMENT_KEYS
    assert doc == expected_doc


def test_documents_doc_slug_when_deleted(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs(data_db)
    data_db.execute(
        update(FamilyDocument)
        .where(FamilyDocument.import_id == "CCLW.executive.2.2")
        .values(document_status="Deleted")
    )

    json_response = _make_doc_fam_lookup_request(
        data_client,
        valid_token,
        "DocSlug2",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "The document CCLW.executive.2.2 is not published"
