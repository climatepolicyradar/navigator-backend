import pytest
from db_client.models.dfce.family import Family, FamilyDocument, FamilyEvent
from db_client.models.document.physical_document import PhysicalDocumentLanguage
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from tests.non_search.routers.documents.setup_doc_fam_lookup import (
    _make_doc_fam_lookup_request,
)
from tests.non_search.setup_helpers import (
    setup_with_docs,
    setup_with_two_docs,
    setup_with_two_docs_multiple_languages,
)

N_FAMILY_OVERVIEW_KEYS = 8
N_DOCUMENT_KEYS = 12


def test_physical_doc_languages(data_client: TestClient, data_db: Session, valid_token):
    setup_with_two_docs(data_db)

    json_response = _make_doc_fam_lookup_request(data_client, valid_token, "DocSlug1")
    document = json_response["document"]
    print(json_response)
    assert document["languages"] == ["eng"]

    json_response = _make_doc_fam_lookup_request(data_client, valid_token, "DocSlug2")
    document = json_response["document"]
    assert document["languages"] == []


def test_physical_doc_languages_not_visible(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs(data_db)
    data_db.execute(update(PhysicalDocumentLanguage).values(visible=False))

    json_response = _make_doc_fam_lookup_request(data_client, valid_token, "DocSlug1")
    document = json_response["document"]
    print(json_response)
    assert document["languages"] == []


def test_physical_doc_multiple_languages(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_two_docs_multiple_languages(data_db)

    json_response = _make_doc_fam_lookup_request(data_client, valid_token, "DocSlug1")
    document = json_response["document"]
    print(json_response)
    assert set(document["languages"]) == set(["fra", "eng"])


def test_documents_doc_slug_returns_not_found(
    data_client: TestClient, data_db: Session, valid_token
):
    setup_with_docs(data_db)
    assert data_db.execute(select(func.count()).select_from(Family)).scalar_one() == 1
    assert (
        data_db.execute(select(func.count()).select_from(FamilyEvent)).scalar_one() == 1
    )

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
                "published_date": "2020-12-25T00:00:00Z",
                "last_updated_date": "2020-12-25T00:00:00Z",
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


def test_documents_doc_slug_returns_404_when_corpora_mismatch(
    data_client: TestClient, data_db: Session, alternative_token
):
    setup_with_two_docs(data_db)

    # Test associations
    json_response = _make_doc_fam_lookup_request(
        data_client,
        alternative_token,
        "DocSlug1",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "Nothing found for DocSlug1"
