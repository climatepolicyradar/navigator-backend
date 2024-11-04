from db_client.models.document.physical_document import PhysicalDocumentLanguage
from fastapi.testclient import TestClient
from sqlalchemy import update
from sqlalchemy.orm import Session

from tests.non_search.routers.documents.setup_doc_fam_lookup import (
    _make_doc_fam_lookup_request,
)
from tests.non_search.setup_helpers import (
    setup_with_two_docs,
    setup_with_two_docs_multiple_languages,
)


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
