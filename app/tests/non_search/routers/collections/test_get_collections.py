from typing import Optional

from db_client.models.dfce import Slug
from fastapi import status
from sqlalchemy.orm import Session
from tests.non_search.setup_helpers import get_default_collections, setup_with_two_docs

COLLECTIONS_ENDPOINT = "/api/v1/collections"
TEST_HOST = "http://localhost:3000/"


def _setup_collection_data(db: Session):
    # Collection
    collection1, _ = get_default_collections()
    import_id = collection1["import_id"]
    setup_with_two_docs(db)

    new_slug = Slug(
        collection_import_id=import_id,
        family_import_id=None,
        family_document_import_id=None,
        name="collection_slug",
    )

    db.add(new_slug)
    db.commit()


def _collection_lookup_request(
    client,
    token,
    slug: str,
    expected_status_code: int = status.HTTP_200_OK,
    origin: Optional[str] = TEST_HOST,
):
    headers = (
        {"app-token": token}
        if origin is None
        else {"app-token": token, "origin": origin}
    )

    response = client.get(f"{COLLECTIONS_ENDPOINT}/{slug}", headers=headers)
    assert response.status_code == expected_status_code, response.text
    return response.json()


def test_endpoint_returns_collections_ok_with_slug(data_db, data_client, valid_token):
    _setup_collection_data(data_db)
    resp = _collection_lookup_request(data_client, valid_token, "collection_slug")

    expected_response = {
        "import_id": "CPR.Collection.1.0",
        "title": "Collection1",
        "description": "CollectionSummary1",
        "families": [
            {"title": "Fam1", "slug": "FamSlug1", "description": "Summary1"},
            {"title": "Fam2", "slug": "FamSlug2", "description": "Summary2"},
        ],
    }

    assert resp == expected_response


def test_endpoint_returns_404_with_invalid_slug(
    data_db,
    data_client,
    valid_token,
):
    _setup_collection_data(data_db)
    resp = _collection_lookup_request(
        data_client,
        valid_token,
        "invalid_slug",
        status.HTTP_404_NOT_FOUND,
    )

    expected_response = {"detail": "Nothing found for invalid_slug"}
    assert resp == expected_response
