from typing import Optional

from fastapi import status

COLLECTIONS_ENDPOINT = "/api/v1/collections"
TEST_HOST = "http://localhost:3000/"


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


def test_endpoint_returns_collections_ok_with_slug(data_client, valid_token):
    resp = _collection_lookup_request(
        data_client, valid_token, "moldova_this_collection"
    )

    breakpoint()
    assert resp
