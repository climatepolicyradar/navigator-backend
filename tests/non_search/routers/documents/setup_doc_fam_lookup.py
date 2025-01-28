from typing import Optional

from fastapi import status

DOCUMENTS_ENDPOINT = "/api/v1/documents"
FAMILIES_ENDPOINT = "/api/v1/families"
TEST_HOST = "http://localhost:3000/"


def _make_doc_fam_lookup_request(
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

    response = client.get(f"{DOCUMENTS_ENDPOINT}/{slug}", headers=headers)
    assert response.status_code == expected_status_code, response.text
    return response.json()


def _make_vespa_fam_lookup_request(
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

    response = client.get(f"{FAMILIES_ENDPOINT}/{slug}", headers=headers)
    assert response.status_code == expected_status_code, response.text
    return response.json()
