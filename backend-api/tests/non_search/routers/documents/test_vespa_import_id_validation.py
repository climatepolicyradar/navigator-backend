"""Boundary validation on the Vespa lookup routes.

Both routes take an import id. Before this guard existed they passed whatever
they were given to the SDK, whose id validation pattern backtracked
catastrophically on slug-shaped input -- freezing every task in production on
2026-10-12. These tests pin that malformed input is now rejected at the
boundary, cheaply, before any Vespa or database work.
"""

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from app.service.pipeline import is_valid_import_id
from tests.non_search.routers.documents.setup_doc_fam_lookup import (
    DOCUMENTS_ENDPOINT,
    FAMILIES_ENDPOINT,
    TEST_HOST,
)

# The slug that froze every backend-api task on 2026-10-12, plus other shapes
# that are not import ids.
MALFORMED_IDS = [
    "CPR-RUS-2006-ce834ca5cf77b221394a8106b6f1bb2a-law",
    "some-ordinary-looking-slug",
    "CCLW.exec.1.2.3",
    "UNFCCC.1.2",
    "CCLW.%25.1.2",
]

# The singular `/document/{import_id}` Vespa route, distinct from the plural
# `/documents/{slug}` route that does take a slug.
VESPA_DOC_ENDPOINT = "/api/v1/document"


def _get(client: TestClient, path: str, token: str):
    return client.get(path, headers={"app-token": token, "origin": TEST_HOST})


@pytest.mark.parametrize("malformed_id", MALFORMED_IDS)
def test_vespa_family_route_rejects_malformed_import_id(
    data_client: TestClient, valid_token, malformed_id
):
    response = _get(data_client, f"{FAMILIES_ENDPOINT}/{malformed_id}", valid_token)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
    assert "is not a valid import id" in response.json()["detail"]


@pytest.mark.parametrize("malformed_id", MALFORMED_IDS)
def test_vespa_document_route_rejects_malformed_import_id(
    data_client: TestClient, valid_token, malformed_id
):
    response = _get(data_client, f"{VESPA_DOC_ENDPOINT}/{malformed_id}", valid_token)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
    assert "is not a valid import id" in response.json()["detail"]


def test_rejection_does_not_require_a_valid_token(data_client: TestClient):
    """Validation runs before token decoding.

    The expensive path was reachable before any auth work, so the guard has to
    be too -- otherwise a bad token would still let the cost through.
    """
    response = data_client.get(
        f"{FAMILIES_ENDPOINT}/{MALFORMED_IDS[0]}",
        headers={"app-token": "not-a-real-token", "origin": TEST_HOST},
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text


@pytest.mark.search
@pytest.mark.parametrize("endpoint", [FAMILIES_ENDPOINT, VESPA_DOC_ENDPOINT])
def test_well_formed_import_id_is_not_rejected_by_validation(
    data_client: TestClient, valid_token, endpoint, test_vespa
):
    """A well-formed id must get past the guard and reach the Vespa lookup.

    Marked `search` because getting past the guard means the request goes on to
    query Vespa -- the whole point is that it is not short-circuited with a 422.
    `test_valid_import_id_passes_the_guard` covers the guard itself without
    needing Vespa.
    """
    response = _get(data_client, f"{endpoint}/CCLW.family.10014.0", valid_token)

    assert response.status_code != status.HTTP_422_UNPROCESSABLE_ENTITY, response.text


def test_valid_import_id_passes_the_guard():
    """The guard itself accepts well-formed ids, independent of any route.

    Keeps the "we did not break valid ids" assertion in the non-search suite,
    where it does not depend on Vespa being reachable.
    """
    assert is_valid_import_id("CCLW.family.10014.0")
    assert is_valid_import_id("CCLW.document.i00000004.n0000")


def test_slug_route_still_accepts_slugs(data_client: TestClient, valid_token):
    """The guard must not leak onto the route that legitimately takes slugs.

    `/documents/{slug}` is a different endpoint from the two Vespa lookups and
    is supposed to receive slugs, so it must not start rejecting them.
    """
    response = _get(
        data_client, f"{DOCUMENTS_ENDPOINT}/some-ordinary-looking-slug", valid_token
    )

    assert response.status_code != status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
