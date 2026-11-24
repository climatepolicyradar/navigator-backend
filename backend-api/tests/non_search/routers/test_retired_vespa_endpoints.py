"""The endpoints that queried Vespa are gated with a 410 now Vespa is retired.

See ENRI-1959. When restoring an endpoint, remove its gate and its case here.
"""

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from tests.non_search.routers.documents.test_vespa_import_id_validation import (
    MALFORMED_IDS,
)

TEST_HOST = "http://localhost:3000/"

# The family and document routes, which also validate an import id once restored.
RETIRED_IMPORT_ID_ROUTES = ["/api/v1/families", "/api/v1/document"]

RETIRED_ENDPOINTS = [
    ("post", "/api/v1/searches"),
    ("post", "/api/v1/searches/download-csv"),
    ("get", "/api/v1/families/CCLW.family.10014.0"),
    ("get", "/api/v1/document/CCLW.document.i00000004.n0000"),
]


@pytest.mark.parametrize("method,path", RETIRED_ENDPOINTS)
def test_retired_vespa_endpoint_returns_gone(
    data_client: TestClient, valid_token, method, path
):
    response = data_client.request(
        method.upper(),
        path,
        headers={"app-token": valid_token, "origin": TEST_HOST},
        json={"query_string": "energy"} if method == "post" else None,
    )

    assert response.status_code == status.HTTP_410_GONE, response.text


@pytest.mark.parametrize("route", RETIRED_IMPORT_ID_ROUTES)
@pytest.mark.parametrize("malformed_id", MALFORMED_IDS)
def test_retired_import_id_route_returns_gone_for_malformed_id(
    data_client: TestClient, valid_token, route, malformed_id
):
    """The gate runs before import-id validation, so a malformed id gets 410, not 422.

    The 422 expectations in `test_vespa_import_id_validation.py` are skipped
    while these routes are retired, and apply again once a gate is removed.
    """
    response = data_client.get(
        f"{route}/{malformed_id}",
        headers={"app-token": valid_token, "origin": TEST_HOST},
    )

    assert response.status_code == status.HTTP_410_GONE, response.text


@pytest.mark.parametrize("route", RETIRED_IMPORT_ID_ROUTES)
def test_retired_import_id_route_returns_gone_without_valid_token(
    data_client: TestClient, route
):
    """The gate runs before token decoding, so a bad token also gets 410."""
    response = data_client.get(
        f"{route}/{MALFORMED_IDS[0]}",
        headers={"app-token": "not-a-real-token", "origin": TEST_HOST},
    )

    assert response.status_code == status.HTTP_410_GONE, response.text
