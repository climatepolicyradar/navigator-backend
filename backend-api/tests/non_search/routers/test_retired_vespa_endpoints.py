"""The endpoints that queried Vespa are gated with a 410 now Vespa is retired.

See ENRI-1959. When restoring an endpoint, remove its gate and its case here.
"""

import pytest
from fastapi import status
from fastapi.testclient import TestClient

TEST_HOST = "http://localhost:3000/"

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
