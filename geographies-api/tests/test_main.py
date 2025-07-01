from datetime import datetime

import pytest
from fastapi.testclient import TestClient

def test_read_family_OPTIONS_200(test_client: TestClient):
    response = test_client.options("geographies/", headers={
        "Origin": "https://cpr.staging.climatepolicyradar.org",
        "Access-Control-Request-Method": "GET",
    })
    assert response.status_code == 200  # nosec B101

def test_read_family_OPTIONS_403(test_client: TestClient):
    response = test_client.options("geographies/", headers={
        "Origin": "https://cclw.staging.climatepolicyradar.org",
        "Access-Control-Request-Method": "GET",
    })
    print(response.text)
    assert response.status_code == 403  # nosec B101
