from fastapi.testclient import TestClient

from app.data.geography_statistics_by_countries import geography_statistics_by_countries
from app.main import app
from app.model import APIItemResponse, Geography

client = TestClient(app)


def test_read_geography_has_statistics():
    response = client.get("/geographies/afghanistan")

    assert response.status_code == 200
    response = APIItemResponse[Geography].model_validate(response.json())
    statistics = response.data.statistics

    assert statistics is not None
    assert statistics.model_dump() == geography_statistics_by_countries["AFG"]


def test_read_geography_does_not_has_statistics():
    response = client.get("/geographies/us-ca")

    assert response.status_code == 200
    response = APIItemResponse[Geography].model_validate(response.json())
    statistics = response.data.statistics

    assert statistics is None
