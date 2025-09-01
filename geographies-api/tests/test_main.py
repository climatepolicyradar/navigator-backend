import pytest
from fastapi.testclient import TestClient

from app.data.geography_statistics_by_countries import geography_statistics_by_countries
from app.main import app
from app.model import APIItemResponse, Geography

client = TestClient(app)


@pytest.mark.parametrize(
    ("slug", "expected_statistics"),
    [
        # We do not have statistics for regions yet
        ("north-america", None),
        # We do have statistics for some countries
        ("afghanistan", geography_statistics_by_countries["AFG"]),
        # But not all countries
        ("holy-see-vatican-city-state", None),
        # We do not have statistics for subdivisions yet
        ("us-ca", None),
    ],
)
def test_read_geography_statistics(slug: str, expected_statistics: dict | None):
    response = client.get(f"/geographies/{slug}")

    assert response.status_code == 200
    response = APIItemResponse[Geography].model_validate(response.json())
    statistics = response.data.statistics

    if statistics is None:
        assert expected_statistics is None
    else:
        assert statistics.model_dump(exclude_none=True) == expected_statistics
