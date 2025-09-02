import pytest
from fastapi.testclient import TestClient

from app.data.geography_statistics_by_countries import geography_statistics_by_countries
from app.main import app

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
    response_json = response.json()
    statistics = response_json["data"]["statistics"]

    if statistics is None:
        assert expected_statistics is None
    else:
        assert statistics == expected_statistics


@pytest.mark.parametrize(
    ("slug", "has_subconcept_of"),
    [
        # There are no subconcepts for regions
        ("sub-saharan-africa", False),
        # we do no _currently_ support subconcepts on countries
        # TODO: add Region `subconcept_of` to Country
        ("north-america", False),
        # we do no _currently_ support subconcepts on countries
        # TODO: add Region `subconcept_of` to Country
        ("us-ca", True),
    ],
)
def test_read_geography_subconcept(slug: str, has_subconcept_of: bool):
    list_response = client.get("/geographies/")
    list_json = list_response.json()
    geography = next(
        geography for geography in list_json["data"] if geography["slug"] == slug
    )
    assert geography is not None
    # we should never include the relationship in the list response
    assert "subconcept_of" not in geography

    # check if we have the subconcept_of in the right place
    item_response = client.get(f"/geographies/{slug}")
    item_json = item_response.json()
    geography = item_json["data"]
    assert ("subconcept_of" in geography) == has_subconcept_of
