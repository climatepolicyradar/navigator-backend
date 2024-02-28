from http.client import NOT_FOUND, OK
from unittest.mock import Mock
from db_client.models.law_policy.geography import GeoStatistics, Geography
from app.api.api_v1.routers.lookups.geo_stats import (
    GeoStatsResponse,
    lookup_geo_stats,
)

TEST_GEO_NAME = "test1"
TEST_GEO_SLUG = "a"
URL_UNDER_TEST = f"/api/v1/geo_stats/{TEST_GEO_SLUG}"

TEST_GEO_SLUG_BAD = "this-is-not-a-geography"
URL_UNDER_TEST_BAD = f"/api/v1/geo_stats/{TEST_GEO_SLUG_BAD}"


def test_endpoint_returns_correct_data(client, test_db):
    """Tests when the db is populated we can get out the data as expected."""

    test_db.add(Geography(id=1, slug="a", display_value="A"))
    test_db.add(Geography(id=2, slug="b", display_value="B"))
    test_db.add(Geography(id=3, slug="c", display_value="C"))
    test_db.flush()

    test_db.add(
        GeoStatistics(
            id=1,
            geography_id=1,
            name="test1",
            legislative_process="lp",
            federal=False,
            federal_details="fd",
            political_groups="pg",
            global_emissions_percent="gep",
            climate_risk_index="cri",
            worldbank_income_group="wb",
            visibility_status="vs",
        )
    )

    test_db.commit()

    response = client.get(
        URL_UNDER_TEST,
    )
    stats = response.json()
    assert response.status_code == OK
    assert stats["geography_slug"] == TEST_GEO_SLUG
    assert stats["name"] == TEST_GEO_NAME
    assert stats["federal"] is False


def test_endpoint_returns_not_found(client, test_db):
    """Tests the fact if the db is populated then 404 is returned for an unknown id."""
    test_db.flush()  # update the session, no need to commit as its just a test

    response = client.get(
        URL_UNDER_TEST_BAD,
    )
    assert response.status_code == NOT_FOUND


def test_endpoint_returns_not_found_empty_db(client):
    """Tests the fact if the db is empty then no error is generated and 404 is returned."""
    response = client.get(
        URL_UNDER_TEST,
    )
    assert response.status_code == NOT_FOUND


def test_queries_db():
    db = Mock()
    query = Mock()
    filter_by = Mock()

    db.query.return_value = query
    query.filter_by.return_value = filter_by
    filter_by.first.return_value = GeoStatsResponse(
        name=TEST_GEO_NAME,
        geography_slug=TEST_GEO_SLUG,
        legislative_process="row.legislative_process",
        federal=True,
        federal_details="row.federal_details",
        political_groups="row.political_groups",
        global_emissions_percent="0.1",
        climate_risk_index="0.2",
        worldbank_income_group="row.worldbank_income_group",
        visibility_status="row.visibility_status",
    )

    response = lookup_geo_stats(TEST_GEO_SLUG, db=db)
    assert db.query.call_count == 2  # lookup geography, then stats
    assert response.geography_slug == TEST_GEO_SLUG
    assert response.name == TEST_GEO_NAME
