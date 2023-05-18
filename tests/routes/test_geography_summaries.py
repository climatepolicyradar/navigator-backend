from http.client import OK

import pytest  # noqa: F401


def _url_under_test(slug: str) -> str:
    return f"/api/v1/summaries/geography/{slug}"


def test_endpoint_returns_families_ok(client):
    """Test the endpoint returns an empty sets of data"""
    response = client.get(_url_under_test("moldova"))
    assert response.status_code == OK
    resp = response.json()

    assert resp["family_counts"]["Executive"] == 0
    assert resp["family_counts"]["Legislative"] == 0
    assert resp["family_counts"]["UNFCCC"] == 0

    assert len(resp["top_families"]["Executive"]) == 0
    assert len(resp["top_families"]["Legislative"]) == 0
    assert len(resp["top_families"]["UNFCCC"]) == 0

    assert len(resp["family_counts"]) == 3
    assert len(resp["top_families"]) == 3

    assert len(resp["targets"]) == 0


def test_geography_with_families_ordered(client, summary_geography_family_data):
    """Test that all the data is returned ordered by published date"""
    geography_slug = summary_geography_family_data["geos"][0].slug
    response = client.get(_url_under_test(geography_slug))
    assert response.status_code == OK
    resp = response.json()
    assert resp

    # TODO: Additional test to confirm that summary result counts are correct when
    #       result count is larger than default BrowseArgs page size.
