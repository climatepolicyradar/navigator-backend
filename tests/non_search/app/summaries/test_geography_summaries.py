from http.client import NOT_FOUND, OK

import pytest


def _url_under_test(geography: str) -> str:
    return f"/api/v1/summaries/geography/{geography}"


def test_endpoint_returns_families_ok_with_slug(data_client):
    """Test the endpoint returns an empty sets of data"""
    response = data_client.get(_url_under_test("moldova"))
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


def test_endpoint_returns_families_ok_with_code(data_client):
    """Test the endpoint returns an empty sets of data"""
    response = data_client.get(_url_under_test("MDA"))
    assert response.status_code == OK
    resp = response.json()

    assert resp["family_counts"]["Executive"] == 0
    assert resp["family_counts"]["Legislative"] == 0
    assert resp["family_counts"]["UNFCCC"] == 0

    assert len(resp["top_families"]["Executive"]) == 0
    assert len(resp["top_families"]["Legislative"]) == 0
    assert len(resp["top_families"]["UNFCCC"]) == 0

    assert len(resp["family_counts"]) == 4
    assert len(resp["top_families"]) == 3

    assert len(resp["targets"]) == 0


def test_geography_with_families_ordered(data_client):
    """Test that all the data is returned ordered by published date"""
    response = data_client.get(_url_under_test("afghanistan"))
    assert response.status_code == OK
    resp = response.json()
    assert resp


@pytest.mark.parametrize(
    ("geo"),
    ["XCC", "Moldova"],
)
def test_endpoint_returns_404_when_not_found(geo, data_client):
    """Test the endpoint returns an empty sets of data"""
    response = data_client.get(_url_under_test(geo))
    assert response.status_code == NOT_FOUND


# TODO: Additional test to confirm that summary result counts are correct when
#       result count is larger than default BrowseArgs page size.
