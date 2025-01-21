from typing import Optional

import pytest
from fastapi import status

TEST_HOST = "http://localhost:3000/"
GEOGRAPHY_PAGE_ENDPOINT = "/api/v1/summaries/geography"


def _make_request(
    client,
    token,
    geography: str,
    expected_status_code: int = status.HTTP_200_OK,
    origin: Optional[str] = TEST_HOST,
):
    headers = (
        {"app-token": token}
        if origin is None
        else {"app-token": token, "origin": origin}
    )

    response = client.get(f"{GEOGRAPHY_PAGE_ENDPOINT}/{geography}", headers=headers)
    assert response.status_code == expected_status_code, response.text
    return response.json()


def test_endpoint_returns_families_ok_with_slug(data_client, valid_token):
    """Test the endpoint returns an empty sets of data"""
    resp = _make_request(data_client, valid_token, "moldova")

    assert len(resp["family_counts"]) == 4
    assert len(resp["top_families"]) == 4

    assert resp["family_counts"]["Executive"] == 0
    assert resp["family_counts"]["Legislative"] == 0
    assert resp["family_counts"]["UNFCCC"] == 0
    assert resp["family_counts"]["MCF"] == 0

    assert len(resp["top_families"]["Executive"]) == 0
    assert len(resp["top_families"]["Legislative"]) == 0
    assert len(resp["top_families"]["UNFCCC"]) == 0
    assert len(resp["top_families"]["MCF"]) == 0

    assert len(resp["targets"]) == 0


def test_endpoint_returns_families_ok_with_code(data_client, valid_token):
    """Test the endpoint returns an empty sets of data"""
    resp = _make_request(data_client, valid_token, "MDA")

    assert len(resp["family_counts"]) == 4
    assert len(resp["top_families"]) == 4

    assert resp["family_counts"]["Executive"] == 0
    assert resp["family_counts"]["Legislative"] == 0
    assert resp["family_counts"]["UNFCCC"] == 0
    assert resp["family_counts"]["MCF"] == 0

    assert len(resp["top_families"]["Executive"]) == 0
    assert len(resp["top_families"]["Legislative"]) == 0
    assert len(resp["top_families"]["UNFCCC"]) == 0
    assert len(resp["top_families"]["MCF"]) == 0

    assert len(resp["targets"]) == 0


def test_geography_with_families_ordered(data_client, valid_token):
    """Test that all the data is returned ordered by published date"""
    resp = _make_request(data_client, valid_token, "afghanistan")
    assert resp


@pytest.mark.parametrize(
    ("geo"),
    ["XCC", "Moldova"],
)
def test_endpoint_returns_404_when_not_found(geo, data_client, valid_token):
    """Test the endpoint returns an empty sets of data"""
    resp = _make_request(
        data_client, valid_token, geo, expected_status_code=status.HTTP_404_NOT_FOUND
    )
    assert resp


# TODO: Additional test to confirm that summary result counts are correct when
#       result count is larger than default BrowseArgs page size.
