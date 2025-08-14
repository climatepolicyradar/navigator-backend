from typing import Optional

import pytest
from db_client.models.dfce.family import Family
from fastapi import status

from tests.non_search.setup_helpers import (
    setup_with_six_families_same_geography,
    setup_with_two_docs,
)

TEST_HOST = "http://localhost:3000/"
GEOGRAPHY_PAGE_ENDPOINT = "/api/v1/summaries/geography"

EXPECTED_NUM_FAMILY_CATEGORIES = 6
EXPECTED_FAMILY_CATEGORIES = {
    "Executive",
    "Legislative",
    "UNFCCC",
    "MCF",
    "Reports",
    "Litigation",
}


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


@pytest.mark.parametrize(
    ("geography"),
    ["india", "IND"],
)
def test_endpoint_returns_summaries_ok(data_client, data_db, valid_token, geography):
    setup_with_two_docs(data_db)

    resp = _make_request(data_client, valid_token, geography)

    assert len(resp["family_counts"]) == EXPECTED_NUM_FAMILY_CATEGORIES
    assert len(resp["top_families"]) == EXPECTED_NUM_FAMILY_CATEGORIES

    assert set(resp["family_counts"].keys()) == EXPECTED_FAMILY_CATEGORIES
    assert set(resp["top_families"].keys()) == EXPECTED_FAMILY_CATEGORIES

    assert resp["family_counts"]["Executive"] == 1
    assert resp["family_counts"]["Legislative"] == 0
    assert resp["family_counts"]["UNFCCC"] == 0
    assert resp["family_counts"]["MCF"] == 0
    assert resp["family_counts"]["Reports"] == 0
    assert resp["family_counts"]["Litigation"] == 0

    expected_top_families = [
        {
            "continuation_token": None,
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "corpus_type_name": "Laws and Policies",
            "family_category": "Executive",
            "family_date": "2019-12-25T00:00:00+00:00",
            "family_description": "Summary2",
            "family_description_match": False,
            "family_documents": [],
            "family_geographies": ["AFG", "IND"],
            "family_last_updated_date": "2019-12-25T00:00:00+00:00",
            "family_metadata": {},
            "family_name": "Fam2",
            "family_slug": "FamSlug2",
            "family_source": "CCLW",
            "family_title_match": False,
            "prev_continuation_token": None,
            "total_passage_hits": 0,
        }
    ]

    assert resp["top_families"]["Executive"] == expected_top_families
    assert not resp["top_families"]["Legislative"]
    assert not resp["top_families"]["UNFCCC"]
    assert not resp["top_families"]["MCF"]
    assert not resp["top_families"]["Reports"]
    assert not resp["top_families"]["Litigation"]

    assert len(resp["targets"]) == 0


def test_geography_with_families_ordered(data_client, data_db, valid_token):
    setup_with_six_families_same_geography(data_db)

    all_families = data_db.query(Family).all()

    assert len(all_families) > 5

    expected_family_dates = sorted(
        all_families, key=lambda family: family.published_date
    )[:5]

    resp = _make_request(data_client, valid_token, "afghanistan")
    top_families = resp["top_families"]["Executive"]

    assert resp["family_counts"]["Executive"] == 5

    response_family_dates = [top_family.family_date for top_family in top_families]

    assert response_family_dates == expected_family_dates


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
