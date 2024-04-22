import pytest
from db_client.models.dfce.geography import Geography
from fastapi import status

from tests.non_search.app.geographies.setup_world_map_helpers import (
    setup_all_docs_published_world_map,
    setup_mixed_doc_statuses_world_map,
)


def _get_expected_keys():
    return ["display_name", "iso_code", "slug", "family_counts"]


def _url_under_test() -> str:
    return "/api/v1/geographies"


def _find_geography_index(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return -1


def test_geo_table_populated(data_db):
    lst = data_db.query(Geography).all()
    assert len(lst) > 0


def test_endpoint_returns_ok_all_docs_per_family_published(data_db, data_client):
    """Check endpoint returns 200 on success"""
    setup_all_docs_published_world_map(data_db)
    response = data_client.get(_url_under_test())
    assert response.status_code == status.HTTP_200_OK
    resp_json = response.json()
    assert len(resp_json) > 1

    idx = _find_geography_index(resp_json, "display_name", "Afghanistan")
    resp = resp_json[idx]

    assert set(["display_name", "iso_code", "slug", "family_counts"]) == set(
        resp.keys()
    )

    assert resp["family_counts"]["EXECUTIVE"] == 1
    assert resp["family_counts"]["LEGISLATIVE"] == 1
    assert resp["family_counts"]["UNFCCC"] == 1

    assert len(resp["family_counts"]) == 3


def test_endpoint_returns_ok_some_docs_per_family_unpublished(data_db, data_client):
    """Check endpoint returns 200 & discounts CREATED & DELETED docs"""
    setup_mixed_doc_statuses_world_map(data_db)
    response = data_client.get(_url_under_test())
    assert response.status_code == status.HTTP_200_OK
    resp_json = response.json()
    assert len(resp_json) > 1

    idx = _find_geography_index(resp_json, "display_name", "Afghanistan")
    resp = resp_json[idx]

    assert set(["display_name", "iso_code", "slug", "family_counts"]) == set(
        resp.keys()
    )

    assert resp["family_counts"]["EXECUTIVE"] == 1
    assert resp["family_counts"]["LEGISLATIVE"] == 1
    assert resp["family_counts"]["UNFCCC"] == 2

    assert len(resp["family_counts"]) == 3


def test_endpoint_returns_404_when_not_found(data_client):
    """Test the endpoint returns a 404 when no world map stats found"""
    response = data_client.get(_url_under_test())
    assert response.status_code == status.HTTP_404_NOT_FOUND
    data = response.json()
    assert data["detail"] == "No stats for world map found"


@pytest.mark.skip(reason="Bad repo and rollback mocks need rewriting")
def test_endpoint_returns_503_when_error(data_client):
    """Test the endpoint returns a 503 on db error"""
    response = data_client.get(_url_under_test())
    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    data = response.json()
    assert data["detail"] == "Database error"
