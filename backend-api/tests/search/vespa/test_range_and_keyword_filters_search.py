from unittest.mock import patch

import pytest
from db_client.models.dfce import Geography
from fastapi import status

from tests.search.vespa.setup_search_tests import (
    VESPA_FIXTURE_COUNT,
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_country_filters__geographies(
    mock_corpora_exist_in_db,
    label,
    query,
    test_vespa,
    data_client,
    data_db,
    monkeypatch,
    valid_token,
):

    _populate_db_families(data_db)
    base_params = {"query_string": query}

    # Get all documents and iterate over their country codes to confirm that each are
    # the specific one that is returned in the query (as they each have a unique
    # country code)
    all_body = _make_search_request(data_client, valid_token, params=base_params)
    families = [f for f in all_body["families"]]
    assert len(families) == VESPA_FIXTURE_COUNT

    for family in families:
        for country_code in family["family_geographies"]:
            country_iso = (
                data_db.query(Geography.value)
                .filter(Geography.value == country_code)
                .scalar()
            )

            params = {
                **base_params,
                **{"keyword_filters": {"countries": [country_iso]}},
            }
            body_with_filters = _make_search_request(
                data_client, valid_token, params=params
            )
            filtered_family_slugs = [
                f["family_slug"] for f in body_with_filters["families"]
            ]
            assert len(filtered_family_slugs) == 1
            assert family["family_slug"] in filtered_family_slugs

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_region_filters(
    mock_corpora_exist_in_db,
    label,
    query,
    test_vespa,
    data_client,
    data_db,
    monkeypatch,
    valid_token,
):

    _populate_db_families(data_db)
    base_params = {"query_string": query}

    # Get regions of all documents and iterate over them
    # to confirm the originals are returned when filtered on
    all_body = _make_search_request(data_client, valid_token, params=base_params)
    families = [f for f in all_body["families"]]
    assert len(families) == VESPA_FIXTURE_COUNT

    for family in families:
        country_codes = family["family_geographies"]

        # Fixture for UNFCCC.non-party.1267.0 has a non geography (XAA)
        if country_codes == ["Other"]:
            return

        country_code = country_codes[0]

        parent_id = (
            data_db.query(Geography)
            .filter(Geography.value == country_code)
            .first()
            .parent_id
        )
        region = data_db.query(Geography).filter(Geography.id == parent_id).first()

        params = {**base_params, **{"keyword_filters": {"regions": [region.slug]}}}
        body_with_filters = _make_search_request(
            data_client, valid_token, params=params
        )
        filtered_family_slugs = [
            f["family_slug"] for f in body_with_filters["families"]
        ]
        assert family["family_slug"] in filtered_family_slugs

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_keyword_region_and_country_filters(
    mock_corpora_exist_in_db,
    label,
    query,
    test_vespa,
    data_client,
    data_db,
    monkeypatch,
    valid_token,
):

    _populate_db_families(data_db)

    # Filtering on one region and one country should return the one match
    base_params = {
        "query_string": query,
        "keyword_filters": {
            "regions": ["europe-central-asia"],
            "countries": ["italy"],
        },
    }

    body = _make_search_request(data_client, valid_token, params=base_params)

    assert len(body["families"]) == 1
    assert body["families"][0]["family_name"] == "National Energy Strategy"

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label,query", [("search", "the"), ("browse", "")])
def test_invalid_keyword_filters(
    mock_corpora_exist_in_db,
    label,
    query,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):

    _populate_db_families(data_db)

    params = {
        "query_string": query,
        "keyword_filters": {
            "countries": ["kenya"],
            "unknown_filter_no1": ["BOOM"],
        },
    }
    _make_search_request(
        data_client,
        valid_token,
        params,
        expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    )

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize(
    "year_range", [(None, None), (1900, None), (None, 2020), (1900, 2020)]
)
def test_year_range_filtered_in(
    mock_corpora_exist_in_db,
    year_range,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):

    _populate_db_families(data_db)

    # Search
    params = {"query_string": "and", "year_range": year_range}
    body = _make_search_request(data_client, valid_token, params=params)
    assert len(body["families"]) > 0

    # Browse
    params = {"query_string": "", "year_range": year_range}
    body = _make_search_request(data_client, valid_token, params=params)
    assert len(body["families"]) > 0

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("year_range", [(None, 2010), (2024, None)])
def test_year_range_filtered_out(
    mock_corpora_exist_in_db,
    year_range,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):

    _populate_db_families(data_db)

    # Search
    params = {"query_string": "and", "year_range": year_range}
    body = _make_search_request(data_client, valid_token, params=params)
    assert len(body["families"]) == 0

    # Browse
    params = {"query_string": "", "year_range": year_range}
    body = _make_search_request(data_client, valid_token, params=params)
    assert len(body["families"]) == 0

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_multiple_filters(
    mock_corpora_exist_in_db,
    label,
    query,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):

    _populate_db_families(data_db)

    params = {
        "query_string": query,
        "keyword_filters": {
            "countries": ["south-korea"],
            "sources": ["CCLW"],
            "categories": ["Legislative"],
        },
        "year_range": (1900, 2020),
    }

    _ = _make_search_request(data_client, valid_token, params)
    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
def test_geo_filter_with_exact(
    mock_corpora_exist_in_db, test_vespa, data_db, monkeypatch, data_client, valid_token
):

    _populate_db_families(data_db)

    params = {
        "query_string": "the potential of district heating",
        "exact_match": True,
        "keyword_filters": {
            "countries": ["italy"],
        },
    }

    response = _make_search_request(data_client, valid_token, params)

    assert len(response["families"]) > 0
    for family in response["families"]:
        assert "ITA" in family["family_geographies"]

    assert mock_corpora_exist_in_db.assert_called
