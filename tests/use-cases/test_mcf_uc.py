from unittest.mock import patch

import pytest

from app.api.api_v1.routers import search
from app.repository.lookups import get_country_slug_from_country_code
from tests.search.vespa.setup_search_tests import (
    VESPA_FIXTURE_COUNT,
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.use_case
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
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
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
            country_slug = get_country_slug_from_country_code(data_db, country_code)

            params = {
                **base_params,
                **{"keyword_filters": {"countries": [country_slug]}},
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
