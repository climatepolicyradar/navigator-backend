from unittest.mock import patch

import pytest

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
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_result_order_score(
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
        "sort_field": "date",
        "sort_order": "asc",
    }
    asc_date_body = _make_search_request(data_client, valid_token, params)
    asc_dates = [f["family_date"] for f in asc_date_body["families"]]

    params["sort_order"] = "desc"
    desc_date_body = _make_search_request(data_client, valid_token, params)
    desc_dates = [f["family_date"] for f in desc_date_body["families"]]

    assert VESPA_FIXTURE_COUNT == len(asc_dates) == len(desc_dates)
    assert asc_dates == list(reversed(desc_dates))
    assert asc_dates[0] < desc_dates[0]
    assert asc_dates[-1] > desc_dates[-1]

    assert mock_corpora_exist_in_db.assert_called


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
def test_result_order_title(
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
        "sort_field": "title",
        "sort_order": "asc",
    }

    # Scope of test is to confirm this does not cause a failure
    _ = _make_search_request(data_client, valid_token, params)

    assert mock_corpora_exist_in_db.assert_called
