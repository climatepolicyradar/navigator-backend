import csv
from io import StringIO
from typing import Mapping
from unittest.mock import patch

import pytest

from app.api.api_v1.routers import search
from tests.search.vespa.setup_search_tests import _populate_db_families

SEARCH_ENDPOINT = "/api/v1/searches"
CSV_DOWNLOAD_ENDPOINT = "/api/v1/searches/download-csv"


def _make_search_request(client, params: Mapping[str, str]):
    response = client.post(SEARCH_ENDPOINT, json=params)
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.search
@pytest.mark.parametrize("exact_match", [True, False])
@pytest.mark.parametrize("query_string", ["", "local"])
def test_csv_content(
    exact_match, query_string, test_vespa, data_db, monkeypatch, data_client
):
    """Make sure that downloaded CSV content matches a given search"""
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)
    params = {
        "exact_match": exact_match,
        "query_string": query_string,
    }
    body = _make_search_request(data_client, params)
    families = body["families"]
    assert len(families) > 0

    csv_response = data_client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json={
            "exact_match": exact_match,
            "query_string": query_string,
        },
    )
    assert csv_response.status_code == 200

    csv_content = csv.DictReader(StringIO(csv_response.text))
    for row, family in zip(csv_content, families):
        assert row["Family Name"] == family["family_name"]
        assert row["Family Summary"] == family["family_description"]
        assert row["Family Publication Date"] == family["family_date"]
        assert row["Category"] == family["family_category"]
        assert row["Geography"] == family["family_geography"]

        # TODO: Add collections to test db setup to provide document level coverage


@pytest.mark.search
@pytest.mark.parametrize("label, query", [("search", "the"), ("browse", "")])
@pytest.mark.parametrize("limit", [100, 250, 500])
def test_csv_download_search_variable_limit(
    label, query, limit, test_vespa, data_db, monkeypatch, data_client, mocker
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    query_spy = mocker.spy(search._VESPA_CONNECTION, "search")

    params = {
        "query_string": query,
        "limit": limit,
        "page_size": 100,
        "offset": 0,
    }

    download_response = data_client.post(
        CSV_DOWNLOAD_ENDPOINT,
        json=params,
    )
    assert download_response.status_code == 200

    actual_params = query_spy.call_args.kwargs["parameters"].model_dump()

    # Check requested params are not changed
    for key, value in params.items():
        assert actual_params[key] == value


@pytest.mark.search
def test_csv_download__ignore_extra_fields(
    test_vespa, data_db, monkeypatch, data_client, mocker
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    params = {
        "query_string": "winter",
    }

    # Ensure extra, unspecified fields don't cause an error
    fields = []
    with patch("app.core.search._CSV_SEARCH_RESPONSE_COLUMNS", fields):
        download_response = data_client.post(
            CSV_DOWNLOAD_ENDPOINT,
            json=params,
        )
    assert download_response.status_code == 200
