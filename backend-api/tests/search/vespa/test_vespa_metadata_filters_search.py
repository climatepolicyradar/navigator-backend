from unittest.mock import patch

import pytest

from tests.search.vespa.setup_search_tests import (
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize(
    "label,query,metadata_filters",
    [
        ("search", "the", [{"name": "sector", "value": "Price"}]),
        (
            "browse",
            "",
            [
                {"name": "topic", "value": "Mitigation"},
                {"name": "instrument", "value": "Capacity building"},
            ],
        ),
    ],
)
def test_metadata_filter(
    mock_corpora_exist_in_db,
    label,
    query,
    metadata_filters,
    test_vespa,
    data_db,
    monkeypatch,
    data_client,
    valid_token,
):
    _populate_db_families(data_db, deterministic_metadata=True)

    response = _make_search_request(
        data_client,
        valid_token,
        {
            "query_string": query,
            "metadata": metadata_filters,
        },
        expected_status_code=200,
    )
    assert len(response["families"]) > 0

    for metadata_filter in metadata_filters:

        for f in response["families"]:
            matched_metadata = [
                m
                for m in f["metadata"]
                if m["name"] == f"family.{metadata_filter["name"]}"
            ]
            assert len(matched_metadata) > 0

            matched_metadata_values = [m["value"] for m in matched_metadata]
            assert metadata_filter["value"] in matched_metadata_values

    assert mock_corpora_exist_in_db.assert_called
