import time

import pytest

from app.api.api_v1.routers import search
from tests.search.vespa.setup_search_tests import (
    _make_search_request,
    _populate_db_families,
)


@pytest.mark.search
def test_case_insensitivity(test_vespa, data_db, monkeypatch, data_client, valid_token):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    lower_body = _make_search_request(data_client, valid_token, {"query_string": "the"})
    upper_body = _make_search_request(data_client, valid_token, {"query_string": "THE"})

    assert lower_body["families"] == upper_body["families"]


@pytest.mark.search
def test_punctuation_ignored(
    test_vespa, data_db, monkeypatch, data_client, valid_token
):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    regular_body = _make_search_request(
        data_client, valid_token, {"query_string": "the"}
    )
    punc_body = _make_search_request(
        data_client, valid_token, {"query_string": ", the."}
    )
    accent_body = _make_search_request(
        data_client, valid_token, {"query_string": "thë"}
    )

    assert (
        sorted([f["family_slug"] for f in punc_body["families"]])
        == sorted([f["family_slug"] for f in regular_body["families"]])
        == sorted([f["family_slug"] for f in accent_body["families"]])
    )


@pytest.mark.search
def test_accents_ignored(test_vespa, data_db, monkeypatch, data_client, valid_token):
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)
    _populate_db_families(data_db)

    start = time.time()
    body = _make_search_request(data_client, valid_token, {"query_string": "the"})
    end = time.time()

    request_time_ms = 1000 * (end - start)
    assert 0 < body["query_time_ms"] < body["total_time_ms"] < request_time_ms
