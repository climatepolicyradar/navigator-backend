import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from tests.non_search.routers.documents.setup_doc_fam_lookup import (
    _make_vespa_fam_lookup_request,
)
from tests.search.vespa.setup_search_tests import _populate_db_families


@pytest.mark.search
def test_families_slug_returns_not_found(
    data_db: Session, data_client: TestClient, valid_token, monkeypatch, test_vespa
):
    _populate_db_families(data_db)

    # Test by slug
    json_response = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        "CCLW.family.9999999999.0",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert (
        json_response["detail"] == "Nothing found for CCLW.family.9999999999.0 in Vespa"
    )


@pytest.mark.search
def test_families_slug_returns_correct_family(
    data_db: Session, data_client: TestClient, valid_token, monkeypatch, test_vespa
):
    _populate_db_families(data_db)

    # Test by slug
    body = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        "CCLW.family.10246.0",
    )

    assert body["total_hits"] == 1
    assert body["total_family_hits"] == 1
    assert len(body["families"]) > 0

    assert body["families"][0]["id"].split("::")[-1] == "CCLW.family.10246.0"
