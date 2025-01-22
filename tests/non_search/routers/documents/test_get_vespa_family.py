import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.service import search
from tests.non_search.routers.documents.setup_doc_fam_lookup import (
    _make_vespa_fam_lookup_request,
)
from tests.non_search.setup_helpers import setup_with_two_docs_one_family
from tests.search.vespa.setup_search_tests import _populate_db_families

N_FAMILY_KEYS = 15


def test_families_slug_returns_not_found_when_not_in_rds(
    data_client: TestClient, valid_token
):

    # Test by slug
    json_response = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        "NonExistentFamSlug",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "Nothing found for NonExistentFamSlug in RDS"


# @patch(
#     "app.api.api_v1.routers.documents.get_family_from_slug",
#     return_value="NonExistentFamSlug",
# )
def test_families_slug_returns_400_when_import_id_invalid(
    data_db: Session, data_client: TestClient, valid_token
):
    setup_with_two_docs_one_family(data_db)

    # Test by slug
    json_response = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        "NonExistentFamSlug",
        expected_status_code=status.HTTP_400_BAD_REQUEST,
    )
    assert (
        json_response["detail"]
        == 'Failed to parse document id: "NonExistentFamSlug". Document ids should be of the form: "id:namespace:schema::data_id"'
    )


# @patch(
#     "app.api.api_v1.routers.documents.get_family_from_slug",
#     return_value="id:doc_search:family_document::NonExistentFamSlug",
# )
def test_families_slug_returns_not_found(
    data_db: Session, data_client: TestClient, valid_token, monkeypatch, test_vespa
):
    _populate_db_families(data_db)

    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    # query_spy = mocker.spy(search._VESPA_CONNECTION, "search")
    # body = _make_search_request(data_client, valid_token, {"query_string": ""})

    # assert body["hits"] > 0
    # assert len(body["families"]) > 0

    # # Should automatically use vespa `all_results` parameter for browse requests
    # assert query_spy.call_args.kwargs["parameters"].all_results
    # query_spy.assert_called_once()

    # Test by slug
    json_response = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        "NonExistentFamSlug",
        expected_status_code=status.HTTP_404_NOT_FOUND,
    )
    assert json_response["detail"] == "Nothing found for NonExistentFamSlug in Vespa"


# @patch(
#     "app.api.api_v1.routers.documents.get_family_from_slug",
#     return_value="id:doc_search:family_document::CCLW.family.1001.0",
# )
def test_families_slug_returns_correct_family(
    data_db: Session, data_client: TestClient, valid_token, monkeypatch, test_vespa
):
    _populate_db_families(data_db)

    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    # Test by slug
    json_response = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        "FamSlug1",
    )
    assert json_response["import_id"] == "CCLW.family.1001.0"


@pytest.mark.parametrize(
    ("slug", "expected_fam"),
    [
        (
            "FamSlug1",
            {
                "title": "Fam1",
                "import_id": "CCLW.family.1001.0",
                "geographies": ["South Asia"],
                "category": "Executive",
                "slug": "FamSlug1",
                "corpus_id": "CCLW.corpus.i00000001.n0000",
                "published_date": "2019-12-25T00:00:00Z",
                "last_updated_date": "2019-12-25T00:00:00Z",
                "metadata": {"color": "pink", "size": "big"},
                "organisation": "CCLW",
                "status": "Published",
                "summary": "Summary1",
            },
        ),
        (
            "FamSlug2",
            {
                "title": "Fam2",
                "import_id": "CCLW.family.2002.0",
                "geographies": ["AFG", "IND"],
                "category": "Executive",
                "slug": "FamSlug2",
                "corpus_id": "CCLW.corpus.i00000001.n0000",
                "published_date": "2019-12-25T00:00:00Z",
                "last_updated_date": "2019-12-25T00:00:00Z",
                "metadata": {"color": "blue", "size": "small"},
                "organisation": "CCLW",
                "status": "Published",
                "summary": "Summary2",
            },
        ),
    ],
)
def test_families_slug_returns_correct_json(
    data_client: TestClient,
    data_db: Session,
    slug,
    expected_fam,
    valid_token,
    monkeypatch,
    test_vespa,
):
    _populate_db_families(data_db)
    monkeypatch.setattr(search, "_VESPA_CONNECTION", test_vespa)

    # Test associations
    # with patch(
    #     "app.api.api_v1.routers.documents.get_family_from_slug",
    #     return_value=f"id:doc_search:family_document::{expected_fam['import_id']}",
    # ):
    json_response = _make_vespa_fam_lookup_request(
        data_client,
        valid_token,
        slug,
    )
    # assert len(json_response) == N_FAMILY_KEYS

    families = [f for f in json_response["families"]]
    assert json_response["hits"] == len(families) == 1
    # family_name = families[0]["family_name"]
    # assert family_name == family_name_query

    # Verify family data correct.
    assert families[0] is None
    # actual_family_data = {
    #     k: v
    #     for k, v in json_response.items()
    #     if k not in ["events", "documents", "collections"]
    # }
    # assert actual_family_data == expected_fam

    # assert actual_family_data == expected_fam
