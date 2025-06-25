from unittest.mock import patch

import pytest
from tests.search.vespa.setup_search_tests import (
    _make_search_request,
    _populate_db_families,
)

from app.models.search import SearchResponse


@pytest.mark.search
@patch(
    "app.api.api_v1.routers.search.AppTokenFactory.verify_corpora_in_db",
    return_value=True,
)
@pytest.mark.parametrize(
    "query,concept_filters",
    [
        ("the", [{"name": "name", "value": "environment"}]),
        (
            "the",
            [
                {"name": "parent_concept_ids_flat", "value": "Q0,"},
                {"name": "id", "value": "0"},
            ],
        ),
        (
            "",
            [
                {"name": "model", "value": "sectors_1"},
                {"name": "id", "value": "0"},
            ],
        ),
    ],
)
def test_concept_filters(
    mock_corpora_exist_in_db,
    query,
    concept_filters,
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
            "concept_filters": concept_filters,
        },
        expected_status_code=200,
    )
    response: SearchResponse = SearchResponse.model_validate(response)

    assert len(response.families) > 0
    for concept_filter in concept_filters:
        for family in response.families:
            for family_document in family.family_documents:
                for passage_match in family_document.document_passage_matches:
                    assert passage_match.concepts
                    if concept_filter["name"] == "parent_concept_ids_flat":
                        assert any(
                            [
                                concept_filter["value"]
                                in concept.parent_concept_ids_flat
                                for concept in passage_match.concepts
                            ]
                        )
                    else:
                        assert any(
                            [
                                concept.__getattribute__(concept_filter["name"])
                                == concept_filter["value"]
                                for concept in passage_match.concepts
                            ]
                        )

    assert mock_corpora_exist_in_db.assert_called
