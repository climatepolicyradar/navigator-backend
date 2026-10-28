"""How the flow reacts to extract failures.

An incomplete extract (a page that couldn't be fetched, or pagination that drifted)
must fail the run *before* anything is cached, so the previous
documents-latest.jsonl is left intact. Individual families failing validation is
a partial success: the rest are still processed and cached.
"""

from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from prefect.client.schemas.objects import StateType

from app.extract.connectors import (
    DuplicateRecordFailure,
    FamilyFetchResult,
    PageFetchFailure,
    RecordValidationFailure,
)
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.navigator_family_etl_pipeline import data_in_pipeline
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorDocumentFactory,
    NavigatorFamilyFactory,
)


def _envelope(import_id: str) -> ExtractedEnvelope:
    family = NavigatorFamilyFactory.build(
        import_id=import_id,
        category="REPORTS",
        created="2020-01-01T00:00:00Z",
        corpus=NavigatorCorpusFactory.build(import_id="UNFCCC.corpus.i00000001.n0000"),
        documents=[NavigatorDocumentFactory.build(events=[], valid_metadata={})],
        events=[],
        collections=[],
        geographies=[],
        concepts=[],
        metadata={},
    )
    return ExtractedEnvelope(
        data=[family],
        raw_payload=[family.model_dump()],
        source_name="navigator_family",
        source_record_id=f"page-{import_id}",
        connector_version="1.0.0",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/", http_status=HTTPStatus.OK
        ),
        task_run_id="task",
        flow_run_id="flow",
    )


@pytest.fixture
def pipeline_mocks():
    with (
        patch("app.navigator_family_etl_pipeline.run_db_migrations"),
        patch("app.navigator_family_etl_pipeline.upload_to_s3"),
        patch("app.navigator_family_etl_pipeline.NavigatorConnector") as connector,
        patch("app.navigator_family_etl_pipeline.cache_jsonl_to_s3") as cache_jsonl,
    ):
        connector.return_value = MagicMock()
        yield connector.return_value, cache_jsonl


@pytest.mark.parametrize(
    "fatal_failure",
    [
        PageFetchFailure(page=2, error="connection reset", task_run_id="task"),
        DuplicateRecordFailure(import_ids=["family.b"], page=2),
    ],
    ids=["page-fetch-failure", "duplicate-record"],
)
def test_incomplete_extract_fails_run_without_caching(pipeline_mocks, fatal_failure):
    connector, cache_jsonl = pipeline_mocks
    connector.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[_envelope("family.a")], failures=[fatal_failure]
    )

    state = data_in_pipeline(return_state=True)

    assert state.type == StateType.FAILED
    assert "Extract incomplete" in str(state.result(raise_on_failure=False))
    cache_jsonl.submit.assert_not_called()


def test_record_validation_failures_still_cache_the_remaining_families(
    pipeline_mocks,
):
    connector, cache_jsonl = pipeline_mocks
    connector.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[_envelope("family.a")],
        failures=[
            RecordValidationFailure(import_ids=["family.bad"], page=1, error="invalid")
        ],
    )

    state = data_in_pipeline(return_state=True)

    assert state.type == StateType.COMPLETED
    cache_jsonl.submit.assert_called_once()
