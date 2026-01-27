from http import HTTPStatus
from unittest.mock import patch

import pytest
import requests
from returns.pipeline import is_successful

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    NavigatorConnector,
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorFamily,
    NavigatorOrganisation,
    PageFetchFailure,
)
from app.extract.enums import CheckPointStorageType
from app.models import ExtractedEnvelope


@pytest.fixture
def base_config():
    return NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage=CheckPointStorageType.DATABASE,
        checkpoint_key_prefix="navigator",
    )


def test_fetch_family_success(base_config):
    """Ensure fetch_family returns an ExtractedEnvelope correctly."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-111"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    mock_response = {
        "data": NavigatorFamily(
            import_id=import_id,
            title="Test Family",
            summary="Family summary",
            corpus=NavigatorCorpus(
                import_id="COR-111",
                corpus_type=NavigatorCorpusType(name="corpus_type"),
                organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
            ),
            documents=[
                NavigatorDocument(
                    import_id=import_id,
                    title="Test Document",
                    events=[],
                )
            ],
            events=[],
            collections=[],
            geographies=[],
            category="REPORTS",
        ).model_dump(),
    }

    with (
        patch.object(connector, "get", return_value=mock_response),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-xyz"),
    ):
        result = connector.fetch_family(import_id, task_run_id, flow_run_id).unwrap()

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.source_name == "navigator_family"
    assert result.metadata.http_status == HTTPStatus.OK
    connector.close()


def test_fetch_family_no_data(base_config):
    """Ensure ValueError is raised when no data key is present in response and returned as a Failure."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-456"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    with patch.object(
        connector, "get", side_effect=ValueError("No family data in response")
    ):
        result = connector.fetch_family(import_id, task_run_id, flow_run_id)

    assert not is_successful(result)
    failure_exception = result.failure()
    assert isinstance(failure_exception, ValueError)
    assert "No family data in response" in str(failure_exception)
    connector.close()


def test_fetch_family_http_error(base_config):
    """Ensure RequestException is caught and returned as Failure."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-789"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    with patch.object(connector, "get", side_effect=Exception("Boom!")):
        result = connector.fetch_family(import_id, task_run_id, flow_run_id)

    assert not is_successful(result)

    failure_exception = result.failure()
    assert isinstance(failure_exception, Exception)
    assert "Boom!" in str(failure_exception)

    connector.close()


def test_fetch_all_families_successfully(base_config):
    """Test successfully fetching families across multiple pages."""
    connector = NavigatorConnector(base_config)
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    mock_page_1 = {
        "data": [
            NavigatorFamily(
                import_id="FAM-001",
                title="Family 1",
                summary="Family 1 summary",
                corpus=NavigatorCorpus(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                    organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
                ),
                documents=[],
                events=[],
                collections=[],
                geographies=[],
                category="UNFCCC",
            ).model_dump(),
            NavigatorFamily(
                import_id="FAM-002",
                title="Family 2",
                summary="Family summary",
                corpus=NavigatorCorpus(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                    organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
                ),
                documents=[],
                events=[],
                collections=[],
                geographies=[],
                category="UNFCCC",
            ).model_dump(),
        ]
    }
    mock_page_2 = {
        "data": [
            NavigatorFamily(
                import_id="FAM-003",
                title="Family 3",
                summary="Family 3 summary",
                corpus=NavigatorCorpus(
                    import_id="COR-002",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                    organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
                ),
                documents=[],
                events=[],
                collections=[],
                geographies=[],
                category="UNFCCC",
            ).model_dump()
        ]
    }
    mock_page_3 = {"data": []}

    with (
        patch.object(
            connector,
            "get",
            side_effect=[mock_page_1, mock_page_2, mock_page_3],
        ),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-123"),
    ):
        result = connector.fetch_all_families(task_run_id, flow_run_id)

    assert result.failure is None
    expected_num_families = 2
    assert len(result.envelopes) == expected_num_families
    assert (
        result.envelopes[0].source_record_id
        == f"{task_run_id}-families-endpoint-page-1"
    )
    assert (
        result.envelopes[1].source_record_id
        == f"{task_run_id}-families-endpoint-page-2"
    )
    connector.close()


def test_fetch_all_families_no_data_returned_from_endpoint(base_config):
    """Test fetching when the first page is empty (no families exist)."""
    connector = NavigatorConnector(base_config)
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    mock_empty_page = {"data": []}

    with patch.object(connector, "get", return_value=mock_empty_page):
        result = connector.fetch_all_families(task_run_id, flow_run_id)

    assert result.failure is None
    assert len(result.envelopes) == 0
    connector.close()


def test_fetch_all_families_handles_successful_retrievals_and_errors(base_config):
    """Test that HTTP error on second page returns partial results and failure."""
    connector = NavigatorConnector(base_config)
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    mock_page_1 = {
        "data": [
            NavigatorFamily(
                import_id="FAM-001",
                title="Family 1",
                summary="Family summary",
                corpus=NavigatorCorpus(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                    organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
                ),
                documents=[],
                events=[],
                collections=[],
                geographies=[],
                category="UNFCCC",
            ).model_dump()
        ]
    }
    http_error = requests.HTTPError("500 Internal Server Error")

    with (
        patch.object(connector, "get", side_effect=[mock_page_1, http_error]),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-123"),
    ):
        result = connector.fetch_all_families(task_run_id, flow_run_id)

    assert result.failure is not None
    assert isinstance(result.failure, PageFetchFailure)
    expected_page_num_to_fail = 2
    assert result.failure.page == expected_page_num_to_fail
    assert "500 Internal Server Error" in result.failure.error
    assert len(result.envelopes) == 1
    assert (
        result.envelopes[0].source_record_id
        == f"{task_run_id}-families-endpoint-page-1"
    )
    connector.close()


def test_fetch_all_families_handles_errors(base_config):
    """Test that unexpected exceptions are caught and returned as failures."""
    connector = NavigatorConnector(base_config)
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    unexpected_error = ValueError("Unexpected parsing error")

    with patch.object(connector, "get", side_effect=unexpected_error):
        result = connector.fetch_all_families(task_run_id, flow_run_id)

    assert result.failure is not None
    assert isinstance(result.failure, PageFetchFailure)
    assert result.failure.page == 1
    assert "Unexpected parsing error" in result.failure.error
    assert len(result.envelopes) == 0
    connector.close()
