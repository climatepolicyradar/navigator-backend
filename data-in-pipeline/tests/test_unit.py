from http import HTTPStatus
from unittest.mock import ANY, MagicMock, patch

import pytest
import requests
from requests.exceptions import HTTPError
from returns.pipeline import is_successful
from returns.result import Failure, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    NavigatorConnector,
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorFamily,
    PageFetchFailure,
)
from app.extract.enums import CheckPointStorageType
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.navigator_document_etl_pipeline import (
    extract,
    load_to_s3,
    process_document_updates,
)


@pytest.fixture
def base_config():
    return NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage=CheckPointStorageType.DATABASE,
        checkpoint_key_prefix="navigator",
    )


@pytest.mark.parametrize("ids, expected", [(["11", "22", "33"], ["11", "22", "33"])])
@pytest.mark.skip(reason="Not implemented")
def test_process_document_updates_flow(ids: list[str], expected: list[str]):
    assert process_document_updates.fn(ids) == expected


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_load_document_success(mock_upload):
    """Test successful document caching."""

    mock_navigator_doc = MagicMock()
    mock_navigator_doc.model_dump_json.return_value = '{"id": "test-123"}'
    mock_navigator_doc.id = "test-123"
    mock_upload.return_value = True

    load_to_s3.fn(mock_navigator_doc)

    mock_upload.assert_called_once_with(
        '{"id": "test-123"}',
        bucket="cpr-cache",
        key="pipelines/data-in-pipeline/navigator_document/test-123.json",
    )


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_load_document_handles_upload_failure(mock_upload):
    """Test handling of S3 upload failure."""

    mock_navigator_doc = MagicMock()
    mock_navigator_doc.model_dump_json.return_value = '{"id": "test-123"}'
    mock_navigator_doc.id = "test-123"
    mock_upload.side_effect = Exception("S3 connection failed")

    with pytest.raises(Exception, match="S3 connection failed"):
        load_to_s3.fn(mock_navigator_doc)


def test_fetch_document_success(base_config):
    """Ensure fetch_document returns an ExtractedEnvelope with valid data."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-123"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    mock_response = {
        "data": NavigatorDocument(
            import_id=import_id, title="Test Document", events=[]
        ).model_dump()
    }

    with (
        patch.object(connector, "get", return_value=mock_response),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-123"),
    ):
        result = connector.fetch_document(import_id, task_run_id, flow_run_id).unwrap()

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.metadata.http_status == HTTPStatus.OK
    connector.close()


def test_fetch_document_no_data(base_config):
    """Ensure ValueError is raised when no data key is present in response."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-456"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    with patch.object(
        connector, "get", side_effect=ValueError("No document data in response")
    ):
        result = connector.fetch_document(import_id, task_run_id, flow_run_id)

    assert not is_successful(result)
    failure_exception = result.failure()
    assert isinstance(failure_exception, ValueError)
    assert "No document data in response" in str(failure_exception)

    connector.close()


def test_fetch_document_http_error(base_config):
    """Ensure RequestException is caught and re-raised."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-789"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    with patch.object(connector, "get", side_effect=Exception("Boom!")):
        result = connector.fetch_document(import_id, task_run_id, flow_run_id)

    assert not is_successful(result)

    failure_exception = result.failure()
    assert isinstance(failure_exception, Exception)
    assert "Boom!" in str(failure_exception)
    connector.close()


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
            corpus=NavigatorCorpus(
                import_id="COR-111", corpus_type=NavigatorCorpusType(name="corpus_type")
            ),
            documents=[
                NavigatorDocument(import_id=import_id, title="Test Document", events=[])
            ],
            events=[],
            collections=[],
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


def test_extract_document_handles_valid_id_success():
    """Test extract task successfully processes a valid document ID."""
    valid_id = "VALID_ID"

    with patch(
        "app.navigator_document_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None
        task_run_id = "task-001"
        flow_run_id = "flow-001"

        mock_connector_instance.fetch_document.return_value = Success(
            ExtractedEnvelope(
                source_record_id=valid_id,
                source_name="navigator_document",
                data=NavigatorDocument(
                    import_id=valid_id,
                    title="A Valid Document",
                    events=[],
                ),
                metadata=ExtractedMetadata(
                    endpoint="www.capsule-corp.com", http_status=HTTPStatus.OK
                ),
                raw_payload="{}",
                connector_version="1.0.0",
                task_run_id=task_run_id,
                flow_run_id=flow_run_id,
            )
        )

        result = extract(valid_id)

    assert is_successful(result)
    extracted = result.unwrap()
    assert extracted.source_record_id == valid_id
    assert extracted.source_name == "navigator_document"
    # The task run id and flow run are generated inside extract by prefect so we can't assert their exact values here
    mock_connector_instance.fetch_document.assert_called_once_with(valid_id, ANY, ANY)


def test_extract_document_propagates_connector_failure():
    """Test extract propagates Failure when connector returns Failure."""
    invalid_id = "INVALID_ID"

    with patch(
        "app.navigator_document_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None

        expected_error = ConnectionError("Failed to fetch document: API timeout")
        mock_connector_instance.fetch_document.return_value = Failure(expected_error)

        result = extract(invalid_id)

    assert not is_successful(result)
    failure_exception = result.failure()
    assert isinstance(failure_exception, ConnectionError)
    assert "API timeout" in str(failure_exception)
    # The task run id and flow run are generated inside extract by prefect so we can't assert their exact values here
    mock_connector_instance.fetch_document.assert_called_once_with(invalid_id, ANY, ANY)


def test_extract_document_handles_http_error():
    """Test extract propagates HTTPError failures from connector."""
    invalid_id = "NOT_FOUND_ID"

    with patch(
        "app.navigator_document_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None

        expected_error = HTTPError("404 Client Error: Not Found")
        mock_connector_instance.fetch_document.return_value = Failure(expected_error)

        result = extract(invalid_id)

    assert not is_successful(result)
    failure_exception = result.failure()
    assert isinstance(failure_exception, HTTPError)
    assert "404" in str(failure_exception)


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
                corpus=NavigatorCorpus(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                ),
                documents=[],
                events=[],
                collections=[],
            ).model_dump(),
            NavigatorFamily(
                import_id="FAM-002",
                title="Family 2",
                corpus=NavigatorCorpus(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                ),
                documents=[],
                events=[],
                collections=[],
            ).model_dump(),
        ]
    }
    mock_page_2 = {
        "data": [
            NavigatorFamily(
                import_id="FAM-003",
                title="Family 3",
                corpus=NavigatorCorpus(
                    import_id="COR-002",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                ),
                documents=[],
                events=[],
                collections=[],
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
    assert len(result.envelopes) == 2
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
                corpus=NavigatorCorpus(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusType(name="corpus_type"),
                ),
                documents=[],
                events=[],
                collections=[],
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
    assert result.failure.page == 2
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
