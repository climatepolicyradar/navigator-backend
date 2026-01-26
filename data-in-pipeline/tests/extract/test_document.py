from http import HTTPStatus
from unittest.mock import ANY, MagicMock, patch

import pytest
from requests.exceptions import HTTPError
from returns.pipeline import is_successful
from returns.result import Failure, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    NavigatorDocument,
)
from app.extract.enums import CheckPointStorageType
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.navigator_document_etl_pipeline import (
    extract,
)


@pytest.fixture
def base_config():
    return NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage=CheckPointStorageType.DATABASE,
        checkpoint_key_prefix="navigator",
    )


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
