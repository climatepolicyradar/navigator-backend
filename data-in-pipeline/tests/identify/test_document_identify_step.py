from http import HTTPStatus
from unittest.mock import patch

import pytest
from returns.pipeline import is_successful

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    NavigatorConnector,
    NavigatorDocument,
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


def test_fetch_document_success(base_config):
    """Ensure fetch_document returns an ExtractedEnvelope with valid data."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-123"
    task_run_id = "task-001"
    flow_run_id = "flow-001"

    mock_response = {
        "data": NavigatorDocument(
            import_id=import_id,
            title="Test Document",
            events=[],
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
