from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import NavigatorConnector
from app.extract.enums import CheckPointStorageType
from app.models import ExtractedEnvelope
from app.navigator_document_etl_pipeline import load_to_s3, process_updates


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
    assert process_updates.fn(ids) == expected


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
        bucket="cpr-production-document-cache",
        key="navigator/test-123.json",
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

    mock_response = {"data": {"import_id": import_id}}

    with (
        patch.object(connector, "get", return_value=mock_response),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-123"),
    ):
        result = connector.fetch_document(import_id)

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.metadata.http_status == HTTPStatus.OK
    connector.close()


def test_fetch_document_no_data(base_config):
    """Ensure ValueError is raised when no data key is present in response."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-456"

    with patch.object(connector, "get", return_value={}):
        with pytest.raises(ValueError, match="No data in response"):
            connector.fetch_document(import_id)
    connector.close()


def test_fetch_document_http_error(base_config):
    """Ensure RequestException is caught and re-raised."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-789"

    with patch.object(connector, "get", side_effect=Exception("Boom!")):
        with pytest.raises(Exception, match="Boom!"):
            connector.fetch_document(import_id)
    connector.close()


def test_fetch_family_success(base_config):
    """Ensure fetch_family returns an ExtractedEnvelope correctly."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-111"

    mock_response = {"data": {"import_id": import_id}}

    with (
        patch.object(connector, "get", return_value=mock_response),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-xyz"),
    ):
        result = connector.fetch_family(import_id)

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.source_name == "navigator_family"
    assert result.metadata.http_status == HTTPStatus.OK
    connector.close()
