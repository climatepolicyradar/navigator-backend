from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from returns.pipeline import is_successful
from returns.result import Failure, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    NavigatorConnector,
    NavigatorCorpus,
    NavigatorDocument,
    NavigatorFamily,
)
from app.extract.enums import CheckPointStorageType
from app.models import ExtractedEnvelope, ExtractedMetadata
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

    mock_response = {
        "data": NavigatorDocument(
            import_id=import_id, title="Test Document"
        ).model_dump()
    }

    with (
        patch.object(connector, "get", return_value=mock_response),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-123"),
    ):
        result = connector.fetch_document(import_id).unwrap()

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.metadata.http_status == HTTPStatus.OK
    connector.close()


def test_fetch_document_no_data(base_config):
    """Ensure ValueError is raised when no data key is present in response."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-456"

    with patch.object(
        connector, "get", side_effect=ValueError("No document data in response")
    ):
        result = connector.fetch_document(import_id)

    assert not is_successful(result)
    failure_exception = result.failure()
    assert isinstance(failure_exception, ValueError)
    assert "No document data in response" in str(failure_exception)

    connector.close()


def test_fetch_document_http_error(base_config):
    """Ensure RequestException is caught and re-raised."""
    connector = NavigatorConnector(base_config)
    import_id = "DOC-789"

    with patch.object(connector, "get", side_effect=Exception("Boom!")):
        result = connector.fetch_document(import_id)

    assert not is_successful(result)

    failure_exception = result.failure()
    assert isinstance(failure_exception, Exception)
    assert "Boom!" in str(failure_exception)
    connector.close()


def test_fetch_family_success(base_config):
    """Ensure fetch_family returns an ExtractedEnvelope correctly."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-111"

    mock_response = {
        "data": NavigatorFamily(
            import_id=import_id,
            title="Test Family",
            corpus=NavigatorCorpus(import_id="COR-111"),
            documents=[NavigatorDocument(import_id=import_id, title="Test Document")],
        ).model_dump()
    }

    with (
        patch.object(connector, "get", return_value=mock_response),
        patch("app.extract.connectors.generate_envelope_uuid", return_value="uuid-xyz"),
    ):
        result = connector.fetch_family(import_id).unwrap()

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.source_name == "navigator_family"
    assert result.metadata.http_status == HTTPStatus.OK
    connector.close()


def test_fetch_family_no_data(base_config):
    """Ensure ValueError is raised when no data key is present in response and returned as a Failure."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-456"

    with patch.object(
        connector, "get", side_effect=ValueError("No family data in response")
    ):
        result = connector.fetch_family(import_id)

    assert not is_successful(result)
    failure_exception = result.failure()
    assert isinstance(failure_exception, ValueError)
    assert "No family data in response" in str(failure_exception)
    connector.close()


def test_fetch_family_http_error(base_config):
    """Ensure RequestException is caught and returned as Failure."""
    connector = NavigatorConnector(base_config)
    import_id = "FAM-789"

    with patch.object(connector, "get", side_effect=Exception("Boom!")):
        result = connector.fetch_family(import_id)

    assert not is_successful(result)

    failure_exception = result.failure()
    assert isinstance(failure_exception, Exception)
    assert "Boom!" in str(failure_exception)

    connector.close()


def test_extract_document_handles_valid_id_success():
    """Test extract task successfully processes a valid document ID."""
    from returns.pipeline import is_successful

    from app.navigator_document_etl_pipeline import extract

    valid_id = "VALID_ID"

    with patch(
        "app.navigator_document_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None

        mock_connector_instance.fetch_document.return_value = Success(
            ExtractedEnvelope(
                source_record_id=valid_id,
                source_name="navigator_document",
                data=NavigatorDocument(
                    import_id=valid_id,
                    title="A Valid Document",
                ),
                metadata=ExtractedMetadata(
                    endpoint="www.capsule-corp.com", http_status=HTTPStatus.OK
                ),
                raw_payload="{}",
                connector_version="1.0.0",
            )
        )

        result = extract(valid_id)

    assert is_successful(result)
    extracted = result.unwrap()
    assert extracted.source_record_id == valid_id
    assert extracted.source_name == "navigator_document"
    mock_connector_instance.fetch_document.assert_called_once_with(valid_id)


def test_extract_document_propagates_connector_failure():
    """Test extract propagates Failure when connector returns Failure."""
    from returns.pipeline import is_successful

    from app.navigator_document_etl_pipeline import extract

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
    mock_connector_instance.fetch_document.assert_called_once_with(invalid_id)


def test_extract_document_handles_http_error():
    """Test extract propagates HTTPError failures from connector."""
    from requests.exceptions import HTTPError
    from returns.pipeline import is_successful

    from app.navigator_document_etl_pipeline import extract

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
