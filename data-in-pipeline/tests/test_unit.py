from unittest.mock import MagicMock, patch

import pytest

from app.connector import NavigatorConnectorConfig
from app.extract.navigator_document import (
    NavigatorDocument,
    _fetch_with_retry,
    extract_navigator_document,
)
from app.models import ExtractedEnvelope
from app.navigator_document_etl_pipeline import load_to_s3, process_updates


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


def test_fetch_with_retry_success(monkeypatch):
    import_id = "DOC-123"
    config = NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage="database",
        checkpoint_key_prefix="navigator",
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"data": {"import_id": import_id}}

    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    with patch(
        "app.extract.navigator_document.requests.Session", return_value=mock_session
    ):
        result = _fetch_with_retry(import_id, config)

    assert isinstance(result, NavigatorDocument)
    assert result.import_id == import_id
    mock_session.get.assert_called_once_with(
        f"{config.base_url}/families/documents/{import_id}",
        timeout=config.timeout_seconds,
        headers={"Accept": "application/json"},
    )


def test_fetch_with_retry_no_data(monkeypatch):
    """Ensure ValueError is raised if 'data' missing in response."""
    import_id = "DOC-456"
    config = NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage="database",
        checkpoint_key_prefix="navigator",
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {}  # Missing 'data'

    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    with patch(
        "app.extract.navigator_document.requests.Session", return_value=mock_session
    ):
        with pytest.raises(ValueError, match="No data in response"):
            _fetch_with_retry(import_id, config)


def test_extract_navigator_document_success(monkeypatch):
    """Ensure extract_navigator_document returns ExtractedEnvelope correctly."""
    import_id = "DOC-789"
    config = NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage="database",
        checkpoint_key_prefix="navigator",
    )

    dummy_doc = NavigatorDocument(import_id=import_id)

    with (
        patch(
            "app.extract.navigator_document._fetch_with_retry", return_value=dummy_doc
        ),
        patch(
            "app.extract.navigator_document.generate_envelope_uuid",
            return_value="uuid-123",
        ),
    ):
        result = extract_navigator_document(import_id, config)

    assert isinstance(result, ExtractedEnvelope)
    assert result.source_record_id == import_id
    assert result.source_name == "navigator-documents"
    assert result.metadata["http_status"] == 200


def test_extract_navigator_document_http_error(monkeypatch):
    """Ensure HTTPError is caught and re-raised."""
    import_id = "DOC-999"
    config = NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage="database",
        checkpoint_key_prefix="navigator",
    )

    with patch("app.extract.navigator_document._fetch_with_retry") as mock_fetch:
        from requests import HTTPError

        mock_fetch.side_effect = HTTPError("Boom!")

        with pytest.raises(HTTPError):
            extract_navigator_document(import_id, config)
