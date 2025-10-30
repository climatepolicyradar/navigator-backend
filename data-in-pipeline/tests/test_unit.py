from unittest.mock import Mock, patch

import pytest

from app.flow import cache_document, process_document_updates


@pytest.mark.parametrize("ids, expected", [(["11", "22", "33"], ["11", "22", "33"])])
@pytest.mark.skip(reason="Not implemented")
def test_process_document_updates_flow(ids: list[str], expected: list[str]):
    assert process_document_updates.fn(ids) == expected


@patch("app.flow.upload_to_s3")
def test_cache_document_success(mock_upload):
    """Test successful document caching."""

    mock_navigator_doc = Mock()
    mock_navigator_doc.model_dump_json.return_value = '{"data": "test"}'
    mock_navigator_doc.data.import_id = "test-123"
    mock_upload.return_value = True

    cache_document.fn(mock_navigator_doc)

    mock_upload.assert_called_once_with(
        '{"data": "test"}',
        bucket="cpr-production-document-cache",
        key="navigator/test-123.json",
    )


@patch("app.flow.upload_to_s3")
def test_cache_document_handles_upload_failure(mock_upload):
    """Test handling of S3 upload failure."""

    mock_navigator_doc = Mock()
    mock_navigator_doc.model_dump_json.return_value = '{"data": "test"}'
    mock_navigator_doc.data.import_id = "test-123"
    mock_upload.side_effect = Exception("S3 connection failed")

    with pytest.raises(Exception, match="S3 connection failed"):
        cache_document.fn(mock_navigator_doc)
