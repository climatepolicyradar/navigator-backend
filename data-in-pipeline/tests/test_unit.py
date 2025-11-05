from unittest.mock import MagicMock, patch

import pytest

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
