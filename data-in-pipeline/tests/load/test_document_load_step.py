from unittest.mock import MagicMock, patch

import pytest

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.enums import CheckPointStorageType
from app.navigator_document_etl_pipeline import (
    load_to_s3,
)


@pytest.fixture
def base_config():
    return NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator-docs",
        checkpoint_storage=CheckPointStorageType.DATABASE,
        checkpoint_key_prefix="navigator",
    )


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
