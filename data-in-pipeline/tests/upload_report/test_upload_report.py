import json
from unittest.mock import patch

import pytest

from app.navigator_family_etl_pipeline import upload_report


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_upload_report_exceeds_limit_uploads_to_s3(mock_upload_to_s3):
    """Test upload to S3 when result exceeds log limit."""
    loaded_ids = [f"doc-{i}" for i in range(150)]
    run_id = "test-run-123"
    result_log_limit = 100

    upload_report.fn(loaded_ids, run_id, result_log_limit)

    mock_upload_to_s3.assert_called_once_with(
        json.dumps(loaded_ids),
        bucket="cpr-cache",
        key="pipelines/data-in-pipeline/navigator_family/test-run-123-result.json",
    )


@patch("app.navigator_family_etl_pipeline.get_logger")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_upload_report_within_limit_logs_ids(mock_upload_to_s3, mock_get_logger):
    """Test logging IDs when result is within log limit."""
    mock_logger = mock_get_logger.return_value
    loaded_ids = ["doc-1", "doc-2", "doc-3"]
    run_id = "test-run-123"
    result_log_limit = 100

    upload_report.fn(loaded_ids, run_id, result_log_limit)

    mock_upload_to_s3.assert_not_called()
    mock_logger.info.assert_called_once_with("Loaded document IDs: %s", loaded_ids)


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_upload_report_handles_upload_failure(mock_upload_to_s3):
    """Test handling of S3 upload failure."""
    loaded_ids = [f"doc-{i}" for i in range(150)]
    run_id = "test-run-123"
    result_log_limit = 100
    mock_upload_to_s3.side_effect = Exception("S3 connection failed")

    with pytest.raises(Exception, match="S3 connection failed"):
        upload_report.fn(loaded_ids, run_id, result_log_limit)
