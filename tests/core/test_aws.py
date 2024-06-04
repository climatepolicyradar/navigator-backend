from unittest.mock import patch

from app.core.aws import S3Client


def test_s3client_get_latest_ingest_start(mock_pipeline_bucket):
    with patch("app.core.aws.PIPELINE_BUCKET", mock_pipeline_bucket):
        start_date = S3Client(False).get_latest_ingest_start()
    assert start_date == "2024-01-02"
