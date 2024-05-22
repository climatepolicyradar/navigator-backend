import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from app.core.aws import S3Client


@pytest.fixture(scope="function")
def mock_aws_creds():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def mock_s3_client(mock_aws_creds):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def mock_pipeline_bucket(mock_s3_client):
    pipeline_bucket = "test_bucket"
    ingest_trigger_root = "input"
    os.environ["PIPELINE_BUCKET"] = pipeline_bucket
    os.environ["INGEST_TRIGGER_ROOT"] = ingest_trigger_root
    mock_s3_client.create_bucket(Bucket=pipeline_bucket)

    test_prefixes = [
        "2024-15-22T21.53.26.945831",
        "2024-01-02T18.10.56.827645",
        "2023-12-10T23.11.27.584565",
        "2023-07-15T14.33.31.783564",
        "2022-11-06T14.57.17.873576",
        "2022-05-03T15.38.21.245423",
    ]
    for prefix in test_prefixes:

        mock_s3_client.put_object(
            Bucket=pipeline_bucket,
            Key=f"{ingest_trigger_root}/{prefix}/test_file.txt",
            Body="data".encode(),
        )
    return pipeline_bucket


def test_s3client_get_latest_ingest_start(mock_pipeline_bucket):
    with patch("app.core.aws.PIPELINE_BUCKET", "test_bucket"):
        start_date = S3Client(False).get_latest_ingest_start()
    assert start_date == "2024-01-02"
