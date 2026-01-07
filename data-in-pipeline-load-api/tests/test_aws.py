"""Tests for AWS helper functions."""

import os
from unittest.mock import patch

import pytest
from moto import mock_aws

from app.aws import (
    get_aws_session,
    get_bucket_name,
    get_s3_client,
    get_secretsmanager_client,
    get_ssm_client,
)


@pytest.fixture
def mock_aws_creds():
    """Mocked AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_REGION"] = "eu-west-1"
    yield

    # Cleanup
    os.environ.pop("AWS_ACCESS_KEY_ID", None)
    os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
    os.environ.pop("AWS_SECURITY_TOKEN", None)
    os.environ.pop("AWS_SESSION_TOKEN", None)
    os.environ.pop("AWS_REGION", None)


@patch("app.aws.boto3.Session")
def test_get_aws_session_with_profile(mock_session_class, mock_aws_creds):
    """Test session creation with AWS_PROFILE set."""
    mock_session = mock_session_class.return_value
    mock_session.profile_name = "test-profile"
    mock_session.region_name = "eu-west-1"

    with patch.dict(os.environ, {"AWS_PROFILE": "test-profile"}):
        session = get_aws_session()
        mock_session_class.assert_called_once_with(
            profile_name="test-profile", region_name="eu-west-1"
        )
        assert session == mock_session


@patch("app.aws.boto3.Session")
def test_get_aws_session_without_profile(mock_session_class, mock_aws_creds):
    """Test session creation without AWS_PROFILE."""
    mock_session = mock_session_class.return_value
    mock_session.profile_name = None
    mock_session.region_name = "us-east-1"

    # Set only AWS_REGION, ensure AWS_PROFILE is not set
    with patch.dict(os.environ, {"AWS_REGION": "us-east-1"}, clear=False):
        os.environ.pop("AWS_PROFILE", None)
        session = get_aws_session()
        mock_session_class.assert_called_once_with(
            profile_name=None, region_name="us-east-1"
        )
        assert session == mock_session


@mock_aws
def test_get_s3_client_returns_client(mock_aws_creds):
    """Test that get_s3_client returns a valid S3 client."""
    client = get_s3_client()
    assert client is not None
    assert client.__class__.__name__ == "S3"


@mock_aws
def test_get_ssm_client_returns_client(mock_aws_creds):
    """Test that get_ssm_client returns a valid SSM client."""
    client = get_ssm_client()
    assert client is not None
    assert client.__class__.__name__ == "SSM"


@mock_aws
def test_get_secretsmanager_client_returns_client(mock_aws_creds):
    """Test that get_secretsmanager_client returns a valid client."""
    client = get_secretsmanager_client()
    assert client is not None
    assert client.__class__.__name__ == "SecretsManager"


def test_get_bucket_name_success():
    """Test successful retrieval of bucket name."""
    with patch.dict(os.environ, {"BUCKET_NAME": "my-test-bucket"}):
        bucket_name = get_bucket_name()
        assert bucket_name == "my-test-bucket"


def test_get_bucket_name_missing():
    """Test that ValueError is raised when BUCKET_NAME is not set."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="BUCKET_NAME is not set"):
            get_bucket_name()
