"""AWS utility functions for interacting with S3 and SSM."""

import json
import logging
import os

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

_LOGGER = logging.getLogger(__name__)


def get_aws_session() -> boto3.Session:
    """
    Get a boto3 session configured with the AWS profile and region from config.

    In local development, uses the AWS_PROFILE.
    In containerized environments (ECS), uses the task IAM role (profile_name=None).
    """
    return boto3.Session(
        profile_name=os.getenv("AWS_PROFILE"), region_name=os.getenv("AWS_REGION")
    )


def get_s3_client() -> BaseClient:
    """Get an S3 client using the configured session."""
    session = get_aws_session()
    return session.client("s3")


def get_ssm_client() -> BaseClient:
    """Get an SSM client using the configured session."""
    session = get_aws_session()
    return session.client("ssm")


def get_bucket_name() -> str:
    """Get and validate the BUCKET_NAME environment variable."""
    bucket_name = os.getenv("BUCKET_NAME")
    if bucket_name is None:
        raise ValueError("BUCKET_NAME is not set")
    return bucket_name


def get_ssm_parameter(name: str, with_decryption: bool = True) -> str:
    """
    Get a parameter from AWS Systems Manager Parameter Store.

    Args:
        name: The name of the parameter to retrieve
        with_decryption: Whether to decrypt SecureString parameters (default: True)

    Returns:
        The parameter value as a string
    """
    ssm = get_ssm_client()
    response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
    return response["Parameter"]["Value"]


def get_secretsmanager_client() -> BaseClient:
    """Get a Secrets Manager client using the configured session."""
    session = get_aws_session()
    return session.client("secretsmanager")


def get_secret(secret_name: str, parse_json: bool = False) -> str | dict:
    """
    Get a secret from AWS Secrets Manager.

    :param secret_name: The name or ARN of the secret to retrieve
    :type secret_name: str
    :param parse_json: Whether to parse the secret as JSON
        (default: False)
    :type parse_json: bool, optional
    :raises ValueError: If the secret cannot be retrieved or parsed
    :return: The secret value as a string, or as a dict if parse_json
        is True
    :rtype: str | dict
    """
    try:
        secrets_client = get_secretsmanager_client()
        response = secrets_client.get_secret_value(SecretId=secret_name)

        secret_string = response.get("SecretString")
        if secret_string is None:
            raise ValueError(f"🔒 Secret '{secret_name}' has no SecretString value")

        if parse_json:
            try:
                parsed = json.loads(secret_string)
                return parsed
            except json.JSONDecodeError as e:
                _LOGGER.exception(
                    f"❌ Failed to parse secret '{secret_name}' as JSON: {e}"
                )
                raise ValueError(f"Secret '{secret_name}' is not valid JSON") from e

        return secret_string

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        _LOGGER.exception(f"❌ Failed to retrieve secret '{secret_name}': {error_code}")
        raise ValueError(
            f"Failed to retrieve secret '{secret_name}': {error_code}"
        ) from e
