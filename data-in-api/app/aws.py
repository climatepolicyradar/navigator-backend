"""AWS utility functions for interacting with S3 and SSM."""

import logging
import os

import boto3
from botocore.client import BaseClient

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


def get_ssm_client() -> BaseClient:
    """Get an SSM client using the configured session."""
    session = get_aws_session()
    return session.client("ssm")


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
