import logging
import os
from typing import Optional

import boto3
from botocore.config import Config

_LOGGER = logging.getLogger(__name__)

AWS_REGION = "eu-west-1"


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(
            connect_timeout=2, retries={"max_attempts": 3, "mode": "standard"}
        ),
    )


def get_api_url() -> str:
    """
    Returns the API URL based on the environment.
    """
    return os.getenv("API_BASE_URL", "https://api.climatepolicyradar.org")


def upload_file(
    client,
    json_content,
    bucket: str,
    key: str,
    content_type: Optional[str] = "application/json; charset=utf-8",
) -> None:
    """
    Upload a file to an S3 bucket by providing its filename.

    :param [str] file_name: name of the file to upload.
    :param [str] bucket: name of the bucket to upload the file to.
    :param [str | None] key: filename of the resulting file on s3. Should include
        the file extension. If not provided, the name of the local file is used.
    :param [str | None] content_type: optional content-type of the file
    """

    # Upload the file
    try:
        if content_type:
            client.put_object(
                Body=json_content, Bucket=bucket, Key=key, ContentType=content_type
            )
        else:
            client.put_object(Body=json_content, Bucket=bucket, Key=key)

    except Exception:
        _LOGGER.exception(f"Uploading {key} encountered an error")
        raise
