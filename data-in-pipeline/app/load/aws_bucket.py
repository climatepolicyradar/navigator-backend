from app.logging_config import ensure_logging_active, get_logger
from app.util import get_s3_client, upload_file

_LOGGER = get_logger()

ensure_logging_active()


def upload_to_s3(json_content, bucket: str, key: str) -> None:
    """
    Uploads JSON content to an S3 bucket.

    :param json_content: JSON content to upload.
    :param bucket: Name of the S3 bucket.
    :param key: Key (filename) for the uploaded content in S3.
    :return: True if upload succeeds, False otherwise.
    """
    s3_client = get_s3_client()
    return upload_file(s3_client, json_content, bucket, key)
