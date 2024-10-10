"""AWS Helper classes."""

import logging
import re

logger = logging.getLogger(__name__)


class S3Document:
    """A class representing an S3 document."""

    def __init__(self, bucket_name: str, region: str, key: str):  # noqa: D107
        self.bucket_name = bucket_name
        self.region = region
        self.key = key

    @property
    def url(self):
        """Return the URL for this S3 document."""
        return f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{self.key}"

    @classmethod
    def from_url(cls, url: str) -> "S3Document":
        """
        Create an S3 document from a URL.

        :param [str] url: The URL of the document to create
        :return [S3Document]: document representing given URL
        """
        bucket_name, region, key = re.findall(
            r"https:\/\/([\w-]+).s3.([\w-]+).amazonaws.com\/([\w.-]+)", url
        )[0]

        return S3Document(bucket_name=bucket_name, region=region, key=key)
