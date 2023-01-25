import boto3

from tests.integration.bulk_import.config import PIPELINE_BUCKET, AWS_REGION
from tests.integration.bulk_import.utils import remove_objects, remove_bucket


def tear_down_test_infrastructure():
    """Remove the AWS infrastructure used in the unit tests."""

    s3_conn = boto3.resource("s3", region_name=AWS_REGION)
    remove_objects(s3=s3_conn, bucket_name=PIPELINE_BUCKET)
    print(f"Removed objects from {PIPELINE_BUCKET}.")

    s3_conn = boto3.client("s3", region_name=AWS_REGION)
    remove_bucket(s3=s3_conn, bucket_name=PIPELINE_BUCKET)
    print(f"Removed {PIPELINE_BUCKET}.")


if __name__ == "__main__":
    tear_down_test_infrastructure()
