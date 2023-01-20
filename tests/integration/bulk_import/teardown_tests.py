import boto3

from cpr_pipeline_reporter.config import AWS_REGION, PIPELINE_BUCKET
from cpr_pipeline_reporter.test.utils import remove_objects, remove_bucket


def tear_down_test_infrastructure():
    """Remove the AWS infrastructure used in the unit tests."""

    s3_conn = boto3.resource("s3", region_name=AWS_REGION)
    remove_objects(s3=s3_conn, bucket_name=PIPELINE_BUCKET)

    s3_conn = boto3.client("s3", region_name=AWS_REGION)
    remove_bucket(s3=s3_conn, bucket_name=PIPELINE_BUCKET)


if __name__ == "__main__":
    tear_down_test_infrastructure()
