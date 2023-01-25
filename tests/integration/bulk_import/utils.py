from botocore.exceptions import ClientError


def remove_bucket(s3, bucket_name: str) -> None:
    """Remove an S3 bucket."""
    try:
        s3.delete_bucket(Bucket=validate_bucket_is_test_bucket(bucket_name))
    except ClientError as e:
        print("The bucket does not exist: {}".format(e))


def remove_objects(s3, bucket_name: str) -> None:
    """Remove all the contents of an s3 bucket."""
    try:
        bucket = s3.Bucket(validate_bucket_is_test_bucket(bucket_name))
        bucket.objects.all().delete()
    except ClientError as e:
        print("The bucket does not exist: {}".format(e))


def build_bucket(s3, bucket_name: str, location: dict) -> None:
    """Build an S3 bucket."""
    s3.create_bucket(
        Bucket=validate_bucket_is_test_bucket(bucket_name),
        CreateBucketConfiguration=location,
    )


def upload_file_to_bucket(
    s3, bucket_name: str, upload_path: str, local_file_path: str
) -> None:
    """Upload a file to an S3 bucket."""
    s3.upload_file(
        local_file_path, validate_bucket_is_test_bucket(bucket_name), upload_path
    )


def validate_bucket_is_test_bucket(bucket: str) -> str:
    """Validate that the bucket is a test bucket."""
    bucket_ = bucket.lower()

    if "test" not in bucket_:
        raise Exception("Test bucket does not contain 'test'.")

    if any(
        substring in bucket_
        for substring in [
            "production",
            "prod",
            "staging",
            "dev",
            "development",
            "sandbox",
        ]
    ):
        raise Exception("Test bucket contains a non-test substring.")

    if "backend" not in bucket_:
        raise Exception("Backend test bucket to destroy does not contain 'backend'")

    return bucket
