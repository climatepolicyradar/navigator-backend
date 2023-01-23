import os
import boto3
import pandas as pd

from tests.integration.bulk_import.config import (
    PIPELINE_BUCKET,
    S3_PREFIXES,
    AWS_REGION,
)
from tests.integration.bulk_import.utils import upload_file_to_bucket, build_bucket
from app.core.validation.cclw.law_policy.process_csv import import_id_from_csv_row


def get_local_path(file_name: str) -> str:
    """Get the path to the test file."""
    return os.path.join(
        os.getcwd(), "tests", "integration", "bulk-import", "data", file_name
    )


def get_csv_document_ids() -> list[str]:
    """Get the import ids from the csv file."""
    # TODO remove hard code
    df = pd.read_csv("bulk_import.csv")
    return [import_id_from_csv_row(row[1]) for row in df.iterrows()]


def setup_test_infrastructure():
    """Set up the test infrastructure for the pipeline reporter."""
    s3_conn = boto3.client("s3", region_name=AWS_REGION)
    location = {"LocationConstraint": AWS_REGION}

    try:
        build_bucket(s3=s3_conn, bucket_name=PIPELINE_BUCKET, location=location)
    # TODO Tighten up with botocore.errorfactory.BucketAlreadyOwnedByYou
    except Exception as e:
        pass

    for prefix in S3_PREFIXES:
        for import_id in get_csv_document_ids():
            # TODO remove hard code
            upload_file_to_bucket(
                s3_conn,
                PIPELINE_BUCKET,
                f"{prefix}/{import_id}.json",
                get_local_path("document.json"),
            )


if __name__ == "__main__":
    setup_test_infrastructure()
