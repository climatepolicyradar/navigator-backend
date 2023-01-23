import os
from typing import Mapping

import boto3
from botocore import errorfactory

import pandas as pd
from tests.integration.bulk_import.config import (
    PIPELINE_BUCKET,
    S3_PREFIXES,
    AWS_REGION,
)
from tests.integration.bulk_import.utils import upload_file_to_bucket, build_bucket

ACTION_ID_FIELD = "Id"
CATEGORY_FIELD = "Category"
DOCUMENT_ID_FIELD = "Document Id"


def import_id_from_csv_row(row: Mapping[str, str]) -> str:
    return f"CCLW.{row[CATEGORY_FIELD]}.{row[ACTION_ID_FIELD]}.{row[DOCUMENT_ID_FIELD]}"


def get_local_path(file_name: str) -> str:
    """Get the path to the test file."""
    return os.path.join(
        os.getcwd(), "tests", "integration", "bulk_import", "data", file_name
    )


def get_csv_document_ids() -> list[str]:
    """Get the import ids from the csv file."""
    # TODO remove hard code
    df = pd.read_csv(get_local_path("bulk_import.csv"))
    return [import_id_from_csv_row(row[1]) for row in df.iterrows()]


def setup_test_infrastructure():
    """Set up the test infrastructure for the pipeline reporter."""
    s3_conn = boto3.client("s3", region_name=AWS_REGION)
    location = {"LocationConstraint": AWS_REGION}

    try:
        build_bucket(s3=s3_conn, bucket_name=PIPELINE_BUCKET, location=location)
    except errorfactory.ClientError as e:
        print(f"Exception when building bucket: {e}")

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
