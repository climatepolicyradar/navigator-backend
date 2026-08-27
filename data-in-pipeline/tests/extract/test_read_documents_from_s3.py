import json

import boto3
from data_in_models.models import Document
from moto import mock_aws

from app.navigator_family_etl_pipeline import read_documents_from_s3
from tests.factories import DocumentFactory


@mock_aws
def test_reads_every_jsonl_file_and_ignores_other_extensions():
    first_file = DocumentFactory.batch(2)
    second_file = DocumentFactory.batch(3)

    aws_region: str = "eu-west-1"
    bucket: str = "snowflake-data-export"
    prefix: str = (
        "production/published/2026-01-01T12:00:00.000000/"
        "pipeline_data_in_documents__document_source"
    )

    files: dict[str, list[Document]] = {
        f"{prefix}/data_0_0_0.jsonl": first_file,
        f"{prefix}/data_0_0_1.jsonl": second_file,
        f"{prefix}/metadata.parquet": [],  # ignored
    }

    client = boto3.client("s3", region_name=aws_region)
    client.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": aws_region}
    )
    for key, contents in files.items():
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body="".join(
                json.dumps(doc.model_dump(mode="json")) + "\n" for doc in contents
            ).encode(),
        )

    documents = read_documents_from_s3.fn(bucket_name=bucket, s3_prefix=prefix)

    assert documents == first_file + second_file
