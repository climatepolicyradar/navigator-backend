import requests
from cloudpathlib import S3Path

from app.core.config import PIPELINE_BUCKET, API_HOST

# TODO should these be a pytest fixture
EXPECTED_S3_FILES = []
EXPECTED_RDS_DOCUMENTS = []


def test_final_bulk_import_state_s3():
    """
    Assert that the final state of the test infrastructure is correct post the bulk import with updates.

    - The relevant json objects should be in the s3 bucket with correct object keys.
    """
    s3_files = S3Path(f"s3://{PIPELINE_BUCKET}").glob("*/*.json")

    assert s3_files == EXPECTED_S3_FILES


def test_final_bulk_import_state_rds():
    """
    Assert that the final state of the test infrastructure is correct post the bulk import with updates.

    - The relevant rows should be in the rds database.
    """

    # TODO send a request to the backend and assert the response is correct
    rds_documents = requests.get(f"http://{API_HOST}/api/v1/documents")

    assert rds_documents == EXPECTED_RDS_DOCUMENTS
