import datetime
import json
from unittest import mock

from app.config import PIPELINE_BUCKET
from app.models.document import DocumentParserInput
from app.service.pipeline import write_documents_to_s3


def test_write_documents_to_s3(test_s3_client, mocker):
    """Really simple check that values are passed to the s3 client correctly"""
    d = DocumentParserInput(
        publication_ts=datetime.datetime(year=2008, month=12, day=25),
        name="name",
        description="description",
        source_url=None,
        download_url=None,
        type="executive",
        source="CCLW",
        import_id="1234-5678",
        slug="geo_2008_name_1234_5678",
        family_import_id="family_1234-5678",
        family_slug="geo_2008_family_1234_5679",
        category="category",
        geography="GEO",
        geographies=["GEO"],
        languages=[],
        metadata={},
    )

    upload_file_mock = mocker.patch.object(test_s3_client, "upload_fileobj")
    datetime_mock = mocker.patch("app.service.pipeline.datetime")
    every_now = datetime.datetime(year=2001, month=12, day=25)
    datetime_mock.now.return_value = every_now

    expected_folder_name = every_now.isoformat().replace(":", ".")
    test_s3_prefix = f"input/{expected_folder_name}"
    write_documents_to_s3(
        test_s3_client,
        test_s3_prefix,
        content={"documents": {d.import_id: d.to_json() for d in [d]}},
    )
    upload_file_mock.assert_called_once_with(
        bucket=PIPELINE_BUCKET,
        key=f"{test_s3_prefix}/db_state.json",
        content_type="application/json",
        fileobj=mock.ANY,
    )
    uploaded_json_documents = json.loads(
        upload_file_mock.mock_calls[0].kwargs["fileobj"].read().decode("utf8")
    )
    assert uploaded_json_documents == {"documents": {d.import_id: d.to_json()}}
