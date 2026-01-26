from unittest.mock import patch

from data_in_models.models import Document

from app.navigator_document_etl_pipeline import process_document_updates


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_process_document_updates_flow(mock_upload):
    mock_upload.return_value = None
    assert process_document_updates(["CCLW.legislative.10695.6311"]) == [
        Document(id="CCLW.legislative.10695.6311", title="Climate Change Act 2022")
    ]


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_process_document_updates_flow_with_invalid_id(mock_upload):
    mock_upload.return_value = None
    assert process_document_updates(["CCLW.INVALID_ID"]) == []
