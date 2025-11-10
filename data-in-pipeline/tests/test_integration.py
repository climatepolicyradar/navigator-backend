from unittest.mock import patch

from app.navigator_document_etl_pipeline import process_updates


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_process_document_updates_flow(mock_upload):
    mock_upload.return_value = None
    assert process_updates(["CCLW.legislative.10695.6311"]) == [
        "CCLW.legislative.10695.6311"
    ]
