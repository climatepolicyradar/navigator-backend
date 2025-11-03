from unittest.mock import patch

from app.flow import process_document_updates


@patch("app.flow.upload_to_s3")
def test_process_document_updates_flow(mock_upload):
    mock_upload.return_value = None
    assert process_document_updates(["CCLW.legislative.10695.6311"]) == [
        "CCLW.legislative.10695.6311"
    ]
