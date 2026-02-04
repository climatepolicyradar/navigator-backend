import json
from unittest.mock import MagicMock, patch

from app.navigator_family_etl_pipeline import upload_report


def make_future(result):
    future = MagicMock()
    future.result.return_value = result
    return future


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.get_logger")
def test_upload_report_uploads_summary(mock_get_logger, mock_upload_to_s3):
    mock_logger = mock_get_logger.return_value

    document_batches = [
        [MagicMock(), MagicMock()],
        [MagicMock()],
    ]

    load_results = [
        make_future("ok"),
        make_future(Exception("db error")),
    ]

    run_id = "test-run-123"

    upload_report.fn(document_batches, load_results, run_id)

    expected_report = {
        "run_id": run_id,
        "total_batches": 2,
        "total_documents": 3,
        "successful_batches": 1,
        "failed_batches": 1,
    }

    mock_upload_to_s3.assert_called_once_with(
        json.dumps(expected_report),
        bucket="cpr-cache",
        key="pipelines/data-in-pipeline/navigator_family/test-run-123-load-report.json",
    )

    mock_logger.info.assert_called_once_with("Uploaded load report for 3 documents")


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_upload_report_all_batches_success(mock_upload_to_s3):
    document_batches = [[MagicMock()], [MagicMock()]]
    load_results = [
        make_future("ok"),
        make_future("ok"),
    ]

    run_id = "test-run-123"

    upload_report.fn(document_batches, load_results, run_id)

    uploaded_json = mock_upload_to_s3.call_args.args[0]
    report = json.loads(uploaded_json)

    assert report["run_id"] == run_id
    assert report["total_batches"] == len(document_batches)
