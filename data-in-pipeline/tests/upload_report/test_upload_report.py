import json
from typing import cast
from unittest.mock import MagicMock, patch

from data_in_models.models import Document

from app.navigator_family_etl_pipeline import upload_report


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.get_logger")
def test_upload_report_uploads_summary(mock_get_logger, mock_upload_to_s3):
    mock_logger = mock_get_logger.return_value

    first_batch_size = 2
    second_batch_size = 1
    total_documents = first_batch_size + second_batch_size
    total_batches = 2

    document_batches: list[list[Document]] = [
        [cast(Document, MagicMock()) for _ in range(first_batch_size)],
        [cast(Document, MagicMock()) for _ in range(second_batch_size)],
    ]

    # Now load_results contains actual results, not futures
    successful_batches_count = 1
    failed_batches_count = 1
    load_results = [
        "ok",  # Success case - string result
        Exception("db error"),  # Failure case - Exception
    ]

    run_id = "test-run-123"

    upload_report.fn(document_batches, load_results, run_id)

    expected_report = {
        "run_id": run_id,
        "total_batches": total_batches,
        "total_documents": total_documents,
        "successful_batches": successful_batches_count,
        "failed_batches": failed_batches_count,
    }

    mock_upload_to_s3.assert_called_once_with(
        json.dumps(expected_report),
        bucket="cpr-cache",
        key=f"pipelines/data-in-pipeline/navigator_family/{run_id}-load-report.json",
    )

    mock_logger.info.assert_called_once_with(
        f"Uploaded load report for {total_documents} documents"
    )


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_upload_report_all_batches_success(mock_upload_to_s3):
    batch_size = 1
    total_batches = 2
    total_documents = batch_size * total_batches

    document_batches: list[list[Document]] = [
        [cast(Document, MagicMock()) for _ in range(batch_size)],
        [cast(Document, MagicMock()) for _ in range(batch_size)],
    ]

    # All successful results - just strings
    successful_batches_count = total_batches
    failed_batches_count = 0
    load_results = ["ok"] * total_batches

    run_id = "test-run-123"

    upload_report.fn(document_batches, load_results, run_id)

    uploaded_json = mock_upload_to_s3.call_args.args[0]
    report = json.loads(uploaded_json)

    assert report["run_id"] == run_id
    assert report["total_batches"] == total_batches
    assert report["total_documents"] == total_documents
    assert report["successful_batches"] == successful_batches_count
    assert report["failed_batches"] == failed_batches_count


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.get_logger")
def test_upload_report_empty_batches(mock_get_logger, mock_upload_to_s3):
    mock_logger = mock_get_logger.return_value

    document_batches: list[list[Document]] = []
    load_results = []
    run_id = "test-run-empty"

    upload_report.fn(document_batches, load_results, run_id)

    uploaded_json = mock_upload_to_s3.call_args.args[0]
    report = json.loads(uploaded_json)

    assert report["run_id"] == run_id
    assert report["total_batches"] == 0
    assert report["total_documents"] == 0
    assert report["successful_batches"] == 0
    assert report["failed_batches"] == 0

    mock_logger.info.assert_called_once_with("Uploaded load report for 0 documents")


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.get_logger")
def test_upload_report_mixed_batch_sizes(mock_get_logger, mock_upload_to_s3):
    mock_logger = mock_get_logger.return_value

    batch_sizes = [3, 7, 5]
    total_batches = len(batch_sizes)
    total_documents = sum(batch_sizes)
    successful_batches_count = 2
    failed_batches_count = 1

    document_batches: list[list[Document]] = [
        [cast(Document, MagicMock()) for _ in range(size)] for size in batch_sizes
    ]

    load_results = [
        "ok",
        Exception("db error"),
        "ok",
    ]

    run_id = "test-run-mixed"

    upload_report.fn(document_batches, load_results, run_id)

    uploaded_json = mock_upload_to_s3.call_args.args[0]
    report = json.loads(uploaded_json)

    assert report["run_id"] == run_id
    assert report["total_batches"] == total_batches
    assert report["total_documents"] == total_documents
    assert report["successful_batches"] == successful_batches_count
    assert report["failed_batches"] == failed_batches_count

    expected_info_message = f"Uploaded load report for {total_documents} documents"
    mock_logger.info.assert_called_once_with(expected_info_message)
