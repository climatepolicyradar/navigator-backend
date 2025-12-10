from datetime import datetime, timezone
from http import HTTPStatus
from unittest.mock import MagicMock, patch

from app.extract.connectors import (
    FamilyFetchResult,
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorFamily,
    NavigatorOrganisation,
    PageFetchFailure,
)
from app.models import Document, ExtractedEnvelope, ExtractedMetadata
from app.navigator_document_etl_pipeline import process_document_updates
from app.navigator_family_etl_pipeline import etl_pipeline


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


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
def test_process_family_updates_flow_multiple_families(
    mock_connector_class, mock_upload
):
    """Test ETL pipeline with multiple families across pages."""
    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    page_1_data = [
        NavigatorFamily(
            import_id="i00000315",
            title="Belgium UNCBD National Targets",
            corpus=NavigatorCorpus(
                import_id="UNFCCC",
                corpus_type=NavigatorCorpusType(name="corpus_type"),
                organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
            ),
            documents=[
                NavigatorDocument(
                    import_id="i00000315",
                    title="Belgium UNCBD National Targets",
                    events=[],
                )
            ],
            events=[],
            collections=[],
        )
    ]

    page_2_data = [
        NavigatorFamily(
            import_id="i00000316",
            title="France UNCBD National Targets",
            corpus=NavigatorCorpus(
                import_id="UNFCCC",
                corpus_type=NavigatorCorpusType(name="corpus_type"),
                organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
            ),
            documents=[
                NavigatorDocument(
                    import_id="i00000316",
                    title="France UNCBD National Targets",
                    events=[],
                )
            ],
            events=[],
            collections=[],
        )
    ]

    envelope_1 = ExtractedEnvelope(
        data=page_1_data,
        id="test-uuid-1",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        raw_payload=page_1_data,
        content_type="application/json",
        connector_version="1.0.0",
        extracted_at=datetime.now(timezone.utc),
        task_run_id="task-001",
        flow_run_id="flow-001",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=1",
            http_status=HTTPStatus.OK,
        ),
    )

    envelope_2 = ExtractedEnvelope(
        data=page_2_data,
        id="test-uuid-2",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-2",
        raw_payload=page_2_data,
        content_type="application/json",
        connector_version="1.0.0",
        extracted_at=datetime.now(timezone.utc),
        task_run_id="task-001",
        flow_run_id="flow-001",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=2",
            http_status=HTTPStatus.OK,
        ),
    )

    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[envelope_1, envelope_2], failure=None
    )

    result = etl_pipeline()

    assert isinstance(result, list)
    assert (
        len(result) == 2
    )  # Only one family should be processed, as per current logic TODO: APP-1419
    assert result[0].id == "i00000315"


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
def test_process_family_updates_flow_extraction_failure(
    mock_connector_class, mock_upload
):
    """Test ETL pipeline when extraction fails completely."""
    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    # Simulate extraction failure
    expected_error = Exception("500 Internal Server Error")
    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[],
        failure=PageFetchFailure(
            page=1, error=str(expected_error), task_run_id="task-001"
        ),
    )

    result = etl_pipeline()

    assert isinstance(result, Exception)
