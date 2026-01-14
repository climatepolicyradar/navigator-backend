from datetime import UTC, datetime
from http import HTTPStatus
from unittest.mock import MagicMock, patch

from requests.exceptions import HTTPError

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


@patch("app.navigator_family_etl_pipeline.run_migrations")
@patch("app.load.load.get_ssm_parameter", return_value="")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.post")
def test_process_family_updates_flow_multiple_families(
    mock_post, mock_connector_class, mock_upload, mock_ssm, mock_run_migrations
):
    """Test ETL pipeline with multiple families across pages."""
    mock_run_migrations.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    mock_post_response = MagicMock()
    mock_post_response.status_code = HTTPStatus.CREATED
    mock_post_response.json.return_value = ["1", "2"]
    mock_post.return_value = mock_post_response

    page_1_data = [
        NavigatorFamily(
            import_id="i00000315",
            title="Belgium UNCBD National Targets",
            summary="Family summary",
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
            summary="Family summary",
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
        extracted_at=datetime.now(UTC),
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
        extracted_at=datetime.now(UTC),
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

    expected_number_of_results = 2
    assert isinstance(result, list)
    assert len(result) == expected_number_of_results
    assert result[0] == "1"


@patch("app.navigator_family_etl_pipeline.run_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
def test_process_family_updates_flow_extraction_failure(
    mock_connector_class,
    mock_upload,
    mock_run_migrations,
):
    """Test ETL pipeline when extraction fails completely."""
    mock_upload.return_value = None
    mock_run_migrations.return_value = None

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


@patch("app.navigator_family_etl_pipeline.run_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.post")
def test_etl_pipeline_load_failure(
    mock_post, mock_connector_class, mock_upload, mock_run_migrations
):
    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    mock_response = MagicMock()
    mock_response.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    mock_response.raise_for_status.side_effect = HTTPError("Server error")
    mock_post.return_value = mock_response

    test_data = [
        NavigatorFamily(
            import_id="i00000315",
            title="Belgium UNCBD National Targets",
            summary="Family summary",
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

    test_envelope = ExtractedEnvelope(
        data=test_data,
        id="test-uuid-1",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        raw_payload=test_data,
        content_type="application/json",
        connector_version="1.0.0",
        extracted_at=datetime.now(UTC),
        task_run_id="task-001",
        flow_run_id="flow-001",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=1",
            http_status=HTTPStatus.OK,
        ),
    )

    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[test_envelope], failure=None
    )

    result = etl_pipeline()

    assert isinstance(result, Exception)
