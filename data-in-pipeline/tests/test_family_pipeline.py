from datetime import UTC, datetime
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError
from returns.result import Failure

from app.extract.connectors import (
    FamilyFetchResult,
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorFamily,
    NavigatorOrganisation,
    PageFetchFailure,
)
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.navigator_family_etl_pipeline import data_in_pipeline
from app.transform.models import NoMatchingTransformations


@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.put")
def test_process_family_updates_flow_multiple_families(
    mock_post, mock_connector_class, mock_upload, mock_run_migrations
):
    """Test ETL pipeline with multiple families across pages."""
    mock_run_migrations.return_value = None

    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    mock_post_response = MagicMock()
    mock_post_response.status_code = 201
    mock_post_response.json.return_value = ["1", "2"]
    mock_post.return_value = mock_post_response

    page_1_data = [
        NavigatorFamily(
            import_id="i00000315",
            title="Belgium UNCBD National Targets",
            summary="Family summary",
            category="REPORTS",
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
            geographies=[],
        )
    ]

    page_2_data = [
        NavigatorFamily(
            import_id="i00000316",
            title="France UNCBD National Targets",
            summary="Family summary",
            category="REPORTS",
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
            geographies=[],
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

    result = data_in_pipeline()

    expected_number_of_results = 2
    assert isinstance(result, list)
    assert len(result) == expected_number_of_results
    assert result[0] == "1"


@patch("app.navigator_family_etl_pipeline.run_db_migrations")
def test_process_family_updates_migrations_failure(mock_run_migrations):
    """Test ETL pipeline when extraction fails completely."""
    mock_run_migrations.side_effect = Exception("500 Internal Server Error")

    # Simulate migrations failure
    with pytest.raises(Exception, match="500 Internal Server Error"):
        data_in_pipeline()


@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
def test_process_family_updates_flow_extraction_failure(
    mock_connector_class, mock_upload, mock_run_migrations
):
    """Test ETL pipeline when extraction fails completely."""
    mock_run_migrations.return_value = None

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

    result = data_in_pipeline()

    assert isinstance(result, Exception)


@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.put")
def test_etl_pipeline_load_failure(
    mock_post, mock_connector_class, mock_upload, mock_run_migrations
):
    mock_run_migrations.return_value = None

    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = HTTPError("Server error")
    mock_post.return_value = mock_response

    test_data = [
        NavigatorFamily(
            import_id="i00000315",
            title="Belgium UNCBD National Targets",
            summary="Family summary",
            category="REPORTS",
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
            geographies=[],
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

    result = data_in_pipeline()

    assert isinstance(result, Exception)


@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.put")
def test_etl_pipeline_partial_transformation_failure(
    mock_put, mock_connector_class, mock_upload, mock_run_migrations
):
    """Test that pipeline continues when some families fail transformation."""
    mock_run_migrations.return_value = None
    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    # Mock successful load for documents that do transform
    mock_put_response = MagicMock()
    mock_put_response.status_code = 201
    mock_put_response.json.return_value = ["valid-family-doc"]
    mock_put.return_value = mock_put_response

    # One valid family, one family that will fail transformation (no matching transformation)
    valid_family = NavigatorFamily(
        import_id="valid-family",
        title="Valid Family",
        summary="Will transform successfully",
        category="REPORTS",
        corpus=NavigatorCorpus(
            import_id="UNFCCC",
            corpus_type=NavigatorCorpusType(name="corpus_type"),
            organisation=NavigatorOrganisation(id=1, name="UNFCCC"),
        ),
        documents=[
            NavigatorDocument(
                import_id="valid-doc",
                title="Valid Document",
                events=[],
            )
        ],
        events=[],
        collections=[],
        geographies=[],
    )

    invalid_family = NavigatorFamily(
        import_id="invalid-family",
        title="Invalid Family",
        summary="",
        category="UNKNOWN_CATEGORY",  # Will cause transformation to fail
        corpus=NavigatorCorpus(
            import_id="UNKNOWN.corpus.i00000001.n0000",
            corpus_type=NavigatorCorpusType(name="Unknown"),
            organisation=NavigatorOrganisation(id=999, name="Unknown"),
        ),
        documents=[],
        events=[],
        collections=[],
        geographies=[],
    )

    envelope = ExtractedEnvelope(
        data=[valid_family, invalid_family],
        id="test-uuid",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        raw_payload=[valid_family, invalid_family],
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
        envelopes=[envelope], failure=None
    )

    result = data_in_pipeline()

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == "valid-family-doc"

    assert mock_put.call_count == 1


@patch("app.navigator_family_etl_pipeline.transform_navigator_family")
@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
def test_etl_pipeline_all_families_fail_transformation(
    mock_connector_class, mock_upload, mock_run_migrations, mock_transform_families
):
    """Test that pipeline fails gracefully when all families fail transformation."""
    mock_run_migrations.return_value = None
    mock_upload.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    # Create families that will all fail transformation
    failing_families = [
        NavigatorFamily(
            import_id=f"failing-family-{i}",
            title=f"Failing Family {i}",
            summary="",
            category="UNKNOWN_CATEGORY",
            corpus=NavigatorCorpus(
                import_id="UNKNOWN.corpus.i00000001.n0000",
                corpus_type=NavigatorCorpusType(name="Unknown"),
                organisation=NavigatorOrganisation(id=999, name="Unknown"),
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=[],
        )
        for i in range(3)
    ]

    envelope = ExtractedEnvelope(
        data=failing_families,
        id="test-uuid",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        raw_payload=failing_families,
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
        envelopes=[envelope], failure=None
    )

    mock_transform_families.side_effect = [
        Failure(NoMatchingTransformations()),
        Failure(NoMatchingTransformations()),
        Failure(NoMatchingTransformations()),
    ]

    result = data_in_pipeline()

    assert isinstance(result, Exception)
    assert "No documents transformed successfully" in str(result)
