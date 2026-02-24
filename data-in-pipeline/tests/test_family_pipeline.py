from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError
from returns.result import Failure, Success

from app.extract.connectors import FamilyFetchResult
from app.models import ExtractedEnvelope, ExtractedMetadata, PipelineResult
from app.navigator_family_etl_pipeline import data_in_pipeline
from app.transform.models import NoMatchingTransformations
from tests.factories import (
    DocumentWithoutRelationshipsFactory,
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorDocumentFactory,
    NavigatorFamilyFactory,
    NavigatorOrganisationFactory,
    PageFetchFailureFactory,
)


@patch("app.navigator_family_etl_pipeline.cache_jsonl_to_s3")
@patch("app.navigator_family_etl_pipeline.cache_parquet_to_s3")
@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.put")
def test_process_family_updates_flow_multiple_families(  # noqa: PLR0913
    mock_post,
    mock_connector_class,
    mock_upload,
    mock_run_migrations,
    mock_cache_parquet_to_s3,
    mock_cache_jsonl_to_s3,
):
    """Test ETL pipeline with multiple families across pages."""
    mock_run_migrations.return_value = None

    mock_upload.return_value = None

    mock_cache_jsonl_to_s3.return_value = None
    mock_cache_parquet_to_s3.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    mock_post_response = MagicMock()
    mock_post_response.status_code = 201
    mock_post_response.json.return_value = ["1", "2"]
    mock_post.return_value = mock_post_response

    corpus = NavigatorCorpusFactory.build(
        import_id="UNFCCC",
        corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
        organisation=NavigatorOrganisationFactory.build(id=1, name="UNFCCC"),
    )
    page_1_data = [
        NavigatorFamilyFactory.build(
            import_id="i00000315",
            title="Belgium UNCBD National Targets",
            summary="Family summary",
            category="REPORTS",
            corpus=corpus,
            documents=[
                NavigatorDocumentFactory.build(
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
        NavigatorFamilyFactory.build(
            import_id="i00000316",
            title="France UNCBD National Targets",
            summary="Family summary",
            category="REPORTS",
            corpus=corpus,
            documents=[
                NavigatorDocumentFactory.build(
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
        raw_payload=page_1_data,
        id="test-uuid-1",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        content_type="application/json",
        connector_version="1.0.0",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=1",
            http_status=HTTPStatus.OK,
        ),
        task_run_id="task-001",
        flow_run_id="flow-001",
    )
    envelope_2 = ExtractedEnvelope(
        data=page_2_data,
        raw_payload=page_2_data,
        id="test-uuid-2",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-2",
        content_type="application/json",
        connector_version="1.0.0",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=2",
            http_status=HTTPStatus.OK,
        ),
        task_run_id="task-001",
        flow_run_id="flow-001",
    )

    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[envelope_1, envelope_2], failure=None
    )

    result = data_in_pipeline()

    assert isinstance(result, PipelineResult)
    assert result.status == "success"


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

    expected_error = Exception("500 Internal Server Error")
    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[],
        failure=PageFetchFailureFactory.build(
            page=1, error=str(expected_error), task_run_id="task-001"
        ),
    )

    result = data_in_pipeline()

    assert isinstance(result, Exception)


@patch("app.navigator_family_etl_pipeline.cache_jsonl_to_s3")
@patch("app.navigator_family_etl_pipeline.cache_parquet_to_s3")
@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.navigator_family_etl_pipeline.load_batch")
def test_etl_pipeline_load_failure(  # noqa: PLR0913
    mock_load_batch_task,
    mock_connector_class,
    mock_upload,
    mock_run_migrations,
    mock_cache_jsonl_to_s3,
    mock_cache_parquet_to_s3,
):
    mock_run_migrations.return_value = None
    mock_upload.return_value = None

    mock_cache_jsonl_to_s3.return_value = None
    mock_cache_parquet_to_s3.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    test_family_id = "i00000315"
    test_family_title = "Belgium UNCBD National Targets"
    test_source_record_id = "task-001-families-endpoint-page-1"
    test_endpoint = "https://api.example.com/families/?page=1"
    expected_error_message = "One or more batches failed to load"

    corpus = NavigatorCorpusFactory.build(
        import_id="UNFCCC",
        corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
        organisation=NavigatorOrganisationFactory.build(id=1, name="UNFCCC"),
    )
    test_data = [
        NavigatorFamilyFactory.build(
            import_id=test_family_id,
            title=test_family_title,
            summary="Family summary",
            category="REPORTS",
            corpus=corpus,
            documents=[
                NavigatorDocumentFactory.build(
                    import_id=test_family_id,
                    title=test_family_title,
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
        raw_payload=test_data,
        id="test-uuid-1",
        source_name="navigator_family",
        source_record_id=test_source_record_id,
        content_type="application/json",
        connector_version="1.0.0",
        metadata=ExtractedMetadata(
            endpoint=test_endpoint,
            http_status=HTTPStatus.OK,
        ),
        task_run_id="task-001",
        flow_run_id="flow-001",
    )

    # Mock connector response
    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[test_envelope], failure=None
    )

    mock_load_batch_task.map.return_value = [HTTPError("Server error")]

    result = data_in_pipeline()

    assert isinstance(result, Exception)
    assert expected_error_message in str(result)

    mock_connector_instance.close.assert_called_once()


@patch("app.navigator_family_etl_pipeline.cache_jsonl_to_s3")
@patch("app.navigator_family_etl_pipeline.cache_parquet_to_s3")
@patch("app.navigator_family_etl_pipeline.transform_navigator_family")
@patch("app.navigator_family_etl_pipeline.run_db_migrations")
@patch("app.navigator_family_etl_pipeline.upload_to_s3")
@patch("app.navigator_family_etl_pipeline.NavigatorConnector")
@patch("app.load.load.requests.put")
def test_etl_pipeline_partial_transformation_failure(  # noqa: PLR0913
    mock_put,
    mock_connector_class,
    mock_upload,
    mock_run_migrations,
    mock_transform_families,
    mock_cache_jsonl_to_s3,
    mock_cache_parquet_to_s3,
):
    """Test that pipeline continues when some families fail transformation."""
    mock_run_migrations.return_value = None
    mock_upload.return_value = None

    mock_cache_jsonl_to_s3.return_value = None
    mock_cache_parquet_to_s3.return_value = None

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.close.return_value = None

    mock_put_response = MagicMock()
    mock_put_response.status_code = 201
    mock_put_response.json.return_value = ["valid-family-doc"]
    mock_put.return_value = mock_put_response

    valid_corpus = NavigatorCorpusFactory.build(
        import_id="UNFCCC",
        corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
        organisation=NavigatorOrganisationFactory.build(id=1, name="UNFCCC"),
    )
    valid_family = NavigatorFamilyFactory.build(
        import_id="valid-family",
        title="Valid Family",
        summary="Will transform successfully",
        category="REPORTS",
        corpus=valid_corpus,
        documents=[
            NavigatorDocumentFactory.build(
                import_id="valid-doc",
                title="Valid Document",
                events=[],
            )
        ],
        events=[],
        collections=[],
        geographies=[],
    )
    invalid_family = NavigatorFamilyFactory.build(
        import_id="invalid-family",
        title="Invalid Family",
        summary="",
        category="UNKNOWN_CATEGORY",
        corpus=NavigatorCorpusFactory.build(
            import_id="UNKNOWN.corpus.i00000001.n0000",
            corpus_type=NavigatorCorpusTypeFactory.build(name="Unknown"),
            organisation=NavigatorOrganisationFactory.build(id=999, name="Unknown"),
        ),
        documents=[],
        events=[],
        collections=[],
        geographies=[],
    )

    envelope = ExtractedEnvelope(
        data=[valid_family, invalid_family],
        raw_payload=[valid_family, invalid_family],
        id="test-uuid",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        content_type="application/json",
        connector_version="1.0.0",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=1",
            http_status=HTTPStatus.OK,
        ),
        task_run_id="task-001",
        flow_run_id="flow-001",
    )

    mock_connector_instance.fetch_all_families.return_value = FamilyFetchResult(
        envelopes=[envelope], failure=None
    )

    mock_transformed_documents = [
        DocumentWithoutRelationshipsFactory.build(
            id="test-doc",
            title="This is the test doc",
            description=None,
            labels=[],
            items=[],
        )
    ]

    mock_transform_families.side_effect = [
        Success(mock_transformed_documents),
        Failure(NoMatchingTransformations()),
    ]

    result = data_in_pipeline()

    assert isinstance(result, PipelineResult)
    assert result.documents_processed == len(mock_transformed_documents)
    assert result.status == "success"


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

    failing_corpus = NavigatorCorpusFactory.build(
        import_id="UNKNOWN.corpus.i00000001.n0000",
        corpus_type=NavigatorCorpusTypeFactory.build(name="Unknown"),
        organisation=NavigatorOrganisationFactory.build(id=999, name="Unknown"),
    )
    failing_families = [
        NavigatorFamilyFactory.build(
            import_id=f"failing-family-{i}",
            title=f"Failing Family {i}",
            summary="",
            category="UNKNOWN_CATEGORY",
            corpus=failing_corpus,
            documents=[],
            events=[],
            collections=[],
            geographies=[],
        )
        for i in range(3)
    ]
    envelope = ExtractedEnvelope(
        data=failing_families,
        raw_payload=failing_families,
        id="test-uuid",
        source_name="navigator_family",
        source_record_id="task-001-families-endpoint-page-1",
        content_type="application/json",
        connector_version="1.0.0",
        metadata=ExtractedMetadata(
            endpoint="https://api.example.com/families/?page=1",
            http_status=HTTPStatus.OK,
        ),
        task_run_id="task-001",
        flow_run_id="flow-001",
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
