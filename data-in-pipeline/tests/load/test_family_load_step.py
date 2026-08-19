from unittest.mock import ANY, patch

from data_in_models.models import Document
from polyfactory.factories.pydantic_factory import ModelFactory
from prefect.client.schemas.objects import State, StateType

from app.navigator_family_etl_pipeline import (
    data_in__load_db,
    load_batch,
)


class DocumentFactory(ModelFactory[Document]): ...


@patch("app.navigator_family_etl_pipeline.load_to_db")
def test_load_family_success(mock_load_to_db):
    """Test successful family loading to database."""
    documents = [
        DocumentFactory.build(id="doc-1", title="Document 1"),
        DocumentFactory.build(id="doc-2", title="Document 2"),
    ]
    expected_ids = [document.id for document in documents]
    mock_load_to_db.return_value = expected_ids

    result = load_batch(documents)

    assert result == expected_ids
    mock_load_to_db.assert_called_once_with(documents)


@patch("app.navigator_family_etl_pipeline.load_to_db")
def test_load_family_handles_load_failure(mock_load_to_db):
    """Test handling of load API failure."""
    documents = [DocumentFactory.build(id="doc-1", title="Document 1")]

    expected_error = Exception("Load API connection failed")
    mock_load_to_db.return_value = expected_error

    result = load_batch(documents)

    assert result == expected_error
    mock_load_to_db.assert_called_once_with(documents)


@patch("app.navigator_family_etl_pipeline.run_db_migrations_task")
@patch("app.navigator_family_etl_pipeline.read_documents_from_s3")
@patch("app.navigator_family_etl_pipeline.load_db")
def test_data_in__load_db(
    mock_load_db,
    mock_read_documents_from_s3,
    mock_run_db_migrations_task,
) -> None:
    """Test the documents read from S3 are loaded to the database."""

    bucket_name: str = "snowflake-data-export"
    s3_prefix: str = (
        "production/published/2026-01-01T12:00:00.000000/"
        "pipeline_data_in_documents__document_source"
    )

    # Success
    documents = DocumentFactory.batch(3)
    mock_run_db_migrations_task.return_value = None
    mock_read_documents_from_s3.return_value = documents
    mock_load_db.return_value = 1

    result = data_in__load_db(bucket_name=bucket_name, s3_prefix=s3_prefix)

    assert result is None
    mock_run_db_migrations_task.assert_called_once()
    mock_read_documents_from_s3.assert_called_once_with(bucket_name, s3_prefix)
    mock_load_db.assert_called_once_with(
        documents=documents, batch_size=500, run_id=ANY
    )

    # Failure
    error_message: str = "One or more batches failed to load"
    mock_load_db.side_effect = Exception(error_message)
    result = data_in__load_db(
        bucket_name=bucket_name, s3_prefix=s3_prefix, return_state=True
    )

    assert isinstance(result, State)
    assert result.type == StateType.FAILED
    assert isinstance(result.result(raise_on_failure=False), Exception)
    assert result.message
    assert error_message in result.message
