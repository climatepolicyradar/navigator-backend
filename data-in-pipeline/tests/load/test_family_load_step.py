from unittest.mock import patch

from data_in_models.models import Document
from polyfactory.factories.pydantic_factory import ModelFactory

from app.navigator_family_etl_pipeline import (
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
