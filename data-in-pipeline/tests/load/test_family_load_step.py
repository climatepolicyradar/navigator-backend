from unittest.mock import patch

from data_in_models.models import Document

from app.navigator_family_etl_pipeline import (
    load_batch,
)


@patch("app.navigator_family_etl_pipeline.load_to_db")
def test_load_family_success(mock_load_to_db):
    """Test successful family loading to database."""
    doc_1 = Document(id="doc-1", title="Document 1")
    doc_2 = Document(id="doc-2", title="Document 2")

    documents = [doc_1, doc_2]
    expected_ids = ["doc-1", "doc-2"]
    mock_load_to_db.return_value = expected_ids

    result = load_batch(documents)

    assert result == expected_ids
    mock_load_to_db.assert_called_once_with(documents)


@patch("app.navigator_family_etl_pipeline.load_to_db")
def test_load_family_handles_load_failure(mock_load_to_db):
    """Test handling of load API failure."""
    doc = Document(id="doc-1", title="Document 1")
    documents = [doc]

    expected_error = Exception("Load API connection failed")
    mock_load_to_db.return_value = expected_error

    result = load_batch(documents)

    assert result == expected_error
    mock_load_to_db.assert_called_once_with(documents)
