from collections.abc import Sequence

from data_in_models.models import Document
from pydantic import BaseModel


def assert_model_equality(actual: BaseModel, expected: BaseModel):  # noqa: F821
    """
    Compare two Pydantic models for equality.

    This is used for better diffing.
    A BaseModel diff is hard to read but dict diffs are a lot more clear.
    """
    assert actual.model_dump() == expected.model_dump()


def assert_model_list_equality(
    actual: Sequence[BaseModel], expected: Sequence[BaseModel]
):  # noqa: F821
    """
    Compare two lists of Pydantic models for equality.

    This is used for better diffing.
    A BaseModel diff is hard to read but dict diffs are a lot more clear.
    """
    actual_dumped = [model.model_dump() for model in actual]
    expected_dumped = [model.model_dump() for model in expected]

    assert actual_dumped == expected_dumped


def _sort_labels(document: Document) -> Document:
    """Return a copy of the document with labels sorted, for order-independent comparison."""
    document = document.model_copy(deep=True)
    document.labels = sorted(document.labels, key=lambda lr: (lr.type, lr.value.id))
    for document_rel in document.documents:
        document_rel.value.labels = sorted(
            document_rel.value.labels, key=lambda lr: (lr.type, lr.value.id)
        )
    return document


def assert_documents_equal(actual: Sequence[Document], expected: Sequence[Document]):
    """Compare documents ignoring label order."""
    actual_dumped = [_sort_labels(document).model_dump() for document in actual]
    expected_dumped = [_sort_labels(document).model_dump() for document in expected]

    assert actual_dumped == expected_dumped
