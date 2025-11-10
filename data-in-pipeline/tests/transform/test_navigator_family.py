import pytest
from returns.result import Success

from app.extract.connectors import NavigatorDocument, NavigatorFamily
from app.models import Document, Identified
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import transform_navigator_family


@pytest.fixture
def navigator_family_with_single_matching_title_document() -> (
    Identified[NavigatorFamily]
):
    return Identified(
        id="123",
        source="navigator_family",
        data=NavigatorFamily(
            import_id="123",
            title="Matching title on family and document",
            documents=[
                NavigatorDocument(
                    import_id="456",
                    title="Matching title on family and document",
                ),
            ],
        ),
    )


@pytest.fixture
def navigator_family_with_no_matching_transformations() -> Identified[NavigatorFamily]:
    return Identified(
        id="123",
        source="navigator_family",
        data=NavigatorFamily(
            import_id="123",
            title="No matches for this family or documents",
            documents=[
                NavigatorDocument(
                    import_id="456",
                    title="Test document 1",
                ),
            ],
        ),
    )


def test_transform_navigator_document_with_single_matching_family(
    navigator_family_with_single_matching_title_document: Identified[NavigatorFamily],
):
    result = transform_navigator_family(
        navigator_family_with_single_matching_title_document
    )
    assert result == Success(
        Document(
            id=navigator_family_with_single_matching_title_document.id,
            title=navigator_family_with_single_matching_title_document.data.title,
        )
    )


def test_no_matching_transformations(
    navigator_family_with_no_matching_transformations: Identified[NavigatorFamily],
):
    result = transform_navigator_family(
        navigator_family_with_no_matching_transformations
    )

    result_failure = result.failure()
    print(result_failure)
    assert isinstance(result_failure, NoMatchingTransformations)
