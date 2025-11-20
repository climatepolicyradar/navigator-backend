import pytest
from returns.result import Success

from app.extract.connectors import NavigatorCorpus, NavigatorDocument, NavigatorFamily
from app.models import Document, DocumentLabelRelationship, Identified, Label
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import TransformerLabel, transform_navigator_family


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
            corpus=NavigatorCorpus(import_id="123"),
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
            corpus=NavigatorCorpus(import_id="123"),
            documents=[
                NavigatorDocument(
                    import_id="456",
                    title="Test document 1",
                ),
            ],
        ),
    )


@pytest.fixture
def navigator_family_with_litigation_corpus_type() -> Identified[NavigatorFamily]:
    return Identified(
        id="123",
        source="navigator_family",
        data=NavigatorFamily(
            import_id="123",
            title="Litigation family",
            corpus=NavigatorCorpus(import_id="Academic.corpus.Litigation.n0000"),
            documents=[
                NavigatorDocument(
                    import_id="456",
                    title="Litigation case",
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
        [
            Document(
                id="456",
                title=navigator_family_with_single_matching_title_document.data.title,
                labels=[
                    DocumentLabelRelationship(
                        type="family",
                        label=Label(
                            id=navigator_family_with_single_matching_title_document.data.import_id,
                            title=navigator_family_with_single_matching_title_document.data.title,
                            type="family",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="transformer",
                        label=TransformerLabel(
                            id="transform_navigator_family_with_single_matching_document",
                            title="transform_navigator_family_with_single_matching_document",
                            type="transformer",
                        ),
                    ),
                ],
                relationships=[],
            )
        ]
    )


def test_no_matching_transformations(
    navigator_family_with_no_matching_transformations: Identified[NavigatorFamily],
):
    result = transform_navigator_family(
        navigator_family_with_no_matching_transformations
    )

    result_failure = result.failure()

    assert isinstance(result_failure, NoMatchingTransformations)


def test_transform_navigator_family_with_litigation_corpus_type(
    navigator_family_with_litigation_corpus_type: Identified[NavigatorFamily],
):
    result = transform_navigator_family(navigator_family_with_litigation_corpus_type)
    assert result == Success(
        [
            Document(
                id="123",
                title="Litigation family",
                labels=[
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            id="Legal case",
                            title="Legal case",
                            type="entity_type",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="transformer",
                        label=TransformerLabel(
                            id="transform_navigator_family_with_litigation_corpus_type",
                            title="transform_navigator_family_with_litigation_corpus_type",
                            type="transformer",
                        ),
                    ),
                ],
                relationships=[],
            )
        ]
    )
