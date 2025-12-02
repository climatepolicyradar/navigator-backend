import pytest
from returns.result import Success

from app.extract.connectors import (
    NavigatorCorpus,
    NavigatorDocument,
    NavigatorEvent,
    NavigatorFamily,
)
from app.models import (
    Document,
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    DocumentWithoutRelationships,
    Identified,
    Label,
)
from app.transform.navigator_family import transform_navigator_family


@pytest.fixture
def navigator_family_with_single_matching_document() -> Identified[NavigatorFamily]:
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamily(
            import_id="family",
            title="Matching title on family and document",
            corpus=NavigatorCorpus(import_id="corpus"),
            documents=[
                NavigatorDocument(
                    import_id="document",
                    title="Matching title on family and document",
                    events=[],
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
                NavigatorDocument(import_id="456", title="Test document 1", events=[]),
            ],
        ),
    )


@pytest.fixture
def navigator_family_with_litigation_corpus_type() -> Identified[NavigatorFamily]:
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamily(
            import_id="family",
            title="Litigation family",
            corpus=NavigatorCorpus(import_id="Academic.corpus.Litigation.n0000"),
            documents=[
                NavigatorDocument(
                    import_id="document",
                    title="Litigation family document",
                    events=[NavigatorEvent(import_id="123", event_type="Decision")],
                ),
                NavigatorDocument(
                    import_id="1.2.3.placeholder",
                    title="Placeholder litigation family document",
                    events=[],
                ),
            ],
        ),
    )


@pytest.fixture
def navigator_family_multilateral_climate_fund_project() -> Identified[NavigatorFamily]:
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamily(
            import_id="family",
            title="Multilateral climate fund project",
            corpus=NavigatorCorpus(import_id="MCF.corpus.AF.n0000"),
            documents=[
                NavigatorDocument(
                    import_id="document_1",
                    title="Multilateral climate fund project document",
                    events=[],
                ),
                NavigatorDocument(
                    import_id="document_2",
                    title="Project document",
                    events=[],
                ),
            ],
        ),
    )


def test_transform_navigator_family_with_single_matching_document(
    navigator_family_with_single_matching_document: Identified[NavigatorFamily],
):
    result = transform_navigator_family(navigator_family_with_single_matching_document)
    assert result == Success(
        [
            Document(
                id="family",
                title=navigator_family_with_single_matching_document.data.title,
                labels=[
                    DocumentLabelRelationship(
                        type="debug",
                        label=Label(
                            type="debug",
                            id="no_family_labels",
                            title="no_family_labels",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="has_version",
                        document=DocumentWithoutRelationships(
                            id="document",
                            title=navigator_family_with_single_matching_document.data.title,
                            labels=[
                                DocumentLabelRelationship(
                                    type="debug",
                                    label=Label(
                                        type="debug",
                                        id="no_document_labels",
                                        title="no_document_labels",
                                    ),
                                ),
                            ],
                        ),
                    ),
                ],
            ),
            Document(
                id="document",
                title=navigator_family_with_single_matching_document.data.title,
                labels=[
                    DocumentLabelRelationship(
                        type="debug",
                        label=Label(
                            type="debug",
                            id="no_document_labels",
                            title="no_document_labels",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="is_version_of",
                        document=DocumentWithoutRelationships(
                            id="family",
                            title=navigator_family_with_single_matching_document.data.title,
                            labels=[
                                DocumentLabelRelationship(
                                    type="debug",
                                    label=Label(
                                        type="debug",
                                        id="no_family_labels",
                                        title="no_family_labels",
                                    ),
                                ),
                            ],
                        ),
                    ),
                ],
            ),
        ]
    )


def test_transform_navigator_family_with_litigation_corpus_type(
    navigator_family_with_litigation_corpus_type: Identified[NavigatorFamily],
):
    result = transform_navigator_family(navigator_family_with_litigation_corpus_type)
    assert result == Success(
        [
            Document(
                id="family",
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
                        type="debug",
                        label=Label(
                            id="no_versions",
                            title="no_versions",
                            type="debug",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="has_member",
                        document=DocumentWithoutRelationships(
                            id="document",
                            title="Litigation family document",
                            labels=[
                                DocumentLabelRelationship(
                                    type="entity_type",
                                    label=Label(
                                        id="Decision",
                                        title="Decision",
                                        type="entity_type",
                                    ),
                                ),
                            ],
                        ),
                    ),
                    DocumentDocumentRelationship(
                        type="has_member",
                        document=DocumentWithoutRelationships(
                            id="1.2.3.placeholder",
                            title="Placeholder litigation family document",
                            labels=[
                                DocumentLabelRelationship(
                                    type="status",
                                    label=Label(
                                        id="obsolete",
                                        title="obsolete",
                                        type="status",
                                    ),
                                ),
                            ],
                        ),
                    ),
                ],
            ),
            Document(
                id="document",
                title="Litigation family document",
                labels=[
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            id="Decision",
                            title="Decision",
                            type="entity_type",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            id="family",
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
                            ],
                        ),
                    )
                ],
            ),
            Document(
                id="1.2.3.placeholder",
                title="Placeholder litigation family document",
                labels=[
                    DocumentLabelRelationship(
                        type="status",
                        label=Label(
                            id="obsolete",
                            title="obsolete",
                            type="status",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            id="family",
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
                            ],
                        ),
                    )
                ],
            ),
        ],
    )


def test_transform_navigator_family_with_multilateral_climate_fund_project(
    navigator_family_multilateral_climate_fund_project: Identified[NavigatorFamily],
):
    result = transform_navigator_family(
        navigator_family_multilateral_climate_fund_project
    )
    assert result == Success(
        [
            Document(
                id="family",
                title="Multilateral climate fund project",
                labels=[
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            id="Multilateral climate fund project",
                            title="Multilateral climate fund project",
                            type="entity_type",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="has_member",
                        document=DocumentWithoutRelationships(
                            id="document_1",
                            title="Multilateral climate fund project document",
                            labels=[
                                DocumentLabelRelationship(
                                    type="debug",
                                    label=Label(
                                        type="debug",
                                        id="no_document_labels",
                                        title="no_document_labels",
                                    ),
                                ),
                            ],
                        ),
                    ),
                    DocumentDocumentRelationship(
                        type="has_version",
                        document=DocumentWithoutRelationships(
                            id="document_2",
                            title="Project document",
                            labels=[
                                DocumentLabelRelationship(
                                    type="debug",
                                    label=Label(
                                        type="debug",
                                        id="no_document_labels",
                                        title="no_document_labels",
                                    ),
                                ),
                            ],
                        ),
                    ),
                ],
            ),
            Document(
                id="document_1",
                title="Multilateral climate fund project document",
                labels=[
                    DocumentLabelRelationship(
                        type="debug",
                        label=Label(
                            type="debug",
                            id="no_document_labels",
                            title="no_document_labels",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            id="family",
                            title="Multilateral climate fund project",
                            labels=[
                                DocumentLabelRelationship(
                                    type="entity_type",
                                    label=Label(
                                        id="Multilateral climate fund project",
                                        title="Multilateral climate fund project",
                                        type="entity_type",
                                    ),
                                ),
                            ],
                        ),
                    )
                ],
            ),
            Document(
                id="document_2",
                title="Project document",
                labels=[
                    DocumentLabelRelationship(
                        type="debug",
                        label=Label(
                            type="debug",
                            id="no_document_labels",
                            title="no_document_labels",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="is_version_of",
                        document=DocumentWithoutRelationships(
                            id="family",
                            title="Multilateral climate fund project",
                            labels=[
                                DocumentLabelRelationship(
                                    type="entity_type",
                                    label=Label(
                                        id="Multilateral climate fund project",
                                        title="Multilateral climate fund project",
                                        type="entity_type",
                                    ),
                                ),
                            ],
                        ),
                    )
                ],
            ),
        ]
    )
