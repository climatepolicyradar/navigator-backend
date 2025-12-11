import datetime

import pytest
from returns.result import Success

from app.extract.connectors import (
    NavigatorCollection,
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorEvent,
    NavigatorFamily,
    NavigatorOrganisation,
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
            title="Matching title on family and document and collection",
            summary="Family summary",
            corpus=NavigatorCorpus(
                import_id="corpus",
                corpus_type=NavigatorCorpusType(name="corpus_type"),
                organisation=NavigatorOrganisation(id=1, name="CCLW"),
            ),
            documents=[
                NavigatorDocument(
                    import_id="document",
                    title="Matching title on family and document and collection",
                    events=[],
                    valid_metadata={
                        "role": ["SUPPORTING LEGISLATION"],
                        "type": ["National Drought Plan (NDP)"],
                    },
                ),
            ],
            events=[
                NavigatorEvent(
                    import_id="Amended",
                    event_type="Amended",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Appealed",
                    event_type="Appealed",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Closed",
                    event_type="Closed",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Declaration Of Climate Emergency",
                    event_type="Declaration Of Climate Emergency",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Dismissed",
                    event_type="Dismissed",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Entered Into Force",
                    event_type="Entered Into Force",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Filing",
                    event_type="Filing",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Granted",
                    event_type="Granted",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Implementation Details",
                    event_type="Implementation Details",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="International Agreement",
                    event_type="International Agreement",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Net Zero Pledge",
                    event_type="Net Zero Pledge",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Other",
                    event_type="Other",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Passed/Approved",
                    event_type="Passed/Approved",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Repealed/Replaced",
                    event_type="Repealed/Replaced",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Set",
                    event_type="Set",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Settled",
                    event_type="Settled",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="Updated",
                    event_type="Updated",
                    date=datetime.datetime(2020, 1, 1),
                ),
            ],
            collections=[
                NavigatorCollection(
                    import_id="collection_matching",
                    title="Matching title on family and document and collection",
                    description="Collection description",
                ),
                NavigatorCollection(
                    import_id="collection",
                    title="Collection title",
                    description="Collection description",
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
            summary="Family summary",
            corpus=NavigatorCorpus(
                import_id="123",
                corpus_type=NavigatorCorpusType(name="corpus_type"),
                organisation=NavigatorOrganisation(id=1, name="CCLW"),
            ),
            documents=[
                NavigatorDocument(import_id="456", title="Test document 1", events=[]),
            ],
            events=[],
            collections=[],
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
            summary="Family summary",
            corpus=NavigatorCorpus(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusType(name="Litigation"),
                organisation=NavigatorOrganisation(id=1, name="CCLW"),
            ),
            documents=[
                NavigatorDocument(
                    import_id="document",
                    title="Litigation family document",
                    events=[
                        NavigatorEvent(
                            import_id="123",
                            event_type="Decision",
                            date=datetime.datetime(2020, 1, 1),
                        )
                    ],
                ),
                NavigatorDocument(
                    import_id="1.2.3.placeholder",
                    title="Placeholder litigation family document",
                    events=[],
                ),
            ],
            events=[
                NavigatorEvent(
                    import_id="123",
                    event_type="Decision",
                    date=datetime.datetime(2020, 1, 1),
                ),
            ],
            collections=[],
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
            summary="Family summary",
            corpus=NavigatorCorpus(
                import_id="MCF.corpus.AF.n0000",
                corpus_type=NavigatorCorpusType(name="AF"),
                organisation=NavigatorOrganisation(id=1, name="CCLW"),
            ),
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
            events=[
                NavigatorEvent(
                    import_id="concept_approved",
                    event_type="Concept Approved",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="project_approved",
                    event_type="Project Approved",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="under_implementation",
                    event_type="Under Implementation",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="project_completed",
                    event_type="Project Completed",
                    date=datetime.datetime(2020, 1, 1),
                ),
                NavigatorEvent(
                    import_id="cancelled",
                    event_type="Cancelled",
                    date=datetime.datetime(2020, 1, 1),
                ),
            ],
            collections=[],
        ),
    )


def test_transform_navigator_family_with_single_matching_document(
    navigator_family_with_single_matching_document: Identified[NavigatorFamily],
):
    result = transform_navigator_family(navigator_family_with_single_matching_document)
    expected_document_from_family = Document(
        id="family",
        title="Matching title on family and document and collection",
        description="Family summary",
        labels=[
            DocumentLabelRelationship(
                type="status",
                label=Label(
                    type="status",
                    id="Canonical",
                    title="Canonical",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Amended",
                    title="Amended",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Appealed",
                    title="Appealed",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Closed",
                    title="Closed",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Declaration of climate emergency",
                    title="Declaration of climate emergency",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Dismissed", title="Dismissed"),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Entered into force",
                    title="Entered into force",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Filing", title="Filing"),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Granted", title="Granted"),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Implementation details",
                    title="Implementation details",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="International agreement",
                    title="International agreement",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Net zero pledge",
                    title="Net zero pledge",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Other", title="Other"),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Passed/Approved",
                    title="Passed/Approved",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(
                    type="activity_status",
                    id="Repealed/Replaced",
                    title="Repealed/Replaced",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Set", title="Set"),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Settled", title="Settled"),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Updated", title="Updated"),
            ),
            DocumentLabelRelationship(
                type="provider",
                label=Label(
                    type="agent",
                    id="Grantham Research Institute",
                    title="Grantham Research Institute",
                ),
            ),
        ],
        relationships=[
            DocumentDocumentRelationship(
                type="member_of",
                document=DocumentWithoutRelationships(
                    id="collection",
                    title="Collection title",
                    labels=[],
                ),
            ),
            DocumentDocumentRelationship(
                type="has_version",
                document=DocumentWithoutRelationships(
                    id="document",
                    title="Matching title on family and document and collection",
                    labels=[
                        DocumentLabelRelationship(
                            type="entity_type",
                            label=Label(
                                type="entity_type",
                                id="Supporting legislation",
                                title="Supporting legislation",
                            ),
                        ),
                        DocumentLabelRelationship(
                            type="entity_type",
                            label=Label(
                                type="entity_type",
                                id="National drought plan (ndp)",
                                title="National drought plan (ndp)",
                            ),
                        ),
                        DocumentLabelRelationship(
                            type="provider",
                            label=Label(
                                type="agent",
                                id="Grantham Research Institute",
                                title="Grantham Research Institute",
                            ),
                        ),
                    ],
                ),
            ),
            DocumentDocumentRelationship(
                type="has_version",
                document=DocumentWithoutRelationships(
                    id="collection_matching",
                    title="Matching title on family and document and collection",
                    labels=[],
                ),
            ),
        ],
    )
    assert result == Success(
        [
            expected_document_from_family,
            Document(
                id="document",
                title="Matching title on family and document and collection",
                labels=[
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            type="entity_type",
                            id="Supporting legislation",
                            title="Supporting legislation",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            type="entity_type",
                            id="National drought plan (ndp)",
                            title="National drought plan (ndp)",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="provider",
                        label=Label(
                            type="agent",
                            id="Grantham Research Institute",
                            title="Grantham Research Institute",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="is_version_of",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
            Document(
                id="collection_matching",
                title="Matching title on family and document and collection",
                labels=[],
                relationships=[
                    DocumentDocumentRelationship(
                        type="is_version_of",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
            Document(
                id="collection",
                title="Collection title",
                labels=[],
                relationships=[
                    DocumentDocumentRelationship(
                        type="has_member",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
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
    expected_document_from_family = Document(
        id="family",
        title="Litigation family",
        description="Family summary",
        labels=[
            DocumentLabelRelationship(
                type="status",
                label=Label(
                    type="status",
                    id="Canonical",
                    title="Canonical",
                ),
            ),
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Legal case",
                    title="Legal case",
                    type="entity_type",
                ),
            ),
            DocumentLabelRelationship(
                type="provider",
                label=Label(
                    type="agent",
                    id="Grantham Research Institute",
                    title="Grantham Research Institute",
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
                        DocumentLabelRelationship(
                            type="provider",
                            label=Label(
                                type="agent",
                                id="Grantham Research Institute",
                                title="Grantham Research Institute",
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
                                id="Obsolete",
                                title="Obsolete",
                                type="status",
                            ),
                        ),
                        DocumentLabelRelationship(
                            type="provider",
                            label=Label(
                                type="agent",
                                id="Grantham Research Institute",
                                title="Grantham Research Institute",
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )
    assert result == Success(
        [
            expected_document_from_family,
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
                    DocumentLabelRelationship(
                        type="provider",
                        label=Label(
                            type="agent",
                            id="Grantham Research Institute",
                            title="Grantham Research Institute",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
            Document(
                id="1.2.3.placeholder",
                title="Placeholder litigation family document",
                labels=[
                    DocumentLabelRelationship(
                        type="status",
                        label=Label(
                            id="Obsolete",
                            title="Obsolete",
                            type="status",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="provider",
                        label=Label(
                            type="agent",
                            id="Grantham Research Institute",
                            title="Grantham Research Institute",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
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
    expected_document_from_family = Document(
        id="family",
        title="Multilateral climate fund project",
        description="Family summary",
        labels=[
            DocumentLabelRelationship(
                type="status",
                label=Label(
                    type="status",
                    id="Canonical",
                    title="Canonical",
                ),
            ),
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Multilateral climate fund project",
                    title="Multilateral climate fund project",
                    type="entity_type",
                ),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                label=Label(
                    id="Concept approved",
                    title="Concept approved",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                label=Label(
                    id="Approved",
                    title="Approved",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                label=Label(
                    id="Under implementation",
                    title="Under implementation",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                label=Label(
                    id="Completed",
                    title="Completed",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            DocumentLabelRelationship(
                type="activity_status",
                label=Label(
                    id="Cancelled",
                    title="Cancelled",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            DocumentLabelRelationship(
                type="provider",
                label=Label(
                    type="agent",
                    id="Grantham Research Institute",
                    title="Grantham Research Institute",
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
                            type="provider",
                            label=Label(
                                type="agent",
                                id="Grantham Research Institute",
                                title="Grantham Research Institute",
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
                            type="provider",
                            label=Label(
                                type="agent",
                                id="Grantham Research Institute",
                                title="Grantham Research Institute",
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )
    assert result == Success(
        [
            expected_document_from_family,
            Document(
                id="document_1",
                title="Multilateral climate fund project document",
                labels=[
                    DocumentLabelRelationship(
                        type="provider",
                        label=Label(
                            type="agent",
                            id="Grantham Research Institute",
                            title="Grantham Research Institute",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="member_of",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    )
                ],
            ),
            Document(
                id="document_2",
                title="Project document",
                labels=[
                    DocumentLabelRelationship(
                        type="provider",
                        label=Label(
                            type="agent",
                            id="Grantham Research Institute",
                            title="Grantham Research Institute",
                        ),
                    ),
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="is_version_of",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    )
                ],
            ),
        ]
    )
