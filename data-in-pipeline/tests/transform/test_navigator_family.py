import datetime

import pytest
from data_in_models.models import (
    Document,
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    DocumentWithoutRelationships,
    Item,
    Label,
)
from returns.result import Success

from app.extract.connectors import (
    NavigatorFamily,
)
from app.models import Identified
from app.transform.navigator_family import transform_navigator_family
from tests.factories import (
    NavigatorCollectionFactory,
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorDocumentFactory,
    NavigatorEventFactory,
    NavigatorFamilyFactory,
    NavigatorOrganisationFactory,
)
from tests.transform.assertions import assert_model_list_equality


def _cclw_corpus():
    return NavigatorCorpusFactory.build(
        import_id="CCLW.corpus.i00000001.n0000",
        corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
        organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
    )


def _standard_events():
    """Event types used by CCLW transformation for activity status labels."""
    base_date = datetime.datetime(2020, 1, 1)
    event_specs = [
        ("Amended", "Amended"),
        ("Appealed", "Appealed"),
        ("Closed", "Closed"),
        ("Declaration Of Climate Emergency", "Declaration Of Climate Emergency"),
        ("Dismissed", "Dismissed"),
        ("Entered Into Force", "Entered Into Force"),
        ("Filing", "Filing"),
        ("Granted", "Granted"),
        ("Implementation Details", "Implementation Details"),
        ("International Agreement", "International Agreement"),
        ("Net Zero Pledge", "Net Zero Pledge"),
        ("Other", "Other"),
        ("Passed/Approved", "Passed/Approved"),
        ("Repealed/Replaced", "Repealed/Replaced"),
        ("Set", "Set"),
        ("Settled", "Settled"),
        ("Updated", "Updated"),
        ("Published", "Published"),
    ]
    return [
        NavigatorEventFactory.build(import_id=iid, event_type=ety, date=base_date)
        for iid, ety in event_specs
    ]


@pytest.fixture
def navigator_family_with_single_matching_document() -> Identified[NavigatorFamily]:
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Matching title on family and document and collection",
            summary="Family summary",
            category="REPORTS",
            corpus=_cclw_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="Matching title on family and document and collection",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    language="en",
                    languages=["en"],
                    events=[],
                    valid_metadata={
                        "role": ["SUPPORTING LEGISLATION"],
                        "type": ["National Drought Plan (NDP)"],
                    },
                ),
            ],
            events=_standard_events(),
            collections=[
                NavigatorCollectionFactory.build(
                    import_id="collection_matching",
                    title="Matching title on family and document and collection",
                    description="Collection description",
                ),
                NavigatorCollectionFactory.build(
                    import_id="collection",
                    title="Collection title",
                    description="Collection description",
                ),
            ],
            geographies=["AU-NSW", "AUS", "XAA"],
        ),
    )


@pytest.fixture
def navigator_family_with_no_matching_transformations() -> Identified[NavigatorFamily]:
    return Identified(
        id="123",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="123",
            title="No matches for this family or documents",
            summary="Family summary",
            category="REPORTS",
            corpus=_cclw_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="456",
                    title="Test document 1",
                    events=[],
                ),
            ],
            events=[],
            collections=[],
            geographies=[],
        ),
    )


@pytest.fixture
def navigator_family_with_litigation_corpus_type() -> Identified[NavigatorFamily]:
    decision_date = datetime.datetime(2020, 1, 1)
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Litigation family",
            summary="Family summary",
            category="REPORTS",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
            ),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="Litigation family document",
                    cdn_object=None,
                    source_url=None,
                    events=[
                        NavigatorEventFactory.build(
                            import_id="123",
                            event_type="Decision",
                            date=decision_date,
                        )
                    ],
                ),
                NavigatorDocumentFactory.build(
                    import_id="1.2.3.placeholder",
                    title="Placeholder litigation family document",
                    cdn_object=None,
                    source_url=None,
                    events=[],
                ),
            ],
            events=[
                NavigatorEventFactory.build(
                    import_id="123",
                    event_type="Decision",
                    date=decision_date,
                ),
            ],
            collections=[],
            geographies=[],
        ),
    )


def _mcf_events():
    base_date = datetime.datetime(2020, 1, 1)
    return [
        NavigatorEventFactory.build(
            import_id="concept_approved",
            event_type="Concept Approved",
            date=base_date,
        ),
        NavigatorEventFactory.build(
            import_id="project_approved",
            event_type="Project Approved",
            date=base_date,
        ),
        NavigatorEventFactory.build(
            import_id="under_implementation",
            event_type="Under Implementation",
            date=base_date,
        ),
        NavigatorEventFactory.build(
            import_id="project_completed",
            event_type="Project Completed",
            date=base_date,
        ),
        NavigatorEventFactory.build(
            import_id="cancelled",
            event_type="Cancelled",
            date=base_date,
        ),
    ]


@pytest.fixture
def navigator_family_multilateral_climate_fund_project() -> Identified[NavigatorFamily]:
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Multilateral climate fund project",
            summary="Family summary",
            category="REPORTS",
            corpus=NavigatorCorpusFactory.build(
                import_id="MCF.corpus.AF.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="AF"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
            ),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document_1",
                    title="Multilateral climate fund project document",
                    cdn_object=None,
                    source_url=None,
                    events=[],
                ),
                NavigatorDocumentFactory.build(
                    import_id="document_2",
                    title="Project document",
                    cdn_object=None,
                    source_url=None,
                    events=[],
                ),
            ],
            events=_mcf_events(),
            collections=[],
            geographies=[],
        ),
    )


@pytest.fixture
def navigator_family_with_duplicate_legal_case() -> Identified[NavigatorFamily]:
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Litigation family",
            category="LITIGATION",
            summary="Family summary",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(
                    id=1, name="Sabin Center for Climate Change Law"
                ),
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=[],
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
                    id="Principal",
                    title="Principal",
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
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                label=Label(type="activity_status", id="Published", title="Published"),
            ),
            DocumentLabelRelationship(
                type="provider",
                label=Label(
                    type="agent",
                    id="Grantham Research Institute",
                    title="Grantham Research Institute",
                ),
            ),
            DocumentLabelRelationship(
                type="geography",
                label=Label(
                    type="agent",
                    id="AU-NSW",
                    title="New South Wales",
                ),
            ),
            DocumentLabelRelationship(
                type="geography",
                label=Label(
                    type="agent",
                    id="AUS",
                    title="Australia",
                ),
            ),
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Guidance",
                    title="Guidance",
                    type="entity_type",
                ),
            ),
        ],
        relationships=[
            DocumentDocumentRelationship(
                type="member_of",
                document=DocumentWithoutRelationships(
                    id="collection",
                    title="Collection title",
                    labels=[
                        DocumentLabelRelationship(
                            type="entity_type",
                            label=Label(
                                id="Collection",
                                title="Collection",
                                type="entity_type",
                            ),
                        )
                    ],
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
                    items=[
                        Item(
                            url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                        ),
                        Item(
                            url="https://source.climatepolicyradar.org/path/to/file.pdf",
                        ),
                    ],
                ),
            ),
            DocumentDocumentRelationship(
                type="has_version",
                document=DocumentWithoutRelationships(
                    id="collection_matching",
                    title="Matching title on family and document and collection",
                    labels=[
                        DocumentLabelRelationship(
                            type="entity_type",
                            label=Label(
                                id="Collection",
                                title="Collection",
                                type="entity_type",
                            ),
                        )
                    ],
                    items=[],
                ),
            ),
        ],
    )
    assert_model_list_equality(
        result.unwrap(),
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
                items=[
                    Item(
                        url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    ),
                    Item(
                        url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    ),
                ],
            ),
            Document(
                id="collection_matching",
                title="Matching title on family and document and collection",
                labels=[
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            id="Collection",
                            title="Collection",
                            type="entity_type",
                        ),
                    )
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
                id="collection",
                title="Collection title",
                labels=[
                    DocumentLabelRelationship(
                        type="entity_type",
                        label=Label(
                            id="Collection",
                            title="Collection",
                            type="entity_type",
                        ),
                    )
                ],
                relationships=[
                    DocumentDocumentRelationship(
                        type="has_member",
                        document=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
        ],
    )


def test_transform_navigator_family_with_litigation_corpus_type_handles_duplicate_label_relationships(
    navigator_family_with_duplicate_legal_case: Identified[NavigatorFamily],
):
    result = transform_navigator_family(navigator_family_with_duplicate_legal_case)

    documents = result.unwrap()
    family_doc = documents[0]

    legal_case_labels = [
        label
        for label in family_doc.labels
        if label.label.id == "Legal case" and label.type == "entity_type"
    ]

    assert len(legal_case_labels) == 1, (
        f"Expected exactly 1 'Legal case' entity_type label after deduplication, "
        f"but found {len(legal_case_labels)}"
    )

    assert legal_case_labels[0].label.id == "Legal case"
    assert legal_case_labels[0].label.title == "Legal case"
    assert legal_case_labels[0].type == "entity_type"


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
                    id="Principal",
                    title="Principal",
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
                    id="Sabin Center for Climate Change Law",
                    title="Sabin Center for Climate Change Law",
                ),
            ),
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Guidance",
                    title="Guidance",
                    type="entity_type",
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
                                id="Sabin Center for Climate Change Law",
                                title="Sabin Center for Climate Change Law",
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
                                id="Sabin Center for Climate Change Law",
                                title="Sabin Center for Climate Change Law",
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
                            id="Sabin Center for Climate Change Law",
                            title="Sabin Center for Climate Change Law",
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
                            id="Sabin Center for Climate Change Law",
                            title="Sabin Center for Climate Change Law",
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
                    id="Principal",
                    title="Principal",
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
                    id="Adaptation Fund",
                    title="Adaptation Fund",
                ),
            ),
            DocumentLabelRelationship(
                type="entity_type",
                label=Label(
                    id="Guidance",
                    title="Guidance",
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
                            type="provider",
                            label=Label(
                                type="agent",
                                id="Adaptation Fund",
                                title="Adaptation Fund",
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
                                id="Adaptation Fund",
                                title="Adaptation Fund",
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
                            id="Adaptation Fund",
                            title="Adaptation Fund",
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
                            id="Adaptation Fund",
                            title="Adaptation Fund",
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
