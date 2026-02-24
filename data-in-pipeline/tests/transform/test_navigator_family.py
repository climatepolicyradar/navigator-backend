import datetime

import pytest
from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Item,
    Label,
    LabelRelationship,
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
            LabelRelationship(
                type="status",
                value=Label(
                    type="status",
                    id="Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Amended",
                    value="Amended",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Appealed",
                    value="Appealed",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Closed",
                    value="Closed",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Declaration of climate emergency",
                    value="Declaration of climate emergency",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Dismissed", value="Dismissed"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Entered into force",
                    value="Entered into force",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Filing", value="Filing"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Granted", value="Granted"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Implementation details",
                    value="Implementation details",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="International agreement",
                    value="International agreement",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Net zero pledge",
                    value="Net zero pledge",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Other", value="Other"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Passed/Approved",
                    value="Passed/Approved",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="Repealed/Replaced",
                    value="Repealed/Replaced",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Set", value="Set"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Settled", value="Settled"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Updated", value="Updated"),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(type="activity_status", id="Published", value="Published"),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="Grantham Research Institute",
                    value="Grantham Research Institute",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="agent",
                    id="AU-NSW",
                    value="New South Wales",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="agent",
                    id="AUS",
                    value="Australia",
                ),
            ),
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="Guidance",
                    value="Guidance",
                    type="entity_type",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="member_of",
                value=DocumentWithoutRelationships(
                    id="collection",
                    title="Collection title",
                    labels=[
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="Collection",
                                value="Collection",
                                type="entity_type",
                            ),
                        )
                    ],
                ),
            ),
            DocumentRelationship(
                type="has_version",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="Matching title on family and document and collection",
                    labels=[
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                type="entity_type",
                                id="Supporting legislation",
                                value="Supporting legislation",
                            ),
                        ),
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                type="entity_type",
                                id="National drought plan (ndp)",
                                value="National drought plan (ndp)",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Grantham Research Institute",
                                value="Grantham Research Institute",
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
            DocumentRelationship(
                type="has_version",
                value=DocumentWithoutRelationships(
                    id="collection_matching",
                    title="Matching title on family and document and collection",
                    labels=[
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="Collection",
                                value="Collection",
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
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            type="entity_type",
                            id="Supporting legislation",
                            value="Supporting legislation",
                        ),
                    ),
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            type="entity_type",
                            id="National drought plan (ndp)",
                            value="National drought plan (ndp)",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Grantham Research Institute",
                            value="Grantham Research Institute",
                        ),
                    ),
                ],
                documents=[
                    DocumentRelationship(
                        type="is_version_of",
                        value=DocumentWithoutRelationships(
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
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="Collection",
                            value="Collection",
                            type="entity_type",
                        ),
                    )
                ],
                documents=[
                    DocumentRelationship(
                        type="is_version_of",
                        value=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
            Document(
                id="collection",
                title="Collection title",
                labels=[
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="Collection",
                            value="Collection",
                            type="entity_type",
                        ),
                    )
                ],
                documents=[
                    DocumentRelationship(
                        type="has_member",
                        value=DocumentWithoutRelationships(
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
        if label.value.id == "Legal case" and label.type == "entity_type"
    ]

    assert len(legal_case_labels) == 1, (
        f"Expected exactly 1 'Legal case' entity_type label after deduplication, "
        f"but found {len(legal_case_labels)}"
    )

    assert legal_case_labels[0].value.id == "Legal case"
    assert legal_case_labels[0].value.value == "Legal case"
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
            LabelRelationship(
                type="status",
                value=Label(
                    type="status",
                    id="Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="Legal case",
                    value="Legal case",
                    type="entity_type",
                ),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="Sabin Center for Climate Change Law",
                    value="Sabin Center for Climate Change Law",
                ),
            ),
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="Guidance",
                    value="Guidance",
                    type="entity_type",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="Litigation family document",
                    labels=[
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="Decision",
                                value="Decision",
                                type="entity_type",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Sabin Center for Climate Change Law",
                                value="Sabin Center for Climate Change Law",
                            ),
                        ),
                    ],
                ),
            ),
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="1.2.3.placeholder",
                    title="Placeholder litigation family document",
                    labels=[
                        LabelRelationship(
                            type="status",
                            value=Label(
                                id="Obsolete",
                                value="Obsolete",
                                type="status",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Sabin Center for Climate Change Law",
                                value="Sabin Center for Climate Change Law",
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
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="Decision",
                            value="Decision",
                            type="entity_type",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Sabin Center for Climate Change Law",
                            value="Sabin Center for Climate Change Law",
                        ),
                    ),
                ],
                documents=[
                    DocumentRelationship(
                        type="member_of",
                        value=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
            Document(
                id="1.2.3.placeholder",
                title="Placeholder litigation family document",
                labels=[
                    LabelRelationship(
                        type="status",
                        value=Label(
                            id="Obsolete",
                            value="Obsolete",
                            type="status",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Sabin Center for Climate Change Law",
                            value="Sabin Center for Climate Change Law",
                        ),
                    ),
                ],
                documents=[
                    DocumentRelationship(
                        type="member_of",
                        value=DocumentWithoutRelationships(
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
            LabelRelationship(
                type="status",
                value=Label(
                    type="status",
                    id="Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="Multilateral climate fund project",
                    value="Multilateral climate fund project",
                    type="entity_type",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="Concept approved",
                    value="Concept approved",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="Approved",
                    value="Approved",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="Under implementation",
                    value="Under implementation",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="Completed",
                    value="Completed",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="Cancelled",
                    value="Cancelled",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="Adaptation Fund",
                    value="Adaptation Fund",
                ),
            ),
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="Guidance",
                    value="Guidance",
                    type="entity_type",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document_1",
                    title="Multilateral climate fund project document",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Adaptation Fund",
                                value="Adaptation Fund",
                            ),
                        ),
                    ],
                ),
            ),
            DocumentRelationship(
                type="has_version",
                value=DocumentWithoutRelationships(
                    id="document_2",
                    title="Project document",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Adaptation Fund",
                                value="Adaptation Fund",
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
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Adaptation Fund",
                            value="Adaptation Fund",
                        ),
                    ),
                ],
                documents=[
                    DocumentRelationship(
                        type="member_of",
                        value=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    )
                ],
            ),
            Document(
                id="document_2",
                title="Project document",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Adaptation Fund",
                            value="Adaptation Fund",
                        ),
                    ),
                ],
                documents=[
                    DocumentRelationship(
                        type="is_version_of",
                        value=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    )
                ],
            ),
        ]
    )
