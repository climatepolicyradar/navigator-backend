import datetime

import pytest
from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Label,
    LabelRelationship,
    LabelWithoutDocumentRelationships,
)

from app.extract.connectors import (
    NavigatorFamily,
)
from app.models import Identified, NavigatorConcept
from app.transform.models import CouldNotTransform
from app.transform.navigator_family import transform_navigator_family
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorDocumentFactory,
    NavigatorEventFactory,
    NavigatorFamilyFactory,
    NavigatorOrganisationFactory,
)
from tests.transform.assertions import assert_model_list_equality


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
            category="LITIGATION",
            last_updated_date="2020-01-0100:00:00Z",
            published_date="2020-01-0100:00:00Z",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="Litigation family document",
                    cdn_object=None,
                    source_url=None,
                    slug="litigation-document-slug",
                    events=[
                        NavigatorEventFactory.build(
                            import_id="123",
                            event_type="Decision",
                            date=decision_date,
                            valid_metadata={
                                "event_type": ["Decision"],
                                "datetime_event_name": ["Decision"],
                                "action_taken": ["Answer filed by federal defendants"],
                                "description": ["Summary of the filed document"],
                            },
                        )
                    ],
                    variant="Original language",
                    md5_sum="aaaaa11111bbbbb",
                    languages=[],
                    document_status="published",
                ),
                NavigatorDocumentFactory.build(
                    import_id="1.2.3.placeholder",
                    title="Placeholder litigation family document",
                    cdn_object=None,
                    source_url=None,
                    slug="placeholder-document-slug",
                    events=[],
                    variant=None,
                    md5_sum="aaaaa11111bbbbb",
                    languages=[],
                    document_status="published",
                ),
            ],
            events=[
                NavigatorEventFactory.build(
                    import_id="123",
                    event_type="Decision",
                    date=decision_date,
                    valid_metadata={
                        "event_type": ["Decision"],
                        "datetime_event_name": ["Decision"],
                    },
                ),
                NavigatorEventFactory.build(
                    import_id="456",
                    event_type="Filing Year For Action",
                    date=decision_date,
                    valid_metadata={
                        "event_type": ["Filing Year For Action"],
                        "datetime_event_name": ["Filing Year For Action"],
                    },
                ),
            ],
            collections=[],
            geographies=[],
            slug="litigation-family-slug",
            metadata={
                "case_number": ["CASE-NUMBER 123"],
                "core_object": ["Core Object 123"],
                "status": ["Decided"],
                "id": ["123456"],
                "original_case_name": ["Original case name"],
            },
            concepts=[],
        ),
    )


@pytest.fixture
def navigator_family_with_litigation_concepts() -> Identified[NavigatorFamily]:
    decision_date = datetime.datetime(2020, 1, 1)
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Litigation family",
            summary="Family summary",
            category="LITIGATION",
            last_updated_date="2020-01-0100:00:00Z",
            published_date="2020-01-0100:00:00Z",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="Sabin"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="Litigation family document",
                    cdn_object=None,
                    source_url=None,
                    slug="litigation-document-slug",
                    events=[
                        NavigatorEventFactory.build(
                            import_id="123",
                            event_type="Decision",
                            date=decision_date,
                            valid_metadata={
                                "event_type": ["Decision"],
                                "datetime_event_name": ["Decision"],
                            },
                        )
                    ],
                    variant="Original language",
                    md5_sum="aaaaa11111bbbbb",
                    languages=[],
                    document_status="published",
                ),
            ],
            events=[
                NavigatorEventFactory.build(
                    import_id="123",
                    event_type="Decision",
                    date=decision_date,
                    valid_metadata={
                        "event_type": ["Decision"],
                        "datetime_event_name": ["Decision"],
                    },
                ),
            ],
            collections=[],
            geographies=[],
            slug="litigation-family-slug",
            concepts=[
                NavigatorConcept(
                    id="High Court of Justice",
                    ids=[],
                    type="legal_entity",
                    relation="jurisdiction",
                    preferred_label="High Court of Justice",
                    subconcept_of_labels=["England and Wales"],
                ),
                NavigatorConcept(
                    id="High Court of Justice (Administrative Court)",
                    ids=[],
                    type="legal_entity",
                    relation="jurisdiction",
                    preferred_label="High Court of Justice (Administrative Court)",
                    subconcept_of_labels=["High Court of Justice"],
                ),
                NavigatorConcept(
                    id="England and Wales",
                    ids=[],
                    type="legal_entity",
                    relation="jurisdiction",
                    preferred_label="England and Wales",
                    subconcept_of_labels=[],
                ),
            ],
        ),
    )


@pytest.fixture
def navigator_family_with_litigation_concept_missing_parent() -> (
    Identified[NavigatorFamily]
):
    decision_date = datetime.datetime(2020, 1, 1)
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Litigation family",
            summary="Family summary",
            category="LITIGATION",
            last_updated_date="2020-01-0100:00:00Z",
            published_date="2020-01-0100:00:00Z",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="Sabin"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="Litigation family document",
                    slug="litigation-document-slug",
                    events=[
                        NavigatorEventFactory.build(
                            import_id="123",
                            event_type="Decision",
                            date=decision_date,
                            valid_metadata={
                                "event_type": ["Decision"],
                                "datetime_event_name": ["Decision"],
                            },
                        )
                    ],
                    variant="Original language",
                    md5_sum="aaaaa11111bbbbb",
                    languages=[],
                    document_status="published",
                ),
            ],
            events=[],
            collections=[],
            geographies=[],
            slug="litigation-family-slug",
            concepts=[
                NavigatorConcept(
                    id="High Court of Justice",
                    ids=[],
                    type="legal_entity",
                    relation="jurisdiction",
                    preferred_label="High Court of Justice",
                    subconcept_of_labels=["Missing Parent Label"],
                ),
            ],
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
            last_updated_date=None,
            published_date=None,
            summary="Family summary",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(
                    id=1, name="Sabin Center for Climate Change Law"
                ),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=[],
            concepts=[],
        ),
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
            LabelRelationship(
                type="status",
                value=Label(
                    type="status",
                    id="status::Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="agent::Sabin Center for Climate Change Law",
                    value="Sabin Center for Climate Change Law",
                    attributes={
                        "attribution_url": "testurl.org",
                        "corpus_text": "Test corpus",
                        "corpus_image_url": "",
                    },
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::LITIGATION",
                    value="LITIGATION",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Litigation",
                    value="Litigation",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Filed",
                    value="Filed",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="Litigation family document",
                    description="Summary of the filed document",
                    labels=[
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="entity_type::Decision",
                                value="Decision",
                                type="entity_type",
                            ),
                        ),
                        LabelRelationship(
                            timestamp=datetime.datetime(2020, 1, 1, 0, 0),
                            type="activity_status",
                            value=Label(
                                attributes={},
                                documents=[],
                                id="activity_status::Filed",
                                labels=[],
                                type="activity_status",
                                value="Filed",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Sabin Center for Climate Change Law",
                                value="Sabin Center for Climate Change Law",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Litigation",
                                value="Litigation",
                                type="category",
                            ),
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "litigation-document-slug",
                        "md5_sum": "aaaaa11111bbbbb",
                        "variant": "Original language",
                        "status": "published",
                        "published_date": "2020-01-0100:00:00Z",
                        "last_updated_date": "2020-01-0100:00:00Z",
                        "action_taken": "Answer filed by federal defendants",
                    },
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
                                id="status::Obsolete",
                                value="Obsolete",
                                type="status",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Sabin Center for Climate Change Law",
                                value="Sabin Center for Climate Change Law",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Litigation",
                                value="Litigation",
                                type="category",
                            ),
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "placeholder-document-slug",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "published",
                        "published_date": "2020-01-0100:00:00Z",
                        "last_updated_date": "2020-01-0100:00:00Z",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "litigation-family-slug",
            "identifier::case_number": "CASE-NUMBER 123",
            "identifier::provider_id": "123456",
            "published_date": "2020-01-0100:00:00Z",
            "last_updated_date": "2020-01-0100:00:00Z",
            "status": "published",
            "core_object": "Core Object 123",
            "original_case_name": "Original case name",
            "case_status": "Decided",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document",
                title="Litigation family document",
                description="Summary of the filed document",
                labels=[
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="entity_type::Decision",
                            value="Decision",
                            type="entity_type",
                        ),
                    ),
                    LabelRelationship(
                        timestamp=datetime.datetime(2020, 1, 1, 0, 0),
                        type="activity_status",
                        value=Label(
                            attributes={},
                            documents=[],
                            id="activity_status::Filed",
                            labels=[],
                            type="activity_status",
                            value="Filed",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Sabin Center for Climate Change Law",
                            value="Sabin Center for Climate Change Law",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Litigation",
                            value="Litigation",
                            type="category",
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
                attributes={
                    "deprecated_slug": "litigation-document-slug",
                    "md5_sum": "aaaaa11111bbbbb",
                    "variant": "Original language",
                    "status": "published",
                    "published_date": "2020-01-0100:00:00Z",
                    "last_updated_date": "2020-01-0100:00:00Z",
                    "action_taken": "Answer filed by federal defendants",
                },
            ),
            Document(
                id="1.2.3.placeholder",
                title="Placeholder litigation family document",
                labels=[
                    LabelRelationship(
                        type="status",
                        value=Label(
                            id="status::Obsolete",
                            value="Obsolete",
                            type="status",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Sabin Center for Climate Change Law",
                            value="Sabin Center for Climate Change Law",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Litigation",
                            value="Litigation",
                            type="category",
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
                attributes={
                    "deprecated_slug": "placeholder-document-slug",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "published",
                    "published_date": "2020-01-0100:00:00Z",
                    "last_updated_date": "2020-01-0100:00:00Z",
                },
            ),
        ],
    )


def test_transform_navigator_family_with_litigation_corpus_type_and_litigation_concepts(
    navigator_family_with_litigation_concepts: Identified[NavigatorFamily],
):
    result = transform_navigator_family(navigator_family_with_litigation_concepts)
    expected_document_from_family = Document(
        id="family",
        title="Litigation family",
        description="Family summary",
        labels=[
            LabelRelationship(
                type="status",
                value=Label(
                    type="status",
                    id="status::Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="agent::Sabin Center for Climate Change Law",
                    value="Sabin Center for Climate Change Law",
                    attributes={
                        "attribution_url": "testurl.org",
                        "corpus_text": "Test corpus",
                        "corpus_image_url": "",
                    },
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::LITIGATION",
                    value="LITIGATION",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Litigation",
                    value="Litigation",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="legal_concept",
                value=LabelWithoutDocumentRelationships(
                    id="jurisdiction::High Court of Justice",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=LabelWithoutDocumentRelationships(
                                id="jurisdiction::England and Wales",
                                labels=[],
                                type="jurisdiction",
                                value="England and Wales",
                            ),
                        )
                    ],
                    type="jurisdiction",
                    value="High Court of Justice",
                ),
            ),
            LabelRelationship(
                type="legal_concept",
                value=LabelWithoutDocumentRelationships(
                    id="jurisdiction::High Court of Justice (Administrative Court)",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=LabelWithoutDocumentRelationships(
                                id="jurisdiction::High Court of Justice",
                                labels=[],
                                type="jurisdiction",
                                value="High Court of Justice",
                            ),
                        )
                    ],
                    type="jurisdiction",
                    value="High Court of Justice (Administrative Court)",
                ),
            ),
            LabelRelationship(
                type="legal_concept",
                value=LabelWithoutDocumentRelationships(
                    id="jurisdiction::England and Wales",
                    labels=[],
                    type="jurisdiction",
                    value="England and Wales",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="Litigation family document",
                    description=None,
                    labels=[
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="entity_type::Decision",
                                value="Decision",
                                type="entity_type",
                            ),
                        ),
                        LabelRelationship(
                            timestamp=datetime.datetime(2020, 1, 1, 0, 0),
                            type="activity_status",
                            value=Label(
                                attributes={},
                                documents=[],
                                id="activity_status::Filed",
                                labels=[],
                                type="activity_status",
                                value="Filed",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Sabin Center for Climate Change Law",
                                value="Sabin Center for Climate Change Law",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Litigation",
                                value="Litigation",
                                type="category",
                            ),
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "litigation-document-slug",
                        "md5_sum": "aaaaa11111bbbbb",
                        "variant": "Original language",
                        "status": "published",
                        "published_date": "2020-01-0100:00:00Z",
                        "last_updated_date": "2020-01-0100:00:00Z",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "litigation-family-slug",
            "published_date": "2020-01-0100:00:00Z",
            "last_updated_date": "2020-01-0100:00:00Z",
            "status": "published",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document",
                title="Litigation family document",
                description=None,
                labels=[
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="entity_type::Decision",
                            value="Decision",
                            type="entity_type",
                        ),
                    ),
                    LabelRelationship(
                        timestamp=datetime.datetime(2020, 1, 1, 0, 0),
                        type="activity_status",
                        value=Label(
                            attributes={},
                            documents=[],
                            id="activity_status::Filed",
                            labels=[],
                            type="activity_status",
                            value="Filed",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Sabin Center for Climate Change Law",
                            value="Sabin Center for Climate Change Law",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Litigation",
                            value="Litigation",
                            type="category",
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
                attributes={
                    "deprecated_slug": "litigation-document-slug",
                    "md5_sum": "aaaaa11111bbbbb",
                    "variant": "Original language",
                    "status": "published",
                    "published_date": "2020-01-0100:00:00Z",
                    "last_updated_date": "2020-01-0100:00:00Z",
                },
            ),
        ],
    )


def test_transform_navigator_family_with_litigation_concepts_missing_parent_label_returns_failure(
    navigator_family_with_litigation_concept_missing_parent: Identified[
        NavigatorFamily
    ],
):
    result = transform_navigator_family(
        navigator_family_with_litigation_concept_missing_parent
    )

    failure_exception = result.swap().unwrap()
    assert isinstance(failure_exception, CouldNotTransform)
    assert (
        "Unknown parent label 'Missing Parent Label' in relation 'jurisdiction'. "
        "See family 'family' for details."
    ) in str(failure_exception)
