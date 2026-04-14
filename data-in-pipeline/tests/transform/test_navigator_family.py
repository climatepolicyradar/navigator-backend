import datetime

import pytest
from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Item,
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
        attribution_url="testurl.org",
        corpus_text="Test corpus",
        corpus_image_url="corpus_image.png",
    )


def _cclw_laws_and_policies_corpus():
    return NavigatorCorpusFactory.build(
        import_id="CCLW.corpus.i00000001.n0000",
        corpus_type=NavigatorCorpusTypeFactory.build(name="Laws and Policies"),
        organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
        attribution_url="testurl.org",
        corpus_text="Test corpus",
        corpus_image_url="corpus_image.png",
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
        NavigatorEventFactory.build(
            import_id=iid,
            event_type=ety,
            date=base_date,
            valid_metadata={
                "event_type": [ety],
                "datetime_event_name": ["Passed/Approved"],
            },
        )
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
            category="EXECUTIVE",
            corpus=_cclw_laws_and_policies_corpus(),
            last_updated_date="2020-01-0100:00:00Z",
            published_date="2020-01-0100:00:00Z",
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="Matching title on family and document and collection",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    language="eng",
                    languages=["eng", "fra"],
                    md5_sum="aaaaa11111bbbbb",
                    events=[],
                    valid_metadata={
                        "role": ["SUPPORTING LEGISLATION"],
                        "type": ["National Drought Plan (NDP)"],
                    },
                    slug="document-slug",
                    document_status="PUBLISHED",
                ),
            ],
            events=_standard_events(),
            collections=[
                NavigatorCollectionFactory.build(
                    import_id="collection_matching",
                    title="Matching title on family and document and collection",
                    description="Collection description",
                    slug="collection-matching-slug",
                ),
                NavigatorCollectionFactory.build(
                    import_id="collection",
                    title="Collection title",
                    description="Collection description",
                    slug="collection-slug",
                ),
            ],
            geographies=["AU-NSW", "AUS", "XAA"],
            metadata={
                "author": ["Test Author"],
                "author_type": ["Person"],
            },
            slug="family-slug",
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
            category="UNFCCC",
            last_updated_date="2020-01-0100:00:00Z",
            published_date="2020-01-0100:00:00Z",
            corpus=_cclw_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="456",
                    title="Test document 1",
                    events=[],
                    document_status="PUBLISHED",
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
                            },
                        )
                    ],
                    variant="Original language",
                    md5_sum="aaaaa11111bbbbb",
                    languages=[],
                    document_status="PUBLISHED",
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
                    document_status="PUBLISHED",
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
            metadata={
                "case_number": ["CASE-NUMBER 123"],
                "core_object": ["Core Object 123"],
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
                    document_status="PUBLISHED",
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
                    document_status="PUBLISHED",
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


def _mcf_events():
    base_date = datetime.datetime(2020, 1, 1)
    return [
        NavigatorEventFactory.build(
            import_id="concept_approved",
            event_type="Concept Approved",
            date=base_date,
            valid_metadata={
                "event_type": ["Concept Approved"],
                "datetime_event_name": ["Project Approved"],
            },
        ),
        NavigatorEventFactory.build(
            import_id="project_approved",
            event_type="Project Approved",
            date=base_date,
            valid_metadata={
                "event_type": ["Project Approved"],
                "datetime_event_name": ["Project Approved"],
            },
        ),
        NavigatorEventFactory.build(
            import_id="under_implementation",
            event_type="Under Implementation",
            date=base_date,
            valid_metadata={
                "event_type": ["Under Implementation"],
                "datetime_event_name": ["Project Approved"],
            },
        ),
        NavigatorEventFactory.build(
            import_id="project_completed",
            event_type="Project Completed",
            date=base_date,
            valid_metadata={
                "event_type": ["Project Completed"],
                "datetime_event_name": ["Project Approved"],
            },
        ),
        NavigatorEventFactory.build(
            import_id="cancelled",
            event_type="Cancelled",
            date=base_date,
            valid_metadata={
                "event_type": ["Cancelled"],
                "datetime_event_name": ["Project Approved"],
            },
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
            category="MCF",
            last_updated_date=None,
            published_date=None,
            corpus=NavigatorCorpusFactory.build(
                import_id="MCF.corpus.AF.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="AF"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document_1",
                    title="Multilateral climate fund project document",
                    cdn_object=None,
                    source_url=None,
                    slug="document-1-slug",
                    events=[],
                    variant="Original language",
                    md5_sum="aaaaa11111bbbbb",
                    language="eng",
                    languages=["eng"],
                    document_status="PUBLISHED",
                ),
                NavigatorDocumentFactory.build(
                    import_id="document_2",
                    title="Project document",
                    cdn_object=None,
                    source_url=None,
                    slug="document-2-slug",
                    events=[],
                    variant="Original language",
                    md5_sum="aaaaa11111bbbbb",
                    language="eng",
                    languages=["eng"],
                    document_status="PUBLISHED",
                ),
            ],
            events=_mcf_events(),
            collections=[],
            geographies=[],
            metadata={
                "region": ["Europe & Central Asia"],
                "sector": ["Public Sector"],
                "status": ["Under Implementation"],
                "project_id": ["XACTMK002A"],
                "external_id": [],
                "project_url": ["https://www.cif.org/projects"],
                "implementing_agency": ["International Bank for Reconstruction"],
                "project_value_fund_spend": ["250000"],
                "project_value_co_financing": ["100000"],
                "approved_ref": ["XACTMK002A"],
            },
            slug="mcf-family-slug",
        ),
    )


@pytest.fixture
def navigator_family_multilateral_climate_fund_guidance() -> (
    Identified[NavigatorFamily]
):
    return Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Multilateral climate fund guidance",
            summary="Family summary",
            category="REPORTS",
            last_updated_date=None,
            published_date=None,
            corpus=NavigatorCorpusFactory.build(
                import_id="MCF.corpus.AF.Guidance",
                corpus_type=NavigatorCorpusTypeFactory.build(name="AF"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=[],
            metadata={},
            slug="mcf-guidance-family-slug",
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
                    id="status::Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Amended/Updated",
                    value="Amended/Updated",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Appealed",
                    value="Appealed",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Closed",
                    value="Closed",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Declaration of climate emergency",
                    value="Declaration of climate emergency",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Dismissed",
                    value="Dismissed",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Entered into force",
                    value="Entered into force",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status", id="activity_status::Filing", value="Filing"
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Granted",
                    value="Granted",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Implementation details",
                    value="Implementation details",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::International agreement",
                    value="International agreement",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Net zero pledge",
                    value="Net zero pledge",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status", id="activity_status::Other", value="Other"
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Passed/Approved",
                    value="Passed/Approved",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Repealed/Replaced",
                    value="Repealed/Replaced",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status", id="activity_status::Set", value="Set"
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Settled",
                    value="Settled",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                timestamp=datetime.datetime(2020, 1, 1),
                value=Label(
                    type="activity_status",
                    id="activity_status::Published",
                    value="Published",
                ),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="agent::Grantham Research Institute",
                    value="Grantham Research Institute",
                    attributes={
                        "attribution_url": "testurl.org",
                        "corpus_text": "Test corpus",
                        "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                    },
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="geography",
                    id="geography::AU-NSW",
                    value="New South Wales",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="geography",
                    id="geography::AUS",
                    value="Australia",
                ),
            ),
            LabelRelationship(
                type="author",
                value=Label(
                    id="person::Test Author",
                    value="Test Author",
                    type="person",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::EXECUTIVE",
                    value="EXECUTIVE",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::Laws and Policies",
                    value="Laws and Policies",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Policy",
                    value="Policy",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="author",
                value=Label(
                    id="author::Test Author",
                    type="author",
                    value="Test Author",
                ),
            ),
            LabelRelationship(
                type="author_type",
                value=Label(
                    id="author_type::Person",
                    type="author_type",
                    value="Person",
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
                                id="entity_type::Collection",
                                value="Collection",
                                type="entity_type",
                            ),
                        )
                    ],
                    attributes={"deprecated_slug": "collection-slug"},
                ),
            ),
            DocumentRelationship(
                type="has_version",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="Matching title on family and document and collection",
                    labels=[
                        LabelRelationship(
                            type="role",
                            value=Label(
                                type="entity_type",
                                id="entity_type::Supporting legislation",
                                value="Supporting legislation",
                            ),
                        ),
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                type="entity_type",
                                id="entity_type::National drought plan (ndp)",
                                value="National drought plan (ndp)",
                            ),
                        ),
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Grantham Research Institute",
                                value="Grantham Research Institute",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="geography::AU-NSW",
                                value="New South Wales",
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="geography::AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="deprecated_category",
                            value=Label(
                                id="deprecated_category::Laws and Policies",
                                value="Laws and Policies",
                                type="deprecated_category",
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Policy",
                                value="Policy",
                                type="category",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::fra",
                                value="fra",
                                type="language",
                            ),
                        ),
                    ],
                    items=[
                        Item(
                            url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                            type="cdn",
                            content_type="application/pdf",
                        ),
                        Item(
                            url="https://source.climatepolicyradar.org/path/to/file.pdf",
                            type="source",
                            content_type="application/pdf",
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "document-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "PUBLISHED",
                    },
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
                                id="entity_type::Collection",
                                value="Collection",
                                type="entity_type",
                            ),
                        )
                    ],
                    items=[],
                    attributes={"deprecated_slug": "collection-matching-slug"},
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "family-slug",
            "published_date": "2020-01-0100:00:00Z",
            "last_updated_date": "2020-01-0100:00:00Z",
            "status": "PUBLISHED",
        },
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
                        type="role",
                        value=Label(
                            type="entity_type",
                            id="entity_type::Supporting legislation",
                            value="Supporting legislation",
                        ),
                    ),
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            type="entity_type",
                            id="entity_type::National drought plan (ndp)",
                            value="National drought plan (ndp)",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Grantham Research Institute",
                            value="Grantham Research Institute",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="geography::AU-NSW",
                            value="New South Wales",
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="geography::AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="deprecated_category",
                        value=Label(
                            id="deprecated_category::Laws and Policies",
                            value="Laws and Policies",
                            type="deprecated_category",
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Policy",
                            value="Policy",
                            type="category",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::fra",
                            value="fra",
                            type="language",
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
                        type="cdn",
                        content_type="application/pdf",
                    ),
                    Item(
                        url="https://source.climatepolicyradar.org/path/to/file.pdf",
                        type="source",
                        content_type="application/pdf",
                    ),
                ],
                attributes={
                    "deprecated_slug": "document-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "PUBLISHED",
                },
            ),
            Document(
                id="collection_matching",
                title="Matching title on family and document and collection",
                attributes={
                    "deprecated_slug": "collection-matching-slug",
                },
                labels=[
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="entity_type::Collection",
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
                attributes={
                    "deprecated_slug": "collection-slug",
                },
                id="collection",
                title="Collection title",
                labels=[
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="entity_type::Collection",
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


@pytest.mark.parametrize(
    "corpus_id, org, provider",
    [
        ("CCLW.corpus.i00000001.n0000", "CCLW", "Grantham Research Institute"),
        ("CPR.corpus.i00000001.n0000", "CPR", "NewClimate Institute"),
        ("CPR.corpus.Goldstandard.n0000", "CPR", "Gold Standard"),
        ("CPR.corpus.i00000589.n0000", "CPR", "Naturebase"),
        ("CPR.corpus.i00000591.n0000", "CPR", "Laws Africa"),
        ("CPR.corpus.i00000592.n0000", "CPR", "UNDRR"),
    ],
)
def test_transform_navigator_family_with_laws_and_policies_corpus_type(
    corpus_id: str, org: str, provider: str
):
    navigator_family_with_laws_and_policies_corpus_type = Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Laws and policies family",
            summary="Family summary",
            category="LEGISLATIVE",
            published_date=None,
            last_updated_date=None,
            corpus=NavigatorCorpusFactory.build(
                import_id=corpus_id,
                corpus_type=NavigatorCorpusTypeFactory.build(name="Laws and Policies"),
                organisation=NavigatorOrganisationFactory.build(id=1, name=org),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="laws-and-policies-family-slug",
            metadata={
                # testing single value
                "topic": ["Mitigation"],
                "sector": ["Economy-wide"],
                "keyword": ["Transport"],
                "framework": ["Mitigation"],
                # testing no value
                "hazard": [],
                # testing multiple values
                "instrument": [
                    "Processes, plans and strategies|Governance",
                    "Planning|Governance",
                ],
            },
        ),
    )
    result = transform_navigator_family(
        navigator_family_with_laws_and_policies_corpus_type
    )
    expected_document_from_family = Document(
        id="family",
        title="Laws and policies family",
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
                    id=f"agent::{provider}",
                    value=provider,
                    attributes={
                        "attribution_url": "testurl.org",
                        "corpus_text": "Test corpus",
                        "corpus_image_url": "",
                    },
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="geography::AUS",
                    value="Australia",
                    type="geography",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::LEGISLATIVE",
                    value="LEGISLATIVE",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::Laws and Policies",
                    value="Laws and Policies",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Law",
                    value="Law",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="topic",
                value=Label(
                    id="topic::Mitigation",
                    value="Mitigation",
                    type="topic",
                ),
            ),
            LabelRelationship(
                type="sector",
                value=Label(
                    id="sector::Economy-wide",
                    value="Economy-wide",
                    type="sector",
                ),
            ),
            LabelRelationship(
                type="keyword",
                value=Label(
                    id="keyword::Transport",
                    value="Transport",
                    type="keyword",
                ),
            ),
            LabelRelationship(
                type="framework",
                value=Label(
                    id="framework::Mitigation",
                    value="Mitigation",
                    type="framework",
                ),
            ),
            LabelRelationship(
                type="instrument",
                value=Label(
                    id="instrument::Processes, plans and strategies|Governance",
                    value="Processes, plans and strategies|Governance",
                    type="instrument",
                ),
            ),
            LabelRelationship(
                type="instrument",
                value=Label(
                    id="instrument::Planning|Governance",
                    value="Planning|Governance",
                    type="instrument",
                ),
            ),
        ],
        documents=[],
        attributes={
            "deprecated_slug": "laws-and-policies-family-slug",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [expected_document_from_family],
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
                                id="entity_type::Decision",
                                value="Decision",
                                type="entity_type",
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
                    ],
                    attributes={
                        "deprecated_slug": "litigation-document-slug",
                        "md5_sum": "aaaaa11111bbbbb",
                        "variant": "Original language",
                        "status": "PUBLISHED",
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
                    ],
                    attributes={
                        "deprecated_slug": "placeholder-document-slug",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "PUBLISHED",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "litigation-family-slug",
            "identifier::case_number": "CASE-NUMBER 123",
            "published_date": "2020-01-0100:00:00Z",
            "last_updated_date": "2020-01-0100:00:00Z",
            "status": "PUBLISHED",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document",
                title="Litigation family document",
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
                    "status": "PUBLISHED",
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
                    "status": "PUBLISHED",
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
                    ],
                    attributes={
                        "deprecated_slug": "litigation-document-slug",
                        "md5_sum": "aaaaa11111bbbbb",
                        "variant": "Original language",
                        "status": "PUBLISHED",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "litigation-family-slug",
            "published_date": "2020-01-0100:00:00Z",
            "last_updated_date": "2020-01-0100:00:00Z",
            "status": "PUBLISHED",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document",
                title="Litigation family document",
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
                    "status": "PUBLISHED",
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
                    id="status::Principal",
                    value="Principal",
                ),
            ),
            LabelRelationship(
                type="entity_type",
                value=Label(
                    id="entity_type::Project",
                    value="Project",
                    type="entity_type",
                ),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="activity_status::Concept approved",
                    value="Concept approved",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="activity_status::Approved",
                    value="Approved",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="activity_status::Under implementation",
                    value="Under implementation",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="activity_status::Completed",
                    value="Completed",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="activity_status",
                value=Label(
                    id="activity_status::Cancelled",
                    value="Cancelled",
                    type="activity_status",
                ),
                timestamp=datetime.datetime(2020, 1, 1),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="agent::Adaptation Fund",
                    value="Adaptation Fund",
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
                    id="deprecated_category::MCF",
                    value="MCF",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Multilateral Climate Fund project",
                    value="Multilateral Climate Fund project",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="sector",
                value=Label(
                    id="sector::Public Sector",
                    type="sector",
                    value="Public Sector",
                ),
            ),
            LabelRelationship(
                type="project_status",
                value=Label(
                    id="project_status::Under Implementation",
                    type="project_status",
                    value="Under Implementation",
                ),
            ),
            LabelRelationship(
                type="implementing_agency",
                value=Label(
                    id="implementing_agency::International Bank for Reconstruction",
                    type="implementing_agency",
                    value="International Bank for Reconstruction",
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
                                id="agent::Adaptation Fund",
                                value="Adaptation Fund",
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
                                id="category::Multilateral Climate Fund project",
                                value="Multilateral Climate Fund project",
                                type="category",
                            ),
                        ),
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="entity_type::Project",
                                value="Project",
                                type="entity_type",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "document-1-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "PUBLISHED",
                    },
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
                                id="agent::Adaptation Fund",
                                value="Adaptation Fund",
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
                                id="category::Multilateral Climate Fund project",
                                value="Multilateral Climate Fund project",
                                type="category",
                            ),
                        ),
                        LabelRelationship(
                            type="entity_type",
                            value=Label(
                                id="entity_type::Project",
                                value="Project",
                                type="entity_type",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "document-2-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "PUBLISHED",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "mcf-family-slug",
            "identifier::project_id": "XACTMK002A",
            "identifier::project_approved_ref": "XACTMK002A",
            "project_co_financing_usd": 100000,
            "project_fund_spend_usd": 250000,
            "project_url": "https://www.cif.org/projects",
            "status": "PUBLISHED",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
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
                            id="agent::Adaptation Fund",
                            value="Adaptation Fund",
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
                            id="category::Multilateral Climate Fund project",
                            value="Multilateral Climate Fund project",
                            type="category",
                        ),
                    ),
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="entity_type::Project",
                            value="Project",
                            type="entity_type",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
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
                    "deprecated_slug": "document-1-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "PUBLISHED",
                },
            ),
            Document(
                id="document_2",
                title="Project document",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Adaptation Fund",
                            value="Adaptation Fund",
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
                            id="category::Multilateral Climate Fund project",
                            value="Multilateral Climate Fund project",
                            type="category",
                        ),
                    ),
                    LabelRelationship(
                        type="entity_type",
                        value=Label(
                            id="entity_type::Project",
                            value="Project",
                            type="entity_type",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
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
                attributes={
                    "deprecated_slug": "document-2-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "PUBLISHED",
                },
            ),
        ],
    )


def test_transform_navigator_family_with_published_and_unpublished_documents():
    navigator_family_with_different_document_statuses = Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Family with different document statuses",
            summary="Family summary",
            category="LEGISLATIVE",
            published_date=None,
            last_updated_date=None,
            corpus=_cclw_laws_and_policies_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document1",
                    title="Matching title on family and document",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    languages=["eng"],
                    md5_sum="aaaaa11111bbbbb",
                    events=[],
                    valid_metadata={},
                    slug="document1-slug",
                    document_status="CREATED",
                ),
                NavigatorDocumentFactory.build(
                    import_id="document2",
                    title="Matching title on family and document",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    languages=["eng"],
                    md5_sum="aaaaa11111cccc",
                    events=[],
                    valid_metadata={},
                    slug="document2-slug",
                    document_status="PUBLISHED",
                ),
            ],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="family-with-different-document-statuses-slug",
            metadata={},
        ),
    )
    result = transform_navigator_family(
        navigator_family_with_different_document_statuses
    )
    expected_document_from_family = Document(
        id="family",
        title="Family with different document statuses",
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
                    id="agent::Grantham Research Institute",
                    value="Grantham Research Institute",
                    attributes={
                        "attribution_url": "testurl.org",
                        "corpus_text": "Test corpus",
                        "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                    },
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="geography::AUS",
                    value="Australia",
                    type="geography",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::LEGISLATIVE",
                    value="LEGISLATIVE",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::Laws and Policies",
                    value="Laws and Policies",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Law",
                    value="Law",
                    type="category",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document1",
                    title="Matching title on family and document",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Grantham Research Institute",
                                value="Grantham Research Institute",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="geography::AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="deprecated_category",
                            value=Label(
                                id="deprecated_category::Laws and Policies",
                                value="Laws and Policies",
                                type="deprecated_category",
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                    ],
                    items=[
                        Item(
                            url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                            type="cdn",
                            content_type="application/pdf",
                        ),
                        Item(
                            url="https://source.climatepolicyradar.org/path/to/file.pdf",
                            type="source",
                            content_type="application/pdf",
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "document1-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "CREATED",
                    },
                ),
            ),
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document2",
                    title="Matching title on family and document",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Grantham Research Institute",
                                value="Grantham Research Institute",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="geography::AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="deprecated_category",
                            value=Label(
                                id="deprecated_category::Laws and Policies",
                                value="Laws and Policies",
                                type="deprecated_category",
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                    ],
                    items=[
                        Item(
                            url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                            type="cdn",
                            content_type="application/pdf",
                        ),
                        Item(
                            url="https://source.climatepolicyradar.org/path/to/file.pdf",
                            type="source",
                            content_type="application/pdf",
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "document2-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111cccc",
                        "status": "PUBLISHED",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "family-with-different-document-statuses-slug",
            "status": "PUBLISHED",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document1",
                title="Matching title on family and document",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Grantham Research Institute",
                            value="Grantham Research Institute",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="geography::AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="deprecated_category",
                        value=Label(
                            id="deprecated_category::Laws and Policies",
                            value="Laws and Policies",
                            type="deprecated_category",
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Law",
                            value="Law",
                            type="category",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
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
                items=[
                    Item(
                        url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                        type="cdn",
                        content_type="application/pdf",
                    ),
                    Item(
                        url="https://source.climatepolicyradar.org/path/to/file.pdf",
                        type="source",
                        content_type="application/pdf",
                    ),
                ],
                attributes={
                    "deprecated_slug": "document1-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "CREATED",
                },
            ),
            Document(
                id="document2",
                title="Matching title on family and document",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Grantham Research Institute",
                            value="Grantham Research Institute",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="geography::AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="deprecated_category",
                        value=Label(
                            id="deprecated_category::Laws and Policies",
                            value="Laws and Policies",
                            type="deprecated_category",
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Law",
                            value="Law",
                            type="category",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
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
                items=[
                    Item(
                        url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                        type="cdn",
                        content_type="application/pdf",
                    ),
                    Item(
                        url="https://source.climatepolicyradar.org/path/to/file.pdf",
                        type="source",
                        content_type="application/pdf",
                    ),
                ],
                attributes={
                    "deprecated_slug": "document2-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111cccc",
                    "status": "PUBLISHED",
                },
            ),
        ],
    )


def test_transform_navigator_family_with_no_published_documents():
    navigator_family_with_no_published_documents = Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Family with no published documents",
            summary="Family summary",
            category="LEGISLATIVE",
            published_date=None,
            last_updated_date=None,
            corpus=_cclw_laws_and_policies_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document1",
                    title="Matching title on family and document",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    languages=["eng"],
                    md5_sum="aaaaa11111bbbbb",
                    events=[],
                    valid_metadata={},
                    slug="document1-slug",
                    document_status="DELETED",
                ),
            ],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="family-with-no-published-documents-slug",
            metadata={},
        ),
    )
    result = transform_navigator_family(navigator_family_with_no_published_documents)
    expected_document_from_family = Document(
        id="family",
        title="Family with no published documents",
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
                    id="agent::Grantham Research Institute",
                    value="Grantham Research Institute",
                    attributes={
                        "attribution_url": "testurl.org",
                        "corpus_text": "Test corpus",
                        "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                    },
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="geography::AUS",
                    value="Australia",
                    type="geography",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::LEGISLATIVE",
                    value="LEGISLATIVE",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::Laws and Policies",
                    value="Laws and Policies",
                    type="deprecated_category",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Law",
                    value="Law",
                    type="category",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document1",
                    title="Matching title on family and document",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="agent::Grantham Research Institute",
                                value="Grantham Research Institute",
                                attributes={
                                    "attribution_url": "testurl.org",
                                    "corpus_text": "Test corpus",
                                    "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                                },
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="geography::AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="deprecated_category",
                            value=Label(
                                id="deprecated_category::Laws and Policies",
                                value="Laws and Policies",
                                type="deprecated_category",
                            ),
                        ),
                        LabelRelationship(
                            type="category",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                    ],
                    items=[
                        Item(
                            url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                            type="cdn",
                            content_type="application/pdf",
                        ),
                        Item(
                            url="https://source.climatepolicyradar.org/path/to/file.pdf",
                            type="source",
                            content_type="application/pdf",
                        ),
                    ],
                    attributes={
                        "deprecated_slug": "document1-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
                        "status": "DELETED",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "family-with-no-published-documents-slug",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document1",
                title="Matching title on family and document",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Grantham Research Institute",
                            value="Grantham Research Institute",
                            attributes={
                                "attribution_url": "testurl.org",
                                "corpus_text": "Test corpus",
                                "corpus_image_url": "https://cdn.climatepolicyradar.org/corpus_image.png",
                            },
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="geography::AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="deprecated_category",
                        value=Label(
                            id="deprecated_category::Laws and Policies",
                            value="Laws and Policies",
                            type="deprecated_category",
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Law",
                            value="Law",
                            type="category",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
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
                items=[
                    Item(
                        url="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                        type="cdn",
                        content_type="application/pdf",
                    ),
                    Item(
                        url="https://source.climatepolicyradar.org/path/to/file.pdf",
                        type="source",
                        content_type="application/pdf",
                    ),
                ],
                attributes={
                    "deprecated_slug": "document1-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "DELETED",
                },
            ),
        ],
    )


def test_transform_navigator_family_with_multilateral_climate_fund_guidance(
    navigator_family_multilateral_climate_fund_guidance: Identified[NavigatorFamily],
):
    result = transform_navigator_family(
        navigator_family_multilateral_climate_fund_guidance
    )
    assert_model_list_equality(
        result.unwrap(),
        [
            Document(
                id="family",
                title="Multilateral climate fund guidance",
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
                        type="entity_type",
                        value=Label(
                            id="entity_type::Guidance",
                            value="Guidance",
                            type="entity_type",
                        ),
                    ),
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="agent::Adaptation Fund",
                            value="Adaptation Fund",
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
                            id="deprecated_category::REPORTS",
                            value="REPORTS",
                            type="deprecated_category",
                        ),
                    ),
                    LabelRelationship(
                        type="category",
                        value=Label(
                            id="category::Multilateral Climate Fund project",
                            value="Multilateral Climate Fund project",
                            type="category",
                        ),
                    ),
                ],
                documents=[],
                attributes={
                    "deprecated_slug": "mcf-guidance-family-slug",
                },
            )
        ],
    )


@pytest.mark.parametrize(
    "corpus_id, expected_category",
    [
        ("UNFCCC.corpus.i00000001.n0000", "category::UN submission"),
        ("UN.corpus.UNCCD.n0000", "category::UN submission"),
        ("UN.corpus.UNCBD.n0000", "category::UN submission"),
        ("OEP.corpus.i00000001.n0000", "category::Report"),
        ("CPR.corpus.i00000002.n0000", "category::Litigation"),
        ("MCF.corpus.AF.n0000", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.CIF.n0000", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.GCF.n0000", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.GEF.n0000", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.AF.Guidance", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.CIF.Guidance", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.GCF.Guidance", "category::Multilateral Climate Fund project"),
        ("MCF.corpus.GEF.Guidance", "category::Multilateral Climate Fund project"),
    ],
)
def test_transform_to_category_corpus_ids(corpus_id: str, expected_category: str):
    navigator_family = Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="Family",
            summary="Family summary",
            category="REPORTS",
            published_date=None,
            last_updated_date=None,
            corpus=NavigatorCorpusFactory.build(
                import_id=corpus_id,
                corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="org"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url=None,
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=[],
            slug="family-slug",
            metadata={},
        ),
    )

    result = transform_navigator_family(navigator_family)
    documents = result.unwrap()
    family_doc = documents[0]

    category_labels = [label for label in family_doc.labels if label.type == "category"]
    assert any(
        label.value.id == expected_category for label in category_labels
    ), f"Expected category '{expected_category}' not found in labels for corpus '{corpus_id}'"
