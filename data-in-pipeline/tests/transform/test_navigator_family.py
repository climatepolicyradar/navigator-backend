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

from app.extract.connectors import (
    NavigatorFamily,
)
from app.models import Identified
from app.transform.navigator_family import (
    _author_label,
    _author_type_label,
    _category_label,
    _deprecated_category_label,
    _entity_type_label,
    _implementing_agency_label,
    _law_type_label,
    _multilateral_climate_fund_label,
    _report_type_label,
    _status_label,
    _topic_label,
    _un_convention_label,
    transform_navigator_family,
)
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
            corpus=_cclw_corpus(),
            last_updated_date="2020-01-01T00:00:00Z",
            published_date="2020-01-01T00:00:00Z",
            created="2020-01-01T00:00:00Z",
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
                    document_status="published",
                ),
            ],
            events=_standard_events(),
            collections=[
                NavigatorCollectionFactory.build(
                    import_id="collection_matching",
                    title="Matching title on family and document and collection",
                    description="Collection description",
                    slug="collection-matching-slug",
                    created="2020-01-01T00:00:00Z",
                    last_modified="2020-01-01T00:00:00Z",
                ),
                NavigatorCollectionFactory.build(
                    import_id="collection",
                    title="Collection title",
                    description="Collection description",
                    slug="collection-slug",
                    created="2020-01-01T00:00:00Z",
                    last_modified="2020-01-01T00:00:00Z",
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
            last_updated_date="2020-01-01T00:00:00Z",
            published_date="2020-01-01T00:00:00Z",
            created="2020-01-01T00:00:00Z",
            corpus=_cclw_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="456",
                    title="Test document 1",
                    events=[],
                    document_status="published",
                ),
            ],
            events=[],
            collections=[],
            geographies=[],
        ),
    )


@pytest.fixture
def navigator_family_with_domain_metadata() -> Identified[NavigatorFamily]:
    return Identified(
        id="123",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="123",
            title="Capsule Corp family with domain metadata",
            summary="Family summary",
            category="UNFCCC",
            last_updated_date="2020-01-01T00:00:00Z",
            published_date="2020-01-01T00:00:00Z",
            created="2020-01-01T00:00:00Z",
            corpus=_cclw_corpus(),
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="documentCapsuleCorp1",
                    title="Test document 1 of Climate Domain",
                    events=[],
                    document_status="published",
                    slug="document-slug",
                    languages=["eng"],
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    md5_sum="TrunksIsTheBest",
                ),
            ],
            events=[],
            collections=[],
            geographies=[],
            metadata={
                "domain": ["Climate", "Nature"],
            },
            slug="family-slug",
        ),
    )


def test_transform_navigator_family_with_single_matching_document(
    navigator_family_with_single_matching_document: Identified[NavigatorFamily],
):
    output = transform_navigator_family(
        navigator_family_with_single_matching_document
    ).unwrap()

    assert output.warnings == []

    family_result = output.documents
    collection_result = output.collection_documents

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
                    type="subdivision",
                    id="subdivision::AU-NSW",
                    value="New South Wales",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=Label(
                                type="country",
                                id="country::AUS",
                                value="Australia",
                            ),
                        ),
                    ],
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="region",
                    id="region::EAS",
                    value="East Asia & Pacific",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="country",
                    id="country::AUS",
                    value="Australia",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=Label(
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                    ],
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
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Policy",
                    value="Policy",
                    type="category",
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
        ],
        documents=[
            DocumentRelationship(
                type="member_of",
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
                    attributes={
                        "deprecated_slug": "collection-matching-slug",
                        "published_date": "2020-01-01T00:00:00Z",
                        "last_updated_date": "2020-01-01T00:00:00Z",
                    },
                ),
            ),
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
                    attributes={
                        "deprecated_slug": "collection-slug",
                        "published_date": "2020-01-01T00:00:00Z",
                        "last_updated_date": "2020-01-01T00:00:00Z",
                    },
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
                                type="role",
                                id="role::Supporting legislation",
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
                                type="subdivision",
                                id="subdivision::AU-NSW",
                                value="New South Wales",
                                labels=[
                                    LabelRelationship(
                                        type="subconcept_of",
                                        value=Label(
                                            type="country",
                                            id="country::AUS",
                                            value="Australia",
                                        ),
                                    ),
                                ],
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="country",
                                id="country::AUS",
                                value="Australia",
                                labels=[
                                    LabelRelationship(
                                        type="subconcept_of",
                                        value=Label(
                                            type="region",
                                            id="region::EAS",
                                            value="East Asia & Pacific",
                                        ),
                                    ),
                                ],
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
                        LabelRelationship(
                            type="status",
                            value=Label(
                                id="status::Merged",
                                value="Merged",
                                type="status",
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
                            type="deprecated_category",
                            value=Label(
                                id="deprecated_category::Laws and Policies",
                                value="Laws and Policies",
                                type="deprecated_category",
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
                        "status": "published",
                        "published_date": "2020-01-01T00:00:00Z",
                        "last_updated_date": "2020-01-01T00:00:00Z",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "family-slug",
            "published_date": "2020-01-01T00:00:00Z",
            "last_updated_date": "2020-01-01T00:00:00Z",
            "status": "published",
        },
    )
    assert_model_list_equality(
        family_result + collection_result,
        [
            expected_document_from_family,
            Document(
                id="document",
                title="Matching title on family and document and collection",
                labels=[
                    LabelRelationship(
                        type="role",
                        value=Label(
                            type="role",
                            id="role::Supporting legislation",
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
                            type="subdivision",
                            id="subdivision::AU-NSW",
                            value="New South Wales",
                            labels=[
                                LabelRelationship(
                                    type="subconcept_of",
                                    value=Label(
                                        type="country",
                                        id="country::AUS",
                                        value="Australia",
                                    ),
                                ),
                            ],
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="region",
                            id="region::EAS",
                            value="East Asia & Pacific",
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="country",
                            id="country::AUS",
                            value="Australia",
                            labels=[
                                LabelRelationship(
                                    type="subconcept_of",
                                    value=Label(
                                        type="region",
                                        id="region::EAS",
                                        value="East Asia & Pacific",
                                    ),
                                ),
                            ],
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
                    LabelRelationship(
                        type="status",
                        value=Label(
                            id="status::Merged",
                            value="Merged",
                            type="status",
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
                        type="deprecated_category",
                        value=Label(
                            id="deprecated_category::Laws and Policies",
                            value="Laws and Policies",
                            type="deprecated_category",
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
                    "status": "published",
                    "published_date": "2020-01-01T00:00:00Z",
                    "last_updated_date": "2020-01-01T00:00:00Z",
                },
            ),
            Document(
                id="collection_matching",
                title="Matching title on family and document and collection",
                attributes={
                    "deprecated_slug": "collection-matching-slug",
                    "published_date": "2020-01-01T00:00:00Z",
                    "last_updated_date": "2020-01-01T00:00:00Z",
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
                        type="has_member",
                        value=DocumentWithoutRelationships(
                            **expected_document_from_family.model_dump()
                        ),
                    ),
                ],
            ),
            Document(
                attributes={
                    "deprecated_slug": "collection-slug",
                    "published_date": "2020-01-01T00:00:00Z",
                    "last_updated_date": "2020-01-01T00:00:00Z",
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
            created="2020-01-01T00:00:00Z",
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
    output = transform_navigator_family(
        navigator_family_with_laws_and_policies_corpus_type
    ).unwrap()

    assert output.warnings == []
    assert output.collection_documents == []

    family_result = output.documents

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
                    type="region",
                    id="region::EAS",
                    value="East Asia & Pacific",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="country::AUS",
                    value="Australia",
                    type="country",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=Label(
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                    ],
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
            LabelRelationship(
                type="category",
                value=Label(
                    id="category::Law",
                    value="Law",
                    type="category",
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
                type="law_type",
                value=Label(
                    id="law_type::Mitigation",
                    value="Framework law",
                    type="law_type",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
                            ),
                        )
                    ],
                ),
            ),
        ],
        documents=[],
        attributes={
            "deprecated_slug": "laws-and-policies-family-slug",
        },
    )
    assert_model_list_equality(
        family_result,
        [expected_document_from_family],
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
            created="2020-01-01T00:00:00Z",
            corpus=_cclw_corpus(),
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
                    document_status="created",
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
                    document_status="published",
                ),
            ],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="family-with-different-document-statuses-slug",
            metadata={},
        ),
    )
    output = transform_navigator_family(
        navigator_family_with_different_document_statuses
    ).unwrap()

    assert output.warnings == []
    assert output.collection_documents == []

    result = output.documents

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
                    type="region",
                    id="region::EAS",
                    value="East Asia & Pacific",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="country::AUS",
                    value="Australia",
                    type="country",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=Label(
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                    ],
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
                type="category",
                value=Label(
                    id="category::Law",
                    value="Law",
                    type="category",
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
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="country",
                                id="country::AUS",
                                value="Australia",
                                labels=[
                                    LabelRelationship(
                                        type="subconcept_of",
                                        value=Label(
                                            type="region",
                                            id="region::EAS",
                                            value="East Asia & Pacific",
                                        ),
                                    ),
                                ],
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
                            type="category",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
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
                        "status": "created",
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
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="country",
                                id="country::AUS",
                                value="Australia",
                                labels=[
                                    LabelRelationship(
                                        type="subconcept_of",
                                        value=Label(
                                            type="region",
                                            id="region::EAS",
                                            value="East Asia & Pacific",
                                        ),
                                    ),
                                ],
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
                            type="category",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
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
                        "status": "published",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "family-with-different-document-statuses-slug",
            "status": "published",
        },
    )
    assert_model_list_equality(
        result,
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
                            type="region",
                            id="region::EAS",
                            value="East Asia & Pacific",
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="country",
                            id="country::AUS",
                            value="Australia",
                            labels=[
                                LabelRelationship(
                                    type="subconcept_of",
                                    value=Label(
                                        type="region",
                                        id="region::EAS",
                                        value="East Asia & Pacific",
                                    ),
                                ),
                            ],
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
                        type="category",
                        value=Label(
                            id="category::Law",
                            value="Law",
                            type="category",
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
                    "status": "created",
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
                            type="region",
                            id="region::EAS",
                            value="East Asia & Pacific",
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="country",
                            id="country::AUS",
                            value="Australia",
                            labels=[
                                LabelRelationship(
                                    type="subconcept_of",
                                    value=Label(
                                        type="region",
                                        id="region::EAS",
                                        value="East Asia & Pacific",
                                    ),
                                ),
                            ],
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
                        type="category",
                        value=Label(
                            id="category::Law",
                            value="Law",
                            type="category",
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
                    "status": "published",
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
            created="2020-01-01T00:00:00Z",
            corpus=_cclw_corpus(),
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
                    document_status="deleted",
                ),
            ],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="family-with-no-published-documents-slug",
            metadata={},
        ),
    )

    output = transform_navigator_family(
        navigator_family_with_no_published_documents
    ).unwrap()

    assert output.warnings == []
    assert output.collection_documents == []

    result = output.documents

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
                    type="region",
                    id="region::EAS",
                    value="East Asia & Pacific",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="country::AUS",
                    value="Australia",
                    type="country",
                    labels=[
                        LabelRelationship(
                            type="subconcept_of",
                            value=Label(
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                    ],
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
                type="category",
                value=Label(
                    id="category::Law",
                    value="Law",
                    type="category",
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
                                type="region",
                                id="region::EAS",
                                value="East Asia & Pacific",
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="country",
                                id="country::AUS",
                                value="Australia",
                                labels=[
                                    LabelRelationship(
                                        type="subconcept_of",
                                        value=Label(
                                            type="region",
                                            id="region::EAS",
                                            value="East Asia & Pacific",
                                        ),
                                    ),
                                ],
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
                            type="category",
                            value=Label(
                                id="category::Law",
                                value="Law",
                                type="category",
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
                        "status": "deleted",
                    },
                ),
            ),
        ],
        attributes={
            "deprecated_slug": "family-with-no-published-documents-slug",
        },
    )
    assert_model_list_equality(
        result,
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
                            type="region",
                            id="region::EAS",
                            value="East Asia & Pacific",
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="country",
                            id="country::AUS",
                            value="Australia",
                            labels=[
                                LabelRelationship(
                                    type="subconcept_of",
                                    value=Label(
                                        type="region",
                                        id="region::EAS",
                                        value="East Asia & Pacific",
                                    ),
                                ),
                            ],
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
                        type="category",
                        value=Label(
                            id="category::Law",
                            value="Law",
                            type="category",
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
                    "status": "deleted",
                },
            ),
        ],
    )


@pytest.mark.parametrize(
    "corpus_id, expected_category",
    [
        ("UNFCCC.corpus.i00000001.n0000", "category::UN submission"),
        ("UN.corpus.UNCCD.n0000", "category::UN submission"),
        ("UN.corpus.UNCBD.n0000", "category::UN submission"),
        ("OEP.corpus.i00000001.n0000", "category::Report"),
        ("CPR.corpus.i00000002.n0000", "category::Corporate Disclosure"),
        ("Academic.corpus.Litigation.n0000", "category::Litigation"),
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
            created="2020-01-01T00:00:00Z",
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
            concepts=[],
        ),
    )

    output = transform_navigator_family(navigator_family).unwrap()

    assert output.warnings == []
    assert output.collection_documents == []

    result = output.documents
    family_doc = result[0]

    category_labels = [label for label in family_doc.labels if label.type == "category"]
    assert any(
        label.value.id == expected_category for label in category_labels
    ), f"Expected category '{expected_category}' not found in labels for corpus '{corpus_id}'"


def test_transform_navigator_family_and_document_with_domain_metadata(
    navigator_family_with_domain_metadata: Identified[NavigatorFamily],
):
    output = transform_navigator_family(navigator_family_with_domain_metadata).unwrap()

    assert output.warnings == []
    assert output.collection_documents == []

    domain_labels = [
        LabelRelationship(
            type="domain",
            value=Label(
                id="domain::Climate",
                type="domain",
                value="Climate",
            ),
        ),
        LabelRelationship(
            type="domain",
            value=Label(
                id="domain::Nature",
                type="domain",
                value="Nature",
            ),
        ),
    ]

    expected_document_from_family = Document(
        id="123",
        title="Capsule Corp family with domain metadata",
        description="Family summary",
        attributes={
            "deprecated_slug": "family-slug",
            "published_date": "2020-01-01T00:00:00Z",
            "last_updated_date": "2020-01-01T00:00:00Z",
            "status": "published",
        },
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="documentCapsuleCorp1",
                    title="Test document 1 of Climate Domain",
                    labels=[
                        *domain_labels,
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
                            type="language",
                            value=Label(
                                id="language::eng",
                                value="eng",
                                type="language",
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
                        "last_updated_date": "2020-01-01T00:00:00Z",
                        "published_date": "2020-01-01T00:00:00Z",
                        "status": "published",
                        "variant": "Original language",
                        "md5_sum": "TrunksIsTheBest",
                    },
                ),
            ),
        ],
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
            *domain_labels,
            LabelRelationship(
                type="deprecated_category",
                value=Label(
                    id="deprecated_category::UNFCCC",
                    value="UNFCCC",
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
        ],
    )

    assert_model_list_equality(
        output.documents,
        [
            expected_document_from_family,
            Document(
                id="documentCapsuleCorp1",
                title="Test document 1 of Climate Domain",
                labels=[
                    *domain_labels,
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
                        type="language",
                        value=Label(
                            id="language::eng",
                            value="eng",
                            type="language",
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
                    "deprecated_slug": "document-slug",
                    "status": "published",
                    "published_date": "2020-01-01T00:00:00Z",
                    "last_updated_date": "2020-01-01T00:00:00Z",
                    "variant": "Original language",
                    "md5_sum": "TrunksIsTheBest",
                },
            ),
        ],
    )


# region labels
def test_author_type_label_returns_label_for_non_stakeholder():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="UNFCCC.corpus.i00000001.n0000"),
        metadata={"author_type": ["Intergovernmental Organization"]},
    )
    labels = _author_type_label(family)
    assert len(labels) == 1
    assert labels[0].type == "author_type"
    assert labels[0].value.id == "author_type::Intergovernmental Organization"


def test_author_type_label_returns_empty_when_no_metadata():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="UNFCCC.corpus.i00000001.n0000"),
        metadata={},
    )
    assert _author_type_label(family) == []


@pytest.mark.parametrize(
    "corpus_import_id, expected_convention",
    [
        ("UNFCCC.corpus.i00000001.n0000", "UNFCCC"),
        ("UN.corpus.UNCCD.n0000", "UNCCD"),
        ("UN.corpus.UNCBD.n0000", "UNCBD"),
    ],
)
def test_un_convention_label_returns_label(corpus_import_id, expected_convention):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id=corpus_import_id),
        metadata={},
    )
    labels = _un_convention_label(family)
    assert len(labels) == 1
    assert labels[0].type == "un_convention"
    assert labels[0].value.id == f"un_convention::{expected_convention}"


def test_un_convention_label_returns_empty_for_non_un_corpus():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={},
    )
    assert _un_convention_label(family) == []


@pytest.mark.parametrize(
    "corpus_import_id, expected_fund",
    [
        ("MCF.corpus.AF.n0000", "Adaptation Fund"),
        ("MCF.corpus.CIF.n0000", "Climate Investment Funds"),
        ("MCF.corpus.GCF.n0000", "Global Environment Facility"),
        ("MCF.corpus.GEF.n0000", "Green Climate Fund"),
    ],
)
def test_multilateral_climate_fund_label_returns_label(corpus_import_id, expected_fund):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id=corpus_import_id),
        metadata={},
    )
    labels = _multilateral_climate_fund_label(family)
    assert len(labels) == 1
    assert labels[0].type == "multilateral_climate_fund"
    assert labels[0].value.id == f"agent::{expected_fund}"


def test_multilateral_climate_fund_label_returns_empty_for_non_mcf_corpus():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={},
    )
    assert _multilateral_climate_fund_label(family) == []


def test_author_label_returns_label():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="UNFCCC.corpus.i00000001.n0000"),
        metadata={"author": ["Greenpeace"]},
    )
    labels = _author_label(family)
    assert len(labels) == 1
    assert labels[0].type == "author"
    assert labels[0].value.id == "author::Greenpeace"


def test_author_label_returns_empty_when_no_metadata():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="UNFCCC.corpus.i00000001.n0000"),
        metadata={},
    )
    assert _author_label(family) == []


def test_category_label_returns_law_for_legislative():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        category="LEGISLATIVE",
    )
    labels = _category_label(family)
    assert len(labels) == 1
    assert labels[0].type == "category"
    assert labels[0].value.id == "category::Law"


def test_category_label_returns_policy_for_executive():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        category="EXECUTIVE",
    )
    labels = _category_label(family)
    assert len(labels) == 1
    assert labels[0].type == "category"
    assert labels[0].value.id == "category::Policy"


def test_category_label_returns_empty_for_laws_and_policies_with_no_matching_category():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        category="UNFCCC",
    )
    assert _category_label(family) == []


@pytest.mark.parametrize(
    "corpus_import_id, expected_category",
    [
        ("UNFCCC.corpus.i00000001.n0000", "UN submission"),
        ("OEP.corpus.i00000001.n0000", "Report"),
        ("Academic.corpus.Litigation.n0000", "Litigation"),
        ("CPR.corpus.i00000002.n0000", "Corporate Disclosure"),
    ],
)
def test_category_label_returns_category_for_non_laws_and_policies(
    corpus_import_id, expected_category
):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id=corpus_import_id),
    )
    labels = _category_label(family)
    assert len(labels) == 1
    assert labels[0].type == "category"
    assert labels[0].value.id == f"category::{expected_category}"


def test_deprecated_category_label_returns_label_for_laws_and_policies_corpus_type():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(
            import_id="CCLW.corpus.i00000001.n0000",
            corpus_type=NavigatorCorpusTypeFactory.build(name="Laws and Policies"),
        ),
    )
    labels = _deprecated_category_label(family)
    assert len(labels) == 1
    assert labels[0].type == "deprecated_category"
    assert labels[0].value.id == "deprecated_category::Laws and Policies"


def test_deprecated_category_label_returns_empty_for_non_laws_and_policies_corpus_type():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(
            import_id="UNFCCC.corpus.i00000001.n0000",
            corpus_type=NavigatorCorpusTypeFactory.build(name="UNFCCC"),
        ),
    )
    assert _deprecated_category_label(family) == []


@pytest.mark.parametrize(
    "corpus_import_id",
    [
        "MCF.corpus.AF.Guidance",
        "MCF.corpus.CIF.Guidance",
        "MCF.corpus.GCF.Guidance",
        "MCF.corpus.GEF.Guidance",
    ],
)
def test_entity_type_label_returns_guidance_for_mcf_guidance_corpus(corpus_import_id):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id=corpus_import_id),
    )
    labels = _entity_type_label(family)
    assert len(labels) == 1
    assert labels[0].type == "entity_type"
    assert labels[0].value.id == "entity_type::Guidance"


@pytest.mark.parametrize(
    "corpus_import_id",
    [
        "MCF.corpus.AF.n0000",
        "MCF.corpus.CIF.n0000",
        "MCF.corpus.GCF.n0000",
        "MCF.corpus.GEF.n0000",
    ],
)
def test_entity_type_label_returns_project_for_mcf_project_corpus(corpus_import_id):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id=corpus_import_id),
    )
    labels = _entity_type_label(family)
    assert len(labels) == 1
    assert labels[0].type == "entity_type"
    assert labels[0].value.id == "entity_type::Project"


def test_entity_type_label_returns_empty_for_non_mcf_corpus():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
    )
    assert _entity_type_label(family) == []


@pytest.mark.parametrize(
    "topic",
    ["Mitigation", "Adaptation", "Loss and Damage"],
)
def test_topic_label_returns_label_for_topic_metadata(topic):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"topic": [topic]},
    )
    labels = _topic_label(family)
    assert len(labels) == 1
    assert labels[0].type == "topic"
    assert labels[0].value.id == f"topic::{topic}"
    assert labels[0].value.value == topic
    assert labels[0].value.type == "topic"


def test_topic_label_includes_subconcept_of_law():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"topic": ["Mitigation"]},
    )
    labels = _topic_label(family)
    assert len(labels) == 1
    subconcept_labels = labels[0].value.labels
    assert len(subconcept_labels) == 1
    assert subconcept_labels[0].type == "subconcept_of"
    assert subconcept_labels[0].value.id == "category::Law"
    assert subconcept_labels[0].value.type == "category"
    assert subconcept_labels[0].value.value == "Law"


def test_topic_label_returns_empty_when_no_topic_key():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={},
    )
    assert _topic_label(family) == []


def test_topic_label_returns_empty_when_topic_is_empty_list():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"topic": []},
    )
    assert _topic_label(family) == []


def test_topic_label_returns_empty_when_topic_first_value_is_empty_string():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"topic": [""]},
    )
    assert _topic_label(family) == []


def test_topic_label_returns_law_type_label_for_mitigation_framework():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"framework": ["Mitigation"]},
    )
    labels = _topic_label(family)
    assert len(labels) == 1
    assert labels[0].type == "law_type"
    assert labels[0].value.id == "law_type::Mitigation"
    assert labels[0].value.value == "Framework law"


def test_topic_label_ignores_non_mitigation_framework_metadata():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"framework": ["Adaptation"]},
    )
    assert _topic_label(family) == []


@pytest.mark.parametrize("framework", ["Adaptation", "Mitigation", "Drm/Drr"])
def test_law_type_label_returns_label_for_framework_values(framework):
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"framework": [framework]},
    )
    labels = _law_type_label(family)
    assert len(labels) == 1
    assert labels[0].type == "law_type"
    assert labels[0].value.id == f"law_type::{framework}"
    assert labels[0].value.value == "Framework law"
    assert labels[0].value.type == "law_type"


def test_law_type_label_includes_subconcept_of_law():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"framework": ["Adaptation"]},
    )
    labels = _law_type_label(family)
    subconcept_labels = labels[0].value.labels
    assert len(subconcept_labels) == 1
    assert subconcept_labels[0].type == "subconcept_of"
    assert subconcept_labels[0].value.id == "category::Law"
    assert subconcept_labels[0].value.type == "category"


def test_law_type_label_returns_empty_when_no_framework_key():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={},
    )
    assert _law_type_label(family) == []


def test_law_type_label_returns_empty_for_non_matching_framework():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"framework": ["Other"]},
    )
    assert _law_type_label(family) == []


def test_law_type_label_returns_empty_when_framework_is_empty_list():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        metadata={"framework": []},
    )
    assert _law_type_label(family) == []


def test_status_label_returns_label_for_status_metadata():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"status": ["Under Implementation"]},
    )
    labels = _status_label(family)
    assert len(labels) == 1
    assert labels[0].type == "status"
    assert labels[0].value.id == "status::Under Implementation"
    assert labels[0].value.value == "Under Implementation"
    assert labels[0].value.type == "status"


def test_status_label_includes_subconcept_of_mcf_project():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"status": ["Under Implementation"]},
    )
    labels = _status_label(family)
    subconcept_labels = labels[0].value.labels
    assert len(subconcept_labels) == 1
    assert subconcept_labels[0].type == "subconcept_of"
    assert subconcept_labels[0].value.id == "entity_type::Project"
    assert subconcept_labels[0].value.type == "entity_type"


def test_status_label_returns_empty_when_no_status_key():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={},
    )
    assert _status_label(family) == []


def test_status_label_returns_empty_when_status_is_empty_list():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"status": []},
    )
    assert _status_label(family) == []


def test_status_label_returns_empty_when_status_first_value_is_empty_string():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"status": [""]},
    )
    assert _status_label(family) == []


def test_implementing_agency_label_returns_label():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"implementing_agency": ["World Bank"]},
    )
    labels = _implementing_agency_label(family)
    assert len(labels) == 1
    assert labels[0].type == "implementing_agency"
    assert labels[0].value.id == "agent::World Bank"
    assert labels[0].value.value == "World Bank"
    assert labels[0].value.type == "agent"


def test_implementing_agency_label_includes_subconcept_of_mcf_project():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"implementing_agency": ["World Bank"]},
    )
    labels = _implementing_agency_label(family)
    subconcept_labels = labels[0].value.labels
    assert len(subconcept_labels) == 1
    assert subconcept_labels[0].type == "subconcept_of"
    assert subconcept_labels[0].value.id == "entity_type::Project"
    assert subconcept_labels[0].value.type == "entity_type"


def test_implementing_agency_label_returns_empty_when_no_metadata():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={},
    )
    assert _implementing_agency_label(family) == []


def test_implementing_agency_label_returns_empty_when_empty_list():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"implementing_agency": []},
    )
    assert _implementing_agency_label(family) == []


def test_implementing_agency_label_returns_empty_when_empty_string():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="MCF.corpus.AF.n0000"),
        metadata={"implementing_agency": [""]},
    )
    assert _implementing_agency_label(family) == []


def test_report_type_label_returns_label_for_iccn_corpus():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="ICCN.corpus.i00000001.n0000"),
    )
    labels = _report_type_label(family)
    assert len(labels) == 1
    assert labels[0].type == "report_type"
    assert labels[0].value.id == "report_type::Climate council report"
    assert labels[0].value.value == "Climate council report"
    assert labels[0].value.type == "agent"


def test_report_type_label_includes_subconcept_of_report():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="ICCN.corpus.i00000001.n0000"),
    )
    labels = _report_type_label(family)
    subconcept_labels = labels[0].value.labels
    assert len(subconcept_labels) == 1
    assert subconcept_labels[0].type == "subconcept_of"
    assert subconcept_labels[0].value.id == "category::Report"
    assert subconcept_labels[0].value.type == "category"


def test_report_type_label_returns_empty_for_non_iccn_corpus():
    family = NavigatorFamilyFactory.build(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
    )
    assert _report_type_label(family) == []
