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
            category="REPORTS",
            corpus=_cclw_corpus(),
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
                ),
                NavigatorCollectionFactory.build(
                    import_id="collection",
                    title="Collection title",
                    description="Collection description",
                ),
            ],
            geographies=["AU-NSW", "AUS", "XAA"],
            metadata={
                "case_number": ["CASE-NUMBER 123"],
                "core_object": ["Core Object 123"],
                "project_value_fund_spend": ["123456789"],
                "project_value_co_financing": ["123456789"],
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
            category="REPORTS",
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
            category="REPORTS",
            last_updated_date="2020-01-0100:00:00Z",
            published_date="2020-01-0100:00:00Z",
            corpus=NavigatorCorpusFactory.build(
                import_id="Academic.corpus.Litigation.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Litigation"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
                attribution_url="testurl.org",
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
            metadata={"case_number": ["CASE-NUMBER 123"]},
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
            category="REPORTS",
            last_updated_date=None,
            published_date=None,
            corpus=NavigatorCorpusFactory.build(
                import_id="MCF.corpus.AF.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="AF"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="CCLW"),
                attribution_url="testurl.org",
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
        attribution_url="testurl.org",
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
                    id="Amended/Updated",
                    value="Amended/Updated",
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
                    type="geography",
                    id="AU-NSW",
                    value="New South Wales",
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    type="geography",
                    id="AUS",
                    value="Australia",
                ),
            ),
            LabelRelationship(
                type="author",
                value=Label(
                    id="Test Author",
                    value="Test Author",
                    type="person",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="Guidance",
                    value="Guidance",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="case_number",
                value=Label(
                    documents=[],
                    id="CASE-NUMBER 123",
                    labels=[],
                    type="case_number",
                    value="CASE-NUMBER 123",
                ),
            ),
            LabelRelationship(
                type="core_object",
                value=Label(
                    documents=[],
                    id="Core Object 123",
                    labels=[],
                    type="core_object",
                    value="Core Object 123",
                ),
            ),
            LabelRelationship(
                type="project_value_fund_spend",
                value=Label(
                    documents=[],
                    id="123456789",
                    labels=[],
                    type="project_value_fund_spend",
                    value="123456789",
                ),
            ),
            LabelRelationship(
                type="project_value_co_financing",
                value=Label(
                    documents=[],
                    id="123456789",
                    labels=[],
                    type="project_value_co_financing",
                    value="123456789",
                ),
            ),
            LabelRelationship(
                type="author_type",
                value=Label(
                    documents=[],
                    id="Person",
                    labels=[],
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
                    attribution_url="testurl.org",
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
                    attribution_url="testurl.org",
                    labels=[
                        LabelRelationship(
                            type="role",
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
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="AU-NSW",
                                value="New South Wales",
                            ),
                        ),
                        LabelRelationship(
                            type="geography",
                            value=Label(
                                type="geography",
                                id="AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="eng",
                                value="eng",
                                type="language",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="fra",
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
                    attribution_url="testurl.org",
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
        attributes={
            "deprecated_slug": "family-slug",
            "identifier::case_number": "CASE-NUMBER 123",
            "project_fund_spend_usd": 123456789,
            "project_co_financing_usd": 123456789,
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
                attribution_url="testurl.org",
                labels=[
                    LabelRelationship(
                        type="role",
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
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="AU-NSW",
                            value="New South Wales",
                        ),
                    ),
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="eng",
                            value="eng",
                            type="language",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="fra",
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
                attribution_url="testurl.org",
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
                attribution_url="testurl.org",
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
                corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
                organisation=NavigatorOrganisationFactory.build(id=1, name=org),
                attribution_url="testurl.org",
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
        attribution_url="testurl.org",
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
                type="provider",
                value=Label(
                    type="agent",
                    id=provider,
                    value=provider,
                ),
            ),
            LabelRelationship(
                type="geography",
                value=Label(
                    id="AUS",
                    value="Australia",
                    type="geography",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="Legislative",
                    value="Legislative",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="topic",
                value=Label(
                    id="Mitigation",
                    value="Mitigation",
                    type="topic",
                ),
            ),
            LabelRelationship(
                type="sector",
                value=Label(
                    id="Economy-wide",
                    value="Economy-wide",
                    type="sector",
                ),
            ),
            LabelRelationship(
                type="keyword",
                value=Label(
                    id="Transport",
                    value="Transport",
                    type="keyword",
                ),
            ),
            LabelRelationship(
                type="framework",
                value=Label(
                    id="Mitigation",
                    value="Mitigation",
                    type="framework",
                ),
            ),
            LabelRelationship(
                type="instrument",
                value=Label(
                    id="Processes, plans and strategies|Governance",
                    value="Processes, plans and strategies|Governance",
                    type="instrument",
                ),
            ),
            LabelRelationship(
                type="instrument",
                value=Label(
                    id="Planning|Governance",
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
        attribution_url="testurl.org",
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
                type="category",
                value=Label(
                    id="Guidance",
                    value="Guidance",
                    type="category",
                ),
            ),
        ],
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="Litigation family document",
                    attribution_url="testurl.org",
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
                    attribution_url="testurl.org",
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
                attribution_url="testurl.org",
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
                attribution_url="testurl.org",
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
                attributes={
                    "deprecated_slug": "placeholder-document-slug",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "PUBLISHED",
                },
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
        attribution_url="testurl.org",
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
                type="entity_type",
                value=Label(
                    id="Project",
                    value="Project",
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
                type="category",
                value=Label(
                    id="Guidance",
                    value="Guidance",
                    type="category",
                ),
            ),
            LabelRelationship(
                type="sector",
                value=Label(
                    id="Public Sector",
                    type="sector",
                    value="Public Sector",
                ),
            ),
            LabelRelationship(
                type="project_status",
                value=Label(
                    id="Under Implementation",
                    type="project_status",
                    value="Under Implementation",
                ),
            ),
            LabelRelationship(
                type="implementing_agency",
                value=Label(
                    id="International Bank for Reconstruction",
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
                    attribution_url="testurl.org",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Adaptation Fund",
                                value="Adaptation Fund",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="eng",
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
                    attribution_url="testurl.org",
                    labels=[
                        LabelRelationship(
                            type="provider",
                            value=Label(
                                type="agent",
                                id="Adaptation Fund",
                                value="Adaptation Fund",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="eng",
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
                attribution_url="testurl.org",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Adaptation Fund",
                            value="Adaptation Fund",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="eng",
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
                attribution_url="testurl.org",
                labels=[
                    LabelRelationship(
                        type="provider",
                        value=Label(
                            type="agent",
                            id="Adaptation Fund",
                            value="Adaptation Fund",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="eng",
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
        attribution_url="testurl.org",
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
                    id="AUS",
                    value="Australia",
                    type="geography",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="Legislative",
                    value="Legislative",
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
                    attribution_url="testurl.org",
                    labels=[
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
                                type="geography",
                                id="AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="eng",
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
                    attribution_url="testurl.org",
                    labels=[
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
                                type="geography",
                                id="AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="eng",
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
                attribution_url="testurl.org",
                labels=[
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
                            type="geography",
                            id="AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="eng",
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
                attribution_url="testurl.org",
                labels=[
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
                            type="geography",
                            id="AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="eng",
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
        attribution_url="testurl.org",
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
                    id="AUS",
                    value="Australia",
                    type="geography",
                ),
            ),
            LabelRelationship(
                type="category",
                value=Label(
                    id="Legislative",
                    value="Legislative",
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
                    attribution_url="testurl.org",
                    labels=[
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
                                type="geography",
                                id="AUS",
                                value="Australia",
                            ),
                        ),
                        LabelRelationship(
                            type="language",
                            value=Label(
                                id="eng",
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
                attribution_url="testurl.org",
                labels=[
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
                            type="geography",
                            id="AUS",
                            value="Australia",
                        ),
                    ),
                    LabelRelationship(
                        type="language",
                        value=Label(
                            id="eng",
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
                attribution_url="testurl.org",
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
                        type="entity_type",
                        value=Label(
                            id="Guidance",
                            value="Guidance",
                            type="entity_type",
                        ),
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
                        type="category",
                        value=Label(
                            id="Guidance",
                            value="Guidance",
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
