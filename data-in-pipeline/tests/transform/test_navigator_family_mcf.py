import datetime

import pytest
from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Label,
    LabelRelationship,
)

from app.extract.connectors import (
    NavigatorFamily,
)
from app.models import Identified
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
