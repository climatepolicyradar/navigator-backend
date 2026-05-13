from datetime import datetime

from data_in_models.models import (
    Document,
    Label,
    LabelRelationship,
)

from app.transform.navigator_family import _transform_navigator_documents
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorDocumentFactory,
    NavigatorEventFactory,
    NavigatorFamilyFactory,
    NavigatorOrganisationFactory,
)
from tests.transform.assertions import assert_model_list_equality


def test_transform_navigator_documents_litigation_events_with_documents():
    litigation_event = NavigatorEventFactory.build(
        import_id="123",
        event_type="Decision",
        title="decision",
        date=datetime(2020, 1, 1),
        metadata={
            "event_type": ["Decision"],
            "datetime_event_name": ["Decision"],
            "action_taken": ["Action taken on decision"],
            "description": ["Summary of the filed document"],
        },
    )
    litigation_family = NavigatorFamilyFactory.build(
        import_id="family",
        title="Litigation family",
        category="LITIGATION",
        last_updated_date=None,
        published_date=None,
        created="2020-01-01T00:00:00Z",
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
        documents=[
            NavigatorDocumentFactory.build(
                import_id="document",
                title="Litigation family document",
                cdn_object=None,
                source_url=None,
                slug="litigation-document-slug",
                events=[litigation_event],
                variant="Original language",
                md5_sum="aaaaa11111bbbbb",
                languages=[],
                document_status="published",
            ),
        ],
        events=[litigation_event],
        collections=[],
        geographies=[],
        concepts=[],
    )

    result, warnings = _transform_navigator_documents(litigation_family)

    assert not warnings

    assert_model_list_equality(
        result,
        [
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
                        timestamp=datetime(2020, 1, 1, 0, 0),
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
                documents=[],
                attributes={
                    "deprecated_slug": "litigation-document-slug",
                    "md5_sum": "aaaaa11111bbbbb",
                    "variant": "Original language",
                    "status": "published",
                    "action_taken": "Action taken on decision",
                },
            ),
        ],
    )


def test_transform_navigator_documents_litigation_events_without_documents():
    litigation_family = NavigatorFamilyFactory.build(
        import_id="family",
        title="Litigation family",
        category="LITIGATION",
        last_updated_date=None,
        published_date=None,
        created="2020-01-01T00:00:00Z",
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
        events=[
            NavigatorEventFactory.build(
                import_id="123",
                event_type="Decision",
                title="decision",
                date=datetime(2020, 1, 1),
                metadata={
                    "event_type": ["Decision"],
                    "datetime_event_name": ["Decision"],
                    "action_taken": ["Action taken on decision"],
                    "description": ["Summary of the filed document"],
                },
            )
        ],
        collections=[],
        geographies=["AUS"],
        concepts=[],
    )

    result, warnings = _transform_navigator_documents(litigation_family)

    assert not warnings

    assert_model_list_equality(
        result,
        [
            Document(
                id="123",
                title="decision",
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
                        timestamp=datetime(2020, 1, 1, 0, 0),
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
                    LabelRelationship(
                        type="geography",
                        value=Label(
                            type="geography",
                            id="geography::AUS",
                            value="Australia",
                        ),
                    ),
                ],
                documents=[],
                attributes={
                    "action_taken": "Action taken on decision",
                    "status": "awaiting_source_file",
                },
            ),
        ],
    )


def test_transform_navigator_documents_litigation_filing_year_for_action_events_are_skipped():
    litigation_family = NavigatorFamilyFactory.build(
        import_id="family",
        title="Litigation family",
        category="LITIGATION",
        last_updated_date=None,
        published_date=None,
        created="2020-01-01T00:00:00Z",
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
        events=[
            NavigatorEventFactory.build(
                import_id="123",
                event_type="Filing Year For Action",
                title="Filing Year For Action",
                date=datetime(2020, 1, 1),
                metadata={
                    "event_type": ["Filing Year For Action"],
                    "datetime_event_name": ["Filing Year For Action"],
                    "action_taken": [],
                    "description": ["Filing Year For Action"],
                },
            )
        ],
        collections=[],
        geographies=[],
        concepts=[],
    )

    result, warnings = _transform_navigator_documents(litigation_family)

    assert not warnings
    assert not result
