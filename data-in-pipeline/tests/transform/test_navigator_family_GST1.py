import pytest
from data_in_models.models import (
    Document,
    Label,
    LabelRelationship,
)

from app.models import Identified
from app.transform.navigator_family import _part_of_gst1, transform_navigator_family
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorFamilyFactory,
    NavigatorOrganisationFactory,
)
from tests.transform.assertions import assert_model_list_equality


@pytest.mark.parametrize(
    "corpus_id, author_type, created_date, expected_result",
    [
        # valid cases
        ("UNFCCC.corpus.i00000001.n0000", "Party", "2023-11-30T00:00:00Z", True),
        ("UNFCCC.corpus.i00000001.n0000", "Non-Party", "2023-11-30T00:00:00Z", True),
        ("UNFCCC.corpus.i00000001.n0000", "Non-Party", "2024-10-15T00:00:00Z", True),
        ("UNFCCC.corpus.i00000001.n0000", "Non-Party", "2024-10-17T00:00:00Z", True),
        ("UNFCCC.corpus.i00000001.n0000", "Non-Party", "2024-11-06T00:00:00Z", True),
        ("UNFCCC.corpus.i00000001.n0000", "Non-Party", "2024-11-15T00:00:00Z", True),
        ("UNFCCC.corpus.i00000001.n0000", "Non-Party", "2024-11-18T00:00:00Z", True),
        #
        # invalid cases - Party
        (
            "Invalid.corpus.i00000001.n0000",
            "Party",
            "2023-11-30T00:00:00Z",
            False,
        ),  # wrong corpus
        (
            "UNFCCC.corpus.i00000001.n0000",
            "Invalid",
            "2023-11-30T00:00:00Z",
            False,
        ),  # wrong author type
        (
            "UNFCCC.corpus.i00000001.n0000",
            "Party",
            "2026-01-01T00:00:00Z",
            False,
        ),  # wrong date
        #
        # invalid cases - Non-Party
        (
            "Invalid.corpus.i00000001.n0000",
            "Non-Party",
            "2023-11-30T00:00:00Z",
            False,
        ),  # wrong corpus
        (
            "UNFCCC.corpus.i00000001.n0000",
            "Non-Party",
            "2026-01-01T00:00:00Z",
            False,
        ),  # wrong date
    ],
)
def test_part_of_gst1_correctly_assesses_if_family_is_part_of_global_stocktake(
    corpus_id, author_type, created_date, expected_result
):
    test_family = NavigatorFamilyFactory.build(
        import_id="family",
        title="GST1 party family",
        summary="Family summary",
        category="EXECUTIVE",
        published_date=None,
        last_updated_date=None,
        created=created_date,
        corpus=NavigatorCorpusFactory.build(
            import_id=corpus_id,
            corpus_type=NavigatorCorpusTypeFactory.build(name="Intl. agreements"),
            organisation=NavigatorOrganisationFactory.build(id=1, name="UNFCCC"),
            attribution_url="testurl.org",
            corpus_text="Test corpus",
            corpus_image_url="corpus_image.png",
        ),
        documents=[],
        events=[],
        collections=[],
        geographies=["AUS"],
        slug="family-with-different-document-statuses-slug",
        metadata={
            "author": ["Australia"],
            "author_type": [author_type],
        },
    )

    assert _part_of_gst1(test_family) == expected_result


def test_transform_navigator_family_UNFCCC_party_submission_to_GST1_label():
    navigator_family_with_GST1_documents = Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="GST1 party family",
            summary="Family summary",
            category="EXECUTIVE",
            published_date=None,
            last_updated_date=None,
            created="2023-11-30T00:00:00Z",
            corpus=NavigatorCorpusFactory.build(
                import_id="UNFCCC.corpus.i00000001.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Intl. agreements"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="UNFCCC"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url="corpus_image.png",
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="family-with-different-document-statuses-slug",
            metadata={
                "author": ["Australia"],
                "author_type": ["Party"],
            },
        ),
    )
    result = transform_navigator_family(navigator_family_with_GST1_documents)
    expected_document_from_family = Document(
        id="family",
        title="GST1 party family",
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
                    id="entity_type::GST1",
                    value="GST1 Submission",
                    type="entity_type",
                ),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="agent::UNFCCC",
                    value="UNFCCC",
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
                type="author",
                value=Label(
                    id="party::Australia",
                    value="Australia",
                    type="party",
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
                type="category",
                value=Label(
                    id="category::UN submission",
                    value="UN submission",
                    type="category",
                ),
            ),
        ],
        documents=[],
        attributes={
            "deprecated_slug": "family-with-different-document-statuses-slug",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [expected_document_from_family],
    )


def test_transform_navigator_family_UNFCCC_non_party_submission_to_GST1_label():
    navigator_family_with_GST1_documents = Identified(
        id="family",
        source="navigator_family",
        data=NavigatorFamilyFactory.build(
            import_id="family",
            title="GST1 party family",
            summary="Family summary",
            category="EXECUTIVE",
            published_date=None,
            last_updated_date=None,
            created="2024-10-17T00:00:00Z",
            corpus=NavigatorCorpusFactory.build(
                import_id="UNFCCC.corpus.i00000001.n0000",
                corpus_type=NavigatorCorpusTypeFactory.build(name="Intl. agreements"),
                organisation=NavigatorOrganisationFactory.build(id=1, name="UNFCCC"),
                attribution_url="testurl.org",
                corpus_text="Test corpus",
                corpus_image_url="corpus_image.png",
            ),
            documents=[],
            events=[],
            collections=[],
            geographies=["AUS"],
            slug="family-with-different-document-statuses-slug",
            metadata={
                "author": ["Australia"],
                "author_type": ["Non-Party"],
            },
        ),
    )
    result = transform_navigator_family(navigator_family_with_GST1_documents)
    expected_document_from_family = Document(
        id="family",
        title="GST1 party family",
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
                    id="entity_type::GST1",
                    value="GST1 Submission",
                    type="entity_type",
                ),
            ),
            LabelRelationship(
                type="provider",
                value=Label(
                    type="agent",
                    id="agent::UNFCCC",
                    value="UNFCCC",
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
                type="author",
                value=Label(
                    id="non-party::Australia",
                    value="Australia",
                    type="non-party",
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
                type="category",
                value=Label(
                    id="category::UN submission",
                    value="UN submission",
                    type="category",
                ),
            ),
        ],
        documents=[],
        attributes={
            "deprecated_slug": "family-with-different-document-statuses-slug",
        },
    )
    assert_model_list_equality(
        result.unwrap(),
        [expected_document_from_family],
    )
