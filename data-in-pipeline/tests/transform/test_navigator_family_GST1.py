import pytest
from data_in_models.models import (
    Document,
    DocumentRelationship,
    DocumentWithoutRelationships,
    Item,
    Label,
    LabelRelationship,
)

from app.models import Identified
from app.transform.navigator_family import (
    _part_of_global_stock_take_1,
    transform_navigator_family,
)
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorDocumentFactory,
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

    assert _part_of_global_stock_take_1(test_family) == expected_result


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
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="GST1 party document",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    language="eng",
                    languages=["eng"],
                    md5_sum="aaaaa11111bbbbb",
                    events=[],
                    valid_metadata={},
                    slug="document-slug",
                    document_status="published",
                ),
            ],
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
                type="process",
                value=Label(
                    id="process::GST1",
                    value="GST1 Submission",
                    type="process",
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
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="GST1 party document",
                    labels=[
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
                            type="process",
                            value=Label(
                                id="process::GST1",
                                value="GST1 Submission",
                                type="process",
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
                            type="category",
                            value=Label(
                                id="category::UN submission",
                                value="UN submission",
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
                        "deprecated_slug": "document-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
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
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document",
                title="GST1 party document",
                labels=[
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
                        type="process",
                        value=Label(
                            id="process::GST1",
                            value="GST1 Submission",
                            type="process",
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
                        type="category",
                        value=Label(
                            id="category::UN submission",
                            value="UN submission",
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
                    "deprecated_slug": "document-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "published",
                },
            ),
        ],
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
            documents=[
                NavigatorDocumentFactory.build(
                    import_id="document",
                    title="GST1 party document",
                    cdn_object="https://cdn.climatepolicyradar.org/path/to/file.pdf",
                    variant="Original language",
                    content_type="application/pdf",
                    source_url="https://source.climatepolicyradar.org/path/to/file.pdf",
                    language="eng",
                    languages=["eng"],
                    md5_sum="aaaaa11111bbbbb",
                    events=[],
                    valid_metadata={},
                    slug="document-slug",
                    document_status="published",
                ),
            ],
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
                type="process",
                value=Label(
                    id="process::GST1",
                    value="GST1 Submission",
                    type="process",
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
        documents=[
            DocumentRelationship(
                type="has_member",
                value=DocumentWithoutRelationships(
                    id="document",
                    title="GST1 party document",
                    labels=[
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
                            type="process",
                            value=Label(
                                id="process::GST1",
                                value="GST1 Submission",
                                type="process",
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
                            type="category",
                            value=Label(
                                id="category::UN submission",
                                value="UN submission",
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
                        "deprecated_slug": "document-slug",
                        "variant": "Original language",
                        "md5_sum": "aaaaa11111bbbbb",
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
        result.unwrap(),
        [
            expected_document_from_family,
            Document(
                id="document",
                title="GST1 party document",
                labels=[
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
                        type="process",
                        value=Label(
                            id="process::GST1",
                            value="GST1 Submission",
                            type="process",
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
                        type="category",
                        value=Label(
                            id="category::UN submission",
                            value="UN submission",
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
                    "deprecated_slug": "document-slug",
                    "variant": "Original language",
                    "md5_sum": "aaaaa11111bbbbb",
                    "status": "published",
                },
            ),
        ],
    )
