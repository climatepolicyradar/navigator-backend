"""RDS row timestamps (`created`, `last_modified`) are passed through as-is.

These are internal "when did this enter / change in our system" timestamps,
distinct from the event-derived `published_date` / `last_updated_date`.
"""

from app.transform.navigator_family import (
    _rds_timestamp_attributes,
    _transform_navigator_collection,
    _transform_navigator_document,
    _transform_navigator_family,
)
from tests.factories import (
    NavigatorCollectionFactory,
    NavigatorCorpusFactory,
    NavigatorDocumentFactory,
    NavigatorFamilyFactory,
)


def _simple_family(**overrides):
    defaults = dict(
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
        documents=[],
        events=[],
        collections=[],
        concepts=[],
        metadata={},
        geographies=[],
        category="Legislative",
    )
    return NavigatorFamilyFactory.build(**{**defaults, **overrides})


def test_rds_timestamp_attributes_passes_values_through_unchanged():
    assert _rds_timestamp_attributes(
        created="2021-03-04T05:06:07.123456", last_modified="2022-01-01T00:00:00Z"
    ) == {
        "created": "2021-03-04T05:06:07.123456",
        "last_modified": "2022-01-01T00:00:00Z",
    }


def test_rds_timestamp_attributes_omits_missing_values():
    assert _rds_timestamp_attributes(
        created="2021-01-01T00:00:00Z", last_modified=None
    ) == {"created": "2021-01-01T00:00:00Z"}


def test_family_attributes_include_rds_timestamps():
    family = _simple_family(
        created="2021-01-01T00:00:00Z", last_modified="2022-01-01T00:00:00Z"
    )

    document, _ = _transform_navigator_family(family)

    assert document.attributes["created"] == "2021-01-01T00:00:00Z"
    assert document.attributes["last_modified"] == "2022-01-01T00:00:00Z"


def test_family_attributes_omit_last_modified_when_not_provided():
    family = _simple_family(last_modified=None)

    document, _ = _transform_navigator_family(family)

    assert "last_modified" not in document.attributes


def test_document_attributes_use_the_documents_own_rds_timestamps():
    navigator_document = NavigatorDocumentFactory.build(
        created="2023-01-01T00:00:00Z",
        last_modified="2024-01-01T00:00:00Z",
        events=[],
        valid_metadata={},
    )
    family = _simple_family(
        created="2001-01-01T00:00:00Z",
        last_modified="2002-01-01T00:00:00Z",
        documents=[navigator_document],
    )

    document, _ = _transform_navigator_document(navigator_document, family)

    assert document.attributes["created"] == "2023-01-01T00:00:00Z"
    assert document.attributes["last_modified"] == "2024-01-01T00:00:00Z"


def test_collection_attributes_include_rds_timestamps():
    collection = NavigatorCollectionFactory.build(
        created="2021-01-01T00:00:00Z", last_modified="2022-01-01T00:00:00Z"
    )

    document = _transform_navigator_collection(collection)

    assert document.attributes["created"] == "2021-01-01T00:00:00Z"
    assert document.attributes["last_modified"] == "2022-01-01T00:00:00Z"


def test_collection_attributes_keep_existing_date_fields_for_backwards_compatibility():
    collection = NavigatorCollectionFactory.build(
        created="2021-01-01T00:00:00Z", last_modified="2022-01-01T00:00:00Z"
    )

    document = _transform_navigator_collection(collection)

    assert document.attributes["published_date"] == "2021-01-01T00:00:00Z"
    assert document.attributes["last_updated_date"] == "2022-01-01T00:00:00Z"
