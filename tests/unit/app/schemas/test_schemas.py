from datetime import datetime

import pytest

from app.api.api_v1.schemas.document import FamilyDocumentResponse
from app.api.api_v1.schemas.search import (
    SearchResponse,
    SearchResponseDocumentPassage,
    SearchResponseFamily,
    SearchResponseFamilyDocument,
)

CLIMATE_LAWS_DOMAIN_PATHS = [
    "climate-laws.org",
    "climate-laws.org/path",
    "climate-laws.org/path/multiple/elements/",
    "anything.climate-laws.org/",
    "anything.climate-laws.org/path/",
    "anything.climate-laws.org/path/multiple/elemeents",
]
NON_CLIMATE_LAWS_DOMAIN_PATHS = [
    None,
    "",
    "not-climate-laws.org/",
    "sub.not-climate-laws.org",
    "example.com/path",
    "not-climate-laws.org/path/multiple/elements/",
]
SCHEMES = ["http", "https"]


@pytest.mark.parametrize("source_domain_path", CLIMATE_LAWS_DOMAIN_PATHS)
@pytest.mark.parametrize("scheme", SCHEMES)
def test_climate_laws_source_url_filtered_from_search(source_domain_path, scheme):
    search_document = SearchResponseFamilyDocument(
        document_title="title",
        document_slug="title_abcd",
        document_type="document_type",
        document_source_url=f"{scheme}://{source_domain_path}",
        document_url="https://cdn.climatepolicyradar.org/title_hash",
        document_content_type=None,
        document_passage_matches=[],
    )
    assert search_document.document_source_url is None


@pytest.mark.parametrize("source_domain_path", NON_CLIMATE_LAWS_DOMAIN_PATHS)
@pytest.mark.parametrize("scheme", SCHEMES)
def test_non_climate_laws_source_url_left_in_search(source_domain_path, scheme):
    if source_domain_path:
        given_url = f"{scheme}://{source_domain_path}"
    else:
        given_url = source_domain_path
    search_document = SearchResponseFamilyDocument(
        document_title="title",
        document_slug="title_abcd",
        document_type="document_type",
        document_source_url=given_url,
        document_url="https://cdn.climatepolicyradar.org/title_hash",
        document_content_type=None,
        document_passage_matches=[],
    )
    assert search_document.document_source_url == given_url


@pytest.mark.parametrize("source_domain_path", CLIMATE_LAWS_DOMAIN_PATHS)
@pytest.mark.parametrize("scheme", SCHEMES)
def test_climate_laws_source_url_filtered_from_document(source_domain_path, scheme):
    document_response = FamilyDocumentResponse(
        import_id="import_id",
        variant=None,
        slug="import_id_abcd",
        title="title",
        md5_sum=None,
        cdn_object=None,
        source_url=f"{scheme}://{source_domain_path}",
        content_type=None,
        language="",
        languages=[],
        document_type="Law",
        document_role="MAIN",
    )
    assert document_response.source_url is None


@pytest.mark.parametrize("source_domain_path", NON_CLIMATE_LAWS_DOMAIN_PATHS)
@pytest.mark.parametrize("scheme", SCHEMES)
def test_non_climate_laws_source_url_left_in_document(source_domain_path, scheme):
    if source_domain_path:
        given_url = f"{scheme}://{source_domain_path}"
    else:
        given_url = source_domain_path
    document_response = FamilyDocumentResponse(
        import_id="import_id",
        variant=None,
        slug="import_id_abcd",
        title="title",
        md5_sum=None,
        cdn_object=None,
        source_url=given_url,
        content_type=None,
        language="",
        languages=[],
        document_type="Law",
        document_role="MAIN",
    )
    assert document_response.source_url == given_url


def test_search_response() -> None:
    """
    Test that instantiating Search Response objects is done correctly.

    Particularly testing of the validators.
    """
    search_response = SearchResponse(
        hits=1,
        total_family_hits=1,
        query_time_ms=1,
        total_time_ms=1,
        families=[
            SearchResponseFamily(
                family_slug="example_slug",
                family_name="Example Family",
                family_description="This is an example family",
                family_category="Example Category",
                family_date=str(
                    datetime.now()
                ),  # You can replace this with an actual date string
                family_last_updated_date=str(
                    datetime.now()
                ),  # You can replace this with an actual date string
                family_source="Example Source",
                family_geography="Example Geography",
                family_geographies=["Example Geography"],
                family_metadata={"key1": "value1", "key2": "value2"},
                corpus_import_id="test.corpus.0.1",
                corpus_type_name="Example Corpus Type",
                family_title_match=True,
                family_description_match=False,
                total_passage_hits=1,
                family_documents=[
                    SearchResponseFamilyDocument(
                        document_title="Document Title",
                        document_slug="Document Slug",
                        document_type="Executive",
                        document_source_url="https://cdn.example.com/file.pdf",
                        document_url=None,
                        document_content_type="application/pdf",
                        document_passage_matches=[
                            SearchResponseDocumentPassage(
                                text="Example",
                                text_block_id="p_0_b_0",
                                text_block_page=0,
                                text_block_coords=None,
                            ),
                            SearchResponseDocumentPassage(
                                text="Example",
                                text_block_id="p_1_b_0",
                                text_block_page=1,
                                text_block_coords=None,
                            ),
                            SearchResponseDocumentPassage(
                                text="Example",
                                text_block_id="p_1_b_2",
                                text_block_page=1,
                                text_block_coords=None,
                            ),
                        ],
                    )
                ],
            )
        ],
    )

    first_document_initial_pages = [
        page.text_block_page
        for page in search_response.families[0]
        .family_documents[0]
        .document_passage_matches
    ]

    search_response_incremented = search_response.increment_pages()

    first_document_incremented_pages = [
        page.text_block_page
        for page in search_response_incremented.families[0]
        .family_documents[0]
        .document_passage_matches
    ]

    assert len(first_document_initial_pages) == len(first_document_incremented_pages)

    assert first_document_initial_pages != first_document_incremented_pages

    expected_pages = []
    for page in first_document_initial_pages:
        if page is None:
            expected_pages.append(page)
        else:
            expected_pages.append(page + 1)

    assert expected_pages == first_document_incremented_pages
