import pytest

from app.api.api_v1.schemas.document import FamilyDocumentResponse
from app.api.api_v1.schemas.search import (
    OpenSearchResponsePassageMatch,
    SearchResponseDocumentPassage,
    SearchResponseFamilyDocument,
    OpenSearchResponseMatchBase,
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
        document_type=None,
        document_role=None,
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
        document_type=None,
        document_role=None,
    )
    assert document_response.source_url == given_url


def test_search_responses() -> None:
    """
    Test that instantiating Search Response objects is done correctly.

    Particularly testing of the validators.
    """
    original_block_page = 0

    original_block_data = {
        "text": "example text",
        "text_block_id": "p_0_b_0",
        "text_block_page": original_block_page,
        "text_block_coords": None,
    }

    base_response_data = {
        "document_name": "Sample Document",
        "document_geography": "USA",
        "document_description": "This is a sample document description.",
        "document_sectors": ["Technology", "Healthcare"],
        "document_source": "Sample Source",
        "document_id": "sample_import_id_123",
        "document_date": "2023-11-22",
        "document_type": "PDF",
        "document_source_url": "https://example.com/sample_document",
        "document_cdn_object": "sample_cdn_object_reference",
        "document_category": "Sample Category",
        "document_content_type": "application/pdf",
        "document_slug": "sample-document",
    }

    # This is used for vespa responses
    default_passage_response = SearchResponseDocumentPassage.parse_obj(
        original_block_data
    )

    assert default_passage_response.text_block_page == original_block_page + 1

    response_base = OpenSearchResponseMatchBase.parse_obj(base_response_data)

    opensearch_passage_response = OpenSearchResponsePassageMatch(
        **response_base.dict(), **original_block_data
    )

    assert opensearch_passage_response.text_block_page == original_block_page + 1

    assert opensearch_passage_response.text_block_page == (
        default_passage_response.text_block_page
    )
