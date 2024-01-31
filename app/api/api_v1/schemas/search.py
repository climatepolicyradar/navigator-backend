from enum import Enum
from typing import List, Mapping, Optional, Sequence

from pydantic import field_validator, Field, BaseModel

from db_client.models.law_policy import FamilyCategory
from . import CLIMATE_LAWS_MATCH
from typing_extensions import Annotated


Coord = tuple[float, float]


class SortOrder(str, Enum):
    """Sort ordering for use building OpenSearch query body."""

    ASCENDING = "asc"
    DESCENDING = "desc"


class SortField(str, Enum):
    """Sort field for use building OpenSearch query body."""

    DATE = "date"
    TITLE = "title"


class JitQuery(str, Enum):
    """Flag used for determining if a jit query is to be used."""

    ENABLED = "enabled"
    DISABLED = "disabled"


class FilterField(str, Enum):
    """Filter field for use building OpenSearch query body."""

    SOURCE = "sources"
    COUNTRY = "countries"
    REGION = "regions"
    CATEGORY = "categories"
    LANGUAGE = "languages"


class IncludedResults(str, Enum):
    """Filter field to exclude specific results from search based on search indices."""

    PDFS_TRANSLATED = "pdfsTranslated"
    HTMLS_NON_TRANSLATED = "htmlsNonTranslated"
    HTMLS_TRANSLATED = "htmlsTranslated"


IncludedResultsList = Optional[Annotated[List[IncludedResults], Field(min_length=1)]]


class SearchRequestBody(BaseModel):
    """The request body expected by the search API endpoint."""

    query_string: str
    exact_match: bool = False
    max_passages_per_doc: int = 10  # TODO: decide on default

    # TODO: Improve filters to allow generics & use filter types
    keyword_filters: Optional[Mapping[FilterField, Sequence[str]]] = None
    year_range: Optional[tuple[Optional[int], Optional[int]]] = None

    sort_field: Optional[SortField] = None
    sort_order: SortOrder = SortOrder.DESCENDING

    include_results: IncludedResultsList = None

    limit: int = 10  # TODO: decide on default
    offset: int = 0

    continuation_token: Optional[str] = None


class SearchResponseDocumentPassage(BaseModel):
    """A Document passage match returned by the search API endpoint."""

    text: str
    text_block_id: str
    text_block_page: Optional[int] = None
    text_block_coords: Optional[Sequence[Coord]] = None


class OpenSearchResponseMatchBase(BaseModel):
    """Describes matches returned by an OpenSearch query"""

    document_name: str
    document_geography: str
    document_description: str
    document_sectors: Sequence[str]
    document_source: str
    document_id: str  # Changed semantics to be import_id, not database id
    document_date: str
    document_type: str
    document_source_url: Optional[str] = None
    document_cdn_object: Optional[str] = None
    document_category: str
    document_content_type: Optional[str] = None
    document_slug: str


class OpenSearchResponseNameMatch(OpenSearchResponseMatchBase):
    """Describes matches returned by OpenSearch on Document name."""

    for_search_document_name: str


class OpenSearchResponseDescriptionMatch(OpenSearchResponseMatchBase):
    """Describes matches returned by OpenSearch on Document description."""

    for_search_document_description: str


class OpenSearchResponsePassageMatch(OpenSearchResponseMatchBase):
    """Describes matches returned by OpenSearch on Document passage."""

    text: str
    text_block_id: str
    text_block_page: Optional[int] = None
    text_block_coords: Optional[Sequence[Coord]] = None


class SearchResponseFamilyDocument(BaseModel):
    """A single document in a search response."""

    document_title: str
    document_slug: str
    document_type: str
    document_source_url: Optional[str] = None
    document_url: Optional[str] = None
    document_content_type: Optional[str] = None
    document_passage_matches: list[SearchResponseDocumentPassage]

    @field_validator("document_source_url")
    @classmethod
    def _filter_climate_laws_url_from_source(cls, v):
        """Make sure we do not return climate-laws.org source URLs to the frontend"""
        if v is None or CLIMATE_LAWS_MATCH.match(v) is not None:
            return None
        return v


class SearchResponseFamily(BaseModel):
    """
    The object that is returned in the response.

    Used to extend with postfix
    """

    family_slug: str
    family_name: str
    family_description: str
    family_category: str
    family_date: str
    family_last_updated_date: str
    family_source: str
    family_geography: str
    family_metadata: dict
    family_title_match: bool
    family_description_match: bool
    family_documents: list[SearchResponseFamilyDocument]


class SearchResponse(BaseModel):
    """The response body produced by the search API endpoint."""

    hits: int
    query_time_ms: int
    total_time_ms: int
    continuation_token: Optional[str] = None

    families: Sequence[SearchResponseFamily]

    def increment_pages(self):
        """PDF page numbers must be incremented from our 0-indexed values."""
        for family_index, family in enumerate(self.families):
            for family_document_index, family_document in enumerate(
                family.family_documents
            ):
                for passage_match_index, passage_match in enumerate(
                    family_document.document_passage_matches
                ):
                    if (
                        passage_match.text_block_page
                        or passage_match.text_block_page == 0
                    ):
                        self.families[family_index].family_documents[
                            family_document_index
                        ].document_passage_matches[
                            passage_match_index
                        ].text_block_page += 1  # type: ignore
        return self


Top5FamilyList = Annotated[List[SearchResponseFamily], Field(max_length=5)]
# Alias required for type hinting
_T5FamL = Top5FamilyList


class GeographySummaryFamilyResponse(BaseModel):
    """Additional information for the geography page over geo stats"""

    family_counts: Mapping[FamilyCategory, int]
    top_families: Mapping[FamilyCategory, _T5FamL]
    targets: Sequence[str]  # TODO: Placeholder for later
