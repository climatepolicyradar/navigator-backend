from enum import Enum
from typing import List, Literal, Mapping, Optional, Sequence

from cpr_sdk.models.search import SearchParameters as DataAccessSearchParameters
from db_client.models.dfce import FamilyCategory
from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator
from typing_extensions import Annotated

from app.core.config import (
    VESPA_SEARCH_LIMIT,
    VESPA_SEARCH_MATCHES_PER_DOC,
)

from . import CLIMATE_LAWS_MATCH

Coord = tuple[float, float]


class SortOrder(str, Enum):
    """Sort ordering for use building query body."""

    ASCENDING = "asc"
    DESCENDING = "desc"


class SortField(str, Enum):
    """Sort field for use building query body."""

    DATE = "date"
    TITLE = "title"


class FilterField(str, Enum):
    """Filter field for use building query body."""

    SOURCE = "sources"
    COUNTRY = "countries"
    REGION = "regions"
    CATEGORY = "categories"
    LANGUAGE = "languages"


BackendFilterValues = Literal[
    "sources", "countries", "regions", "categories", "languages"
]
BackendKeywordFilter = Optional[Mapping[BackendFilterValues, Sequence[str]]]


class SearchRequestBody(DataAccessSearchParameters):
    """The request body expected by the search API endpoint."""

    # Query string should be required in backend (its not in dal)
    query_string: str

    # We need to add `keyword_filters` here because the items recieved from the frontend
    # need processing to be ready for vespa (key name change & geo slugs to geo codes)
    keyword_filters: BackendKeywordFilter = None

    # The following can be removed once we move away from limit-offset pagination
    offset: int = 0
    _page_size: int = PrivateAttr(default=10)

    @model_validator(mode="after")
    def backend_limit_handling(self):
        """
        Backend specific requirements for limit values

        This caps moth the passage and family limits to the backend limit, as well as
        differentiating between Vespas limit and the backends limit:

        For vespa the limit is the size per group result
        For the backend this is essentially a page within that
        """

        self.max_hits_per_family = min(
            self.max_hits_per_family, VESPA_SEARCH_MATCHES_PER_DOC
        )
        self.limit = min(self.limit, VESPA_SEARCH_LIMIT)
        self._page_size = self.limit
        self.limit = VESPA_SEARCH_LIMIT
        return self


class SearchResponseDocumentPassage(BaseModel):
    """A Document passage match returned by the search API endpoint."""

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
    total_passage_hits: int
    family_documents: list[SearchResponseFamilyDocument]
    continuation_token: Optional[str] = None
    prev_continuation_token: Optional[str] = None


class SearchResponse(BaseModel):
    """The response body produced by the search API endpoint."""

    hits: int
    total_family_hits: int
    query_time_ms: int
    total_time_ms: int
    continuation_token: Optional[str] = None
    this_continuation_token: Optional[str] = None
    prev_continuation_token: Optional[str] = None

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
