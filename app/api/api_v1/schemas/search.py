from enum import Enum
from typing import List, Literal, Mapping, Optional, Sequence

from cpr_sdk.models.search import SearchParameters as CprSdkSearchParameters
from db_client.models.dfce import FamilyCategory
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetJsonSchemaHandler,
    PrivateAttr,
    field_validator,
    model_validator,
)
from pydantic.json_schema import JsonSchemaValue
from pydantic_core.core_schema import CoreSchema
from typing_extensions import Annotated

from app.api.api_v1.schemas import CLIMATE_LAWS_MATCH
from app.core.config import VESPA_SEARCH_LIMIT, VESPA_SEARCH_MATCHES_PER_DOC

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


class SearchRequestBody(CprSdkSearchParameters):
    """The request body expected by the search API endpoint."""

    model_config = ConfigDict(use_attribute_docstrings=True)

    # Query string should be required in backend (its not in dal)
    # trunk-ignore(pyright/reportIncompatibleVariableOverride)
    query_string: str
    """
    A string representation of the search to be performed.
    For example: 'Adaptation strategy'"
    """

    # We need to add `keyword_filters` here because the items recieved from the frontend
    # need processing to be ready for vespa (key name change & geo slugs to geo codes)
    keyword_filters: BackendKeywordFilter = None
    """
    This is an object containing a map of fields and their values "
    to filter on. The allowed fields for the keys are: 
    "sources", "countries", "regions", "categories", "languages"
    """

    offset: int = 0
    """
    Where to start from in the number of query result that was 
    retrieved from the search database.
    """

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

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        """
        Custom documentation definitions

        The json schema is used for documentation. "[Modifying this method] doesn't
        affect the core schema, which is used for validation and serialization."
        https://docs.pydantic.dev/latest/concepts/json_schema/#implementing-__get_pydantic_json_schema__
        """
        json_schema = handler(core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)

        # Fields to hide from documentation
        json_schema["properties"].pop("filters")
        json_schema["properties"].pop("all_results")
        json_schema["properties"].pop("documents_only")

        return json_schema


class SearchResponseDocumentPassage(BaseModel):
    """A Document passage match returned by the search API endpoint."""

    text: str
    text_block_id: str
    text_block_page: Optional[int] = None
    text_block_coords: Optional[Sequence[Coord]] = None


class SearchResponseFamilyDocument(BaseModel):
    """A single document in a search response."""

    model_config = ConfigDict(use_attribute_docstrings=True)

    document_title: str
    """
    The title of the document.
    """

    document_slug: str
    """
    The slug that forms part of the URL to navigate to the particular document.
    Example, with a slug of  `national-climate-change-adaptation-strategy_06f8, a URL
    can be created to the document as:
    https://app.climatepolicyradar.org/documents/national-climate-change-adaptation-strategy_06f8
    """

    document_type: str
    """
    The type of document, for example: “Strategy”
    """

    document_source_url: Optional[str] = None
    """
    The source url of the external site that was used to ingest into the system.
    """

    document_url: Optional[str] = None
    """
    The CDN url of where the document can be found within our system.
    """

    document_content_type: Optional[str] = None
    """
    The content_type of the document found at the `document_url`. For example:
    “application/pdf” or “text/html”.
    """

    document_passage_matches: list[SearchResponseDocumentPassage]
    """
    This is a list of passages that match the search criteria within this document.
    
    The length of which is affected by max_passages_per_doc in the request.
    """

    @field_validator("document_source_url")
    @classmethod
    def _filter_climate_laws_url_from_source(cls, v):
        """Make sure we do not return climate-laws.org source URLs to the frontend"""
        if v is None or CLIMATE_LAWS_MATCH.match(v) is not None:
            return None
        return v


class SearchResponseFamily(BaseModel):
    """The object that is returned in the response."""

    model_config = ConfigDict(use_attribute_docstrings=True)

    family_slug: str
    """
    The slug that forms part of the URL to navigate to the family.
    
    Example, with a slug of  climate-change-adaptation-strategy_1882, a URL can be
    created to this family of documents as: https://app.climatepolicyradar.org/document/climate-change-adaptation-strategy_1882
    """

    family_name: str
    """
    The name of the family.
    """

    family_description: str
    """
    The description of the family.
    """

    family_category: str
    """
    The family category
    """

    family_date: str
    """
    The date the family of documents was published, this is from the corresponding
    Passed/Approved event for this family.
    """

    family_last_updated_date: str
    """
    The date the family of documents was published, this is from the most recent event
    of this family of documents.
    """

    family_source: str
    """
    The source, currently organisation name. Either “CCLW” or “UNFCCC”
    """

    family_geography: str
    """
    The geographical location of the family in ISO 3166-1 alpha-3
    """

    family_metadata: dict
    """
    An object if metadata for the family, the schema will change given the family_source
    """

    family_title_match: bool
    """
    True if the search is matched within the family's title
    """

    family_description_match: bool
    """
    True if the search is matched within the family's description.
    """

    total_passage_hits: int
    """
    Full number of passage matches in the search database for this family
    """

    family_documents: list[SearchResponseFamilyDocument]

    continuation_token: Optional[str] = None
    """
    Passage level continuation token. Can be used in conjunction with the family level 
    `this continuation_token` to get the next page of passages for this specific family
    """

    prev_continuation_token: Optional[str] = None
    """
    Passage level continuation token. Can be used in conjunction with the family level 
    `this continuation_token` to get the previous page of passages for this specific
    family
    """


class SearchResponse(BaseModel):
    """The response body produced by the search API endpoint."""

    model_config = ConfigDict(use_attribute_docstrings=True)

    hits: int
    """The number of documents retrieved within the current search query"""

    total_family_hits: int
    """The total hits available in the search database"""

    query_time_ms: int
    """Time for the query to run in the search database"""

    total_time_ms: int
    """query_time + extra processing"""

    continuation_token: Optional[str] = None
    """
    A token that can be sent in a followup request to the search endpoint in order to
    get the next page from the search database for this specific query.
    """

    this_continuation_token: Optional[str] = None
    """Relevant when using passage level continuations on a search page"""

    prev_continuation_token: Optional[str] = None
    """
    A token that can be sent in a followup request to the search endpoint in order to
    get the previous page from the search database for this specific query.
    """

    families: Sequence[SearchResponseFamily]
    """Search result families"""

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
