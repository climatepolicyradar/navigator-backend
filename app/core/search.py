import csv
import json
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
import string

from opensearchpy import OpenSearch
from opensearchpy import JSONSerializer as jss
from sentence_transformers import SentenceTransformer
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.search import (
    FilterField,
    OpenSearchResponseDescriptionMatch,
    OpenSearchResponseNameMatch,
    OpenSearchResponseMatchBase,
    OpenSearchResponsePassageMatch,
    SearchRequestBody,
    SearchResponse,
    SearchResultsResponse,
    SearchDocumentResponse,
    SearchResponseDocumentPassage,
    SortField,
    SortOrder,
    IncludedResults,
)
from app.core.config import (
    OPENSEARCH_INDEX_INNER_PRODUCT_THRESHOLD,
    OPENSEARCH_INDEX_MAX_DOC_COUNT,
    OPENSEARCH_INDEX_MAX_PASSAGES_PER_DOC,
    OPENSEARCH_INDEX_KNN_K_VALUE,
    OPENSEARCH_INDEX_N_PASSAGES_TO_SAMPLE_PER_SHARD,
    OPENSEARCH_INDEX_NAME_BOOST,
    OPENSEARCH_INDEX_DESCRIPTION_BOOST,
    OPENSEARCH_INDEX_EMBEDDED_TEXT_BOOST,
    OPENSEARCH_INDEX_NAME_KEY,
    OPENSEARCH_INDEX_DESCRIPTION_KEY,
    OPENSEARCH_INDEX_DESCRIPTION_EMBEDDING_KEY,
    OPENSEARCH_INDEX_INDEX_KEY,
    OPENSEARCH_INDEX_TEXT_BLOCK_KEY,
    OPENSEARCH_INDEX_ENCODER,
    OPENSEARCH_URL,
    OPENSEARCH_INDEX_PREFIX,
    OPENSEARCH_USERNAME,
    OPENSEARCH_PASSWORD,
    OPENSEARCH_REQUEST_TIMEOUT,
    OPENSEARCH_USE_SSL,
    OPENSEARCH_VERIFY_CERTS,
    OPENSEARCH_SSL_WARNINGS,
    OPENSEARCH_JIT_MAX_DOC_COUNT,
    PUBLIC_APP_URL,
)
from app.core.util import to_cdn_url
from app.db.models.law_policy import (
    FamilyDocument,
    FamilyMetadata,
    Slug,
    Collection,
    CollectionFamily,
)


_LOGGER = logging.getLogger(__name__)

_ENCODER = SentenceTransformer(
    model_name_or_path=OPENSEARCH_INDEX_ENCODER,
    cache_folder=os.environ.get("INDEX_ENCODER_CACHE_FOLDER", "/models"),
)
# Map a sort field type to the document key used by OpenSearch
_SORT_FIELD_MAP: Mapping[SortField, str] = {
    SortField.DATE: "document_date",
    SortField.TITLE: "document_name",
}
# TODO: Map a filter field type to the document key used by OpenSearch
_FILTER_FIELD_MAP: Mapping[FilterField, str] = {
    FilterField.SOURCE: "document_source",
    FilterField.COUNTRY: "document_geography",
    FilterField.SECTOR: "document_sector",
    FilterField.TYPE: "document_type",
    FilterField.CATEGORY: "document_category",
    FilterField.KEYWORD: "document_keyword",
    FilterField.LANGUAGE: "document_language",
}
_REQUIRED_FIELDS = ["document_name"]
_DEFAULT_SORT_ORDER = SortOrder.DESCENDING
_JSON_SERIALIZER = jss()
_CSV_SEARCH_RESPONSE_COLUMNS = [
    "Collection name",
    "Collection summary",
    "Family name",
    "Family summary",
    "Family URL",
    "Document title",
    "Document URL",
    "Document role",
    "Document variant",
    "Geography ISO",
    "Category",
    "Sectors",
    "Instruments",
    "Frameworks",
    "Responses",
    "Natural Hazards",
    "Document Type",
    "Language",
    "Keywords",
    "Geography",
]


def _innerproduct_threshold_to_lucene_threshold(ip_thresh: float) -> float:
    """
    Map inner product to lucene threashold.

    Opensearch documentation on mapping similarity functions to Lucene thresholds is
    here: https://github.com/opensearch-project/k-NN/blob/main/src/main/java/org/opensearch/knn/index/SpaceType.java#L33

    It defines 'inner product' as negative inner product i.e. a distance rather than
    similarity measure, so we reverse the signs of inner product here compared to the
    docs.
    """  # noqa: E501
    if ip_thresh > 0:
        return ip_thresh + 1
    else:
        return 1 / (1 - ip_thresh)


def load_sensitive_query_terms() -> set[str]:
    """
    Return sensitive query terms from the first column of a TSV file.

    Outputs are lowercased for case-insensitive matching.

    :return [set[str]]: sensitive query terms
    """
    tsv_path = Path(__file__).parent / "sensitive_query_terms.tsv"
    with open(tsv_path, "r") as tsv_file:
        reader = csv.reader(tsv_file, delimiter="\t")

        # first column is group name, second column is keyword
        sensitive_terms = set([row[1].lower().strip() for row in reader])

    return sensitive_terms


@dataclass(frozen=True)
class OpenSearchQueryConfig:
    """Configuration for searches sent to OpenSearch."""

    name_boost: int = OPENSEARCH_INDEX_NAME_BOOST
    description_boost: int = OPENSEARCH_INDEX_DESCRIPTION_BOOST
    embedded_text_boost: int = OPENSEARCH_INDEX_EMBEDDED_TEXT_BOOST
    lucene_threshold: float = _innerproduct_threshold_to_lucene_threshold(
        OPENSEARCH_INDEX_INNER_PRODUCT_THRESHOLD
    )  # TODO: tune me separately for descriptions?
    max_doc_count: int = OPENSEARCH_INDEX_MAX_DOC_COUNT
    max_passages_per_doc: int = OPENSEARCH_INDEX_MAX_PASSAGES_PER_DOC
    n_passages_to_sample_per_shard: int = (
        OPENSEARCH_INDEX_N_PASSAGES_TO_SAMPLE_PER_SHARD
    )
    k = OPENSEARCH_INDEX_KNN_K_VALUE
    jit_max_doc_count: int = OPENSEARCH_JIT_MAX_DOC_COUNT


@dataclass
class OpenSearchConfig:
    """Config for accessing an OpenSearch instance."""

    url: str = OPENSEARCH_URL
    username: str = OPENSEARCH_USERNAME
    password: str = OPENSEARCH_PASSWORD
    index_prefix: str = OPENSEARCH_INDEX_PREFIX
    request_timeout: int = OPENSEARCH_REQUEST_TIMEOUT
    use_ssl: bool = OPENSEARCH_USE_SSL
    verify_certs: bool = OPENSEARCH_VERIFY_CERTS
    ssl_show_warnings: bool = OPENSEARCH_SSL_WARNINGS


@dataclass
class OpenSearchResponse:
    """Opensearch response container."""

    raw_response: Mapping[str, Any]
    request_time_ms: int


class OpenSearchEncoder(json.JSONEncoder):
    """Special json encoder for OpenSearch types"""

    def default(self, obj):
        """Override"""
        return _JSON_SERIALIZER.default(obj)


class OpenSearchConnection:
    """OpenSearch connection helper, allows query based on config."""

    def __init__(
        self,
        opensearch_config: OpenSearchConfig,
    ):
        self._opensearch_config = opensearch_config
        self._opensearch_connection: Optional[OpenSearch] = None
        self._sensitive_query_terms = load_sensitive_query_terms()

    def query_families(
        self,
        search_request_body: SearchRequestBody,
        opensearch_internal_config: OpenSearchQueryConfig,
        document_extra_info: Mapping[str, Mapping[str, str]],
        preference: Optional[str],
    ) -> SearchResponse:
        """Build & make an OpenSearch query based on the given request body."""

        opensearch_request = build_opensearch_request_body(
            search_request=search_request_body,
            opensearch_internal_config=opensearch_internal_config,
            sensitive_query_terms=self._sensitive_query_terms,
        )

        indices = self._get_opensearch_indices_to_query(search_request_body)

        opensearch_response_body = self.raw_query(
            opensearch_request.query, preference, indices
        )

        return process_search_response_body_families(
            opensearch_response_body,
            document_extra_info,
            limit=search_request_body.limit,
            offset=search_request_body.offset,
        )

    def _get_opensearch_indices_to_query(
        self, search_request: SearchRequestBody
    ) -> str:
        """
        Get the OpenSearch indices to query based on the request body.

        :param [SearchRequestBody] search_request: The search request body.
        :return [str]: a comma-separated string of indices.
        """

        # By default we just query the index containing names and descriptions,
        # and the non-translated PDFs
        indices_include = [
            f"{self._opensearch_config.index_prefix}_core",
            f"{self._opensearch_config.index_prefix}_pdfs_non_translated",
        ]

        if search_request.include_results is None:
            return ",".join(indices_include)

        if IncludedResults.PDFS_TRANSLATED in search_request.include_results:
            indices_include.append(
                f"{self._opensearch_config.index_prefix}_pdfs_translated"
            )

        if IncludedResults.HTMLS_TRANSLATED in search_request.include_results:
            indices_include.append(
                f"{self._opensearch_config.index_prefix}_htmls_translated"
            )

        if IncludedResults.HTMLS_NON_TRANSLATED in search_request.include_results:
            indices_include.append(
                f"{self._opensearch_config.index_prefix}_htmls_non_translated"
            )

        return ",".join(indices_include)

    def raw_query(
        self,
        request_body: Mapping[str, Any],
        preference: Optional[str],
        indices: str,
    ) -> OpenSearchResponse:
        """Query the configured OpenSearch instance with a JSON OpenSearch body."""

        if self._opensearch_connection is None:
            login_details = (
                self._opensearch_config.username,
                self._opensearch_config.password,
            )
            self._opensearch_connection = OpenSearch(
                [self._opensearch_config.url],
                http_auth=login_details,
                use_ssl=self._opensearch_config.use_ssl,
                veriy_certs=self._opensearch_config.verify_certs,
                ssl_show_warn=self._opensearch_config.ssl_show_warnings,
            )

        start = time.time_ns()
        response = self._opensearch_connection.search(
            body=request_body,
            index=indices,
            request_timeout=self._opensearch_config.request_timeout,
            preference=preference,
        )
        end = time.time_ns()
        search_request_time = round((end - start) / 1e6)

        _LOGGER.info(
            "Search request completed",
            extra={
                "props": {
                    "search_request": json.dumps(request_body, cls=OpenSearchEncoder),
                    "search_request_time": search_request_time,
                },
            },
        )

        return OpenSearchResponse(
            raw_response=response,
            request_time_ms=search_request_time,
        )


def _year_range_filter(
    year_range: tuple[Optional[int], Optional[int]]
) -> Optional[dict[str, Any]]:
    """
    Get an Opensearch filter for year range.

    The filter returned is between the first term of `year_range` and the last term,
    and is inclusive. Either value can be set to None to only apply one year constraint.
    """

    policy_year_conditions = {}
    if year_range[0] is not None:
        policy_year_conditions["gte"] = f"01/01/{year_range[0]}"
    if year_range[1] is not None:
        policy_year_conditions["lte"] = f"31/12/{year_range[1]}"

    if policy_year_conditions:
        return {"range": {"document_date": policy_year_conditions}}

    return None


class QueryBuilder:
    """Helper class for building OpenSearch queries."""

    def __init__(self, config: OpenSearchQueryConfig):
        self._config = config
        self._request_body: dict[str, Any] = {}

    @property
    def query(self) -> Mapping[str, Any]:
        """Property to allow access to the build request body."""

        return self._request_body

    def _with_search_term_base(self):
        self._request_body = {
            "size": 0,  # only return aggregations
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1,
                },
            },
            "aggs": {
                "sample": {
                    "sampler": {
                        "shard_size": self._config.n_passages_to_sample_per_shard
                    },
                    "aggs": {
                        "top_docs": {
                            "terms": {
                                "field": OPENSEARCH_INDEX_INDEX_KEY,
                                "order": {"top_hit": _DEFAULT_SORT_ORDER.value},
                                "size": self._config.max_doc_count,
                            },
                            "aggs": {
                                "top_passage_hits": {
                                    "top_hits": {
                                        "_source": {
                                            "excludes": [
                                                "text_embedding",
                                                OPENSEARCH_INDEX_DESCRIPTION_EMBEDDING_KEY,  # noqa: E501
                                            ]
                                        },
                                        "size": self._config.max_passages_per_doc,
                                    },
                                },
                                "top_hit": {"max": {"script": {"source": "_score"}}},
                                _SORT_FIELD_MAP[SortField.DATE]: {
                                    "stats": {
                                        "field": _SORT_FIELD_MAP[SortField.DATE],
                                    },
                                },
                            },
                        },
                    },
                },
                "no_unique_docs": {"cardinality": {"field": "document_slug"}},
            },
        }

    def with_semantic_query(self, query_string: str, knn: bool):
        """Configure the query to search semantically for a given query string."""

        embedding = _ENCODER.encode(query_string)
        self._with_search_term_base()
        self._request_body["query"]["bool"]["should"] = [
            {
                "bool": {
                    "should": [
                        {
                            "match": {
                                OPENSEARCH_INDEX_NAME_KEY: {
                                    "query": query_string,
                                    "operator": "and",
                                    "minimum_should_match": "2<66%",
                                    # all terms if there are 2 or less, otherwise
                                    # 66% of terms (rounded down)
                                }
                            }
                        },
                        {
                            "match_phrase": {
                                OPENSEARCH_INDEX_NAME_KEY: {
                                    "query": query_string,
                                    "boost": 2,  # TODO: configure?
                                }
                            }
                        },
                    ],
                    "boost": self._config.name_boost,
                }
            },
            {
                "bool": {
                    "should": [
                        {
                            "match": {
                                OPENSEARCH_INDEX_DESCRIPTION_KEY: {
                                    "query": query_string,
                                    "boost": 3,
                                    "operator": "and",
                                    "minimum_should_match": "2<66%",
                                    # all terms if there are 2 or less, otherwise
                                    # 66% of terms (rounded down)
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                    "boost": self._config.description_boost,
                },
            },
            {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "text": {
                                    "query": query_string,
                                    "operator": "and",
                                    "minimum_should_match": "2<66%",
                                    # all terms if there are 2 or less, otherwise
                                    # 66% of terms (rounded down)
                                },
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                    "boost": self._config.embedded_text_boost,
                }
            },
        ]

        if knn:
            self._request_body["query"]["bool"]["should"][1]["bool"]["should"].append(
                {
                    "function_score": {
                        "query": {
                            "knn": {
                                OPENSEARCH_INDEX_DESCRIPTION_EMBEDDING_KEY: {
                                    "vector": embedding,
                                    "k": self._config.k,
                                },
                            },
                        },
                        "min_score": self._config.lucene_threshold,
                    }
                }
            )

            self._request_body["query"]["bool"]["should"][2]["bool"]["should"].append(
                {
                    "function_score": {
                        "query": {
                            "knn": {
                                "text_embedding": {
                                    "vector": embedding,
                                    "k": self._config.k,
                                },
                            },
                        },
                        "min_score": self._config.lucene_threshold,
                    }
                }
            )

    def with_exact_query(self, query_string: str):
        """Configure the query to search for an exact match to a given query string."""

        self._with_search_term_base()
        self._request_body["query"]["bool"]["should"] = [
            # Document title matching
            {
                "match_phrase": {
                    OPENSEARCH_INDEX_NAME_KEY: {
                        "query": query_string,
                        "boost": self._config.name_boost,
                    },
                }
            },
            # Document description matching
            {
                "match_phrase": {
                    OPENSEARCH_INDEX_DESCRIPTION_KEY: {
                        "query": query_string,
                        "boost": self._config.description_boost,
                    },
                }
            },
            # Text passage matching
            {
                "match_phrase": {
                    "text": {
                        "query": query_string,
                    },
                }
            },
        ]

    def with_keyword_filter(self, field: FilterField, values: Sequence[str]):
        """Add a keyword filter to the configured query."""
        filters = self._request_body["query"]["bool"].get("filter") or []

        filters.append({"terms": {_FILTER_FIELD_MAP[field]: values}})
        self._request_body["query"]["bool"]["filter"] = filters

    def with_year_range_filter(self, year_range: tuple[Optional[int], Optional[int]]):
        """Add a year range filter to the configured query."""

        year_range_filter = _year_range_filter(year_range)
        if year_range_filter is not None:
            filters = self._request_body["query"]["bool"].get("filter") or []
            filters.append(year_range_filter)
            self._request_body["query"]["bool"]["filter"] = filters

    def with_search_order(self, field: SortField, order: SortOrder):
        """Set sort order for search results."""
        terms_field = self._request_body["aggs"]["sample"]["aggs"]["top_docs"]["terms"]

        if field == SortField.DATE:
            terms_field["order"] = {f"{_SORT_FIELD_MAP[field]}.avg": order.value}
        elif field == SortField.TITLE:
            terms_field["order"] = {"_key": order.value}
        else:
            raise RuntimeError(f"Unknown sort ordering field: {field}")

    def with_required_fields(self, required_fields: Sequence[str]):
        """Ensure that required fields are present in opensearch responses."""
        must_clause = self._request_body["query"]["bool"].get("must") or []
        must_clause.extend(
            [{"exists": {"field": field_name}} for field_name in required_fields]
        )
        self._request_body["query"]["bool"]["must"] = must_clause


def build_opensearch_request_body(
    search_request: SearchRequestBody,
    opensearch_internal_config: Optional[OpenSearchQueryConfig] = None,
    sensitive_query_terms: set[str] = set(),
) -> QueryBuilder:
    """Build a complete OpenSearch request body."""

    search_config = opensearch_internal_config or OpenSearchQueryConfig(
        max_passages_per_doc=search_request.max_passages_per_doc,
    )
    builder = QueryBuilder(search_config)

    # Strip punctuation and leading and trailing whitespace from query string
    search_request.query_string = search_request.query_string.translate(
        str.maketrans("", "", string.punctuation)
    ).strip()

    if search_request.exact_match:
        builder.with_exact_query(search_request.query_string)
    else:
        sensitive_terms_in_query = [
            term
            for term in sensitive_query_terms
            if term in search_request.query_string.lower()
        ]

        # If the query contains any sensitive terms, and the length of the
        # shortest sensitive term is >=50% of the length of the query by
        # number of words, then disable KNN
        if (
            sensitive_terms_in_query
            and len(min(sensitive_terms_in_query, key=len).split(" "))
            / len(search_request.query_string.split(" "))
            >= 0.5
        ):
            use_knn = False
        else:
            use_knn = True

        builder.with_semantic_query(search_request.query_string, knn=use_knn)

    if search_request.sort_field is not None:
        builder.with_search_order(
            search_request.sort_field,
            search_request.sort_order or _DEFAULT_SORT_ORDER,
        )

    if _REQUIRED_FIELDS:
        builder.with_required_fields(_REQUIRED_FIELDS)

    if search_request.keyword_filters is not None:
        for keyword, values in search_request.keyword_filters.items():
            builder.with_keyword_filter(keyword, values)

    if search_request.year_range is not None:
        builder.with_year_range_filter(search_request.year_range)

    return builder


################## NEW ######################
from app.api.api_v1.schemas.search import (  # noqa: E402
    SearchResponse,
    SearchResponseFamilyDocument,
    SearchResponseFamily,
)

# FIXME: move import during post-families cleanup


def process_search_response_body_families(
    opensearch_response_body: OpenSearchResponse,
    document_extra_info: Mapping[str, Mapping[str, str]],
    limit: int = 10,
    offset: int = 0,
) -> SearchResponse:
    search_json_response = opensearch_response_body.raw_response
    search_response_document = None
    search_response_family = None
    unknown_document_ids = set()

    # Aggregate into families using OrderedDict to preserve the response relevance order
    families: OrderedDict[str, SearchResponseFamily] = OrderedDict()

    result_docs = search_json_response["aggregations"]["sample"]["top_docs"]["buckets"]
    for result_doc in result_docs:
        title_match = False
        description_match = False
        for document_match in result_doc["top_passage_hits"]["hits"]["hits"]:
            document_match_source = document_match["_source"]
            # Skip documents from Opensearch that do not exist in RDS
            if document_match_source["document_id"] not in document_extra_info:
                unknown_document_ids.add(document_match_source["document_id"])
                continue

            if OPENSEARCH_INDEX_NAME_KEY in document_match_source:
                # Validate as a title match
                doc_match = OpenSearchResponseNameMatch(**document_match_source)
                if search_response_document is None:
                    search_response_document = create_search_response_family_document(
                        doc_match,
                        document_extra_info,
                    )
                title_match = True
            elif OPENSEARCH_INDEX_DESCRIPTION_KEY in document_match_source:
                # Validate as a description match
                doc_match = OpenSearchResponseDescriptionMatch(**document_match_source)
                if search_response_document is None:
                    search_response_document = create_search_response_family_document(
                        doc_match,
                        document_extra_info,
                    )
                description_match = True
            elif OPENSEARCH_INDEX_TEXT_BLOCK_KEY in document_match_source:
                # Process as a text block
                doc_match = OpenSearchResponsePassageMatch(**document_match_source)
                if search_response_document is None:
                    search_response_document = create_search_response_family_document(
                        doc_match,
                        document_extra_info,
                    )

                response_passage = SearchResponseDocumentPassage(
                    text=doc_match.text,
                    text_block_id=doc_match.text_block_id,
                    text_block_page=doc_match.text_block_page + 1,
                    text_block_coords=doc_match.text_block_coords,
                )
                search_response_document.document_passage_matches.append(
                    response_passage
                )
            else:
                _LOGGER.error("Unexpected data in match results")
                continue

            family_id = document_extra_info[doc_match.document_id]["family_import_id"]
            search_response_family = families.get(family_id)
            if search_response_family is None:
                search_response_family = create_search_response_family(
                    doc_match,
                    document_extra_info,
                )
                families[family_id] = search_response_family

        if search_response_document is None or search_response_family is None:
            _LOGGER.error(
                "Unexpected document encountered, not attempting to include in results"
            )
        else:
            search_response_family.family_title_match = (
                title_match or search_response_family.family_title_match
            )
            search_response_family.family_description_match = (
                description_match or search_response_family.family_description_match
            )
            search_response_family.family_documents.append(search_response_document)

        search_response_document = None
        search_response_family = None

    if unknown_document_ids:
        _LOGGER.error(
            "Unknown document IDs were encountered in Opensearch response",
            extra={"props": {"unknown document IDs": list(unknown_document_ids)}},
        )

    search_response = SearchResponse(
        hits=len(families),
        query_time_ms=opensearch_response_body.request_time_ms,
        families=list(families.values())[offset : offset + limit],
    )

    return search_response


def create_search_response_family_document(
    opensearch_match: OpenSearchResponseMatchBase,
    document_family_info: Mapping[str, Mapping[str, str]],
) -> SearchResponseFamilyDocument:
    document_info = document_family_info[opensearch_match.document_id]
    return SearchResponseFamilyDocument(
        document_title=document_info["title"],
        document_type=opensearch_match.document_type,
        document_source_url=opensearch_match.document_source_url,
        document_url=to_cdn_url(opensearch_match.document_cdn_object),
        document_content_type=opensearch_match.document_content_type,
        document_slug=document_info["slug"],
        document_passage_matches=[],
    )


def create_search_response_family(
    opensearch_match: OpenSearchResponseMatchBase,
    document_family_info: Mapping[str, Mapping[str, str]],
) -> SearchResponseFamily:
    document_info = document_family_info[opensearch_match.document_id]
    return SearchResponseFamily(
        family_slug=document_info["family_slug"],
        family_name=document_info["family_title"],
        family_description=document_info["family_description"],
        family_category=document_info["family_category"],
        family_date=document_info["family_published_date"],
        family_last_updated_date=document_info["family_last_updated_date"],
        family_source=opensearch_match.document_source,
        family_geography=opensearch_match.document_geography,
        family_metadata={},  # FIXME: complete?
        family_title_match=False,
        family_description_match=False,
        family_documents=[],
    )


#################################
########## DEPRECATED ###########
#################################
def process_search_response_body(
    opensearch_response_body: OpenSearchResponse,
    limit: int = 10,
    offset: int = 0,
) -> SearchResultsResponse:
    opensearch_json_response = opensearch_response_body.raw_response
    search_response = SearchResultsResponse(
        hits=opensearch_json_response["aggregations"]["no_unique_docs"]["value"],
        query_time_ms=opensearch_response_body.request_time_ms,
        documents=[],
    )
    search_response_document = None

    result_docs = opensearch_json_response["aggregations"]["sample"]["top_docs"][
        "buckets"
    ]
    for result_doc in result_docs[offset : offset + limit]:
        for document_match in result_doc["top_passage_hits"]["hits"]["hits"]:
            document_match_source = document_match["_source"]
            if OPENSEARCH_INDEX_NAME_KEY in document_match_source:
                # Validate as a title match
                name_match = OpenSearchResponseNameMatch(**document_match_source)
                if search_response_document is None:
                    search_response_document = create_search_response_document(
                        name_match
                    )
                search_response_document.document_title_match = True
            elif OPENSEARCH_INDEX_DESCRIPTION_KEY in document_match_source:
                # Validate as a description match
                desc_match = OpenSearchResponseDescriptionMatch(**document_match_source)
                if search_response_document is None:
                    search_response_document = create_search_response_document(
                        desc_match
                    )
                search_response_document.document_description_match = True
            elif OPENSEARCH_INDEX_TEXT_BLOCK_KEY in document_match_source:
                # Process as a text block
                passage_match = OpenSearchResponsePassageMatch(**document_match_source)
                if search_response_document is None:
                    search_response_document = create_search_response_document(
                        passage_match
                    )

                response_passage = SearchResponseDocumentPassage(
                    text=passage_match.text,
                    text_block_id=passage_match.text_block_id,
                    text_block_page=passage_match.text_block_page + 1,
                    text_block_coords=passage_match.text_block_coords,
                )
                search_response_document.document_passage_matches.append(
                    response_passage
                )
            else:
                raise RuntimeError("Unexpected data in match results")

        if search_response_document is None:
            raise RuntimeError("Unexpected document match with no matching passages")

        search_response.documents.append(search_response_document)
        search_response_document = None

    return search_response


def process_browse_response_body(
    opensearch_response_body: OpenSearchResponse,
) -> SearchResultsResponse:
    opensearch_json_response = opensearch_response_body.raw_response
    search_response = SearchResultsResponse(
        hits=opensearch_json_response["hits"]["total"]["value"],
        query_time_ms=opensearch_response_body.request_time_ms,
        documents=[],
    )

    result_docs = opensearch_json_response["hits"]["hits"]
    for result_doc in result_docs:
        search_response_document = create_search_response_document(
            OpenSearchResponseDescriptionMatch(**result_doc["_source"])
        )
        search_response.documents.append(search_response_document)

    return search_response


def create_search_response_document(
    opensearch_match: OpenSearchResponseMatchBase,
):
    return SearchDocumentResponse(
        document_name=opensearch_match.document_name,
        document_description=opensearch_match.document_description,
        document_geography=opensearch_match.document_geography,
        document_sectors=opensearch_match.document_sectors,
        document_source=opensearch_match.document_source,
        document_date=opensearch_match.document_date,
        document_id=opensearch_match.document_id,
        document_type=opensearch_match.document_type,
        document_category=opensearch_match.document_category,
        document_source_url=opensearch_match.document_source_url,
        document_url=to_cdn_url(opensearch_match.document_cdn_object),
        document_content_type=opensearch_match.document_content_type,
        document_slug=opensearch_match.document_slug,
        document_title_match=False,
        document_description_match=False,
        document_passage_matches=[],
        document_postfix=None,
    )


def _get_extra_csv_info(
    db: Session,
    families: Sequence[SearchResponseFamily],
) -> Mapping[str, Any]:
    all_family_slugs = [f.family_slug for f in families]
    all_document_slugs = [d.document_slug for f in families for d in f.family_documents]

    slug_and_family_metadata = (
        db.query(Slug, FamilyMetadata)
        .filter(Slug.name.in_(all_family_slugs))
        .join(FamilyMetadata, FamilyMetadata.family_import_id == Slug.family_import_id)
        .all()
    )
    slug_and_family_document = (
        db.query(Slug, FamilyDocument)
        .filter(Slug.name.in_(all_document_slugs))
        .join(
            FamilyDocument, FamilyDocument.import_id == Slug.family_document_import_id
        )
        .all()
    )
    # For now there is max one collection per family
    slug_and_collection = (
        db.query(Slug, Collection)
        .filter(Slug.name.in_(all_family_slugs))
        .join(
            CollectionFamily, CollectionFamily.family_import_id == Slug.family_import_id
        )
        .join(Collection, Collection.import_id == CollectionFamily.collection_import_id)
        .all()
    )
    extra_csv_info = {
        "metadata": {
            slug.name: meta.value for (slug, meta) in slug_and_family_metadata
        },
        "language": {
            slug.name: language.language_name
            for (slug, family_document) in slug_and_family_document
            for language in family_document.physical_document.languages
        },
        "collection": {
            slug.name: collection for (slug, collection) in slug_and_collection
        },
    }

    return extra_csv_info


def process_result_into_csv(
    db: Session,
    search_response: SearchResponse,
) -> str:
    csv_result_io = StringIO("")
    writer = csv.DictWriter(
        csv_result_io,
        fieldnames=_CSV_SEARCH_RESPONSE_COLUMNS,
    )
    writer.writeheader()

    extra_info_query = _get_extra_csv_info(db, search_response.families)
    url_base = f"{PUBLIC_APP_URL}/documents"
    for family in search_response.families:
        _LOGGER.info(f"Family: {family}")
        family_metadata = extra_info_query["metadata"].get(family.family_slug)
        if family_metadata is None:
            _LOGGER.error(f"Failed to find metadata for '{family.family_slug}'")

        sector_metadata = ";".join(family_metadata.get("sector", []))
        instrument_metadata = ";".join(family_metadata.get("instrument", []))
        framework_metadata = ";".join(family_metadata.get("framework", []))
        response_metadata = ";".join(family_metadata.get("topic", []))
        hazard_metadata = ";".join(family_metadata.get("hazard", []))
        keyword_metadata = ";".join(family_metadata.get("keyword", []))

        collection_name = ""
        collection_summary = ""
        collection = extra_info_query["collection"].get(family.family_slug)
        if collection is not None:
            collection_name = collection.title
            collection_summary = collection.description

        if family.family_documents:
            for document in family.family_documents:
                _LOGGER.info(f"Document: {document}")
                document_slug = document.document_slug
                document_language = extra_info_query["language"].get(document_slug)
                if document_language is None:
                    _LOGGER.warning(f"Failed to find language for '{document_slug}'")
                row = {
                    "Collection Name": collection_name,
                    "Collection Summary": collection_summary,
                    "Family Name": family.family_name,
                    "Family Summary": family.family_description,
                    "Family Date": family.family_date,
                    "Family URL": f"{url_base}/{family.family_slug}",
                    "Document Title": document.document_title,
                    "Document URL": f"{url_base}/{document.document_slug}",
                    "Document type": document.document_type,
                    "Geography ISO": family.family_geography,
                    "Category": family.family_category,
                    "Sectors": sector_metadata,
                    "Instruments": instrument_metadata,
                    "Frameworks": framework_metadata,
                    "Responses": response_metadata,
                    "Natural Hazards": hazard_metadata,
                    "Keywords": keyword_metadata,
                    "Language": document_language or "",
                }
                writer.writerow(row)
        else:
            row = {
                "Collection Name": collection_name,
                "Collection Summary": collection_summary,
                "Family Name": family.family_name,
                "Family Summary": family.family_description,
                "Family Date": family.family_date,
                "Family URL": f"{url_base}/{family.family_slug}",
                "Document Title": "",
                "Document URL": "",
                "Document type": "",
                "Geography ISO": family.family_geography,
                "Category": family.family_category,
                "Sectors": sector_metadata,
                "Instruments": instrument_metadata,
                "Frameworks": framework_metadata,
                "Responses": response_metadata,
                "Natural Hazards": hazard_metadata,
                "Keywords": keyword_metadata,
                "Language": "",
            }
            writer.writerow(row)

    csv_result_io.seek(0)
    return csv_result_io.read()
