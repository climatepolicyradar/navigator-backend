"""
Searches for documents.

All endpoints should perform document searches using the SearchRequestBody as
its input. The individual endpoints will return different responses tailored
for the type of document search being performed.
"""
import json
import logging
from typing import Mapping, Sequence, Union

from fastapi import APIRouter, Request, BackgroundTasks, Depends
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.search import (
    SearchRequestBody,
    SearchResponse,
    SearchResultsResponse,
    SearchDocumentResponse,
    SortField,
)
from app.core.browse import BrowseArgs, browse_rds, browse_rds_families
from app.core.jit_query_wrapper import jit_query_wrapper, jit_query_families_wrapper
from app.core.lookups import get_countries_for_region, get_country_by_slug
from app.core.search import (
    FilterField,
    OpenSearchConnection,
    OpenSearchConfig,
    OpenSearchQueryConfig,
)
from app.db.crud.deprecated_document import get_postfix_map
from app.db.crud.document import DocumentExtraCache
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

# Use configured environment for router
_OPENSEARCH_CONFIG = OpenSearchConfig()
_OPENSEARCH_CONNECTION = OpenSearchConnection(opensearch_config=_OPENSEARCH_CONFIG)
_OPENSEARCH_INDEX_CONFIG = OpenSearchQueryConfig()
_DOCUMENT_EXTRA_INFO_CACHE = DocumentExtraCache()

search_router = APIRouter()


def _map_old_category_to_new(supplied_category: str) -> str:
    """Temporarily translate old category strings into new values when searching"""
    if supplied_category.lower() == "law":
        return "Legislative"
    if supplied_category.lower() == "policy":
        return "Executive"
    return supplied_category


def _map_new_category_to_old(sdr: SearchDocumentResponse) -> SearchDocumentResponse:
    """Temporarily translate new category strings into old values when searching"""
    if sdr.document_category.lower() == "legislative":
        sdr.document_category = "Law"
    if sdr.document_category.lower() == "executive":
        sdr.document_category = "Policy"
    return sdr


@search_router.post("/searches")
def search_documents(
    request: Request,
    search_body: SearchRequestBody,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    group_documents: bool = False,
) -> Union[SearchResponse, SearchResultsResponse]:
    """Search for documents matching the search criteria."""
    # FIXME: The returned Union type should have `SearchResultsResponse` removed when
    #        the frontend supports grouping by family. We will need to tidy up & remove
    #        unused definitions at that point.

    _LOGGER.info(
        f"Search request (jit={search_body.jit_query})",
        extra={
            "props": {
                "search_request": json.loads(search_body.json()),
            }
        },
    )

    if search_body.keyword_filters is not None:
        search_body.keyword_filters = process_search_keyword_filters(
            db,
            search_body.keyword_filters,
        )

    if group_documents:
        if not search_body.query_string:
            # Service browse requests from RDS
            return browse_rds_families(
                db=db,
                req=_get_browse_args_from_search_request_body(search_body),
            )

        return jit_query_families_wrapper(
            _OPENSEARCH_CONNECTION,
            background_tasks=background_tasks,
            search_request_body=search_body,
            opensearch_internal_config=_OPENSEARCH_INDEX_CONFIG,
            document_extra_info=_DOCUMENT_EXTRA_INFO_CACHE.get_document_extra_info(db),
            preference="default_search_preference",
        )
    else:
        if not search_body.query_string:
            # Service browse requests from RDS
            return browse_rds(
                db=db,
                req=_get_browse_args_from_search_request_body(search_body),
            )

        # For searches, map old categories to new category names in opensearch
        if search_body.keyword_filters is not None:
            if categories := search_body.keyword_filters.get(FilterField.CATEGORY):
                mapped_categories = [_map_old_category_to_new(c) for c in categories]
                keyword_filters = dict(search_body.keyword_filters)
                keyword_filters[FilterField.CATEGORY] = mapped_categories
                search_body.keyword_filters = keyword_filters

        doc_results: SearchResultsResponse = jit_query_wrapper(
            _OPENSEARCH_CONNECTION,
            background_tasks=background_tasks,
            search_request_body=search_body,
            opensearch_internal_config=_OPENSEARCH_INDEX_CONFIG,
            preference="default_search_preference",
        )
        # Now augment the search results with db data to form the response
        doc_ids = [doc.document_id for doc in doc_results.documents]
        postfix_map = get_postfix_map(db, doc_ids)
        return SearchResultsResponse(
            hits=doc_results.hits,
            query_time_ms=doc_results.query_time_ms,
            documents=[
                SearchDocumentResponse(
                    **{
                        **_map_new_category_to_old(doc).dict(),
                        **{"document_postfix": postfix_map[doc.document_id]},
                    }
                )
                for doc in doc_results.documents
            ],
        )


def _get_browse_args_from_search_request_body(
    search_body: SearchRequestBody,
) -> BrowseArgs:
    keyword_filters = search_body.keyword_filters
    if keyword_filters is None:
        country_codes = None
        categories = None
    else:
        country_codes = keyword_filters.get(FilterField.COUNTRY)
        categories = keyword_filters.get(FilterField.CATEGORY)
    start_year, end_year = search_body.year_range or [None, None]
    return BrowseArgs(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year,
        categories=categories,
        sort_field=search_body.sort_field or SortField.DATE,
        sort_order=search_body.sort_order,
        limit=search_body.limit,
        offset=search_body.offset,
    )


def process_search_keyword_filters(
    db: Session,
    request_filters: Mapping[FilterField, Sequence[str]],
) -> Mapping[FilterField, Sequence[str]]:
    filter_map = {}

    for field, values in request_filters.items():
        if field == FilterField.REGION:
            field = FilterField.COUNTRY
            filter_values = []
            for geo_slug in values:
                filter_values.extend(
                    [g.value for g in get_countries_for_region(db, geo_slug)]
                )
        elif field == FilterField.COUNTRY:
            filter_values = [
                country.value
                for geo_slug in values
                if (country := get_country_by_slug(db, geo_slug)) is not None
            ]
        else:
            filter_values = values

        if filter_values:
            values = filter_map.get(field, [])
            values.extend(filter_values)
            # Be consistent in ordering for search
            values = sorted(list(set(values)))
            filter_map[field] = values

    return filter_map
