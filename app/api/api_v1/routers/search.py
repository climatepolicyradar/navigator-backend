"""Searches for documents.

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
)
from app.core.jit_query_wrapper import jit_query_wrapper, jit_query_families_wrapper
from app.core.lookups import get_countries_for_region, get_country_by_slug
from app.core.search import (
    FilterField,
    OpenSearchConnection,
    OpenSearchConfig,
    OpenSearchQueryConfig,
)
from app.db.crud.document import get_postfix_map, get_document_extra
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

# Use configured environment for router
_OPENSEARCH_CONFIG = OpenSearchConfig()
_OPENSEARCH_CONNECTION = OpenSearchConnection(opensearch_config=_OPENSEARCH_CONFIG)
_OPENSEARCH_INDEX_CONFIG = OpenSearchQueryConfig()

search_router = APIRouter()


@search_router.post("/searches")
def search_documents(
    request: Request,
    search_body: SearchRequestBody,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
) -> Union[SearchResponse, SearchResultsResponse]:
    """Search for documents matching the search criteria."""

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

    if search_body.group_documents:
        document_extra = get_document_extra(db)
        return jit_query_families_wrapper(
            _OPENSEARCH_CONNECTION,
            background_tasks=background_tasks,
            search_request_body=search_body,
            opensearch_internal_config=_OPENSEARCH_INDEX_CONFIG,
            document_extra_info=document_extra,
            preference="default_search_preference",
        )
    else:
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
                        **doc.dict(),
                        **{"document_postfix": postfix_map[doc.document_id]},
                    }
                )
                for doc in doc_results.documents
            ],
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
