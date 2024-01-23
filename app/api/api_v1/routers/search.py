"""
Searches for documents.

All endpoints should perform document searches using the SearchRequestBody as
its input. The individual endpoints will return different responses tailored
for the type of document search being performed.
"""
import json
import logging
from datetime import datetime
from io import BytesIO
from typing import Mapping, Sequence

from cpr_data_access.exceptions import QueryError
from cpr_data_access.search_adaptors import VespaSearchAdapter
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.search import SearchRequestBody, SearchResponse, SortField
from app.core.aws import S3Document, get_s3_client
from app.core.browse import BrowseArgs, browse_rds_families
from app.core.config import (
    AWS_REGION,
    DOC_CACHE_BUCKET,
    INGEST_CYCLE_START,
    PUBLIC_APP_URL,
    VESPA_SECRETS_LOCATION,
    VESPA_URL,
)
from app.core.download import (
    generate_data_dump_as_csv,
)
from app.core.lookups import get_countries_for_region, get_country_by_slug
from app.core.search import (
    ENCODER,
    FilterField,
    OpenSearchConfig,
    OpenSearchConnection,
    OpenSearchQueryConfig,
    create_vespa_search_params,
    process_result_into_csv,
    process_vespa_search_response,
)
from app.db.crud.document import DocumentExtraCache
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

# Use configured environment for router
_OPENSEARCH_CONFIG = OpenSearchConfig()
_OPENSEARCH_CONNECTION = OpenSearchConnection(opensearch_config=_OPENSEARCH_CONFIG)
_OPENSEARCH_INDEX_CONFIG = OpenSearchQueryConfig()
_DOCUMENT_EXTRA_INFO_CACHE = DocumentExtraCache()

_VESPA_CONNECTION = VespaSearchAdapter(
    instance_url=VESPA_URL,
    cert_directory=VESPA_SECRETS_LOCATION,
    embedder=ENCODER,
)

search_router = APIRouter()


def _search_request(
    db: Session, search_body: SearchRequestBody, use_vespa: bool = True
) -> SearchResponse:
    if search_body.keyword_filters is not None and use_vespa is False:
        search_body.keyword_filters = process_search_keyword_filters(
            db,
            search_body.keyword_filters,
        )
    is_browse_request = not search_body.query_string
    if is_browse_request:
        # Service browse requests from RDS
        return browse_rds_families(
            db=db,
            req=_get_browse_args_from_search_request_body(search_body),
        )
    else:
        if use_vespa:
            data_access_search_params = create_vespa_search_params(db, search_body)
            # TODO: we may wish to cache responses to improve pagination performance
            try:
                data_access_search_response = _VESPA_CONNECTION.search(
                    parameters=data_access_search_params
                )
            except QueryError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Query"
                )
            return process_vespa_search_response(
                db,
                data_access_search_response,
                limit=search_body.limit,
                offset=search_body.offset,
            ).increment_pages()
        else:
            return _OPENSEARCH_CONNECTION.query_families(
                search_request_body=search_body,
                opensearch_internal_config=_OPENSEARCH_INDEX_CONFIG,
                document_extra_info=_DOCUMENT_EXTRA_INFO_CACHE.get_document_extra_info(
                    db
                ),
                preference="default_search_preference",
            ).increment_pages()


@search_router.post("/searches")
def search_documents(
    request: Request,
    search_body: SearchRequestBody,
    db=Depends(get_db),
    use_vespa: bool = True,
) -> SearchResponse:
    """Search for documents matching the search criteria."""
    _LOGGER.info(
        "Search request",
        extra={
            "props": {
                "search_request": json.loads(search_body.json()),
            }
        },
    )

    _LOGGER.info(
        "Starting search...",
    )
    return _search_request(db=db, search_body=search_body, use_vespa=use_vespa)


@search_router.post("/searches/download-csv")
def download_search_documents(
    request: Request,
    search_body: SearchRequestBody,
    db=Depends(get_db),
    use_vespa: bool = True,
) -> StreamingResponse:
    """Download a CSV containing details of documents matching the search criteria."""
    _LOGGER.info(
        "Search download request",
        extra={
            "props": {
                "search_request": json.loads(search_body.json()),
            }
        },
    )
    # Always download all results
    search_body.offset = 0
    # TODO: properly configure the limit - for now we override if under 100 or not set
    search_body.limit = max(search_body.limit, 100)
    is_browse = not bool(search_body.query_string)

    _LOGGER.info(
        "Starting search...",
    )
    search_response = _search_request(
        db=db,
        search_body=search_body,
        use_vespa=use_vespa,
    )
    content_str = process_result_into_csv(db, search_response, is_browse=is_browse)

    _LOGGER.debug(f"Downloading search results as CSV: {content_str}")
    return StreamingResponse(
        content=BytesIO(content_str.encode("utf-8")),
        headers={
            "Content-Type": "text/csv",
            "Content-Disposition": "attachment; filename=results.csv",
        },
    )


@search_router.get("/searches/download-all-data")
def download_all_search_documents(db=Depends(get_db)) -> StreamingResponse:
    """Download a CSV containing details of all the documents in the corpus."""
    _LOGGER.info("Whole data download request")

    if INGEST_CYCLE_START is None or PUBLIC_APP_URL is None or DOC_CACHE_BUCKET is None:
        if INGEST_CYCLE_START is None:
            _LOGGER.error("{INGEST_CYCLE_START} is not set")
        if PUBLIC_APP_URL is None:
            _LOGGER.error("{PUBLIC_APP_URL} is not set")
        if DOC_CACHE_BUCKET is None:
            _LOGGER.error("{DOC_CACHE_BUCKET} is not set")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Missing required environment variables",
        )

    aws_environment = "production" if "dev" not in PUBLIC_APP_URL else "staging"
    data_dump_s3_key = f"navigator/{aws_environment}_data_dump_{INGEST_CYCLE_START}.csv"

    s3_client = get_s3_client()
    valid_credentials = s3_client.is_connected()

    s3_document = S3Document(DOC_CACHE_BUCKET, AWS_REGION, data_dump_s3_key)
    if not s3_client.document_exists(s3_document):
        _LOGGER.info(f"Generating dump for ingest cycle w/c {INGEST_CYCLE_START}...")
        df_as_csv = generate_data_dump_as_csv(db)

        if valid_credentials is False:
            _LOGGER.error("Cannot connect to AWS.")
        else:
            response = s3_client.upload_fileobj(
                df_as_csv, DOC_CACHE_BUCKET, data_dump_s3_key
            )
            if response is False:
                _LOGGER.error("Failed to upload object to s3: %s", response)

        if s3_client.document_exists(s3_document):
            _LOGGER.debug("Finished uploading data dump to s3")

    else:
        _LOGGER.debug("File already exists in S3. Fetching...")

    s3_file = s3_client.download_file(s3_document)

    _LOGGER.debug(f"Downloading all documents as of '{INGEST_CYCLE_START}' as CSV")
    timestamp = datetime.now()
    filename = f"whole_database_dump-{timestamp}.csv"
    return StreamingResponse(
        content=BytesIO(s3_file.read()),
        headers={
            "Content-Type": "text/csv",
            "Content-Disposition": f"attachment; filename={filename}",
        },
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
