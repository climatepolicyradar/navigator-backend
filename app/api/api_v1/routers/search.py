"""
Searches for documents.

All endpoints should perform document searches using the SearchRequestBody as
its input. The individual endpoints will return different responses tailored
for the type of document search being performed.
"""
import json
import logging
from io import BytesIO
from typing import Mapping, Sequence, Optional

from cpr_data_access.exceptions import QueryError
from cpr_data_access.models.search import filter_fields
from cpr_data_access.search_adaptors import VespaSearchAdapter
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from starlette.responses import RedirectResponse

from app.api.api_v1.schemas.search import SearchRequestBody, SearchResponse, SortField
from app.core.aws import S3Document, get_s3_client
from app.core.browse import BrowseArgs, browse_rds_families
from app.core.config import (
    AWS_REGION,
    CDN_DOMAIN,
    DOC_CACHE_BUCKET,
    INGEST_CYCLE_START,
    PUBLIC_APP_URL,
    VESPA_SECRETS_LOCATION,
    VESPA_URL,
)
from app.core.download import create_data_download_zip_archive
from app.core.search import (
    ENCODER,
    FilterField,
    create_vespa_search_params,
    process_result_into_csv,
    process_vespa_search_response,
    _convert_filters,
)
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

_VESPA_CONNECTION = VespaSearchAdapter(
    instance_url=VESPA_URL,
    cert_directory=VESPA_SECRETS_LOCATION,
    embedder=ENCODER,
)

search_router = APIRouter()


def _search_request(
    db: Session, search_body: SearchRequestBody, use_vespa: bool = True
) -> SearchResponse:
    is_browse_request = not search_body.query_string
    if is_browse_request:
        # Service browse requests from RDS
        if search_body.keyword_filters is not None:
            search_body.keyword_filters = process_search_keyword_filters(
                db,
                search_body.keyword_filters,
            )

        return browse_rds_families(
            db=db,
            req=_get_browse_args_from_search_request_body(search_body),
        )
    else:
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
def download_all_search_documents(db=Depends(get_db)) -> RedirectResponse:
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

    s3_prefix = "navigator/dumps"
    data_dump_s3_key = f"{s3_prefix}/whole_data_dump-{INGEST_CYCLE_START}.zip"

    s3_client = get_s3_client()
    valid_credentials = s3_client.is_connected()
    if not valid_credentials:
        _LOGGER.info("Error connecting to S3 AWS")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Error connecting to AWS"
        )

    s3_document = S3Document(DOC_CACHE_BUCKET, AWS_REGION, data_dump_s3_key)
    if valid_credentials is True and (not s3_client.document_exists(s3_document)):
        aws_env = "production" if "dev" not in PUBLIC_APP_URL else "staging"
        _LOGGER.info(
            f"Generating {aws_env} dump for ingest cycle w/c {INGEST_CYCLE_START}..."
        )

        # After writing to a file buffer the position stays at the end whereas when you
        # upload a buffer, it starts from the position it is currently in. We need to
        # add the seek(0) to reset the buffer position to the beginning before writing
        # to S3 to avoid creating an empty file.
        zip_buffer = create_data_download_zip_archive(INGEST_CYCLE_START, db)
        zip_buffer.seek(0)

        try:
            response = s3_client.upload_fileobj(
                bucket=DOC_CACHE_BUCKET,
                key=data_dump_s3_key,
                content_type="application/zip",
                fileobj=zip_buffer,
            )
            if response is False:
                _LOGGER.error("Failed to upload archive to s3: %s", response)
            else:
                _LOGGER.info(f"Finished uploading data archive to {DOC_CACHE_BUCKET}")

        except Exception as e:
            _LOGGER.error(e)

    s3_document = S3Document(DOC_CACHE_BUCKET, AWS_REGION, data_dump_s3_key)
    if s3_client.document_exists(s3_document):
        _LOGGER.info("Redirecting to CDN data dump location...")
        redirect_url = f"https://{CDN_DOMAIN}/{data_dump_s3_key}"
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    _LOGGER.info(f"Can't find data dump for {INGEST_CYCLE_START} in {DOC_CACHE_BUCKET}")
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


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
) -> Optional[Mapping[FilterField, Sequence[str]]]:
    filters = _convert_filters(db, request_filters)
    if not filters:
        return None

    # Switch back to pg names needed for browse
    filter_fields_switch_back = {v: k for k, v in filter_fields.items()}
    # Special case for browse where regions/countries are treated as countries
    filter_fields_switch_back["family_geography"] = "countries"

    filter_map = {}
    for key, value in filters.items():
        sorted_values = sorted(list(set(value)))
        filter_map[filter_fields_switch_back[key]] = sorted_values

    return filter_map
