"""
Searches for documents.

All endpoints should perform document searches using the SearchRequestBody as
its input. The individual endpoints will return different responses tailored
for the type of document search being performed.
"""

import logging
from io import BytesIO
from typing import Annotated

from cpr_sdk.exceptions import QueryError
from cpr_sdk.search_adaptors import VespaSearchAdapter
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from starlette.responses import RedirectResponse

from app.api.api_v1.schemas.search import SearchRequestBody, SearchResponse
from app.core.aws import S3Document, get_s3_client
from app.core.config import (
    AWS_REGION,
    CDN_DOMAIN,
    DOC_CACHE_BUCKET,
    INGEST_TRIGGER_ROOT,
    PIPELINE_BUCKET,
    PUBLIC_APP_URL,
    VESPA_SECRETS_LOCATION,
    VESPA_URL,
)
from app.core.download import create_data_download_zip_archive
from app.core.search import (
    ENCODER,
    create_vespa_search_params,
    process_result_into_csv,
    process_vespa_search_response,
)
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

_VESPA_CONNECTION = VespaSearchAdapter(
    instance_url=VESPA_URL,
    cert_directory=VESPA_SECRETS_LOCATION,
    embedder=ENCODER,
)

search_router = APIRouter()


def _search_request(db: Session, search_body: SearchRequestBody) -> SearchResponse:
    is_browse_request = not search_body.query_string
    if is_browse_request:
        search_body.all_results = True
        search_body.documents_only = True
        search_body.exact_match = False
    try:
        data_access_search_params = create_vespa_search_params(db, search_body)
        data_access_search_response = _VESPA_CONNECTION.search(
            parameters=data_access_search_params
        )
    except QueryError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Query: {' '.join(e.args)}",
        )
    return process_vespa_search_response(
        db,
        data_access_search_response,
        limit=search_body.page_size,
        offset=search_body.offset,
    ).increment_pages()


@search_router.post("/searches")
def search_documents(
    request: Request,
    search_body: Annotated[
        SearchRequestBody,
        Body(
            openapi_examples={
                "simple": {
                    "summary": "Simple Search Example",
                    "description": "Perform a simple search for matching passages",
                    "value": {
                        "query_string": "Energy Prices",
                    },
                },
                "browse request": {
                    "summary": "Browse Example",
                    "description": "Perform a browse request within a year range",
                    "value": {
                        "query_string": "",
                        "year_range": [2000, None],
                        "sort_field": "date",
                        "sort_order": "desc",
                    },
                },
                "filters": {
                    "summary": "Filter example",
                    "description": "Filtering and using exact match",
                    "value": {
                        "query_string": "Just transition",
                        "exact_match": True,
                        "keyword_filters": {
                            "sources": ["CCLW"],
                            "categories": ["Legislative"],
                        },
                    },
                },
            }
        ),
    ],
    db=Depends(get_db),
) -> SearchResponse:
    """
    Search for documents matching the search criteria and filters.

    There is no authentication required for using this interface. We ask that users be
    respectful of its use and remind users that data is available to download on
    request.

    The search endpoint behaves in two distinct ways:
        - "Browse" mode is when an empty `query_string` is provided. This is intended
        for document level search using filters. Individual passages are not returned.
        - "Search" mode is when a `query_string` is present. This matches against
        individual document passages.

    The request and response object is otherwise identical for both.

    The results can be paginated via a combination of limit, offset and continuation
    tokens. The limit/offset slices the results after they have been retrieved from
    the search database. The continuation token can be used to get the next set of
    results from the search database. See the request schema for more details.
    """
    _LOGGER.info(
        "Search request",
        extra={
            "props": {
                "search_request": search_body.model_dump(),
            }
        },
    )

    _LOGGER.info(
        "Starting search...",
    )
    return _search_request(db=db, search_body=search_body)


@search_router.post("/searches/download-csv", include_in_schema=False)
def download_search_documents(
    request: Request,
    search_body: SearchRequestBody,
    db=Depends(get_db),
) -> StreamingResponse:
    """Download a CSV containing details of documents matching the search criteria."""

    _LOGGER.info(
        "Search download request",
        extra={
            "props": {
                "search_request": search_body.model_dump(),
            }
        },
    )
    is_browse = not bool(search_body.query_string)

    _LOGGER.info(
        "Starting search...",
    )
    search_response = _search_request(
        db=db,
        search_body=search_body,
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


@search_router.get("/searches/download-all-data", include_in_schema=False)
def download_all_search_documents(db=Depends(get_db)) -> RedirectResponse:
    """Download a CSV containing details of all the documents in the corpus."""
    _LOGGER.info("Whole data download request")

    if PIPELINE_BUCKET is None or PUBLIC_APP_URL is None or DOC_CACHE_BUCKET is None:
        if PIPELINE_BUCKET is None:
            _LOGGER.error("{PIPELINE_BUCKET} is not set")
        if PUBLIC_APP_URL is None:
            _LOGGER.error("{PUBLIC_APP_URL} is not set")
        if DOC_CACHE_BUCKET is None:
            _LOGGER.error("{DOC_CACHE_BUCKET} is not set")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Missing required environment variables",
        )

    s3_client = get_s3_client()
    latest_ingest_start = s3_client.get_latest_ingest_start(
        PIPELINE_BUCKET, INGEST_TRIGGER_ROOT
    )

    s3_prefix = "navigator/dumps"
    data_dump_s3_key = f"{s3_prefix}/whole_data_dump-{latest_ingest_start}.zip"

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
            f"Generating {aws_env} dump for ingest cycle w/c {latest_ingest_start}..."
        )

        # After writing to a file buffer the position stays at the end whereas when you
        # upload a buffer, it starts from the position it is currently in. We need to
        # add the seek(0) to reset the buffer position to the beginning before writing
        # to S3 to avoid creating an empty file.
        zip_buffer = create_data_download_zip_archive(latest_ingest_start, db)
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

    _LOGGER.info(
        f"Can't find data dump for {latest_ingest_start} in {DOC_CACHE_BUCKET}"
    )
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
