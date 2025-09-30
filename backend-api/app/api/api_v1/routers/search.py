"""
Searches for documents.

All endpoints should perform document searches using the SearchRequestBody as
its input. The individual endpoints will return different responses tailored
for the type of document search being performed.


FastAPI docs:
> If you are using third party libraries that tell you to call them with await, like:

> results = await some_library()
> Then, declare your path operation functions with async def like:

> @app.get('/')
> async def read_results():
>     results = await some_library()
>     return results

https://fastapi.tiangolo.com/async/
"""

import logging
from io import BytesIO
from typing import Annotated, Sequence, cast

from cpr_sdk.search_adaptors import VespaSearchAdapter
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from starlette.responses import RedirectResponse

from app.clients.aws.client import get_s3_client
from app.clients.aws.s3_document import S3Document
from app.clients.db.session import get_db
from app.config import (
    AWS_REGION,
    DOCUMENT_CACHE_BUCKET,
    INGEST_TRIGGER_ROOT,
    PIPELINE_BUCKET,
    PUBLIC_APP_URL,
)
from app.errors import ValidationError
from app.models.search import SearchRequestBody, SearchResponse
from app.service.custom_app import AppTokenFactory
from app.service.download import (
    create_data_download_zip_archive,
    process_result_into_csv,
)
from app.service.search import get_s3_doc_url_from_cdn, make_search_request
from app.service.vespa import get_vespa_search_adapter
from app.telemetry import convert_to_loggable_string
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__name__)


search_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


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
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
    vespa_search_adapter: VespaSearchAdapter = Depends(get_vespa_search_adapter),
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
                "search_request": convert_to_loggable_string(search_body.model_dump()),
                "app_token": str(app_token),
            }
        },
    )

    # Decode the app token and validate it.
    #
    # First corpora validation is app token against DB. At least one of the app token
    # corpora IDs must be present in the DB to continue the search request.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    # If the search request IDs are null, we want to search using the app token corpora.
    if search_body.corpus_import_ids == [] or search_body.corpus_import_ids is None:
        search_body.corpus_import_ids = cast(Sequence, token.allowed_corpora_ids)

    # For the second validation, search request corpora Ids are validated against the
    # app token corpora IDs if the search request param 'corpus_import_ids' is not None.
    # corpus_import_ids must be a subset of app token IDs.
    token.validate_subset(
        set(search_body.corpus_import_ids), cast(set, token.allowed_corpora_ids)
    )

    _LOGGER.info(
        "Starting search...",
        extra={
            "props": {
                "search_request": convert_to_loggable_string(search_body.model_dump())
            }
        },
    )
    return make_search_request(
        db=db,
        search_body=search_body,
        vespa_search_adapter=vespa_search_adapter,
    )


@search_router.post("/searches/download-csv", include_in_schema=False)
def download_search_documents(
    request: Request,
    search_body: SearchRequestBody,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
    vespa_search_adapter: VespaSearchAdapter = Depends(get_vespa_search_adapter),
) -> StreamingResponse:
    """Download a CSV containing details of documents matching the search criteria."""
    token = AppTokenFactory()

    _LOGGER.info(
        "Search download request",
        extra={
            "props": {
                "search_request": convert_to_loggable_string(search_body.model_dump()),
                "app_token": str(app_token),
            }
        },
    )

    # Decode the app token and validate it.
    #
    # First corpora validation is app token against DB. At least one of the app token
    # corpora IDs must be present in the DB to continue the search request.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    # If the search request IDs are null, we want to search using the app token corpora.
    if search_body.corpus_import_ids == [] or search_body.corpus_import_ids is None:
        search_body.corpus_import_ids = cast(Sequence, token.allowed_corpora_ids)

    # For the second validation, search request corpora Ids are validated against the
    # app token corpora IDs if the search request param 'corpus_import_ids' is not None.
    # corpus_import_ids must be a subset of app token IDs.
    token.validate_subset(
        set(search_body.corpus_import_ids), cast(set, token.allowed_corpora_ids)
    )

    is_browse = not bool(search_body.query_string)

    _LOGGER.info(
        "Starting search...",
        extra={
            "props": {
                "search_request": convert_to_loggable_string(search_body.model_dump())
            }
        },
    )
    try:
        search_response = make_search_request(
            db=db,
            search_body=search_body,
            vespa_search_adapter=vespa_search_adapter,
        )
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Query: {' '.join(e.args)}",
        )

    content_str = process_result_into_csv(
        db, search_response, token.aud, is_browse=is_browse, theme=token.sub
    )

    _LOGGER.debug(f"Downloading search results as CSV: {content_str}")
    return StreamingResponse(
        content=BytesIO(content_str.encode("utf-8")),
        headers={
            "Content-Type": "text/csv",
            "Content-Disposition": "attachment; filename=results.csv",
        },
    )


@search_router.get("/searches/download-all-data", include_in_schema=False)
def download_all_search_documents(
    request: Request, app_token: Annotated[str, Header()], db=Depends(get_db)
) -> RedirectResponse:
    """Download a CSV containing details of all the documents in the corpus."""
    _LOGGER.info(
        "Whole data download request",
        extra={
            "props": {
                "app_token": str(app_token),
            }
        },
    )

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    if (
        PIPELINE_BUCKET is None
        or PUBLIC_APP_URL is None
        or DOCUMENT_CACHE_BUCKET is None
    ):
        if PIPELINE_BUCKET is None:
            _LOGGER.error("{PIPELINE_BUCKET} is not set")
        if PUBLIC_APP_URL is None:
            _LOGGER.error("{PUBLIC_APP_URL} is not set")
        if DOCUMENT_CACHE_BUCKET is None:
            _LOGGER.error("{DOCUMENT_CACHE_BUCKET} is not set")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Missing required environment variables",
        )

    s3_client = get_s3_client()
    latest_ingest_start = s3_client.get_latest_ingest_start(
        PIPELINE_BUCKET, INGEST_TRIGGER_ROOT
    )

    s3_prefix = "navigator/dumps"
    data_dump_s3_key = (
        f"{s3_prefix}/{token.sub}-whole_data_dump-{latest_ingest_start}.zip"
    )

    valid_credentials = s3_client.is_connected()
    if not valid_credentials:
        _LOGGER.info("Error connecting to S3 AWS")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Error connecting to AWS"
        )

    s3_document = S3Document(DOCUMENT_CACHE_BUCKET, AWS_REGION, data_dump_s3_key)
    if valid_credentials is True and (not s3_client.document_exists(s3_document)):
        aws_env = (
            "production"
            if all(x not in PUBLIC_APP_URL.lower() for x in ["staging", "localhost"])
            else "staging"
        )
        _LOGGER.info(
            f"Generating {token.sub} {aws_env} dump for ingest cycle w/c {latest_ingest_start}..."
        )

        # After writing to a file buffer the position stays at the end whereas when you
        # upload a buffer, it starts from the position it is currently in. We need to
        # add the seek(0) to reset the buffer position to the beginning before writing
        # to S3 to avoid creating an empty file.
        # Handle case where PUBLIC_APP_URL and token audience might already include
        # protocol
        is_localhost = "localhost" in PUBLIC_APP_URL.lower()
        scheme = "http" if is_localhost else "https"
        if is_localhost or token.aud is None:
            if PUBLIC_APP_URL.lower().startswith(("http://", "https://")):
                url_base = PUBLIC_APP_URL.lower()
            else:
                url_base = f"{scheme}://{PUBLIC_APP_URL.lower()}"

        else:
            url_base = (
                f"{scheme}://{token.aud.lower()}"
                if not token.aud.lower().startswith(("http://", "https://"))
                else token.aud.lower()
            )

        zip_buffer = create_data_download_zip_archive(
            latest_ingest_start,
            token.allowed_corpora_ids,
            db,
            token.sub.lower() if token.sub else None,
            url_base,
        )
        zip_buffer.seek(0)

        try:
            response = s3_client.upload_fileobj(
                bucket=DOCUMENT_CACHE_BUCKET,
                key=data_dump_s3_key,
                content_type="application/zip",
                fileobj=zip_buffer,
            )
            if response is False:
                _LOGGER.error("Failed to upload archive to s3: %s", response)
            else:
                _LOGGER.info(
                    f"Finished uploading data archive to {DOCUMENT_CACHE_BUCKET}"
                )

        except Exception as e:
            _LOGGER.error(e)

    s3_document = S3Document(DOCUMENT_CACHE_BUCKET, AWS_REGION, data_dump_s3_key)
    redirect_url = get_s3_doc_url_from_cdn(s3_client, s3_document, data_dump_s3_key)
    if redirect_url is not None:
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    _LOGGER.info(
        f"Can't find data dump for {latest_ingest_start} in {DOCUMENT_CACHE_BUCKET}"
    )
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
