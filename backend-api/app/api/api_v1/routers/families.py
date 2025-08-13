import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request

from app.clients.db.session import get_db
from app.models.family import HomepageCountsResponse
from app.repository.family import (
    _convert_to_dto,
    count_families_per_category_per_corpus,
    count_families_per_category_per_corpus_latest_ingest_cycle,
)
from app.service.custom_app import AppTokenFactory
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__file__)

families_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


@families_router.get("/homepage-counts", response_model=HomepageCountsResponse)
def get_homepage_counts(
    request: Request, app_token: Annotated[str, Header()], db=Depends(get_db)
):
    """Get the count of families by category per corpus for the homepage."""
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    return _convert_to_dto(
        count_families_per_category_per_corpus(db, token.allowed_corpora_ids)
    )


@families_router.get(
    "/homepage-counts-latest-ingest-cycle", response_model=HomepageCountsResponse
)
def get_homepage_counts_latest_ingest_cycle(
    request: Request, app_token: Annotated[str, Header()], db=Depends(get_db)
):
    """Get the count of families by category per corpus for the homepage."""
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    return _convert_to_dto(
        count_families_per_category_per_corpus_latest_ingest_cycle(
            db, token.allowed_corpora_ids
        )
    )
