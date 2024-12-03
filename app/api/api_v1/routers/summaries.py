"""
Summaries for pages.

Like searches but with pre-defined results based on the summary context.
"""

import logging
from typing import Annotated

from db_client.models.dfce import FamilyCategory, Geography
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from app.clients.db.session import get_db
from app.models.search import BrowseArgs, GeographySummaryFamilyResponse
from app.repository.lookups import get_country_slug_from_country_code, is_country_code
from app.repository.search import browse_rds_families
from app.service.custom_app import AppTokenFactory

_LOGGER = logging.getLogger(__name__)

summary_router = APIRouter()


@summary_router.get(
    "/summaries/geography/{geography_string}",
    summary="Gets a summary of the documents associated with a geography.",
    response_model=GeographySummaryFamilyResponse,
)
def search_by_geography(
    request: Request,
    geography_string: str,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
):
    """Searches the documents filtering by geography and grouping by category."""

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    geography_slug = None
    if is_country_code(db, geography_string):
        geography_slug = get_country_slug_from_country_code(db, geography_string)

    if geography_slug is None:
        geography_slug = geography_string

    _LOGGER.info(
        f"Getting geography summary for {geography_slug}",
        extra={
            "props": {"geography_slug": geography_slug},
            "allowed_corpora_ids": token.allowed_corpora_ids,
        },
    )

    exists = bool(
        db.query(Geography).filter_by(slug=geography_slug).one_or_none() is not None
    )
    if not exists:
        msg = (
            f"No geography with slug or country code matching '{geography_slug}' found"
        )
        _LOGGER.error(msg)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)

    top_families = {}
    family_counts = {}

    for cat in FamilyCategory:
        results = browse_rds_families(
            db,
            BrowseArgs(
                geography_slugs=[geography_slug],
                categories=[cat],
                offset=0,
                limit=None,
                corpora_ids=token.allowed_corpora_ids,
            ),
        )
        family_counts[cat] = len(results.families)
        top_families[cat] = list(results.families[:5])

    # TODO: Add targets
    targets = []

    return GeographySummaryFamilyResponse(
        family_counts=family_counts,
        top_families=top_families,
        targets=targets,
    )
