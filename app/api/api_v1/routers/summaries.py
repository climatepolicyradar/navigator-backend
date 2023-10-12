"""
Summaries for pages.

Like searches but with pre-defined results based on the summary context.
"""
import logging
from fastapi import APIRouter, Depends, Request

from app.api.api_v1.schemas.search import GeographySummaryFamilyResponse
from app.core.browse import BrowseArgs, browse_rds_families
from app.core.lookups import get_country_slug_from_country_code, is_country_code
from app.db.models.law_policy import FamilyCategory
from app.db.session import get_db

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
    db=Depends(get_db),
):
    """Searches the documents filtering by geography and grouping by category."""

    geography_slug = None
    if is_country_code(db, geography_string):
        geography_slug = get_country_slug_from_country_code(db, geography_string)

    if geography_slug is None:
        geography_slug = geography_string

    _LOGGER.info(
        f"Getting geography summary for {geography_slug}",
        extra={"props": {"geography_slug": geography_slug}},
    )
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
