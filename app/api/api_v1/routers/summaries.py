"""
Summaries for pages.

Like searches but with pre-defined results based on the summary context.
"""
import logging
from fastapi import APIRouter, Depends, Request

from app.api.api_v1.schemas.search import (
    CategoryName,
    GeographySummaryResponse,
    GeographySummaryDocumentResponse,
    GeographySummaryFamilyResponse,
)
from app.core.browse import BrowseArgs, browse_rds, browse_rds_families
from app.db.models.law_policy import FamilyCategory
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

summary_router = APIRouter()


@summary_router.get(
    "/summaries/country/{geography_slug}",
    summary="Gets a summary of the documents associated with a country.",
    response_model=GeographySummaryResponse,
)
@summary_router.get(
    "/summaries/geography/{geography_slug}",
    summary="Gets a summary of the documents associated with a geography.",
    response_model=GeographySummaryResponse,
)
def search_by_country(
    request: Request,
    geography_slug: str,
    db=Depends(get_db),
    group_documents: bool = False,
):
    """Searches the documents filtering by country and grouping by category."""
    _LOGGER.info(
        f"Getting geography summary for {geography_slug}",
        extra={"props": {"geography_slug": geography_slug}},
    )
    if group_documents:
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
    else:
        top_documents = {}
        document_counts = {}

        for cat in CategoryName:
            results = browse_rds(
                db, BrowseArgs(geography_slugs=[geography_slug], categories=[cat])
            )
            document_counts[cat] = len(results.documents)
            top_documents[cat] = list(results.documents[:5])

        # TODO: Add targets
        targets = []

        return GeographySummaryDocumentResponse(
            document_counts=document_counts,
            top_documents=top_documents,
            targets=targets,
        )
