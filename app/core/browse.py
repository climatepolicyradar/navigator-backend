"""Functions to support browsing the RDS document structure"""

from datetime import datetime
from logging import getLogger
from time import perf_counter
from typing import Optional, Sequence, cast

from pydantic import BaseModel
from sqlalchemy import extract
from sqlalchemy.orm import Session

from app.db.models.deprecated.document import (
    Category,
    Document,
    Geography,
    DocumentType,
)
from app.api.api_v1.schemas.search import (
    SearchDocumentResponse,
    SearchResponse,
    SearchResponseFamily,
    SearchResultsResponse,
    SortField,
    SortOrder,
)
from app.db.models.law_policy.family import (
    Family,
    FamilyOrganisation,
    FamilyStatus,
)
from app.db.models.app import Organisation

_LOGGER = getLogger(__name__)


class BrowseArgs(BaseModel):
    """Arguments for the browse_rds function"""

    geography_slugs: Optional[Sequence[str]] = None
    country_codes: Optional[Sequence[str]] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    categories: Optional[Sequence[str]] = None
    sort_field: SortField = SortField.DATE
    sort_order: SortOrder = SortOrder.DESCENDING
    offset: Optional[int] = 0
    limit: Optional[int] = 10


def to_search_response_family(
    family: Family,
    geography: Geography,
    organisation: Organisation,
) -> SearchResponseFamily:
    family_published_date = ""
    if family.published_date is not None:
        family_published_date = family.published_date.isoformat()

    family_last_updated_date = ""
    if family.last_updated_date is not None:
        family_last_updated_date = family.last_updated_date.isoformat()

    return SearchResponseFamily(
        family_slug=cast(str, family.slugs[-1].name),
        family_name=cast(str, family.title),
        family_description=cast(str, family.description),
        family_category=str(family.family_category),
        family_date=family_published_date,
        family_last_updated_date=family_last_updated_date,
        family_source=cast(str, organisation.name),  # FIXME: check links to source
        family_geography=cast(str, geography.value),
        family_metadata={},  # FIXME: Add metadata
        # ↓ Stuff we don't currently use for search ↓
        family_title_match=False,
        family_description_match=False,
        family_documents=[],  # TODO: are these required?
    )


def browse_rds_families(
    db: Session,
    req: BrowseArgs,
) -> SearchResponse:
    """Browse RDS"""

    t0 = perf_counter()
    query = (
        db.query(Family, Geography, Organisation)
        .join(Geography, Family.geography_id == Geography.id)
        .join(
            FamilyOrganisation, FamilyOrganisation.family_import_id == Family.import_id
        )
        .join(Organisation, Organisation.id == FamilyOrganisation.organisation_id)
        .filter(Family.family_status == FamilyStatus.PUBLISHED)
    )

    if req.geography_slugs is not None:
        query = query.filter(Geography.slug.in_(req.geography_slugs))

    if req.country_codes is not None:
        query = query.filter(Geography.value.in_(req.country_codes))

    if req.categories is not None:
        query = query.filter(Family.family_category.in_(req.categories))

    if req.sort_field == SortField.TITLE:
        if req.sort_order == SortOrder.DESCENDING:
            query = query.order_by(Family.title.desc())
        else:
            query = query.order_by(Family.title.asc())

    _LOGGER.debug("Starting families query")
    families = [
        to_search_response_family(family, geography, organisation)
        for (family, geography, organisation) in query.all()
    ]
    _LOGGER.debug("Finished families query")

    # Dates are calculated, and therefore sorting cannot be implemented in the query
    if req.start_year is not None:
        compare_date = datetime(year=req.start_year, month=1, day=1).isoformat()
        families = list(
            filter(
                lambda f: f.family_date != "" and f.family_date >= compare_date,
                families,
            )
        )

    if req.end_year is not None:
        compare_date = datetime(year=req.end_year, month=12, day=31).isoformat()
        families = list(
            filter(
                lambda f: f.family_date != "" and f.family_date <= compare_date,
                families,
            )
        )

    if req.sort_field == SortField.DATE:
        families = sorted(
            list(filter(lambda f: f.family_date != "", families)),
            key=lambda f: f.family_date,
            reverse=req.sort_order == SortOrder.DESCENDING,
        ) + list(filter(lambda f: f.family_date == "", families))

    offset = req.offset or 0
    limit = req.limit or len(families)

    return SearchResponse(
        hits=len(families),
        query_time_ms=int((perf_counter() - t0) * 1e3),
        families=families[offset : offset + limit],
    )


############################################################
######################## DEPRECATED ########################
############################################################
def to_search_resp_doc(row: dict) -> SearchDocumentResponse:
    return SearchDocumentResponse(
        document_id=row["import_id"],
        document_slug=row["slug"],
        document_name=row["name"],
        document_description=row["description"],
        document_date=row["publication_ts"].strftime("%d/%m/%Y"),
        document_category=row["category"],
        document_geography=row["country_code"],
        # ↓ Stuff we don't currently use for search ↓
        document_sectors=[],  # empty placeholder for tests to pass
        document_source="",
        document_type="",
        document_source_url="",
        document_url="",
        document_content_type="",
        document_title_match=False,
        document_description_match=False,
        document_passage_matches=[],
        document_postfix="",
    )


def browse_rds(db: Session, req: BrowseArgs) -> SearchResultsResponse:
    """Browse RDS"""

    t0 = perf_counter()
    query = (
        db.query(
            Document.slug,
            Document.import_id,
            Document.name,
            Document.description,
            Document.publication_ts,
            Category.name.label("category"),
            Geography.display_value.label("country_name"),
            Geography.value.label("country_code"),
        )
        .join(Geography, Document.geography_id == Geography.id)
        .join(DocumentType, Document.type_id == DocumentType.id)
        .join(Category, Document.category_id == Category.id)
    )

    if req.geography_slugs is not None:
        query = query.filter(Geography.slug.in_(req.geography_slugs))

    if req.country_codes is not None:
        query = query.filter(Geography.value.in_(req.country_codes))

    if req.start_year is not None:
        query = query.filter(extract("year", Document.publication_ts) >= req.start_year)

    if req.end_year is not None:
        query = query.filter(extract("year", Document.publication_ts) <= req.end_year)

    if req.categories is not None:
        query = query.filter(Category.name.in_(req.categories))

    if req.sort_field == SortField.DATE:
        if req.sort_order == SortOrder.DESCENDING:
            query = query.order_by(Document.publication_ts.desc().nulls_last())
        else:
            query = query.order_by(Document.publication_ts.asc().nulls_last())
    else:
        if req.sort_order == SortOrder.DESCENDING:
            query = query.order_by(Document.name.desc())
        else:
            query = query.order_by(Document.name.asc())

    documents = [to_search_resp_doc(dict(row)) for row in query.all()]

    offset = req.offset or 0
    limit = req.limit or len(documents)

    return SearchResultsResponse(
        hits=len(documents),
        query_time_ms=int((perf_counter() - t0) * 1e3),
        documents=documents[offset : offset + limit],
    )
