"""Functions to support browsing the RDS document structure"""

from time import perf_counter
from typing import Optional, cast
from pydantic import BaseModel
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
)
from app.db.models.law_policy import Family, FamilyOrganisation
from app.db.models.app import Organisation
from sqlalchemy import extract
from sqlalchemy.orm import Session


class BrowseArgs(BaseModel):
    """Arguements for the browse_rds function"""

    geography_slug: Optional[str] = None
    country_code: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    category: Optional[str] = None


def to_search_response_family(
    family: Family,
    geography: Geography,
    organisation: Organisation,
) -> SearchResponseFamily:
    return SearchResponseFamily(
        family_slug=cast(str, family.slugs[-1].name),
        family_name=cast(str, family.title),
        family_description=cast(str, family.description),
        family_source=cast(str, organisation.name),  # FIXME: check links to source
        family_geography=cast(str, geography.value),
        family_metadata={},  # FIXME: Add metadata
        # ↓ Stuff we don't currently use for search ↓
        family_title_match=False,
        family_description_match=False,
        family_documents=[],  # TODO: are these required?
    )


def browse_rds_families(db: Session, req: BrowseArgs) -> SearchResponse:
    """Browse RDS"""

    t0 = perf_counter()
    query = (
        db.query(Family)
        .join(Geography, Family.geography_id == Geography.id)
        .join(FamilyOrganisation, FamilyOrganisation.family_id == Family.id)
        .join(Organisation, Organisation.id == FamilyOrganisation.organisation_id)
    )

    if req.geography_slug is not None:
        query = query.filter(Geography.slug == req.geography_slug)

    if req.country_code is not None:
        query = query.filter(Geography.value == req.country_code)

    # FIXME: sort out years for Families
    # if req.start_year is not None:
    #     query = query.filter(extract("year", Family.publication_ts) >= req.start_year)

    # if req.end_year is not None:
    #     query = query.filter(extract("year", Family.publication_ts) <= req.end_year)

    # query = query.order_by(Family.publication_ts.desc())

    if req.category is not None:
        query = query.filter(Family.category_name == req.category)

    families = [
        to_search_response_family(family, geography, organisation)
        for (family, geography, _, organisation) in query
    ]

    return SearchResponse(
        hits=len(families),
        query_time_ms=int((perf_counter() - t0) * 1e3),
        families=families,
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
        document_date=str(row["publication_ts"]),
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

    if req.geography_slug is not None:
        query = query.filter(Geography.slug == req.geography_slug)

    if req.country_code is not None:
        query = query.filter(Geography.value == req.country_code)

    if req.start_year is not None:
        query = query.filter(extract("year", Document.publication_ts) >= req.start_year)

    if req.end_year is not None:
        query = query.filter(extract("year", Document.publication_ts) <= req.end_year)

    if req.category is not None:
        query = query.filter(Category.name == req.category)

    query = query.order_by(Document.publication_ts.desc())

    documents = [to_search_resp_doc(dict(row)) for row in query.all()]

    return SearchResultsResponse(
        hits=len(documents),
        query_time_ms=int((perf_counter() - t0) * 1e3),
        documents=documents,
    )
