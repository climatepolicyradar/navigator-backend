"""Functions to support browsing the RDS document structure"""

from datetime import datetime
from logging import getLogger
from time import perf_counter_ns
from typing import cast

from db_client.models.dfce.family import (
    Corpus,
    DocumentStatus,
    Family,
    FamilyCorpus,
    FamilyDocument,
)
from db_client.models.organisation import Organisation
from sqlalchemy.orm import Session

from app.models.search import (
    BrowseArgs,
    SearchResponse,
    SearchResponseFamily,
    SortField,
    SortOrder,
)
from app.repository.geography import get_geo_subquery
from app.telemetry import observe

_LOGGER = getLogger(__name__)


@observe(name="to_search_response_family")
def to_search_response_family(
    family: Family,
    corpus: Corpus,
    geography_value: str,
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
        family_source=cast(str, organisation.name),
        corpus_import_id=cast(str, corpus.import_id),
        corpus_type_name=cast(str, corpus.corpus_type_name),
        family_geographies=[row.value for row in family.geographies],
        family_title_match=False,
        family_description_match=False,
        # ↓ Stuff we don't currently use for browse ↓
        total_passage_hits=0,
        family_metadata={},
        family_documents=[],
    )


@observe(name="browse_rds_families")
def browse_rds_families(db: Session, req: BrowseArgs) -> tuple[int, SearchResponse]:
    """Browse RDS"""

    t0 = perf_counter_ns()
    geo_subquery = get_geo_subquery(db, req.geography_slugs, req.country_codes)
    # Subquery to find families with at least one published document
    # Avoid using calculated family_status field
    published_families = (
        db.query(FamilyDocument.family_import_id)
        .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .distinct()
        .subquery()
    )
    query = (
        db.query(Family, Corpus, geo_subquery.c.value, Organisation)  # type: ignore
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .join(Corpus, FamilyCorpus.corpus_import_id == Corpus.import_id)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .join(
            published_families,
            published_families.c.family_import_id == Family.import_id,
        )
        .filter(geo_subquery.c.family_import_id == Family.import_id)  # type: ignore
    )

    if req.categories is not None:
        query = query.filter(Family.family_category.in_(req.categories))

    if req.corpora_ids is not None and req.corpora_ids != []:
        query = query.filter(Corpus.import_id.in_(req.corpora_ids))

    if req.sort_field == SortField.TITLE:
        if req.sort_order == SortOrder.DESCENDING:
            query = query.order_by(Family.title.desc())
        else:
            query = query.order_by(Family.title.asc())

    _LOGGER.debug("Starting families query")
    families_count = query.count()
    top_five_families = query.limit(5).all()
    families = [
        to_search_response_family(family, corpus, geography_value, organisation)
        for (family, corpus, geography_value, organisation) in top_five_families
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
    time_taken = int((perf_counter_ns() - t0) / 1e6)

    return (
        families_count,
        SearchResponse(
            hits=families_count,
            total_family_hits=families_count,
            query_time_ms=time_taken,
            total_time_ms=time_taken,
            families=families[offset : offset + limit],
        ),
    )
