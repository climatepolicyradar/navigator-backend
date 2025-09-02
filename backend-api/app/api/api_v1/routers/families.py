import logging
from typing import Annotated

from db_client.models.dfce.family import (
    Corpus,
    DocumentStatus,
    Family,
    FamilyCorpus,
    FamilyDocument,
    FamilyGeography,
)
from db_client.models.dfce.geography import Geography
from db_client.models.dfce.metadata import FamilyMetadata
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import func
from sqlalchemy.orm import lazyload

from app.clients.db.session import get_db
from app.models.search import LatestUpdatedFamilyResponse
from app.repository.family import (
    _convert_to_dto,
    count_families_per_category_per_corpus,
    count_families_per_category_per_corpus_latest_ingest_cycle,
)
from app.service.custom_app import AppTokenFactory
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__file__)

families_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


@families_router.get("/homepage-counts", response_model=dict[str, int])
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
    "/homepage-counts-latest-ingest-cycle", response_model=dict[str, int]
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


@families_router.get(
    "/latest-published",
    summary="Gets five most recently published families.",
)
def latest_published(
    request: Request,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
) -> list[LatestUpdatedFamilyResponse]:
    """Retrieve the five most recently published families.

    This endpoint returns the latest five published family records,
    sorted by their modified date in descending order.

    :param Request request: The incoming request object.
    :param Annotated[str, Header()] app_token: App token containing
        the allowed corpora access.
    :param Depends[get_db] db: Database session dependency.
    :return list[LatestUpdateFamilyResponse]: A list of the five most recently published
        families.
    """

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    allowed_corpora_ids = token.allowed_corpora_ids

    if not allowed_corpora_ids:
        _LOGGER.error(
            "No allowed corpora IDs found in the app token",
            extra={
                "props": {"allowed_corpora_ids": allowed_corpora_ids},
            },
        )
        raise HTTPException(
            status_code=400,
            detail="No corpora ids provided.",
        )

    _LOGGER.info(
        "Getting latest published families",
        extra={
            "allowed_corpora_ids": allowed_corpora_ids,
        },
    )

    published_families = (
        db.query(FamilyDocument.family_import_id)
        .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .distinct()
        .subquery()
    )

    geographies_subquery = (
        db.query(
            FamilyGeography.family_import_id,
            func.array_agg(Geography.value).label("geography_values"),
        )
        .join(Geography, Geography.id == FamilyGeography.geography_id)
        .group_by(FamilyGeography.family_import_id)
        .subquery()
    )

    query = (
        db.query(
            Family,
            geographies_subquery.c.geography_values,
            FamilyMetadata,
        )
        .join(FamilyMetadata, FamilyMetadata.family_import_id == Family.import_id)
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .join(Corpus, FamilyCorpus.corpus_import_id == Corpus.import_id)
        .join(
            published_families,
            published_families.c.family_import_id == Family.import_id,
        )
        .filter(geographies_subquery.c.family_import_id == Family.import_id)
        .filter(Corpus.import_id.in_(allowed_corpora_ids))
        .order_by(Family.last_modified.desc())
        .limit(5)
        .options(lazyload("*"))
    )

    families = query.all()

    return [
        to_latest_published_response_family(family, geographies, metadata)
        for family, geographies, metadata in families
    ]


def to_latest_published_response_family(
    family: Family, geographies: list[str], metadata: FamilyMetadata
) -> LatestUpdatedFamilyResponse:
    return LatestUpdatedFamilyResponse(
        import_id=str(family.import_id),
        title=str(family.title),
        description=str(family.description),
        family_category=str(family.family_category),
        published_date=str(family.published_date),  # type: ignore
        last_modified=str(family.last_modified),
        metadata=dict(metadata.value),  # type: ignore
        geographies=geographies,
        slugs=[str(slug) for slug in family.slugs] if family.slugs else [],
    )
