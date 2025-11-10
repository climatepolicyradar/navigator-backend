import logging
from typing import Annotated

from db_client.models.dfce.family import Corpus, Family, FamilyCorpus
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import lazyload

from app.clients.db.session import get_db
from app.models.search import LatestFamilyResponse
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
    "/latest",
    summary="Gets five most recently added families.",
)
def latest(
    request: Request,
    app_token: Annotated[str, Header()],
    limit: int = 5,
    db=Depends(get_db),
) -> list[LatestFamilyResponse]:
    """Retrieve the five most recently added families.

    This endpoint returns the five most recently added family records,
    sorted by their created date in descending order.

    :param Request request: The incoming request object.
    :param Annotated[str, Header()] app_token: App token containing
        the allowed corpora access.
    :param Depends[get_db] db: Database session dependency.
    :return list[LatestFamilyResponse]: A list of the five most recently added
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
        "Getting latest families",
        extra={
            "allowed_corpora_ids": allowed_corpora_ids,
        },
    )

    query = (
        db.query(
            Family,
        )
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .join(Corpus, FamilyCorpus.corpus_import_id == Corpus.import_id)
        .filter(Corpus.import_id.in_(allowed_corpora_ids))
        .order_by(Family.created.desc())
        .limit(limit)
        .options(lazyload("*"))
    )

    families = query.all()

    return [to_latest_response_family(family) for family in families]


def get_latest_slug(slugs) -> str | None:
    if not slugs:
        return None

    if len(slugs) == 1:
        return slugs[0].name

    latest_slug = max(slugs, key=lambda slug: slug.created)
    return latest_slug.name


def to_latest_response_family(family: Family) -> LatestFamilyResponse:
    return LatestFamilyResponse(
        import_id=str(family.import_id),
        title=str(family.title),
        created=str(family.created),
        slug=get_latest_slug(family.slugs),
    )
