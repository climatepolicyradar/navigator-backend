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
from app.service.custom_app import AppTokenFactory
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__name__)

latest_published_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


@latest_published_router.get(
    "/latest_published",
    summary="Gets five most recently published families.",
)
def latest_published(
    request: Request,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
):
    """Retrieve the five most recently published families.

    This endpoint returns the latest five published family records,
    sorted by their modified date in descending order.

    :param Request request: The incoming request object.
    :param Annotated[str, Header()] app_token: App token containing
        the allowed corpora access.
    :param Depends[get_db] db: Database session dependency.
    :return List[Family]: A list of the five most recently published
        families.
    """

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    allowed_corpora_ids = token.allowed_corpora_ids

    if allowed_corpora_ids is None or allowed_corpora_ids == []:
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
):
    return {
        "import_id": family.import_id,
        "title": family.title,
        "description": family.description,
        "family_category": family.family_category,
        "published_date": family.published_date,
        "last_modified": family.last_modified,
        "metadata": metadata.value,
        "geographies": geographies,
        "slugs": family.slugs,
    }
