import logging

import sqlalchemy
from db_client.models.dfce.family import (
    DocumentStatus,
    Family,
    FamilyDocument,
    FamilyGeography,
)
from db_client.models.dfce.geography import Geography
from db_client.models.dfce.metadata import FamilyMetadata
from fastapi import APIRouter, Depends
from sqlalchemy import ARRAY, String, func

from app.clients.db.session import get_db
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__name__)

latest_published_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


@latest_published_router.get(
    "/latest_published",
    summary="Gets five most recently published families.",
)
def latest_published(
    db=Depends(get_db),
):
    """Gets five most recently published families."""

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

    empty_array = sqlalchemy.cast([], ARRAY(String))
    query = (
        db.query(
            Family,
            func.coalesce(geographies_subquery.c.geography_values, empty_array).label(
                "geography_values"
            ),
            FamilyMetadata,
        )
        .outerjoin(
            geographies_subquery,
            geographies_subquery.c.family_import_id == Family.import_id,
        )
        .join(FamilyMetadata, FamilyMetadata.family_import_id == Family.import_id)
        # .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        # .join(Corpus, FamilyCorpus.corpus_import_id == Corpus.import_id)
        # .join(Organisation, Organisation.id == Corpus.organisation_id)
        # .join(
        #     published_families,
        #     published_families.c.family_import_id == Family.import_id,
        # )
    )
    families = query.limit(5).all()
    return [
        {
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
        for family, geographies, metadata in families
    ]
