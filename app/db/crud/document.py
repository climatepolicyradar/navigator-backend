"""
Functions to support the documents endpoints

old functions (non DFC) are moved to the deprecated_documents.py file.
"""
import logging
from typing import Optional, cast
from sqlalchemy.orm import Session
from app.api.api_v1.schemas.document import (
    FamilyAndDocumentsResponse,
    FamilyDocumentsResponse,
    FamilyEventsResponse,
)
from app.db.models.document.physical_document import PhysicalDocument
from app.db.models.law_policy.family import Family, FamilyDocument, FamilyEvent, Slug
from app.db.models.law_policy.geography import Geography
from app.db.models.law_policy.metadata import FamilyMetadata

_LOGGER = logging.getLogger(__file__)


def get_family_and_documents(
    db: Session, slug: str
) -> Optional[FamilyAndDocumentsResponse]:
    """
    Get a document along with the family information.

    :param Session db: connection to db
    :param str slug: id of document
    :return DocumentWithFamilyResponse: response object
    """

    db_objects = (
        db.query(Family, Geography, Slug, FamilyMetadata)
        .filter(Family.geography_id == Geography.id)
        .filter(Family.import_id == FamilyMetadata.family_import_id)
        .filter(Slug.name == slug)
    ).first()

    if not db_objects:
        _LOGGER.warning("No family found for slug", extra={"slug": slug})
        return None

    family: Family
    family, geography, slug, family_metadata = db_objects
    import_id = family.import_id

    db_slugs = (db.query(Slug).filter(Slug.family_import_id == import_id)).all()

    slugs = [s.name for s in db_slugs]

    db_events = (
        db.query(FamilyEvent).filter(FamilyEvent.family_import_id == import_id)
    ).all()

    events = [
        FamilyEventsResponse(
            title=e.title, date=e.date, event_type=e.event_type_name, status=e.status
        )
        for e in db_events
    ]

    db_documents = (
        db.query(FamilyDocument, PhysicalDocument)
        .filter(FamilyDocument.family_import_id == import_id)
        .filter(FamilyDocument.physical_document_id == PhysicalDocument.id)
    )

    documents = [
        FamilyDocumentsResponse(
            variant=d.variant_name,
            slugs=[],
            # What follows is off PhysicalDocument
            title=pd.title,
            md5_sum=pd.md5_sum,
            cdn_object=pd.cdn_object,
            source_url=pd.source_url,
            content_type=pd.content_type,
        )
        for d, pd in db_documents
    ]

    return FamilyAndDocumentsResponse(
        title=cast(str, family.title),
        geography=cast(str, geography.value),
        category=cast(str, family.family_category),
        status=cast(str, family.family_status),
        slugs=slugs,
        events=events,
        documents=documents,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
    )
