"""
Functions to support the documents endpoints

old functions (non DFC) are moved to the deprecated_documents.py file.
"""
import logging
from typing import Optional, cast
from sqlalchemy.orm import Session
from app.api.api_v1.schemas.document import (
    CollectionOverviewResponse,
    FamilyAndDocumentsResponse,
    FamilyDocumentsResponse,
    FamilyEventsResponse,
)
from app.db.models.app.users import Organisation
from app.db.models.document.physical_document import PhysicalDocument
from app.db.models.law_policy.collection import Collection, CollectionFamily
from app.db.models.law_policy.family import (
    Family,
    FamilyDocument,
    FamilyEvent,
    FamilyOrganisation,
    Slug,
)
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
        db.query(
            Family, Geography, Slug, FamilyMetadata, FamilyOrganisation, Organisation
        )
        .filter(Family.geography_id == Geography.id)
        .filter(Family.import_id == FamilyMetadata.family_import_id)
        .filter(Family.import_id == Slug.family_import_id)
        .filter(Family.import_id == FamilyOrganisation.family_import_id)
        .filter(FamilyOrganisation.organisation_id == Organisation.id)
        .filter(Slug.name == slug)
    ).first()

    if not db_objects:
        _LOGGER.warning("No family found for slug", extra={"slug": slug})
        return None

    family: Family
    family, geography, slug, family_metadata, organisation = db_objects
    import_id = cast(str, family.import_id)

    slugs = _get_slugs_for_family_import_id(db, import_id)
    events = _get_events_for_family_import_id(db, import_id)
    documents = _get_documents_for_family_import_id(db, import_id)
    collections = _get_collections_for_family_import_id(db, import_id)

    return FamilyAndDocumentsResponse(
        organisation=cast(str, organisation.name),
        title=cast(str, family.title),
        summary=cast(str, family.description),
        geography=cast(str, geography.value),
        category=cast(str, family.family_category),
        status=cast(str, family.family_status),
        metadata=cast(dict, family_metadata.value),
        slugs=slugs,
        events=events,
        documents=documents,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
        collections=collections,
    )


def _get_slugs_for_family_import_id(db: Session, import_id: str) -> list[str]:
    db_slugs = (db.query(Slug).filter(Slug.family_import_id == import_id)).all()
    return [s.name for s in db_slugs]


def _get_slugs_for_family_document_import_id(db: Session, import_id: str):
    db_slugs = (
        db.query(Slug).filter(Slug.family_document_import_id == import_id)
    ).all()
    return [s.name for s in db_slugs]


def _get_collections_for_family_import_id(
    db: Session, import_id: str
) -> list[CollectionOverviewResponse]:
    db_collections = (
        db.query(Collection)
        .join(
            CollectionFamily,
            Collection.import_id == CollectionFamily.collection_import_id,
        )
        .filter(CollectionFamily.family_import_id == import_id)
    ).all()

    return [
        CollectionOverviewResponse(
            title=c.title, description=c.description, import_id=c.import_id
        )
        for c in db_collections
    ]


def _get_events_for_family_import_id(
    db: Session, import_id: str
) -> list[FamilyEventsResponse]:
    db_events = (
        db.query(FamilyEvent).filter(FamilyEvent.family_import_id == import_id)
    ).all()

    events = [
        FamilyEventsResponse(
            title=e.title, date=e.date, event_type=e.event_type_name, status=e.status
        )
        for e in db_events
    ]

    return events


def _get_documents_for_family_import_id(
    db: Session, import_id: str
) -> list[FamilyDocumentsResponse]:
    db_documents = (
        db.query(FamilyDocument, PhysicalDocument)
        .filter(FamilyDocument.family_import_id == import_id)
        .filter(FamilyDocument.physical_document_id == PhysicalDocument.id)
    )

    documents = [
        FamilyDocumentsResponse(
            variant=d.variant_name,
            slugs=_get_slugs_for_family_document_import_id(db, d.import_id),
            # What follows is off PhysicalDocument
            title=pd.title,
            md5_sum=pd.md5_sum,
            cdn_object=pd.cdn_object,
            source_url=pd.source_url,
            content_type=pd.content_type,
        )
        for d, pd in db_documents
    ]

    return documents
