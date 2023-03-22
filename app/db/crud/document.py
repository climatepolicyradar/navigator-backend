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
    FamilyContext,
    FamilyDocumentResponse,
    FamilyDocumentWithContextResponse,
    FamilyEventsResponse,
)
from app.db.models.app.users import Organisation
from app.db.models.document.physical_document import (
    PhysicalDocument,
    PhysicalDocumentLanguage,
    Language,
)
from app.db.models.law_policy.collection import Collection, CollectionFamily
from app.db.models.law_policy.family import (
    Family,
    FamilyDocument,
    FamilyEvent,
    Slug,
    FamilyOrganisation,
)
from app.db.models.law_policy.geography import Geography
from app.db.models.law_policy.metadata import FamilyMetadata
from app.core.util import to_cdn_url

_LOGGER = logging.getLogger(__file__)


def get_slugged_objects(db: Session, slug: str) -> tuple[Optional[str], Optional[str]]:
    """
    Matches the slug name to a FamilyDocument or Family import_id

    :param Session db: connection to db
    :param str slug: slug name to match
    :return tuple[Optional[str], Optional[str]]: the FamilyDocument import id or
    the Family import_id
    """
    return (
        db.query(Slug.family_document_import_id, Slug.family_import_id).filter(
            Slug.name == slug
        )
    ).one_or_none()


def get_family_document_and_context(
    db: Session, family_document_import_id: str
) -> Optional[FamilyDocumentWithContextResponse]:
    db_objects = (
        db.query(Family, FamilyDocument, PhysicalDocument, Geography)
        .filter(FamilyDocument.import_id == family_document_import_id)
        .filter(Family.import_id == FamilyDocument.family_import_id)
        .filter(FamilyDocument.physical_document_id == PhysicalDocument.id)
        .filter(Family.geography_id == Geography.id)
    ).one_or_none()

    if not db_objects:
        return None

    family, document, physical_document, geography = db_objects

    import_id = cast(str, family.import_id)
    slugs = _get_slugs_for_family_import_id(db, import_id)

    family = FamilyContext(
        title=cast(str, family.title),
        import_id=import_id,
        geography=cast(str, geography.value),
        slugs=slugs,
        category=family.family_category,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
    )
    document = FamilyDocumentResponse(
        import_id=document.import_id,
        variant=document.variant_name,
        slugs=_get_slugs_for_family_document_import_id(db, document.import_id),
        title=physical_document.title,
        md5_sum=physical_document.md5_sum,
        cdn_object=to_cdn_url(physical_document.cdn_object),
        source_url=physical_document.source_url,
        content_type=physical_document.content_type,
        language=_get_language_for_phys_doc(db, physical_document.id),
        document_type=document.document_type,
    )

    return FamilyDocumentWithContextResponse(family=family, document=document)


def _get_language_for_phys_doc(db: Session, physical_document_id: str) -> str:
    language = (
        db.query(Language)
        .filter(PhysicalDocumentLanguage.document_id == physical_document_id)
        .filter(Language.id == PhysicalDocumentLanguage.language_id)
    ).one_or_none()

    return cast(str, language.code) if hasattr(language, "code") else ""


def get_family_and_documents(
    db: Session, import_id: str
) -> Optional[FamilyAndDocumentsResponse]:
    """
    Get a document along with the family information.

    :param Session db: connection to db
    :param str import_id: id of document
    :return DocumentWithFamilyResponse: response object
    """

    db_objects = (
        db.query(Family, Geography, FamilyMetadata, FamilyOrganisation, Organisation)
        .filter(Family.import_id == import_id)
        .filter(Family.geography_id == Geography.id)
        .filter(import_id == FamilyMetadata.family_import_id)
        .filter(import_id == FamilyOrganisation.family_import_id)
        .filter(FamilyOrganisation.organisation_id == Organisation.id)
    ).one_or_none()

    if not db_objects:
        _LOGGER.warning("No family found for import_id", extra={"slug": import_id})
        return None

    family: Family
    (
        family,
        geography,
        family_metadata,
        _,
        organisation,
    ) = db_objects

    slugs = _get_slugs_for_family_import_id(db, import_id)
    events = _get_events_for_family_import_id(db, import_id)
    documents = _get_documents_for_family_import_id(db, import_id)
    collections = _get_collections_for_family_import_id(db, import_id)

    return FamilyAndDocumentsResponse(
        organisation=cast(str, organisation.name),
        import_id=import_id,
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
) -> list[FamilyDocumentResponse]:
    db_documents = (
        db.query(FamilyDocument, PhysicalDocument)
        .filter(FamilyDocument.family_import_id == import_id)
        .filter(FamilyDocument.physical_document_id == PhysicalDocument.id)
    )

    documents = [
        FamilyDocumentResponse(
            import_id=d.import_id,
            variant=d.variant_name,
            slugs=_get_slugs_for_family_document_import_id(db, d.import_id),
            # What follows is off PhysicalDocument
            title=pd.title,
            md5_sum=pd.md5_sum,
            cdn_object=to_cdn_url(pd.cdn_object),
            source_url=pd.source_url,
            content_type=pd.content_type,
            language=_get_language_for_phys_doc(db, pd.id),
            document_type=d.document_type,
        )
        for d, pd in db_documents
    ]

    return documents
