"""
Functions to support the documents endpoints

old functions (non DFC) are moved to the deprecated_documents.py file.
"""
import logging
from datetime import datetime, timedelta
import os
from typing import Mapping, Optional, Sequence, Tuple, cast

from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import (
    CollectionOverviewResponse,
    FamilyAndDocumentsResponse,
    FamilyContext,
    FamilyDocumentResponse,
    FamilyDocumentWithContextResponse,
    FamilyEventsResponse,
    LinkableFamily,
)
from app.db.models.app.users import Organisation
from app.db.models.document.physical_document import (
    Language,
    PhysicalDocument,
    PhysicalDocumentLanguage,
)
from app.db.models.law_policy.collection import Collection, CollectionFamily
from app.db.models.law_policy.family import (
    DocumentStatus,
    Family,
    FamilyDocument,
    FamilyStatus,
    Slug,
    FamilyOrganisation,
)
from app.db.models.law_policy.geography import Geography
from app.db.models.law_policy.metadata import FamilyMetadata
from app.core.util import to_cdn_url

_LOGGER = logging.getLogger(__file__)

# Set default cache timeout to 1-day, this can be revisited later.
_DOCUMENT_CACHE_TTL: int = int(os.environ.get("DOCUMENT_CACHE_TTL_MS", "86400000"))


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
) -> FamilyDocumentWithContextResponse:
    db_objects = (
        db.query(Family, FamilyDocument, PhysicalDocument, Geography)
        .filter(FamilyDocument.import_id == family_document_import_id)
        .filter(Family.import_id == FamilyDocument.family_import_id)
        .filter(FamilyDocument.physical_document_id == PhysicalDocument.id)
        .filter(Family.geography_id == Geography.id)
    ).one_or_none()

    if not db_objects:
        _LOGGER.warning(
            "No family document found for import_id",
            extra={"slug": family_document_import_id},
        )
        raise ValueError(
            f"No family document found for import_id: {family_document_import_id}"
        )

    family, document, physical_document, geography = db_objects

    if (
        family.family_status != FamilyStatus.PUBLISHED
        or document.document_status != DocumentStatus.PUBLISHED
    ):
        raise ValueError(f"The document {family_document_import_id} is not published")

    family_context = FamilyContext(
        title=cast(str, family.title),
        import_id=cast(str, family.import_id),
        geography=cast(str, geography.value),
        slug=family.slugs[0].name,
        category=family.family_category,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
    )
    langs = _get_visible_languages_for_phys_doc(db, physical_document)
    response = FamilyDocumentResponse(
        import_id=document.import_id,
        variant=document.variant_name,
        slug=document.slugs[0].name,
        title=physical_document.title,
        md5_sum=physical_document.md5_sum,
        cdn_object=to_cdn_url(physical_document.cdn_object),
        source_url=physical_document.source_url,
        content_type=physical_document.content_type,
        language=(langs[0] if len(langs) > 0 else ""),
        languages=langs,
        document_type=document.document_type,
        document_role=document.document_role,
    )

    return FamilyDocumentWithContextResponse(family=family_context, document=response)


def _get_visible_languages_for_phys_doc(
    db: Session, physical_document: PhysicalDocument
) -> Sequence[str]:
    result = (
        db.query(PhysicalDocumentLanguage.visible, Language.language_code)
        .join(Language, Language.id == PhysicalDocumentLanguage.language_id)
        .filter(PhysicalDocumentLanguage.document_id == physical_document.id)
        .all()
    )
    if result is None or len(result) == 0:
        return []
    return [cast(str, code) for visible, code in result if visible]


def get_family_and_documents(db: Session, import_id: str) -> FamilyAndDocumentsResponse:
    """
    Get a document along with the family information.

    :param Session db: connection to db
    :param str import_id: id of document
    :return DocumentWithFamilyResponse: response object
    """

    db_objects = (
        db.query(Family, Geography, FamilyMetadata, FamilyOrganisation, Organisation)
        .filter(Family.import_id == import_id)
        .join(Geography, Family.geography_id == Geography.id)
        .join(FamilyMetadata, import_id == FamilyMetadata.family_import_id)
        .join(FamilyOrganisation, import_id == FamilyOrganisation.family_import_id)
        .filter(FamilyOrganisation.organisation_id == Organisation.id)
    ).one_or_none()

    if not db_objects:
        _LOGGER.warning("No family found for import_id", extra={"slug": import_id})
        raise ValueError(f"No family found for import_id: {import_id}")

    family: Family
    (
        family,
        geography,
        family_metadata,
        _,
        organisation,
    ) = db_objects

    if family.family_status != FamilyStatus.PUBLISHED:
        raise ValueError(f"Family {import_id} is not published")

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
        slug=cast(str, family.slugs[0].name),
        events=_get_events_for_family(family),
        documents=documents,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
        collections=collections,
    )


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
            title=c.title,
            description=c.description,
            import_id=c.import_id,
            families=[
                LinkableFamily(slug=data[0], title=data[1], description=data[2])
                for data in db.query(Slug.name, Family.title, Family.description)
                .filter(CollectionFamily.collection_import_id == c.import_id)
                .filter(CollectionFamily.family_import_id == Family.import_id)
                .join(Slug, Slug.family_import_id == Family.import_id)
                .all()
            ],
        )
        for c in db_collections
    ]


def _get_events_for_family(family: Family) -> list[FamilyEventsResponse]:
    events = [
        FamilyEventsResponse(
            title=cast(str, e.title),
            date=cast(datetime, e.date),
            event_type=cast(str, e.event_type_name),
            status=cast(str, e.status),
        )
        for e in family.events
    ]

    return events


def _get_documents_for_family_import_id(
    db: Session, import_id: str
) -> list[FamilyDocumentResponse]:
    db_documents = (
        db.query(FamilyDocument)
        .filter(FamilyDocument.family_import_id == import_id)
        .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
    )

    def make_response(d: FamilyDocument) -> FamilyDocumentResponse:
        langs = _get_visible_languages_for_phys_doc(db, d.physical_document)
        return FamilyDocumentResponse(
            import_id=cast(str, d.import_id),
            variant=cast(str, d.variant_name),
            slug=cast(str, d.slugs[0].name),
            # What follows is off PhysicalDocument
            title=cast(str, d.physical_document.title),
            md5_sum=cast(str, d.physical_document.md5_sum),
            cdn_object=to_cdn_url(cast(str, d.physical_document.cdn_object)),
            source_url=cast(str, d.physical_document.source_url),
            content_type=cast(str, d.physical_document.content_type),
            language=(langs[0] if d.physical_document.languages else ""),
            languages=langs,
            document_type=cast(str, d.document_type),
            document_role=cast(str, d.document_role),
        )

    return [make_response(d) for d in db_documents]


class DocumentExtraCache:
    """
    A simple cache for document -> family info mapping details.

    TODO: Replace this simple per-process cache mechanism with a shared cache.
    """

    def __init__(self):
        self._ttl = timedelta(milliseconds=_DOCUMENT_CACHE_TTL)
        self._timestamp = datetime.utcnow() - self._ttl
        self._doc_extra_info: Mapping[str, Mapping[str, str]] = {}

    def get_document_extra_info(self, db: Session) -> Mapping[str, Mapping[str, str]]:
        """
        Get a map from document_id to useful properties for processing.

        :param [Session] db: Database session to query
        :return [Mapping[str, Mapping[str, str]]]: A mapping from document import_id to
            document slug, family slug & family import id details.
        """
        if datetime.utcnow() - self._timestamp >= self._ttl:
            self._doc_extra_info = self._query_document_extra_info(db)
            self._timestamp = datetime.utcnow()
        return self._doc_extra_info

    def _query_document_extra_info(
        self, db: Session
    ) -> Mapping[str, Mapping[str, str]]:
        document_data: list[Tuple[FamilyDocument, Family]] = (
            db.query(FamilyDocument, Family)
            .join(Family, FamilyDocument.family_import_id == Family.import_id)
            .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
            .all()
        )
        return {
            family_document.import_id: {
                "slug": family_document.slugs[-1].name,
                "title": family_document.physical_document.title,
                "family_slug": family.slugs[-1].name,
                "family_import_id": family.import_id,
                "family_title": family.title,
                "family_description": family.description,
                "family_category": family.family_category,
                "family_status": family.family_status,
                "family_published_date": (
                    family.published_date.isoformat()
                    if family.published_date is not None
                    else ""
                ),
                "family_last_updated_date": (
                    family.last_updated_date.isoformat()
                    if family.last_updated_date is not None
                    else ""
                ),
            }
            for (family_document, family) in document_data
        }
