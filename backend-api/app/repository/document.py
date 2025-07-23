"""Database helper functions for the documents entity."""

import logging
import os
from datetime import datetime
from typing import Optional, Sequence, cast

from db_client.models.dfce.collection import Collection, CollectionFamily
from db_client.models.dfce.family import (
    Corpus,
    DocumentStatus,
    Family,
    FamilyCorpus,
    FamilyDocument,
    FamilyStatus,
    Slug,
)
from db_client.models.dfce.metadata import FamilyMetadata
from db_client.models.document.physical_document import PhysicalDocument
from db_client.models.organisation.organisation import Organisation
from sqlalchemy import bindparam, func, text
from sqlalchemy.orm import Session
from sqlalchemy.types import ARRAY, String

from app.models.document import (
    CollectionOverviewResponse,
    FamilyAndDocumentsResponse,
    FamilyContext,
    FamilyDocumentResponse,
    FamilyDocumentWithContextResponse,
    FamilyEventsResponse,
    LinkableFamily,
)
from app.repository.collection import get_collection_slug_from_import_id
from app.repository.geography import get_geo_subquery
from app.repository.helpers import get_query_template
from app.repository.lookups import doc_type_from_family_document_metadata
from app.service.util import to_cdn_url
from app.telemetry import observe

_LOGGER = logging.getLogger(__file__)


@observe(name="get_slugged_objects")
def get_slugged_objects(
    db: Session, slug: str, allowed_corpora: Optional[list[str]] = None
) -> tuple[Optional[str], Optional[str]]:
    """Match the slug name to a FamilyDocument or Family import ID.

    This function also contains logic to only get the import ID for the
    family or document if the slug given is associated with a family
    that belongs to the list of allowed corpora.

    :param Session db: connection to db
    :param str slug: slug name to match
    :param Optional[list[str]] allowed_corpora: The corpora IDs to look
        for the slugged object in.
    :return tuple[Optional[str], Optional[str]]: the FamilyDocument
        import id or the Family import_id.
    """
    if allowed_corpora not in [None, []]:
        query_template = text(
            get_query_template(
                os.path.join("app", "repository", "sql", "slug_lookup.sql")
            )
        )

        query_template = query_template.bindparams(
            bindparam("slug_name", type_=String),
            bindparam(
                "allowed_corpora_ids", value=allowed_corpora, type_=ARRAY(String)
            ),
        )
        query = db.execute(
            query_template, {"slug_name": slug, "allowed_corpora_ids": allowed_corpora}
        )
    else:
        query = db.query(Slug.family_document_import_id, Slug.family_import_id).filter(
            Slug.name == slug
        )

    result = query.one_or_none()
    if result is None:
        return (None, None)

    DOC_INDEX = 0
    doc_id = cast(str, result[DOC_INDEX]) if result[DOC_INDEX] is not None else None

    FAM_INDEX = 1
    fam_id = cast(str, result[FAM_INDEX]) if result[FAM_INDEX] is not None else None

    return doc_id, fam_id


@observe(name="get_family_document_and_context")
def get_family_document_and_context(
    db: Session, family_document_import_id: str
) -> FamilyDocumentWithContextResponse:
    geo_subquery = get_geo_subquery(
        db, family_document_import_id=family_document_import_id
    )
    db_objects = (
        db.query(
            Family,
            FamilyDocument,
            PhysicalDocument,
            func.array_agg(geo_subquery.c.value).label(  # type: ignore
                "geographies"
            ),  # Aggregate geographies
            FamilyCorpus,
        )
        .filter(FamilyDocument.import_id == family_document_import_id)
        .filter(Family.import_id == FamilyDocument.family_import_id)
        .filter(FamilyDocument.physical_document_id == PhysicalDocument.id)
        .filter(FamilyCorpus.family_import_id == Family.import_id)
        .filter(geo_subquery.c.family_import_id == Family.import_id)  # type: ignore
        .group_by(Family, FamilyDocument, PhysicalDocument, FamilyCorpus)
    ).first()

    if not db_objects:
        _LOGGER.warning(
            "No family document found for import_id",
            extra={"slug": family_document_import_id},
        )
        raise ValueError(
            f"No family document found for import_id: {family_document_import_id}"
        )

    family, document, physical_document, geographies, family_corpus = db_objects

    if (
        family.family_status != FamilyStatus.PUBLISHED
        or document.document_status != DocumentStatus.PUBLISHED
    ):
        raise ValueError(f"The document {family_document_import_id} is not published")

    family_context = FamilyContext(
        title=cast(str, family.title),
        import_id=cast(str, family.import_id),
        geographies=geographies,
        slug=family.slugs[0].name,
        category=family.family_category,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
        corpus_id=family_corpus.corpus_import_id,
    )
    visible_languages = _get_visible_languages_for_phys_doc(physical_document)
    response = FamilyDocumentResponse(
        import_id=document.import_id,
        variant=document.variant_name,
        slug=document.slugs[0].name,
        title=physical_document.title,
        md5_sum=physical_document.md5_sum,
        cdn_object=to_cdn_url(physical_document.cdn_object),
        source_url=physical_document.source_url,
        content_type=physical_document.content_type,
        language=(visible_languages[0] if visible_languages else ""),
        languages=visible_languages,
        document_type=doc_type_from_family_document_metadata(document),
        document_role=(
            document.valid_metadata["role"][0]
            if "role" in document.valid_metadata.keys()
            else ""
        ),
    )

    return FamilyDocumentWithContextResponse(family=family_context, document=response)


def _get_visible_languages_for_phys_doc(
    physical_document: PhysicalDocument,
) -> Sequence[str]:
    return [
        lang.language.language_code
        for lang in physical_document.language_wrappers
        if lang.visible
    ]


@observe(name="get_family_and_documents")
def get_family_and_documents(db: Session, import_id: str) -> FamilyAndDocumentsResponse:
    """
    Get a document along with the family information.

    :param Session db: connection to db
    :param str import_id: id of document
    :return DocumentWithFamilyResponse: response object
    """

    geo_subquery = get_geo_subquery(db)
    db_objects = (
        db.query(
            Family,
            func.array_agg(geo_subquery.c.value).label(  # type: ignore
                "geographies"
            ),  # Aggregate geographies
            FamilyMetadata,
            Organisation,
            FamilyCorpus,
        )
        .join(FamilyMetadata, Family.import_id == FamilyMetadata.family_import_id)
        .join(FamilyCorpus, Family.import_id == FamilyCorpus.family_import_id)
        .join(Corpus, Corpus.import_id == FamilyCorpus.corpus_import_id)
        .join(Organisation, Corpus.organisation_id == Organisation.id)
        .filter(Family.import_id == import_id)
        .filter(geo_subquery.c.family_import_id == Family.import_id)  # type: ignore
        .group_by(Family, FamilyMetadata, Organisation, FamilyCorpus)
    ).first()

    if not db_objects:
        _LOGGER.warning("No family found for import_id", extra={"slug": import_id})
        raise ValueError(f"No family found for import_id: {import_id}")

    family, geographies, family_metadata, organisation, family_corpus = db_objects

    if family.family_status != FamilyStatus.PUBLISHED:
        raise ValueError(f"Family {import_id} is not published")

    documents = _get_documents_for_family_import_id(db, import_id)
    collections = _get_collections_for_family_import_id(db, import_id)

    return FamilyAndDocumentsResponse(
        organisation=cast(str, organisation.name),
        import_id=import_id,
        title=cast(str, family.title),
        summary=cast(str, family.description),
        geographies=geographies,
        category=cast(str, family.family_category),
        status=cast(str, family.family_status),
        metadata=cast(dict, family_metadata.value),
        slug=cast(str, family.slugs[0].name),
        events=_get_events_for_family(family),
        documents=documents,
        published_date=family.published_date,
        last_updated_date=family.last_updated_date,
        collections=collections,
        corpus_id=family_corpus.corpus_import_id,
    )


@observe(name="get_collections_for_family_import_id")
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
            slug=get_collection_slug_from_import_id(db, c.import_id),
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


@observe(name="get_events_for_family")
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
        visible_languages = _get_visible_languages_for_phys_doc(d.physical_document)
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
            language=(visible_languages[0] if visible_languages else ""),
            languages=visible_languages,
            document_type=doc_type_from_family_document_metadata(d),
            document_role=(
                cast(str, d.valid_metadata["role"][0])  # type:ignore
                if "role" in d.valid_metadata.keys()
                else ""
            ),
        )

    return [make_response(d) for d in db_documents]
