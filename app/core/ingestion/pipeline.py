import logging
from datetime import datetime, timezone
from typing import Sequence, Tuple, cast

from db_client.models.dfce import Collection, CollectionFamily, DocumentStatus
from db_client.models.dfce.family import (
    Corpus,
    Family,
    FamilyCorpus,
    FamilyDocument,
    PhysicalDocument,
)
from db_client.models.dfce.metadata import FamilyMetadata
from db_client.models.organisation import Organisation
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import DocumentParserInput
from app.core.lookups import doc_type_from_family_document_metadata
from app.db.crud.geography import get_geo_subquery

_LOGGER = logging.getLogger(__name__)

MetadataType = dict[str, list[str]]


def generate_pipeline_ingest_input(db: Session) -> Sequence[DocumentParserInput]:
    """Generates a complete view of the current document database as pipeline input"""
    _LOGGER.info("Running pipeline family query")
    geo_subquery = get_geo_subquery(db)

    query = (
        db.query(
            Family, FamilyDocument, FamilyMetadata, geo_subquery.c.value, Organisation, Corpus, PhysicalDocument, Collection  # type: ignore
        )
        .join(Family, Family.import_id == FamilyDocument.family_import_id)
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .join(Corpus, Corpus.import_id == FamilyCorpus.corpus_import_id)
        .join(FamilyMetadata, Family.import_id == FamilyMetadata.family_import_id)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .join(
            PhysicalDocument, PhysicalDocument.id == FamilyDocument.physical_document_id
        )
        .filter(FamilyDocument.document_status != DocumentStatus.DELETED)
        .filter(geo_subquery.c.family_import_id == Family.import_id)  # type: ignore
    )

    query_result = cast(
        Sequence[
            Tuple[
                Family,
                FamilyDocument,
                FamilyMetadata,
                str,
                Organisation,
                Corpus,
                PhysicalDocument,
                Collection,
            ]
        ],
        query.all(),
    )
    fallback_date = datetime(1900, 1, 1, tzinfo=timezone.utc)
    _LOGGER.info("Running pipeline document query")
    documents: Sequence[DocumentParserInput] = [
        DocumentParserInput(
            name=cast(str, family.title),  # All documents in a family indexed by title
            document_title=cast(str, physical_document.title),
            description=cast(str, family.description),
            category=str(family.family_category),
            publication_ts=family.published_date or fallback_date,
            import_id=cast(str, family_document.import_id),
            slug=cast(str, family_document.slugs[-1].name),
            family_import_id=cast(str, family.import_id),
            family_slug=cast(str, family.slugs[-1].name),
            source_url=(
                cast(str, family_document.physical_document.source_url)
                if family_document.physical_document is not None
                else None
            ),
            download_url=None,
            type=doc_type_from_family_document_metadata(family_document),
            source=cast(str, organisation.name),
            geography=cast(str, geography),
            geographies=[cast(str, geography)],
            corpus_import_id=cast(str, corpus.import_id),
            corpus_type_name=cast(str, corpus.corpus_type_name),
            collection_title=(
                cast(str, collection.title) if collection is not None else None
            ),
            collection_summary=(
                cast(str, collection.description) if collection is not None else None
            ),
            languages=[
                cast(str, lang.name)
                for lang in (
                    family_document.physical_document.languages
                    if family_document.physical_document is not None
                    else []
                )
            ],
            metadata=flatten_pipeline_metadata(
                cast(MetadataType, family_metadata.value),
                cast(MetadataType, family_document.valid_metadata),
            ),
        )
        for (
            family,
            family_document,
            family_metadata,
            geography,
            organisation,
            corpus,
            physical_document,
            collection,
        ) in query_result
    ]

    database_doc_count = db.query(PhysicalDocument).count()
    if len(documents) > database_doc_count:
        raise ValueError(
            "Potential Row Explosion. Ingest input is returning more documents than "
            f"exist in the database {len(documents)}:{database_doc_count}"
        )

    return documents


def flatten_pipeline_metadata(
    family_metadata: MetadataType, document_metadata: MetadataType
) -> MetadataType:
    """Combines metadata objects ready for the pipeline"""

    metadata = {}

    for k, v in family_metadata.items():
        metadata[f"family.{k}"] = v

    for k, v in document_metadata.items():
        metadata[f"document.{k}"] = v

    return metadata
