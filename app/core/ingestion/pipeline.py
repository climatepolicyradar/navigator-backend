import logging
from datetime import datetime, timezone
from typing import Sequence, Tuple, cast

from db_client.models.dfce import (
    DocumentStatus,
    FamilyGeography,
    FamilyMetadata,
    Geography,
)
from db_client.models.dfce.family import (
    Corpus,
    Family,
    FamilyCorpus,
    FamilyDocument,
    PhysicalDocument,
)
from db_client.models.organisation import Organisation
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import DocumentParserInput
from app.repository.lookups import doc_type_from_family_document_metadata

_LOGGER = logging.getLogger(__name__)

MetadataType = dict[str, list[str]]


def _get_single_geo_query(db: Session):
    geo_subquery = (
        db.query(
            func.min(Geography.value).label("value"),
            func.min(Geography.slug).label("slug"),
            FamilyGeography.family_import_id,
        )
        .join(FamilyGeography, FamilyGeography.geography_id == Geography.id)
        .filter(FamilyGeography.family_import_id == Family.import_id)
        .group_by(Geography.value, Geography.slug, FamilyGeography.family_import_id)
    )
    """ NOTE: This is an intermediate step to migrate to multi-geography support.
    We grab the minimum geography value for each family to use as a fallback for a single geography.
    This is because there is no rank for geography values and we need to pick one.
    This also looks dodgy as the "value" and "slug" may not match up.
    However, the browse code only uses one of these values, so it should be fine.
    Don't forget this is temporary and will be removed once multi-geography support is implemented.
    Also remember to update the pipeline query to pass these in when changing this.
    """
    return geo_subquery.subquery("geo_subquery")


def generate_pipeline_ingest_input(db: Session) -> Sequence[DocumentParserInput]:
    """Generate a view of the current document db as pipeline input.

    :param Session db: The db session to query against.
    :return Sequence[DocumentParserInput]: A list of DocumentParserInput
        objects that can be used by the pipeline.
    """
    geo_subquery = _get_single_geo_query(db)
    query = (
        db.query(
            Family,
            FamilyDocument,
            FamilyMetadata,
            geo_subquery.c.value,
            Organisation,
            Corpus,
            PhysicalDocument,  # type: ignore
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
            ]
        ],
        query.all(),
    )

    _LOGGER.info("Parsing pipeline query data")

    fallback_date = datetime(1900, 1, 1, tzinfo=timezone.utc)
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
            collection_title=None,
            collection_summary=None,
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
        ) in query_result
    ]

    # TODO: Revert to raise a ValueError when the issue is resolved
    database_doc_count = (
        db.query(FamilyDocument)
        .filter(FamilyDocument.document_status != DocumentStatus.DELETED)
        .count()
    )
    if len(documents) > database_doc_count:
        _LOGGER.warning(
            "Potential Row Explosion. Ingest input is returning more documents than exist in the database",
            extra={
                "ingest_count": len(documents),
                "database_count": database_doc_count,
            },
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
