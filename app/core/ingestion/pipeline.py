import logging
from datetime import datetime, timezone
from typing import Sequence, Tuple, cast

from db_client.models.dfce import DocumentStatus
from db_client.models.dfce.family import (
    Corpus,
    Family,
    FamilyCorpus,
    FamilyDocument,
    PhysicalDocument,
)
from db_client.models.dfce.metadata import FamilyMetadata
from db_client.models.organisation import Organisation
from sqlalchemy import func
from sqlalchemy.orm import Session, lazyload

from app.api.api_v1.schemas.document import DocumentParserInput
from app.repository.geography import get_geo_subquery
from app.repository.lookups import doc_type_from_family_document_metadata

_LOGGER = logging.getLogger(__name__)

MetadataType = dict[str, list[str]]


def fetch_geographies(db: Session) -> Sequence[Tuple[str, list[str]]]:
    """Fetch unique geographies associated with each family.

    This function queries the database to retrieve a list of geographies
    associated with each family. It aggregates the geographies into a
    JSON array for each family, as one family can now have multiple
    associated geographies.

    :param Session db: The db session to query against.
    :return Sequence[Tuple[str, list[str]]]: A list of tuples, each
        containing a family_import_id and a list of associated geos.
    """
    _LOGGER.info("Running pipeline geographies query")
    geo_subquery = get_geo_subquery(db)
    return (
        db.query(
            geo_subquery.c.family_import_id,  # type: ignore
            func.json_agg(func.distinct(geo_subquery.c.value)).label("geographies"),  # type: ignore
        )
        .group_by(geo_subquery.c.family_import_id)  # type: ignore
        .subquery()
    )


def generate_pipeline_ingest_input_query(
    db: Session,
) -> Sequence[
    Tuple[
        Family,
        FamilyDocument,
        FamilyMetadata,
        list[str],
        Organisation,
        Corpus,
        PhysicalDocument,
    ]
]:
    """Get a list of non-deleted docs and their associated meta & geos.

    This function combines the results of three separate queries:
    family and metadata, documents, and geographies by iterating over
    the results of these queries, matching families with their documents
    and geographies based on the family_import_id. The final result is a
    list of tuples, containing the required information to construct a
    DocumentParserInput object.

    :param Session db: The db session to query against.
    :return Sequence[Tuple[
        Family,
        FamilyDocument,
        FamilyMetadata,
        list[str],
        Organisation,
        Corpus,
        PhysicalDocument
    ]]: A list of tuples containing the information needed to populate
        a DocumentParserInput object.
    """

    _LOGGER.info("Combining results of pipeline subqueries")

    # Subquery to aggregate geographies
    geography_subquery = fetch_geographies(db)

    # Main query
    query = (
        db.query(
            Family,
            FamilyDocument,
            FamilyMetadata,
            geography_subquery.c.geographies,  # type: ignore
            Organisation,
            Corpus,
            PhysicalDocument,
        )
        .select_from(FamilyDocument)
        .join(Family, Family.import_id == FamilyDocument.family_import_id)
        .join(FamilyMetadata, Family.import_id == FamilyMetadata.family_import_id)
        .join(FamilyCorpus, Family.import_id == FamilyCorpus.family_import_id)
        .join(Corpus, Corpus.import_id == FamilyCorpus.corpus_import_id)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .join(
            PhysicalDocument, PhysicalDocument.id == FamilyDocument.physical_document_id
        )
        .join(
            geography_subquery,
            geography_subquery.c.family_import_id == Family.import_id,  # type: ignore
        )
        .filter(FamilyDocument.document_status != DocumentStatus.DELETED)
        .filter(geography_subquery.c.family_import_id == Family.import_id)  # type: ignore
        .options(
            # Disable any default eager loading as this was causing multiplicity due to
            # implicit joins in relationships on the selected models.
            lazyload("*")
        )
    )

    results = cast(
        Sequence[
            Tuple[
                Family,
                FamilyDocument,
                FamilyMetadata,
                list[str],
                Organisation,
                Corpus,
                PhysicalDocument,
            ]
        ],
        query.all(),
    )
    return results


def generate_pipeline_ingest_input(db: Session) -> Sequence[DocumentParserInput]:
    """Generate a view of the current document db as pipeline input.

    :param Session db: The db session to query against.
    :return Sequence[DocumentParserInput]: A list of DocumentParserInput
        objects that can be used by the pipeline.
    """

    results = generate_pipeline_ingest_input_query(db)

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
            geography=cast(
                str, geographies[0]
            ),  # First geography for backward compatibility
            geographies=geographies,
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
            geographies,
            organisation,
            corpus,
            physical_document,
        ) in results
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
