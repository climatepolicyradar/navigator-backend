import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Sequence, cast

import pandas as pd
from db_client.models.dfce import DocumentStatus
from db_client.models.dfce.family import FamilyDocument
from fastapi import Depends

from app.api.api_v1.schemas.document import DocumentParserInput
from app.clients.db.session import get_db

_LOGGER = logging.getLogger(__name__)

MetadataType = dict[str, list[str]]


@lru_cache()
def generate_pipeline_ingest_input_query():
    """Read query for non-deleted docs and their associated data."""
    with open("./app/repository/sql/pipeline.sql", "r") as file:
        return file.read()


def get_pipeline_data(db=Depends(get_db)):
    """Get non-deleted docs and their associated data from the db.

    Use the pipeline query to query the current database to get a list
    of non deleted documents and their associated data, family info,
    metadata, languages, and geographies.

    The final result is a DataFrame containing the required information
    to construct a DocumentParserInput object.

    :param Session db: The db session to query against.
    :return pd.DataFrame: DataFrame containing current view of documents
        in database.
    """
    _LOGGER.info("Running pipeline query")
    query = generate_pipeline_ingest_input_query()
    with db.connection() as conn:
        df = pd.read_sql(query, conn.connection)
        return df


def parse_document_object(row: pd.Series) -> DocumentParserInput:
    """Parse DataFrame row into DocumentParserInput object.

    :param pd.Series row: A pandas series containing a row that
        represents a family document and its related context.
    :return DocumentParserInput: A DocumentParserInput object
        representing the family document record & its context.
    """
    fallback_date = datetime(1900, 1, 1, tzinfo=timezone.utc)
    return DocumentParserInput(
        # All documents in a family indexed by title
        name=cast(str, row.get("family_title")),
        document_title=cast(str, row.get("physical_document_title")),
        description=cast(str, row.get("family_description")),
        category=str(row.get("family_category")),
        publication_ts=cast(
            datetime,
            (
                pd.to_datetime(cast(str, row.get("family_published_date")))
                if row.get("family_published_date") is not None
                else fallback_date
            ),
        ),
        import_id=cast(str, row.get("family_document_import_id")),
        # This gets the most recently added document slug.
        slug=cast(str, row.get("family_document_slug")),
        family_import_id=cast(str, row.get("family_import_id")),
        # This gets the most recently added family slug.
        family_slug=cast(str, row.get("family_slug")),
        source_url=(
            cast(str, row.get("physical_document_source_url"))
            if row.get("physical_document_source_url") is not None
            else None
        ),
        download_url=None,
        type=cast(str, row.get("family_document_type", default="")),
        source=cast(str, row.get("organisation_name")),
        geography=cast(
            str,
            (
                cast(list, row.get("geographies", default=[""]))[0]
                if row.get("geographies", default=[""]) is not None
                else []
            ),
        ),  # First geography for backward compatibility
        geographies=cast(list, row.get("geographies")),
        corpus_import_id=cast(str, row.get("corpus_import_id")),
        corpus_type_name=cast(str, row.get("corpus_type_name")),
        collection_title=None,
        collection_summary=None,
        languages=[
            cast(str, lang)
            for lang in (
                cast(list, row.get("languages"))
                if row.get("languages") is not None
                else []
            )
        ],
        metadata=_flatten_pipeline_metadata(
            cast(MetadataType, row.get("family_metadata")),
            cast(MetadataType, row.get("family_document_metadata")),
        ),
    )


def generate_pipeline_ingest_input(db=Depends(get_db)) -> Sequence[DocumentParserInput]:
    """Generate a view of the current document db as pipeline input.

    :param Session db: The db session to query against.
    :return Sequence[DocumentParserInput]: A list of DocumentParserInput
        objects that can be used by the pipeline.
    """
    results = get_pipeline_data(db)

    _LOGGER.info("Parsing pipeline query data")
    documents: Sequence[DocumentParserInput] = [
        parse_document_object(row) for index, row in results.iterrows()
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


def _flatten_pipeline_metadata(
    family_metadata: MetadataType, document_metadata: MetadataType
) -> MetadataType:
    """Combines metadata objects ready for the pipeline"""

    metadata = {}

    for k, v in family_metadata.items():
        metadata[f"family.{k}"] = v

    for k, v in document_metadata.items():
        metadata[f"document.{k}"] = v

    return metadata
