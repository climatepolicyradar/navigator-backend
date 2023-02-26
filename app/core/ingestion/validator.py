from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.metadata import build_metadata, Taxonomy

from app.core.ingestion.utils import IngestContext


def validate_document_row(
    context: IngestContext,
    row: DocumentIngestRow,
    taxonomy: Taxonomy,
) -> None:
    """
    Validate the constituent elements that represent this row.

    :param [Session] db: the connection to the database.
    :param [Organisation] organisation: The organisation context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    :param [Taxonomy] taxonomy: the Taxonomy against which metadata should be validated.
    """
    result, _ = build_metadata(taxonomy, row)
    context.results.append(result)
