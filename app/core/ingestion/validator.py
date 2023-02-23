from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.metadata import build_metadata
from app.core.ingestion.organisation import get_organisation_taxonomy

from app.core.ingestion.utils import IngestContext


def validate_document_row(
    db: Session, context: IngestContext, row: DocumentIngestRow
) -> None:
    """
    Validate the constituent elements that represent this row.

    :param [Session] db: the connection to the database.
    :param [Organisation] organisation: The organisation context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    """
    _, taxonomy = get_organisation_taxonomy(db, context.org_id)
    result, _ = build_metadata(taxonomy, row)
    context.results.append(result)
