from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.metadata import build_metadata
from app.core.ingestion.organisation import get_organisation_taxonomy

from app.core.ingestion.utils import IngestContext


def validate_row(db: Session, context: IngestContext, row: IngestRow) -> None:
    """
    Validates the constituent elements that represent this row.

    Args:
        db (Session): the connection to the database.
        organisation (Organisation): The organisation context.
        row (IngestRow): the IngestRow object of the current CSV row.

    Returns:
        int: number of failures encountered
    """
    _, taxonomy = get_organisation_taxonomy(db, context.org_id)
    result, _ = build_metadata(taxonomy, row)
    context.results.append(result)
