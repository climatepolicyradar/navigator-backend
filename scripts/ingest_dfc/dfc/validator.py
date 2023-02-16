from sqlalchemy.orm import Session
from scripts.ingest_dfc.dfc.metadata import build_metadata
from scripts.ingest_dfc.dfc.organisation import get_organisation_taxonomy

from scripts.ingest_dfc.utils import DfcRow, IngestContext


def validate_row(db: Session, context: IngestContext, row: DfcRow) -> None:
    """
    Validates the constituent elements that represent this row.

    Args:
        db (Session): the connection to the database.
        organisation (Organisation): The organisation context.
        row (DfcRow): the DfcRow object of the current CSV row.

    Returns:
        int: number of failures encountered
    """
    _, taxonomy = get_organisation_taxonomy(db, context.org_id)
    result, _ = build_metadata(taxonomy, row)
    context.results.append(result)
