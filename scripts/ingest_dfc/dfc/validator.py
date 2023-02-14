from typing import cast
from sqlalchemy.orm import Session
from app.db.models.app.users import Organisation
from scripts.ingest_dfc.dfc.metadata import validate_metadata
from scripts.ingest_dfc.dfc.organisation import get_organisation_taxonomy

from scripts.ingest_dfc.utils import DfcRow


def validate_row(db: Session, org_id: int, row: DfcRow) -> bool:
    """Validates the constituent elements that represent this row.

    Args:
        db (Session): the connection to the database.
        organisation (Organisation): The organisation context.
        row (DfcRow): the DfcRow object of the current CSV row.

    Returns:
        bool: returns True if validation passed
    """
    print("VALID")
    taxonomy = get_organisation_taxonomy(db, org_id)
    validate_metadata(taxonomy, row)

    return True
