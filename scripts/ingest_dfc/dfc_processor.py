from typing import Callable, Tuple, cast

from sqlalchemy.orm import Session

from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from app.db.models.app.users import Organisation
from scripts.ingest_dfc.dfc_row import (
    collection_from_row,
    DfcRow,
    family_from_row,
)
from scripts.ingest_dfc.utils import get_or_create, to_dict


ValidateFunc = Callable[[], bool]
ProcessFunc = Callable[[DfcRow], bool]


def ingest_row(db: Session, row: DfcRow) -> dict:
    """Creates the constituent elements in the database that will represent this row.

    Args:
        db (Session): the connection to the database.
        row (DfcRow): the DfcRow object of the current CSV row

    Returns:
        dict: _description_
    """
    result = {}
    import_id = row.cpr_document_id

    print("- Creating organisation")
    organisation = get_or_create(db, Organisation, name="CCLW")
    result["organisation"] = to_dict(organisation)

    print(f"- Creating FamilyDocument for import {import_id}")
    result["family"] = {}
    family = family_from_row(db, row, cast(int, organisation.id), result)

    print(f"- Creating Collection if required for import {import_id}")
    result["collection"] = {}
    collection_from_row(
        db, row, cast(int, organisation.id), cast(int, family.id), result["collection"]
    )

    return result


def get_dfc_processor(db: Session) -> Tuple[ValidateFunc, ProcessFunc]:
    """Gets the validation and process function for ingesting a CSV.

    Args:
        db (Session): the connection to the database

    Returns:
        Tuple[ValidateFunc, ProcessFunc]: A tuple of functions
    """

    def validate() -> bool:
        """Returns if we should be processing - there used to be a lot more to this."""
        num_new_documents = db.query(PhysicalDocument).count()
        num_old_documents = db.query(Document).count()
        print(
            f"Found {num_new_documents} new documents and {num_old_documents} old documents"
        )
        return True  # num_new_documents == 0 and num_old_documents > 0

    def process(row: DfcRow) -> bool:
        """Processes the row into the db."""
        print(f"Processing row: {row.row_number}")

        # No need to start transaction as there is one already started.

        result = ingest_row(db, row=row)
        # mypprint(result)

        # Return False for now so we just process one element
        # FIXME: Change this return value
        return True  # rows_processed < 2

    return validate, process
