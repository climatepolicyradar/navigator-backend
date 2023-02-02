from typing import Tuple
from sqlalchemy.orm import Session

from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from dfc_csv_reader import Row
from ingest import ingest_document


def get_dfc_processor(db: Session):

    def validate() -> bool:
        """Returns if we should be processing - there used to be a lot more to this."""
        num_new_documents = db.query(PhysicalDocument).count()
        print(f"Found {num_new_documents} new documents")
        return num_new_documents == 0

    def process(row: Row):
        """Processes the row into the db."""
        print(f"Processing row: {row.row_number}")

        # No need to start transaction as there is one already started.

        ingest_document(db, row=row)

        # TODO: Commit the changes
        # db.commit()

        # Return False for now so we just process one element
        # TODO: Change this return value
        return row.row_number < 2

    return validate, process
