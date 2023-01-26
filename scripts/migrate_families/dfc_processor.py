from pprint import pprint
from typing import Tuple
from sqlalchemy.orm import Session

from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from dfc_csv_reader import Row
from ingest import ingest_physical_document


def get_dfc_processor(db: Session):
    def validate() -> Tuple[int, int]:
        num_old_documents = db.query(Document).count()
        num_new_documents = db.query(PhysicalDocument).count()
        print(f"Found {num_new_documents} new and {num_old_documents} old documents")
        return num_new_documents == 0

    def process(row: Row):
        print(f"Processing row: {row.row_number}")

        # No need to start transaction as there is one already started.

        new_phys_doc = ingest_physical_document(db, row=row)
        print(f"Created new pd {new_phys_doc.id}")

        db.commit()

        # Return False for now so we just process one element
        # TODO: Change this return value
        return False

    return validate, process
