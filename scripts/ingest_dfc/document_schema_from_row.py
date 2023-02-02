from datetime import datetime
from app.db.models.document import PhysicalDocument
from dfc_csv_reader import Row
from sqlalchemy.orm import Session

from scripts.ingest_dfc.utils import DEFAULT_POLICY_DATE, to_dict

def document_schema_from_row(db: Session, row: Row) -> dict:
    result = {}
    physical_document = PhysicalDocument(
        title=row.document_title,
        source_url=row.documents,
        date=datetime(row.year, 1, 1) if row.year else DEFAULT_POLICY_DATE ,
    )

    # TODO: Add languages
    #print(row.language)

    db.add(physical_document)
    db.commit()
    result["physical_document"] = to_dict(physical_document)
    return result
