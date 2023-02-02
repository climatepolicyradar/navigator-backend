from app.db.models.app.users import Organisation

from dfc_csv_reader import Row
from sqlalchemy.orm import Session
from scripts.ingest_dfc.collection_from_row import collection_from_row
from scripts.ingest_dfc.document_schema_from_row import document_schema_from_row
from scripts.ingest_dfc.family_from_row import family_from_row

from utils import get_or_create, to_dict
"""
if year is missing - you the default with has 9999
if only a year - then use without 99999

"""
def ingest_row(db: Session, row: Row) -> dict:
    """Creates a PhysicalDocument and a FamilyDocument together."""
    result = {}
    import_id = row.cpr_document_id

    print("- Creating organisation")
    result["organisation"] = to_dict(get_or_create(db, Organisation, name="CCLW"))
    
    print(f"- Creating physical document for import {import_id}")
    result["doc_schema"] = document_schema_from_row(db, row)

    print(f"- Creating family for import {import_id}")
    result["family_schema"] = family_from_row(db, result["organisation"]["id"], row, result["doc_schema"]["physical_document"]["id"])

    print(f"- Creating collection for import {import_id}")
    result["collection_schema"] = collection_from_row(db, result["organisation"]["id"], row, result["family_schema"]["family"]["id"])

    return result
