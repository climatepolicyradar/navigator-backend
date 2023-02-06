from datetime import datetime
from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import Language, PhysicalDocumentLanguage
from sqlalchemy.orm import Session
from scripts.ingest_dfc.dfc_row.dfc_row import DfcRow

from scripts.ingest_dfc.utils import UNDEFINED_DATA_TIME, to_dict

def document_schema_from_row(db: Session, row: DfcRow) -> dict:
    """Creates the document part of the schema from the row.

    Args:
        db (Session): connection to the database.
        row (DfcRow): the row built from the CSV.

    Returns:
        dict : a created dictionary to describe what was created.
    """
    result = {}
    physical_document = PhysicalDocument(
        title=row.document_title,
        source_url=row.documents,
        date=datetime(row.year, 1, 1) if row.year else UNDEFINED_DATA_TIME ,
    )

    db.add(physical_document)
    db.commit()
    result["physical_document"] = to_dict(physical_document)

    print(f"- Getting language: {row.language}")
    lang = db.query(Language).filter(Language.name == row.language).first()

    if lang is not None:
        result["language"] = to_dict(lang)
        physical_document_language = PhysicalDocumentLanguage(
            language_id=lang.id,
            document_id=physical_document.id
        )
        db.add(lang)
        db.commit()
        result["physical_document_language"] = to_dict(physical_document_language)


    return result
