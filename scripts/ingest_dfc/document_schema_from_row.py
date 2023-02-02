from datetime import datetime
from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import Language, PhysicalDocumentLanguage
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
