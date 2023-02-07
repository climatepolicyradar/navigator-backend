from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import Language, PhysicalDocumentLanguage
from scripts.ingest_dfc.dfc_row.dfc_row import DfcRow
from scripts.ingest_dfc.utils import (
    UNDEFINED_DATA_TIME,
    PublicationDateAccuracy,
    to_dict,
)


def physical_document_from_row(db: Session, row: DfcRow, result: dict[str, Any]) -> PhysicalDocument:
    """Creates the document part of the schema from the row.

    Args:
        db (Session): connection to the database.
        row (DfcRow): the row built from the CSV.

    Returns:
        dict : a created dictionary to describe what was created.
    """
    document_date = (
        datetime(row.year, 1, 1, 0, 0, 0, PublicationDateAccuracy.YEAR_ACCURACY.value)
        if row.year
        else UNDEFINED_DATA_TIME
    )
    # TODO: Use event data too to inform publication date
    physical_document = PhysicalDocument(
        title=row.document_title,
        source_url=row.documents,
        date=document_date,
    )
    db.add(physical_document)
    db.commit()
    result["physical_document"] = to_dict(physical_document)

    print(f"- Getting language: {row.language}")
    lang = db.query(Language).filter(Language.name == row.language).first()

    if lang is not None:
        result["language"] = to_dict(lang)
        physical_document_language = PhysicalDocumentLanguage(
            language_id=lang.id, document_id=physical_document.id
        )
        db.add(lang)
        db.commit()
        result["physical_document_language"] = to_dict(physical_document_language)
    else:
        raise ValueError(f"Unknown language: {row.language}")

    return physical_document
