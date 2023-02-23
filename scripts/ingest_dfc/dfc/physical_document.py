from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import Language, PhysicalDocumentLanguage

from scripts.ingest_dfc.utils import (
    UNDEFINED_DATA_TIME,
    DfcRow,
    PublicationDateAccuracy,
    to_dict,
)


def physical_document_from_row(
    db: Session, row: DfcRow, existing_document: Document, result: dict[str, Any]
) -> PhysicalDocument:
    """
    Create the document part of the schema from the row.

    :param [Session] db: connection to the database.
    :param [DrfRow] row: the row built from the CSV.
    :return [dict[str, Any]]: a dictionary to describe what was created.
    """
    document_date = (
        datetime(row.year, 1, 1, 0, 0, 0, PublicationDateAccuracy.YEAR_ACCURACY)
        if row.year
        else UNDEFINED_DATA_TIME
    )
    physical_document = PhysicalDocument(
        title=row.document_title,
        source_url=row.get_first_url(),
        date=document_date,
        cdn_object=existing_document.cdn_object,
        md5_sum=existing_document.md5_sum,
        content_type=existing_document.content_type,
    )
    db.add(physical_document)
    db.flush()
    result["physical_document"] = to_dict(physical_document)

    print(f"- Getting language: {row.language}")
    lang = db.query(Language).filter(Language.name == row.language).one_or_none()

    if lang is not None:
        result["language"] = to_dict(lang)
        physical_document_language = PhysicalDocumentLanguage(
            language_id=lang.id, document_id=physical_document.id
        )
        db.add(lang)
        db.flush()
        result["physical_document_language"] = to_dict(physical_document_language)

    return physical_document
