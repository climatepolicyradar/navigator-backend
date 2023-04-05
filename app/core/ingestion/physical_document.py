from typing import Any

from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.utils import to_dict

from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import Language, PhysicalDocumentLanguage


def create_physical_document_from_row(
    db: Session,
    row: DocumentIngestRow,
    result: dict[str, Any],
) -> PhysicalDocument:
    """
    Create the document part of the schema from the row.

    :param [Session] db: connection to the database.
    :param [IngestRow] row: the row built from the CSV.
    :param [Document] existing_document: existing Document from which to retrieve data.
    :return [dict[str, Any]]: a dictionary to describe what was created.
    """
    physical_document = PhysicalDocument(
        title=row.document_title,
        source_url=row.get_first_url(),
        md5_sum=None,
        content_type=None,
        cdn_object=None,
    )
    db.add(physical_document)
    db.flush()
    result["physical_document"] = to_dict(physical_document)

    lang = db.query(Language).filter(Language.name == row.language).one_or_none()
    if lang is not None:
        result["language"] = to_dict(lang)
        physical_document_language = PhysicalDocumentLanguage(
            language_id=lang.id, document_id=physical_document.id
        )
        db.add(physical_document_language)
        db.flush()
        result["physical_document_language"] = to_dict(physical_document_language)

    return physical_document
