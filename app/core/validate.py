"""Functions to support validating the data across sources."""

from sqlalchemy.orm import Session

from app.db.models.deprecated.document import (
    Document,
)
from app.db.models.document import PhysicalDocument
from app.db.models.law_policy import FamilyDocument, DocumentStatus


def get_physical_documents_matching_source_url(
    db: Session, filter_url: str
) -> list[tuple[str, str]]:
    """Get a list of all source_urls and cdn paths matching the filter."""
    filter = f"%{filter_url}%"
    rows = (
        db.query(PhysicalDocument.source_url, PhysicalDocument.cdn_object)
        .filter(PhysicalDocument.source_url.like(filter))
        .all()
    )
    return [tuple(r) for r in rows]


def document_source_urls(db: Session) -> list[tuple[str, str]]:
    """Get a list of all source_urls and cdn paths for deprecated documents."""
    documents = (db.query(Document)).all()

    return [(document.source_url, document.cdn_object) for document in documents]


def family_document_ids(db: Session) -> list[str]:
    """Get a list of all document IDs for published family documents."""
    family_documents = (
        db.query(FamilyDocument).filter(
            FamilyDocument.document_status == DocumentStatus.PUBLISHED,
        )
    ).all()

    return [family_document.import_id for family_document in family_documents]


def document_ids(db: Session) -> list[str]:
    """Get a list of all document IDs for published family documents."""
    documents = (db.query(Document)).all()

    return [document.import_id for document in documents]
