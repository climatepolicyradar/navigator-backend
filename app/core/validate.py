"""Functions to support validating the data across sources."""

from sqlalchemy.orm import Session

from app.db.models.deprecated.document import (
    Document,
)
from app.db.models.document import PhysicalDocument
from app.db.models.law_policy import FamilyDocument, DocumentStatus


def physical_document_source_urls(db: Session) -> list[tuple[str, str]]:
    """Get a list of all physical document source_urls and cdn paths (even archived)."""
    physical_documents = (db.query(PhysicalDocument)).all()

    return [
        (physical_document.source_url, physical_document.cdn_object)
        for physical_document in physical_documents
    ]


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
