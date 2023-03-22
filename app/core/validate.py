"""Functions to support validating the data across sources."""

from sqlalchemy.orm import Session

from app.db.models.deprecated.document import (
    Document,
)
from app.db.models.document import PhysicalDocument


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
