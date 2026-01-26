import logging
from typing import List

from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import DocumentLabelLink as DBDocumentLabelLink
from data_in_models.db_models import Item as DBItem
from data_in_models.db_models import Label as DBLabel
from data_in_models.models import Document as DocumentInput
from sqlalchemy.exc import DisconnectionError, OperationalError
from sqlmodel import Session, select

_LOGGER = logging.getLogger(__name__)


def check_db_health(db: Session) -> bool:
    """Check database connection health.

    Performs a simple query to verify the database is accessible
    and responsive.

    :return: True if database is healthy, False otherwise
    :rtype: bool
    """
    try:
        result = db.exec(select(1))
        return result.first() is not None
    except (OperationalError, DisconnectionError):
        _LOGGER.exception("Database health check failed")
    except Exception:
        _LOGGER.exception("Unexpected error during health check")
    return False


def create_or_update_documents(
    db: Session, documents: List[DocumentInput]
) -> List[str]:
    """
    Upsert a list of documents and their related entities.
    Returns list of document IDs processed.
    """
    processed_ids = []

    for doc_in in documents:
        existing = db.exec(select(DBDocument).where(DBDocument.id == doc_in.id)).first()
        if existing:
            existing.title = doc_in.title
            existing.description = doc_in.description
            current_doc = existing
        else:
            current_doc = DBDocument(
                id=doc_in.id,
                title=doc_in.title,
                description=doc_in.description,
            )
            db.add(current_doc)

        _delete_items_for_document(db, doc_in.id)
        db.flush()

        for item in doc_in.items:

            db.add(DBItem(document_id=doc_in.id, url=item.url, id=item.url))

        _delete_label_relationships_for_document(db, doc_in.id)
        db.flush()

        for rel in doc_in.labels:
            label = db.exec(select(DBLabel).where(DBLabel.id == rel.label.id)).first()
            if not label:
                label = DBLabel(
                    id=rel.label.id, title=rel.label.title, type=rel.label.type
                )
                db.add(label)
            db.add(
                DBDocumentLabelLink(
                    document_id=doc_in.id,
                    label_id=rel.label.id,
                    relationship_type=rel.type,
                    timestamp=rel.timestamp,
                )
            )

        processed_ids.append(doc_in.id)

    db.commit()
    return processed_ids


def _delete_items_for_document(db: Session, document_id: str):
    items = db.exec(select(DBItem).where(DBItem.document_id == document_id)).all()
    for item in items:
        db.delete(item)


def _delete_label_relationships_for_document(db: Session, document_id: str):
    rels = db.exec(
        select(DBDocumentLabelLink).where(
            DBDocumentLabelLink.document_id == document_id
        )
    ).all()
    for rel in rels:
        db.delete(rel)
