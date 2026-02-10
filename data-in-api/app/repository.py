import logging

from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import DocumentDocumentLink as DBDocumentDocumentLink
from data_in_models.db_models import DocumentLabelLink as DBDocumentLabelLink
from data_in_models.models import Document as DocumentOutput
from data_in_models.models import (
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    DocumentWithoutRelationships,
)
from data_in_models.models import Item as ItemOutput
from data_in_models.models import Label as LabelOutput
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


def get_all_documents(
    db: Session,
    page: int = 1,
    page_size: int = 20,
    label_id: str | None = None,
) -> list[DocumentOutput]:
    """
    Retrieve all documents.

    :param db: Database session
    :param page: Page number for pagination
    :param page_size: Number of items per page
    :return: List of all documents
    """
    try:
        offset = (page - 1) * page_size
        query = select(DBDocument)

        if label_id:
            query = (
                query.join(DBDocumentLabelLink)
                .where(DBDocumentLabelLink.label_id == label_id)
                .distinct()
            )

        query = query.offset(offset).limit(page_size)
        db_documents = db.exec(query).all()
        _LOGGER.debug(f"Retrieved {len(db_documents)} documents from the database.")
        return [_map_db_document_to_schema(db, db_doc) for db_doc in db_documents]

    except (OperationalError, DisconnectionError):
        db.rollback()
        _LOGGER.exception("System error during document retrieval operation")
        raise
    except Exception as e:
        _LOGGER.exception(f"Failed to retrieve all documents: {str(e)}")
        raise e


def get_document_by_id(db: Session, document_id: str) -> DocumentOutput | None:
    """
    Retrieve a single document by ID.

    :param db: Database session
    :param document_id: Document ID
    :return: Document schema or None if not found
    """
    try:
        query = select(DBDocument).where(DBDocument.id == document_id)
        db_doc = db.exec(query).first()

        if not db_doc:
            return None
        _LOGGER.debug(f"Retrieved document {document_id} from the database.")
        return _map_db_document_to_schema(db, db_doc)

    except (OperationalError, DisconnectionError):
        db.rollback()
        _LOGGER.exception("System error during document retrieval operation")
        raise
    except Exception as e:
        _LOGGER.exception(f"Failed to retrieve document {document_id}: {str(e)}")
        db.rollback()
        raise e


def _map_db_document_to_schema(db: Session, db_doc: DBDocument) -> DocumentOutput:
    """
    Map database document to Pydantic schema with all relationships.

    :param db_doc: Database document model
    :return: Pydantic document schema
    """

    items = [ItemOutput(url=item.url) for item in db_doc.items]

    labels = [
        DocumentLabelRelationship(
            type=link.relationship_type,
            label=LabelOutput(
                id=link.label.id,
                title=link.label.title,
                type=link.label.type,
            ),
            timestamp=link.timestamp,
        )
        for link in db_doc.labels
    ]

    db_relationships = db.exec(
        select(DBDocumentDocumentLink).where(
            DBDocumentDocumentLink.source_document_id == db_doc.id
        )
    ).all()

    relationships = []
    for link in db_relationships:
        related_doc = db.exec(
            select(DBDocument).where(DBDocument.id == link.related_document_id)
        ).first()

        if related_doc:
            relationships.append(
                DocumentDocumentRelationship(
                    type=link.relationship_type,
                    timestamp=link.timestamp,
                    document=DocumentWithoutRelationships(
                        id=related_doc.id,
                        title=related_doc.title,
                        description=related_doc.description,
                        labels=[
                            DocumentLabelRelationship(
                                type=lbl_link.relationship_type,
                                label=LabelOutput(
                                    id=lbl_link.label.id,
                                    title=lbl_link.label.title,
                                    type=lbl_link.label.type,
                                ),
                                timestamp=lbl_link.timestamp,
                            )
                            for lbl_link in related_doc.labels
                        ],
                        items=[ItemOutput(url=item.url) for item in related_doc.items],
                    ),
                )
            )

    return DocumentOutput(
        id=db_doc.id,
        title=db_doc.title,
        description=db_doc.description,
        labels=labels,
        items=items,
        relationships=relationships,
    )
