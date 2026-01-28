import logging

from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import DocumentLabelLink as DBDocumentDocumentLink
from data_in_models.models import Document as DocumentOutput
from data_in_models.models import DocumentLabelRelationship
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


def get_all_documents(db: Session) -> list[DocumentOutput]:
    """
    Retrieve all documents with all relationships eagerly loaded.

    :param db: Database session
    :return: List of all documents
    """
    try:
        query = select(DBDocument)
        db_documents = db.exec(query).all()

        return [_map_db_document_to_schema(db, db_doc) for db_doc in db_documents]

    except Exception as e:
        _LOGGER.exception(f"Failed to retrieve all documents: {str(e)}")
        raise


def get_document_by_id(db: Session, document_id: str) -> DocumentOutput | None:
    """
    Retrieve a single document by ID with all relationships eagerly loaded.

    :param db: Database session
    :param document_id: Document ID
    :return: Document schema or None if not found
    """
    try:
        query = select(DBDocument).where(DBDocument.id == document_id)
        db_doc = db.exec(query).first()

        if not db_doc:
            return None

        return _map_db_document_to_schema(db, db_doc)

    except Exception as e:
        _LOGGER.exception(f"Failed to retrieve document {document_id}: {str(e)}")
        raise


def _map_db_document_to_schema(db: Session, db_doc: DBDocument) -> DocumentOutput:
    """
    Map database document to Pydantic schema with all relationships.

    :param db_doc: Database document model
    :return: Pydantic document schema
    """
    # Map items
    items = [ItemOutput(url=item.url) for item in db_doc.items]

    # Map labels with relationships
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

    relationships = [
        # DocumentDocumentRelationship(
        #     type=link.relationship_type,
        #     timestamp=link.timestamp,
        #     document=DocumentWithoutRelationships(
        #         id=link.related_document_id,
        #         title="",  # populated later if needed
        #         description=None,  # populated later if needed
        #         labels=[],
        #         items=[],
        #     ),
        # )
        # for link in db_relationships
    ]

    return DocumentOutput(
        id=db_doc.id,
        title=db_doc.title,
        description=db_doc.description,
        labels=labels,
        items=items,
        relationships=relationships,
    )
