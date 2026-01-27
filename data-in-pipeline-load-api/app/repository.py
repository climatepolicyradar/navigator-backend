import logging
from datetime import UTC, datetime

from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import DocumentLabelLink as DBDocumentLabelLink
from data_in_models.db_models import Item as DBItem
from data_in_models.db_models import Label as DBLabel
from data_in_models.models import Document as DocumentInput
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DisconnectionError, IntegrityError, OperationalError
from sqlmodel import Session, delete, select

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
    db: Session, documents: list[DocumentInput]
) -> list[str]:
    """
    Upsert a list of documents and their related entities.

    This operation is idempotent - running it multiple times with the same
    input produces the same result. All operations occur in a single transaction.

    :param db: Database session
    :param documents: List of documents to upsert
    :return: List of document IDs successfully processed
    :raises ValueError: If input validation fails
    :raises Exception: If database operation fails (transaction will be rolled back)
    """

    processed_ids = []

    try:
        processed_ids = []

        for doc_in in documents:
            # Upsert main document using INSERT ... ON CONFLICT
            stmt = (
                insert(DBDocument)
                .values(
                    id=doc_in.id,
                    title=doc_in.title,
                    description=doc_in.description,
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "title": doc_in.title,
                        "description": doc_in.description,
                        "updated_at": datetime.now(UTC),
                    },
                )
            )
            db.exec(stmt)

            _upsert_items_for_document(db, doc_in.id, doc_in.items)
            _upsert_labels_and_relationships(db, doc_in.id, doc_in.labels)

            processed_ids.append(doc_in.id)

        db.commit()
        _LOGGER.info(f"Successfully upserted {len(processed_ids)} documents")
        return processed_ids

    except ValueError:
        db.rollback()
        _LOGGER.exception("Validation failed for document upsert operation")
        raise
    except (OperationalError, DisconnectionError):
        db.rollback()
        _LOGGER.exception("System error during document upsert operation")
        raise
    except Exception:
        db.rollback()
        _LOGGER.exception(
            f"Failed to upsert documents. Processed {len(processed_ids)} of {len(documents)} documents before failure."
        )
        raise


def _upsert_items_for_document(
    db: Session, document_id: str, incoming_items: list
) -> None:
    """Upsert items using INSERT ... ON CONFLICT. No race condition.

    :param db: Database session
    :param document_id: Document ID to upsert items for
    :param incoming_items: List of items from input
    """
    incoming_ids: list[str] = []

    # Upsert each item atomically
    for item in incoming_items:
        stmt = (
            insert(DBItem)
            .values(
                id=item.id,
                document_id=document_id,
                url=item.url,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            .on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "url": item.url,
                    "document_id": document_id,
                    "updated_at": datetime.now(UTC),
                },
            )
        )
        db.exec(stmt)
        incoming_ids.append(item.id)

    # Delete orphaned items (items no longer in input)
    if incoming_ids:
        db.exec(
            delete(DBItem).where(
                DBItem.document_id == document_id, ~DBItem.id.in_(incoming_ids)
            )
        )
    else:
        # No incoming items, delete all items for this document
        db.exec(delete(DBItem).where(DBItem.document_id == document_id))


def _upsert_labels_and_relationships(
    db: Session, document_id: str, label_relationships: list
) -> None:
    """Upsert labels and their relationships using INSERT ... ON CONFLICT.

    :param db: Database session
    :param document_id: Document ID
    :param label_relationships: List of DocumentLabelRelationship objects
    """
    incoming_label_ids: list[str] = []

    for rel in label_relationships:
        label_stmt = (
            insert(DBLabel)
            .values(
                id=rel.label.id,
                title=rel.label.title,
                type=rel.label.type,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            .on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "title": rel.label.title,
                    "type": rel.label.type,
                    "updated_at": datetime.now(UTC),
                },
            )
        )
        db.exec(label_stmt)

        link_stmt = (
            insert(DBDocumentLabelLink)
            .values(
                document_id=document_id,
                label_id=rel.label.id,
                relationship_type=rel.type,
                timestamp=rel.timestamp,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            .on_conflict_do_update(
                index_elements=["document_id", "label_id"],
                set_={
                    "relationship_type": rel.type,
                    "timestamp": rel.timestamp,
                    "updated_at": datetime.now(UTC),
                },
            )
        )
        db.exec(link_stmt)
        incoming_label_ids.append(rel.label.id)

    if incoming_label_ids:
        db.exec(
            delete(DBDocumentLabelLink).where(
                DBDocumentLabelLink.document_id == document_id,
                ~DBDocumentLabelLink.label_id.in_(incoming_label_ids),
            )
        )
    else:
        db.exec(
            delete(DBDocumentLabelLink).where(
                DBDocumentLabelLink.document_id == document_id
            )
        )


def create_documents(db: Session, documents: list[DocumentInput]) -> list[str]:
    """
    Create new documents and related entities.

    This operation is atomic:
    - All documents are created successfully, OR
    - Any conflict causes the entire batch to fail and rollback

    No partial writes are allowed.

    :param db: Database session
    :param documents: List of documents to create
    :return: List of created document IDs
    :raises ValueError: On any constraint violation (document/item/label ID conflict)
    """
    created_ids: list[str] = []

    try:
        for doc_in in documents:
            doc_stmt = insert(DBDocument).values(
                id=doc_in.id,
                title=doc_in.title,
                description=doc_in.description,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            db.exec(doc_stmt)

            for item in doc_in.items:
                item_stmt = (
                    insert(DBItem)
                    .values(
                        id=item.id,
                        document_id=doc_in.id,
                        url=item.url,
                        created_at=datetime.now(UTC),
                        updated_at=datetime.now(UTC),
                    )
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                db.exec(item_stmt)

            for rel in doc_in.labels:
                label_stmt = (
                    insert(DBLabel)
                    .values(
                        id=rel.label.id,
                        title=rel.label.title,
                        type=rel.label.type,
                        created_at=datetime.now(UTC),
                        updated_at=datetime.now(UTC),
                    )
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                db.exec(label_stmt)

                link_stmt = insert(DBDocumentLabelLink).values(
                    document_id=doc_in.id,
                    label_id=rel.label.id,
                    relationship_type=rel.type,
                    timestamp=rel.timestamp,
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
                db.exec(link_stmt)

            created_ids.append(doc_in.id)

        db.commit()
        _LOGGER.info(f"Successfully created {len(created_ids)} documents strictly")
        return created_ids

    except IntegrityError:
        db.rollback()
        _LOGGER.exception("Create failed due to integrity constraint violation")
        raise
    except (OperationalError, DisconnectionError):
        db.rollback()
        _LOGGER.exception("System error during document upsert operation")
        raise
    except Exception:
        db.rollback()
        _LOGGER.exception(
            f"Unexpected failure during strict create. "
            f"Attempted to create {len(documents)} documents."
        )
        raise
