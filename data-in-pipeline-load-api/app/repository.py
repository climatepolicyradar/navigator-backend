import logging
from datetime import UTC, datetime
from typing import TypedDict
from uuid import uuid4

from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import (
    DocumentDocumentRelationship as DBDocumentRelationship,
)
from data_in_models.db_models import DocumentLabelRelationship as DBDocumentLabelLink
from data_in_models.db_models import Item as DBItem
from data_in_models.db_models import Label as DBLabel
from data_in_models.db_models import LabelLabelRelationship as DBLabelLabelLink
from data_in_models.models import Document as DocumentInput
from data_in_models.models import (
    DocumentRelationship as DocumentDocumentRelationshipInput,
)
from sqlalchemy import tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DisconnectionError, IntegrityError, OperationalError
from sqlmodel import Session, delete, select

_LOGGER = logging.getLogger(__name__)


class LabelRelationshipRow(TypedDict):
    label_id: str
    related_label_id: str
    type: str
    timestamp: datetime | None
    created_at: datetime
    updated_at: datetime


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

    now = datetime.now(UTC)

    try:
        processed_ids = []

        unique_labels = {
            rel.value.id: rel.value for doc_in in documents for rel in doc_in.labels
        }
        if unique_labels:
            label_stmt = insert(DBLabel).values(
                [
                    {
                        "id": label.id,
                        "value": label.value,
                        "type": label.type,
                        "attributes": label.attributes,
                        "created_at": now,
                        "updated_at": now,
                    }
                    for label in unique_labels.values()
                ]
            )
            label_stmt = label_stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "value": label_stmt.excluded.value,
                    "type": label_stmt.excluded.type,
                    "attributes": label_stmt.excluded.attributes,
                    "updated_at": now,
                },
                where=(
                    (DBLabel.value != label_stmt.excluded.value)
                    | (DBLabel.type != label_stmt.excluded.type)
                ),
            )
            db.exec(label_stmt)

            labels_with_label_relationship: list[LabelRelationshipRow] = [
                {
                    "label_id": label.id,
                    "related_label_id": rel.value.id,
                    "type": rel.type,
                    "timestamp": rel.timestamp,
                    "created_at": now,
                    "updated_at": now,
                }
                for label in unique_labels.values()
                for rel in (label.labels or [])
            ]

            if labels_with_label_relationship:
                sync_label_relationships(db, labels_with_label_relationship, now)

        for doc_in in documents:
            # Upsert main document using INSERT ... ON CONFLICT
            stmt = (
                insert(DBDocument)
                .values(
                    id=doc_in.id,
                    title=doc_in.title,
                    description=doc_in.description,
                    attributes=doc_in.attributes,
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "title": doc_in.title,
                        "description": doc_in.description,
                        "attributes": doc_in.attributes,
                        "updated_at": now,
                    },
                )
            )
            db.exec(stmt)

            _upsert_items_for_document(db, doc_in.id, doc_in.items)
            _upsert_labels_and_relationships(db, doc_in.id, doc_in.labels)
            _upsert_document_document_relationships(db, doc_in.id, doc_in.documents)
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
    """Replace all items for a document. Deletes existing items and inserts new ones.

    :param db: Database session
    :param document_id: Document ID to upsert items for
    :param incoming_items: List of items from input
    """

    if incoming_items:
        # Delete all existing items for this document
        db.exec(delete(DBItem).where(DBItem.document_id == document_id))

        item_stmt = insert(DBItem).values(
            [
                {
                    "id": str(uuid4()),
                    "document_id": document_id,
                    "url": item.url,
                    "type": item.type,
                    "content_type": item.content_type,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }
                for item in incoming_items
            ]
        )

        db.exec(item_stmt)


def _upsert_labels_and_relationships(
    db: Session, document_id: str, label_relationships: list
) -> None:
    """Upsert document-label relationships using INSERT ... ON CONFLICT.

    Labels themselves are bulk-upserted before this is called.

    :param db: Database session
    :param document_id: Document ID
    :param label_relationships: List of DocumentLabelRelationship objects
    """
    now = datetime.now(UTC)

    if not label_relationships:
        db.exec(
            delete(DBDocumentLabelLink).where(
                DBDocumentLabelLink.document_id == document_id
            )
        )
        return

    unique_rels = {rel.value.id: rel for rel in label_relationships}

    rows = [
        {
            "document_id": document_id,
            "label_id": rel.value.id,
            "type": rel.type,
            "timestamp": rel.timestamp,
            "created_at": now,
            "updated_at": now,
        }
        for rel in unique_rels.values()
    ]

    stmt = insert(DBDocumentLabelLink).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["document_id", "label_id"],
        set_={
            "type": stmt.excluded.type,
            "timestamp": stmt.excluded.timestamp,
            "updated_at": now,
        },
        where=(DBDocumentLabelLink.type != stmt.excluded.type),
    )
    db.exec(stmt)

    db.exec(
        delete(DBDocumentLabelLink).where(
            DBDocumentLabelLink.document_id == document_id,
            ~DBDocumentLabelLink.label_id.in_(unique_rels.keys()),  # type: ignore[attr-defined]
        )
    )


def _upsert_document_document_relationships(
    db: Session,
    document_id: str,
    relationships: list[DocumentDocumentRelationshipInput],
) -> None:
    now = datetime.now(UTC)

    if not relationships:
        db.exec(
            delete(DBDocumentRelationship).where(
                DBDocumentRelationship.document_id == document_id
            )
        )
        return

    unique_targets = {rel.value.id: rel.value for rel in relationships}

    target_rows = [
        {
            "id": target.id,
            "title": target.title,
            "description": target.description,
            "attributes": target.attributes,
            "created_at": now,
            "updated_at": now,
        }
        for target in unique_targets.values()
    ]

    # Bulk insert targets (do nothing if they already exist)
    db.exec(insert(DBDocument).values(target_rows).on_conflict_do_nothing())

    rel_rows = [
        {
            "document_id": document_id,
            "related_document_id": rel.value.id,
            "type": rel.type,
            "timestamp": rel.timestamp,
            "created_at": now,
            "updated_at": now,
        }
        for rel in relationships
    ]

    rel_stmt = insert(DBDocumentRelationship).values(rel_rows)

    rel_stmt = rel_stmt.on_conflict_do_update(
        index_elements=["document_id", "related_document_id"],
        set_={
            "type": rel_stmt.excluded.type,
            "timestamp": rel_stmt.excluded.timestamp,
            "updated_at": now,
        },
        where=(DBDocumentRelationship.type != rel_stmt.excluded.type),
    )

    db.exec(rel_stmt)

    # --- 3. Delete removed relationships ---
    incoming_target_ids = [rel.value.id for rel in relationships]

    db.exec(
        delete(DBDocumentRelationship).where(
            DBDocumentRelationship.document_id == document_id,
            ~DBDocumentRelationship.related_document_id.in_(incoming_target_ids),  # type: ignore[attr-defined]
        )
    )


def sync_label_relationships(
    db: Session,
    labels_with_label_relationship: list[LabelRelationshipRow],
    now: datetime,
):
    """
    Upsert concept-subconcept label relationships and remove old relationships
    for sub concepts where parent concepts have changed.

    :param db: Database session
    :param labels_with_label_relationship: List of dicts with keys: label_id, related
    _label_id, type, timestamp
    :param now: Current timestamp for created_at/updated_at fields
    """

    label_relationship_ids: set[tuple[str, str]] = set()
    sub_concept_ids: set[str] = set()

    for rel in labels_with_label_relationship:
        sub_concept_ids.add(rel["label_id"])
        label_relationship_ids.add((rel["label_id"], rel["related_label_id"]))

    # Delete old relationships
    delete_stmt = delete(DBLabelLabelLink).where(
        DBLabelLabelLink.label_id.in_(sub_concept_ids),  # type: ignore[attr-defined]
        ~tuple_(DBLabelLabelLink.label_id, DBLabelLabelLink.related_label_id).in_(  # type: ignore[attr-defined]
            label_relationship_ids
        ),
    )
    db.exec(delete_stmt)

    # Upsert current relationships
    stmt = insert(DBLabelLabelLink).values(labels_with_label_relationship)
    stmt = stmt.on_conflict_do_update(
        index_elements=["label_id", "related_label_id"],
        set_={
            "type": stmt.excluded.type,
            "timestamp": stmt.excluded.timestamp,
            "updated_at": now,
        },
        where=(DBLabelLabelLink.type != stmt.excluded.type),
    )
    db.exec(stmt)


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
                attributes=doc_in.attributes,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            db.exec(doc_stmt)

            for item in doc_in.items:
                item_stmt = (
                    insert(DBItem)
                    .values(
                        id=str(uuid4()),
                        document_id=doc_in.id,
                        url=item.url,
                        type=item.type,
                        content_type=item.content_type,
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
                        id=rel.value.id,
                        value=rel.value.value,
                        type=rel.value.type,
                        attributes=rel.value.attributes,
                        created_at=datetime.now(UTC),
                        updated_at=datetime.now(UTC),
                    )
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                db.exec(label_stmt)

                link_stmt = insert(DBDocumentLabelLink).values(
                    document_id=doc_in.id,
                    label_id=rel.value.id,
                    type=rel.type,
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
