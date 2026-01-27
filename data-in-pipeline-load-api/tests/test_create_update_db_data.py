from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from data_in_models.db_models import Document, DocumentLabelLink, Item, Label
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from app.repository import create_documents, create_or_update_documents


def create_mock_document_input(doc_id, title="Test Doc", labels=None, items=None):
    """Create a mock DocumentInput object for testing."""
    mock_doc = Mock()
    mock_doc.id = doc_id
    mock_doc.title = title
    mock_doc.description = None
    mock_doc.labels = labels or []
    mock_doc.items = items or []
    return mock_doc


def create_mock_label(label_id="label_1", title="Test Label"):
    """Create a mock label relationship."""
    mock_label = Mock()
    mock_label.id = label_id
    mock_label.title = title
    mock_label.type = "status"

    mock_rel = Mock()
    mock_rel.type = "tag"
    mock_rel.timestamp = datetime.now(UTC)
    mock_rel.label = mock_label

    return mock_rel


def create_mock_item(item_id="item_1", url="https://example.com"):
    """Create a mock item."""
    mock_item = Mock()
    mock_item.id = item_id
    mock_item.url = url
    return mock_item


def test_upsert_creates_new_document(session):
    """Test creating a new document with no labels or items."""

    doc_input = create_mock_document_input("test-doc-1", "Test Document")
    result = create_or_update_documents(session, [doc_input])

    assert result == ["test-doc-1"]

    saved_doc = session.get(Document, "test-doc-1")
    assert saved_doc is not None
    assert saved_doc.title == "Test Document"
    assert saved_doc.created_at is not None
    assert saved_doc.updated_at is not None


def test_upsert_creates_document_with_labels_and_items(session):
    """Test creating a document with associated labels and items."""

    label = create_mock_label("label_1", "Important")
    item = create_mock_item("item_1", "https://example.com/file.pdf")
    doc_input = create_mock_document_input(
        "test-doc-2", "Document with Labels", labels=[label], items=[item]
    )

    result = create_or_update_documents(session, [doc_input])

    assert result == ["test-doc-2"]

    doc = session.get(Document, "test-doc-2")
    assert doc.title == "Document with Labels"

    label_entity = session.get(Label, "label_1")
    assert label_entity is not None
    assert label_entity.title == "Important"

    link = session.get(DocumentLabelLink, ("test-doc-2", "label_1"))
    assert link is not None
    assert link.relationship_type == "tag"

    item_entity = session.get(Item, "item_1")
    assert item_entity is not None
    assert item_entity.url == "https://example.com/file.pdf"
    assert item_entity.document_id == "test-doc-2"


def test_update_existing_document(session):
    """Test updating an existing document with new data."""
    initial_doc = create_mock_document_input("update-doc", "Old Title")
    create_or_update_documents(session, [initial_doc])

    updated_doc = create_mock_document_input("update-doc", "New Title")
    result = create_or_update_documents(session, [updated_doc])

    assert result == ["update-doc"]

    doc = session.get(Document, "update-doc")
    assert doc.title == "New Title"


def test_idempotent_operation(session):
    """Test that calling the function twice with same data has same result."""
    doc_input = create_mock_document_input("idempotent-doc", "Same Document")
    label = create_mock_label("label_2", "Test")
    doc_input.labels = [label]

    result1 = create_or_update_documents(session, [doc_input])
    result2 = create_or_update_documents(session, [doc_input])

    assert result1 == ["idempotent-doc"]
    assert result2 == ["idempotent-doc"]

    docs = session.exec(select(Document)).all()
    assert len(docs) == 1

    labels = session.exec(select(Label)).all()
    assert len(labels) == 1


def test_batch_document_creation(session):
    """Test creating multiple documents in one call."""

    documents = [
        create_mock_document_input("batch-1", "First Batch Doc"),
        create_mock_document_input("batch-2", "Second Batch Doc"),
        create_mock_document_input("batch-3", "Third Batch Doc"),
    ]

    item = create_mock_item("batch-item", "https://batch.com")
    documents[1].items = [item]

    result = create_or_update_documents(session, documents)

    assert result == ["batch-1", "batch-2", "batch-3"]

    saved_docs = session.exec(select(Document)).all()
    assert len(saved_docs) == len(documents)

    items = session.exec(select(Item)).all()
    assert len(items) == 1
    assert items[0].document_id == "batch-2"


def test_create_document_success(session):
    """Test successfully creating a new document with labels and items."""
    label = create_mock_label("label_1", "Important")
    item = create_mock_item("item_1", "https://example.com/file.pdf")
    doc_input = create_mock_document_input(
        "test-doc-2", "Document with Labels", labels=[label], items=[item]
    )

    result = create_documents(session, [doc_input])

    assert result == ["test-doc-2"]

    doc = session.get(Document, "test-doc-2")
    assert doc.title == "Document with Labels"

    label_entity = session.get(Label, "label_1")
    assert label_entity is not None
    assert label_entity.title == "Important"

    link = session.get(DocumentLabelLink, ("test-doc-2", "label_1"))
    assert link is not None
    assert link.relationship_type == "tag"

    item_entity = session.get(Item, "item_1")
    assert item_entity is not None
    assert item_entity.url == "https://example.com/file.pdf"
    assert item_entity.document_id == "test-doc-2"


def test_document_id_conflict(session):
    """Test that duplicate document ID causes failure."""

    doc1 = create_mock_document_input("duplicate-id", "First Document")
    create_documents(session, [doc1])

    doc2 = create_mock_document_input("duplicate-id", "Second Document")

    with pytest.raises(IntegrityError):
        create_documents(session, [doc2])

    docs = session.exec(select(Document)).all()
    assert len(docs) == 0


def test_item_id_conflict(session):
    """Test that duplicate item ID causes failure."""

    item1 = create_mock_item("duplicate-item", "https://first.com")
    doc1 = create_mock_document_input("doc-1", "First Doc", items=[item1])
    create_documents(session, [doc1])

    item2 = create_mock_item("duplicate-item", "https://second.com")
    doc2 = create_mock_document_input("doc-2", "Second Doc", items=[item2])

    with pytest.raises(IntegrityError):
        create_documents(session, [doc2])

    docs = session.exec(select(Document)).all()
    assert len(docs) == 0


def test_label_id_conflict(session):
    """Test that duplicate label ID causes failure."""

    label1 = create_mock_label("duplicate-label", "Label One")
    doc1 = create_mock_document_input("doc-1", "First Doc", labels=[label1])
    create_documents(session, [doc1])

    label2 = Mock()
    label2.id = "duplicate-label"
    label2.title = "Different Title"
    label2.type = "status"

    rel2 = Mock()
    rel2.type = "tag"
    rel2.timestamp = datetime.now(UTC)
    rel2.label = label2

    doc2 = create_mock_document_input("doc-2", "Second Doc", labels=[rel2])

    with pytest.raises(IntegrityError):
        create_documents(session, [doc2])

    docs = session.exec(select(Document)).all()
    assert len(docs) == 0
