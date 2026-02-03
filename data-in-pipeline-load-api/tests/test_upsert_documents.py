from datetime import UTC, datetime

import pytest
from data_in_models.db_models import (
    Document,
    DocumentDocumentLink,
    DocumentLabelLink,
    Item,
    Label,
)
from data_in_models.models import Document as DocumentInput
from data_in_models.models import (
    DocumentDocumentRelationship as DocumentDocumentRelationshipInput,
)
from data_in_models.models import (
    DocumentLabelRelationship as DocumentLabelRelationshipInput,
)
from data_in_models.models import (
    DocumentWithoutRelationships as DocumentWithoutRelationshipsInput,
)
from data_in_models.models import Item as ItemInput
from data_in_models.models import Label as LabelInput
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from app.repository import create_documents, create_or_update_documents


def create_mock_document_input(
    doc_id, title="Test Doc", labels=None, items=None, relationships=None
):
    """Create a mock DocumentInput object for testing."""
    return DocumentInput(
        id=doc_id,
        title=title,
        description=None,
        labels=labels or [],
        items=items or [],
        relationships=relationships or [],
    )


def create_mock_label(label_id="label_1", title="Test Label"):
    """Create a mock label relationship."""
    label = LabelInput(id=label_id, title=title, type="status")

    return DocumentLabelRelationshipInput(
        type="tag", timestamp=datetime.now(UTC), label=label
    )


def create_mock_item(item_id="item_1", url="https://example.com"):
    """Create a mock item."""
    return ItemInput(
        id=item_id,
        url=url,
    )


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

    assert len(doc.items) == 1
    item_entity = doc.items[0]
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

    result = create_or_update_documents(session, documents)  # type: ignore[reportAttributeAccessIssue]

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

    assert len(doc.items) == 1
    item_entity = doc.items[0]
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


def test_shared_label_does_not_raise_error(session):
    """Two documents sharing the same label ID should succeed"""
    shared_label = create_mock_label("common-label", "Urgent")

    doc1 = create_mock_document_input("doc-1", "First Doc", labels=[shared_label])
    doc2 = create_mock_document_input("doc-2", "Second Doc", labels=[shared_label])

    result = create_documents(session, [doc1, doc2])

    assert result == ["doc-1", "doc-2"]


def test_create_document_with_document_relationship(session):
    """Test creating a new document with a document relationship."""

    mock_target_doc = DocumentWithoutRelationshipsInput(
        id="target-doc-1", title="Target Document", description="Target description"
    )

    mock_rel = DocumentDocumentRelationshipInput(
        type="has_member", timestamp=datetime.now(UTC), document=mock_target_doc
    )

    mock_doc = DocumentInput(
        id="source-doc-1",
        title="Source Document",
        description="Source description",
        labels=[],
        items=[],
        relationships=[mock_rel],
    )

    result = create_or_update_documents(session, [mock_doc])

    assert result == ["source-doc-1"]

    source = session.get(Document, "source-doc-1")
    target = session.get(Document, "target-doc-1")

    assert source is not None
    assert target is not None
    assert target.title == "Target Document"

    relationships = session.exec(
        select(DocumentDocumentLink).where(
            DocumentDocumentLink.source_document_id == "source-doc-1"
        )
    ).all()

    assert len(relationships) == 1
    rel = relationships[0]
    assert rel.related_document_id == "target-doc-1"
    assert rel.relationship_type == "has_member"
    assert rel.source_document_id == "source-doc-1"


def test_empty_relationship_list_clears_existing_relationships(session):
    """When relationships list is empty, existing relationships should be removed."""

    mock_target_doc = DocumentWithoutRelationshipsInput(
        id="target-doc-1", title="Target Document", description="Target description"
    )

    mock_rel = DocumentDocumentRelationshipInput(
        type="has_member", timestamp=datetime.now(UTC), document=mock_target_doc
    )

    mock_doc = DocumentInput(
        id="simple-source",
        title="Simple Source",
        description=None,
        labels=[],
        items=[],
        relationships=[mock_rel],
    )
    create_or_update_documents(session, [mock_doc])

    exists_query = select(DocumentDocumentLink).where(
        DocumentDocumentLink.source_document_id == "simple-source"
    )
    existing_rel = session.exec(exists_query).first()
    assert existing_rel is not None, "Relationship should exist after creation"

    mock_doc.relationships = []
    create_or_update_documents(session, [mock_doc])

    cleared_rel = session.exec(exists_query).first()
    assert (
        cleared_rel is None
    ), "Relationship should be removed when empty list provided"


def test_new_relationship_added_to_existing_document(session):
    """Test that a new relationship is added to an existing document's relationships."""

    mock_target1 = DocumentWithoutRelationshipsInput(
        id="target-doc-1", title="First Target", description=None
    )

    mock_relationship1 = DocumentDocumentRelationshipInput(
        type="has_member", timestamp=None, document=mock_target1
    )

    mock_document = DocumentInput(
        id="source-doc-001",
        title="Source Document",
        description=None,
        labels=[],
        items=[],
        relationships=[mock_relationship1],
    )

    create_or_update_documents(session, [mock_document])

    relationships_after_first = session.exec(
        select(DocumentDocumentLink).where(
            DocumentDocumentLink.source_document_id == "source-doc-001"
        )
    ).all()

    assert len(relationships_after_first) == 1

    mock_target2 = DocumentWithoutRelationshipsInput(
        id="target-doc-2", title="Second Target", description=None
    )

    mock_relationship2 = DocumentDocumentRelationshipInput(
        type="references", timestamp=None, document=mock_target2
    )

    mock_document.relationships = [mock_relationship1, mock_relationship2]

    create_or_update_documents(session, [mock_document])

    relationships_after_second = session.exec(
        select(DocumentDocumentLink).where(
            DocumentDocumentLink.source_document_id == "source-doc-001"
        )
    ).all()

    assert len(relationships_after_second) == len(
        mock_document.relationships
    ), f"Expected {len(mock_document.relationships)} relationships, got {len(relationships_after_second)}"

    rel_doc_ids = {r.related_document_id for r in relationships_after_second}
    assert rel_doc_ids == {"target-doc-1", "target-doc-2"}

    for rel in relationships_after_second:
        if rel.related_document_id == "target-doc-1":
            assert rel.relationship_type == "has_member"
        elif rel.related_document_id == "target-doc-2":
            assert rel.relationship_type == "references"
