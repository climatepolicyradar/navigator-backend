from datetime import UTC, datetime

import pytest
from data_in_models.db_models import (
    Document,
    DocumentDocumentRelationship,
    DocumentLabelRelationship,
    Item,
    Label,
    LabelLabelRelationship,
)
from data_in_models.models import Document as DocumentInput
from data_in_models.models import (
    DocumentRelationship as DocumentDocumentRelationshipInput,
)
from data_in_models.models import (
    DocumentWithoutRelationships as DocumentWithoutRelationshipsInput,
)
from data_in_models.models import Item as ItemInput
from data_in_models.models import Label as LabelInput
from data_in_models.models import LabelRelationship as DocumentLabelRelationshipInput
from data_in_models.models import (
    LabelWithoutDocumentRelationships as LabelWithoutDocumentRelationshipsInput,
)
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
        documents=relationships or [],
    )


def create_mock_label(label_id="label_1", value="Test Label", attributes={}):
    """Create a mock label relationship."""
    label = LabelInput(id=label_id, value=value, type="status", attributes=attributes)

    return DocumentLabelRelationshipInput(
        type="tag", timestamp=datetime.now(UTC), value=label
    )


def create_mock_item(
    url="https://example.com", type="source", content_type="application/pdf"
):
    """Create a mock item."""
    return ItemInput(url=url, type=type, content_type=content_type)


def create_mock_label_with_parent(
    label_id: str,
    label_value: str,
    parent_id: str,
    parent_value: str,
    rel_type: str = "subconcept_of",
) -> list[DocumentLabelRelationshipInput]:
    """Create a child label with a parent, plus the parent as a standalone label.
    Returns both so the parent is written to the label table before the link row."""
    parent = LabelWithoutDocumentRelationshipsInput(
        id=parent_id,
        value=parent_value,
        type="category",
        attributes={},
        labels=[],
    )
    child = LabelWithoutDocumentRelationshipsInput(
        id=label_id,
        value=label_value,
        type="category",
        attributes={},
        labels=[
            DocumentLabelRelationshipInput(type=rel_type, value=parent, timestamp=None)
        ],
    )
    return [
        DocumentLabelRelationshipInput(type="concept", value=child, timestamp=None),
        DocumentLabelRelationshipInput(type="concept", value=parent, timestamp=None),
    ]


def test_upsert_creates_new_document(session):
    """Test creating a new document with no labels or items."""

    doc_input = create_mock_document_input("test-doc-1", "Test Document")
    doc_input.attributes = {"priority": 1.0, "active": True}
    result = create_or_update_documents(session, [doc_input])

    assert result == ["test-doc-1"]

    saved_doc = session.get(Document, "test-doc-1")
    assert saved_doc is not None
    assert saved_doc.title == "Test Document"
    assert saved_doc.attributes == {"priority": 1.0, "active": True}
    assert saved_doc.created_at is not None
    assert saved_doc.updated_at is not None


def test_upsert_creates_document_with_labels_and_items(session):
    """Test creating a document with associated labels and items."""

    label = create_mock_label("label_1", "Important")
    item = create_mock_item("https://example.com/file.pdf", "source", "html")
    doc_input = create_mock_document_input(
        "test-doc-2", "Document with Labels", labels=[label], items=[item]
    )

    result = create_or_update_documents(session, [doc_input])

    assert result == ["test-doc-2"]

    doc = session.get(Document, "test-doc-2")
    assert doc.title == "Document with Labels"

    label_entity = session.get(Label, "label_1")
    assert label_entity is not None
    assert label_entity.value == "Important"

    link = session.get(DocumentLabelRelationship, ("test-doc-2", "label_1"))
    assert link is not None
    assert link.type == "tag"

    assert len(doc.items) == 1
    item_entity = doc.items[0]
    assert item_entity.url == "https://example.com/file.pdf"
    assert item_entity.document_id == "test-doc-2"


def test_update_existing_document(session):
    """Test updating an existing document with new data."""
    initial_label = create_mock_label("update-label", "Label title", {})
    initial_doc = create_mock_document_input("update-doc", "Old Title", [initial_label])
    create_or_update_documents(session, [initial_doc])

    updated_label = create_mock_label(
        "update-label", "Label title", {"test-attribute": "updated-test-value"}
    )
    updated_doc = create_mock_document_input("update-doc", "New Title", [updated_label])
    result = create_or_update_documents(session, [updated_doc])

    assert result == ["update-doc"]

    doc = session.get(Document, "update-doc")
    assert doc.title == "New Title"

    label = session.get(Label, "update-label")
    assert label.attributes == {"test-attribute": "updated-test-value"}


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

    item = create_mock_item("https://batch.com", "source", "html")
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
    item = create_mock_item("https://example.com/file.pdf", "source", "html")
    doc_input = create_mock_document_input(
        "test-doc-2", "Document with Labels", labels=[label], items=[item]
    )

    result = create_documents(session, [doc_input])

    assert result == ["test-doc-2"]

    doc = session.get(Document, "test-doc-2")
    assert doc.title == "Document with Labels"

    label_entity = session.get(Label, "label_1")
    assert label_entity is not None
    assert label_entity.value == "Important"

    link = session.get(DocumentLabelRelationship, ("test-doc-2", "label_1"))
    assert link is not None
    assert link.type == "tag"

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
        type="has_member", timestamp=datetime.now(UTC), value=mock_target_doc
    )

    mock_doc = DocumentInput(
        id="source-doc-1",
        title="Source Document",
        description="Source description",
        labels=[],
        items=[],
        documents=[mock_rel],
    )

    result = create_or_update_documents(session, [mock_doc])

    assert result == ["source-doc-1"]

    source = session.get(Document, "source-doc-1")
    target = session.get(Document, "target-doc-1")

    assert source is not None
    assert target is not None
    assert target.title == "Target Document"

    relationships = session.exec(
        select(DocumentDocumentRelationship).where(
            DocumentDocumentRelationship.document_id == "source-doc-1"
        )
    ).all()

    assert len(relationships) == 1
    rel = relationships[0]
    assert rel.related_document_id == "target-doc-1"
    assert rel.type == "has_member"
    assert rel.document_id == "source-doc-1"


def test_empty_relationship_list_clears_existing_relationships(session):
    """When relationships list is empty, existing relationships should be removed."""

    mock_target_doc = DocumentWithoutRelationshipsInput(
        id="target-doc-1", title="Target Document", description="Target description"
    )

    mock_rel = DocumentDocumentRelationshipInput(
        type="has_member", timestamp=datetime.now(UTC), value=mock_target_doc
    )

    mock_doc = DocumentInput(
        id="simple-source",
        title="Simple Source",
        description=None,
        labels=[],
        items=[],
        documents=[mock_rel],
    )
    create_or_update_documents(session, [mock_doc])

    exists_query = select(DocumentDocumentRelationship).where(
        DocumentDocumentRelationship.document_id == "simple-source"
    )
    existing_rel = session.exec(exists_query).first()
    assert existing_rel is not None, "Relationship should exist after creation"

    mock_doc.documents = []
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
        type="has_member", timestamp=None, value=mock_target1
    )

    mock_document = DocumentInput(
        id="source-doc-001",
        title="Source Document",
        description=None,
        labels=[],
        items=[],
        documents=[mock_relationship1],
    )

    create_or_update_documents(session, [mock_document])

    relationships_after_first = session.exec(
        select(DocumentDocumentRelationship).where(
            DocumentDocumentRelationship.document_id == "source-doc-001"
        )
    ).all()

    assert len(relationships_after_first) == 1

    mock_target2 = DocumentWithoutRelationshipsInput(
        id="target-doc-2", title="Second Target", description=None
    )

    mock_relationship2 = DocumentDocumentRelationshipInput(
        type="references", timestamp=None, value=mock_target2
    )

    mock_document.documents = [mock_relationship1, mock_relationship2]

    create_or_update_documents(session, [mock_document])

    relationships_after_second = session.exec(
        select(DocumentDocumentRelationship).where(
            DocumentDocumentRelationship.document_id == "source-doc-001"
        )
    ).all()

    assert len(relationships_after_second) == len(
        mock_document.documents
    ), f"Expected {len(mock_document.documents)} relationships, got {len(relationships_after_second)}"

    rel_doc_ids = {r.related_document_id for r in relationships_after_second}
    assert rel_doc_ids == {"target-doc-1", "target-doc-2"}

    for rel in relationships_after_second:
        if rel.related_document_id == "target-doc-1":
            assert rel.type == "has_member"
        elif rel.related_document_id == "target-doc-2":
            assert rel.type == "references"


def test_label_with_parent_creates_link_row(session):
    """A label with a parent should write a row to LabelLabelRelationship."""
    label_rel = create_mock_label_with_parent(
        label_id="Endangered Species Act (US)",
        label_value="Endangered Species Act (US)",
        parent_id="Federal Statutory Claims (US)",
        parent_value="Federal Statutory Claims (US)",
    )
    doc = create_mock_document_input("doc-1", labels=label_rel)

    create_or_update_documents(session, [doc])

    link = session.get(
        LabelLabelRelationship,
        ("Endangered Species Act (US)", "Federal Statutory Claims (US)"),
    )
    assert link is not None
    assert link.type == "subconcept_of"


def test_label_without_parent_creates_no_link_row(session):
    """A label with an empty labels list should not write to LabelLabelRelationship."""
    label_rel = create_mock_label("orphan-label", "Orphan")
    doc = create_mock_document_input("doc-1", labels=[label_rel])

    create_or_update_documents(session, [doc])

    links = session.exec(select(LabelLabelRelationship)).all()
    assert len(links) == 0


def test_label_link_is_idempotent(session):
    """Upserting the same label-label relationship twice should produce one row."""
    label_rel = create_mock_label_with_parent(
        label_id="child", label_value="Child", parent_id="parent", parent_value="Parent"
    )
    doc = create_mock_document_input("doc-1", labels=label_rel)

    create_or_update_documents(session, [doc])
    create_or_update_documents(session, [doc])

    links = session.exec(select(LabelLabelRelationship)).all()
    assert len(links) == 1


def test_label_link_updated_when_parent_changes(session):
    """When a label's parent changes, the old link row should be removed and a new one written."""
    label_rel = create_mock_label_with_parent(
        label_id="child",
        label_value="Child",
        parent_id="parent-a",
        parent_value="Parent A",
    )
    doc = create_mock_document_input("doc-1", labels=label_rel)
    create_or_update_documents(session, [doc])

    assert session.get(LabelLabelRelationship, ("child", "parent-a")) is not None

    label_rel_new_parent = create_mock_label_with_parent(
        label_id="child",
        label_value="Child",
        parent_id="parent-b",
        parent_value="Parent B",
    )
    doc.labels = label_rel_new_parent
    create_or_update_documents(session, [doc])

    assert session.get(LabelLabelRelationship, ("child", "parent-a")) is None
    assert session.get(LabelLabelRelationship, ("child", "parent-b")) is not None


def test_multiple_labels_some_with_parents(session):
    """A mix of labels with and without parents should only write link rows for those with parents."""
    child_rel = create_mock_label_with_parent(
        label_id="child",
        label_value="Child",
        parent_id="parent",
        parent_value="Parent",
    )
    orphan_rel = create_mock_label("orphan", "Orphan")
    total_labels = child_rel + [orphan_rel]
    doc = create_mock_document_input("doc-1", labels=child_rel + [orphan_rel])

    create_or_update_documents(session, [doc])

    labels = session.exec(select(Label)).all()
    assert len(labels) == len(total_labels)  # child, parent, orphan

    links = session.exec(select(LabelLabelRelationship)).all()
    assert len(links) == 1
    assert links[0].label_id == "child"
    assert links[0].related_label_id == "parent"
