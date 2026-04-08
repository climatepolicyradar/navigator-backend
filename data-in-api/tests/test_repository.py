import logging

import pytest
from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import (
    DocumentDocumentRelationship as DBDocumentDocumentLink,
)
from data_in_models.db_models import DocumentLabelRelationship as DBDocumentLabelLink
from data_in_models.db_models import Item as DBItem
from data_in_models.db_models import Label as DBLabel
from data_in_models.models import Document as DocumentOutput
from sqlmodel import Session

from app.repository import get_all_documents, get_document_by_id, select_label

_LOGGER = logging.getLogger(__name__)


def create_label(
    session: Session,
    label_id: str,
    value: str,
    type_: str = "entity_type",
    attributes: dict[str, str | float | bool] = {},
) -> DBLabel:
    label = DBLabel(id=label_id, value=value, type=type_, attributes=attributes)
    session.add(label)
    session.commit()
    return label


def create_document(
    session: Session,
    doc_id: str,
    title: str,
    description: str = "",
    attributes: dict[str, str | float | bool] = {},
) -> DBDocument:
    doc = DBDocument(
        id=doc_id, title=title, description=description, attributes=attributes
    )
    session.add(doc)
    session.commit()
    return doc


# trunk-ignore(ruff/PLR0913)
def add_item_to_document(
    session: Session, doc_id: str, item_id: str, url: str, type: str, content_type: str
) -> DBItem:
    item = DBItem(
        id=item_id, document_id=doc_id, url=url, type=type, content_type=content_type
    )
    session.add(item)
    session.commit()
    return item


def link_document_label(
    session: Session, doc_id: str, label_id: str, type: str = "entity_type"
) -> DBDocumentLabelLink:
    link = DBDocumentLabelLink(document_id=doc_id, label_id=label_id, type=type)
    session.add(link)
    session.commit()
    return link


def link_documents(
    session: Session,
    source_id: str,
    target_id: str,
    type: str = "member_of",
) -> DBDocumentDocumentLink:
    link = DBDocumentDocumentLink(
        document_id=source_id,
        related_document_id=target_id,
        type=type,
    )
    session.add(link)
    session.commit()
    return link


@pytest.fixture
def session_with_documents(session: Session):
    """Set up two documents with labels, items, and a relationship."""
    create_label(session, "Main", "Main")
    create_label(session, "Law", "Law")

    create_document(session, "doc1", "Document 1", "First doc", {"status": "PUBLISHED"})
    create_document(session, "doc2", "Document 2", "Second doc", {"status": "DELETED"})

    add_item_to_document(
        session,
        "doc1",
        "item1",
        "https://example.com/1",
        type="cdn",
        content_type="html",
    )
    add_item_to_document(
        session,
        "doc2",
        "item2",
        "https://example.com/2",
        type="source",
        content_type="html",
    )

    link_document_label(session, "doc1", "Main")
    link_document_label(session, "doc2", "Law")

    link_documents(session, "doc1", "doc2")

    return session


def test_get_all_documents_with_relationships(session_with_documents: Session):
    documents = get_all_documents(session_with_documents, page=1, page_size=10)
    total_mocked_documents = 2

    assert len(documents) == total_mocked_documents

    doc1: DocumentOutput = next(d for d in documents if d.id == "doc1")
    assert doc1.title == "Document 1"
    assert doc1.items[0].url == "https://example.com/1"
    assert doc1.items[0].type == "cdn"
    assert doc1.items[0].content_type == "html"
    assert doc1.labels[0].value.value == "Main"
    assert doc1.documents[0].value.id == "doc2"
    assert doc1.documents[0].value.items[0].url == "https://example.com/2"


def test_get_document_by_id_with_relationships(session_with_documents: Session):
    doc1 = get_document_by_id(session_with_documents, "doc1")

    assert doc1 is not None
    assert doc1.id == "doc1"
    assert doc1.labels[0].value.id == "Main"
    assert doc1.items[0].url == "https://example.com/1"
    assert doc1.items[0].type == "cdn"
    assert doc1.items[0].content_type == "html"
    assert doc1.documents[0].type == "member_of"
    assert doc1.documents[0].value.id == "doc2"


def test_get_document_by_id_not_found(session_with_documents: Session):
    doc = get_document_by_id(session_with_documents, "nonexistent")
    assert doc is None


def test_get_all_documents_filter_by_label_existing(session_with_documents: Session):

    documents = get_all_documents(
        session_with_documents, page=1, page_size=10, label_id="Main"
    )

    assert len(documents) == 1
    doc = documents[0]
    assert doc.id == "doc1"
    assert any(lbl.value.id == "Main" for lbl in doc.labels)


def test_get_all_documents_filter_by_status(session_with_documents: Session):

    documents = get_all_documents(
        session_with_documents, page=1, page_size=10, status="PUBLISHED"
    )

    assert len(documents) == 1
    doc = documents[0]
    assert doc.id == "doc1"
    assert doc.attributes["status"] == "PUBLISHED"


def test_get_all_documents_filter_when_no_status(session: Session):
    create_document(
        session,
        "fam_doc",
        "Document from family",
        "Document from family description",
    )

    documents = get_all_documents(session, page=1, page_size=10, status="PUBLISHED")

    assert len(documents) == 0


def test_get_all_documents_filter_by_label_nonexistent(session_with_documents: Session):

    documents = get_all_documents(
        session_with_documents, page=1, page_size=10, label_id="NonExistentLabel"
    )

    assert len(documents) == 0


def test_select_label_with_attributes(session: Session):
    """Test that attributes are properly mapped from DBLabel to LabelOutput."""
    test_attributes = {
        "confidence": 0.95,
        "source": "manual",
        "verified": True,
        "count": 42,
    }

    create_label(
        session,
        label_id="test_label",
        value="TestLabel",
        type_="entity_type",
        attributes=test_attributes,
    )

    result = select_label(session, "test_label")

    assert result is not None
    assert result.id == "test_label"
    assert result.value == "TestLabel"
    assert result.type == "entity_type"
    assert result.attributes == test_attributes


def test_select_label_not_found(session: Session):
    """Test that select_label returns None for non-existent label."""
    result = select_label(session, "nonexistent_label")
    assert result is None


def test_select_label_with_empty_attributes(session: Session):
    """Test label with empty attributes dict."""
    create_label(session, label_id="empty_attr_label", value="EmptyAttr", attributes={})

    result = select_label(session, "empty_attr_label")

    assert result is not None
    assert result.attributes == {}


def test_select_label_with_null_attributes(session: Session):
    """Test handling of legacy NULL attributes in DB."""

    # Insert bad legacy data directly
    label = DBLabel(
        id="null_attr_label",
        value="NullAttr",
        type="entity_type",
        attributes=None,  # simulate corrupted/legacy DB state
    )
    session.add(label)
    session.commit()

    result = select_label(session, "null_attr_label")

    assert result is not None
    assert result.attributes == {}


def test_document_labels_include_attributes(session_with_documents: Session):
    """Test that label attributes appear in top-level document labels."""
    # Create a label with attributes and link to doc1
    create_label(
        session_with_documents,
        "labeled_attr",
        "LabeledAttr",
        type_="category",
        attributes={"confidence": 0.98, "source": "ml_model", "priority": 1},
    )
    link_document_label(session_with_documents, "doc1", "labeled_attr", type="category")

    result = get_document_by_id(session_with_documents, "doc1")
    assert result is not None

    # Find our specific label in the response
    target_label = next(
        (lbl for lbl in result.labels if lbl.value.id == "labeled_attr"), None
    )

    assert target_label is not None
    assert target_label.value.attributes == {
        "confidence": 0.98,
        "source": "ml_model",
        "priority": 1,
    }
    assert target_label.type == "category"
