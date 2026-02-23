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

from app.repository import get_all_documents, get_document_by_id

_LOGGER = logging.getLogger(__name__)


def create_label(
    session: Session, label_id: str, value: str, type_: str = "entity_type"
) -> DBLabel:
    label = DBLabel(id=label_id, value=value, type=type_)
    session.add(label)
    session.commit()
    return label


def create_document(
    session: Session, doc_id: str, title: str, description: str = ""
) -> DBDocument:
    doc = DBDocument(id=doc_id, title=title, description=description)
    session.add(doc)
    session.commit()
    return doc


def add_item_to_document(
    session: Session, doc_id: str, item_id: str, url: str
) -> DBItem:
    item = DBItem(id=item_id, document_id=doc_id, url=url)
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
def setup_documents(session: Session):
    """Set up two documents with labels, items, and a relationship."""
    create_label(session, "Main", "Main")
    create_label(session, "Law", "Law")

    create_document(session, "doc1", "Document 1", "First doc")
    create_document(session, "doc2", "Document 2", "Second doc")

    add_item_to_document(session, "doc1", "item1", "https://example.com/1")
    add_item_to_document(session, "doc2", "item2", "https://example.com/2")

    link_document_label(session, "doc1", "Main")
    link_document_label(session, "doc2", "Law")

    link_documents(session, "doc1", "doc2")

    return session


def test_get_all_documents_with_relationships(setup_documents: Session):
    session = setup_documents
    documents = get_all_documents(session, page=1, page_size=10)
    total_mocked_documents = 2

    assert len(documents) == total_mocked_documents

    doc1: DocumentOutput = next(d for d in documents if d.id == "doc1")
    assert doc1.title == "Document 1"
    assert doc1.items[0].url == "https://example.com/1"
    assert doc1.labels[0].value.value == "Main"
    assert doc1.documents[0].value.id == "doc2"
    assert doc1.documents[0].value.items[0].url == "https://example.com/2"


def test_get_document_by_id_with_relationships(setup_documents: Session):
    session = setup_documents
    doc1 = get_document_by_id(session, "doc1")

    assert doc1 is not None
    assert doc1.id == "doc1"
    assert doc1.labels[0].value.id == "Main"
    assert doc1.items[0].url == "https://example.com/1"
    assert doc1.documents[0].type == "member_of"
    assert doc1.documents[0].value.id == "doc2"


def test_get_document_by_id_not_found(setup_documents: Session):
    session = setup_documents
    doc = get_document_by_id(session, "nonexistent")
    assert doc is None
