import logging

import pytest
from data_in_models.db_models import Document as DBDocument
from data_in_models.db_models import (
    DocumentDocumentRelationship as DBDocumentDocumentLink,
)
from data_in_models.db_models import DocumentLabelRelationship as DBDocumentLabelLink
from data_in_models.db_models import Item as DBItem
from data_in_models.db_models import Label as DBLabel
from data_in_models.db_models import LabelLabelRelationship as DBLabelLabelRelationship
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


def link_label_to_parent(
    session: Session,
    label_id: str,
    parent_id: str,
    type: str = "subconcept_of",
) -> DBLabelLabelRelationship:
    link = DBLabelLabelRelationship(
        label_id=label_id,
        related_label_id=parent_id,
        type=type,
    )
    session.add(link)
    session.commit()
    return link


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
def setup_documents(session: Session):
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


def test_get_all_documents_with_relationships(setup_documents: Session):
    documents = get_all_documents(setup_documents, page=1, page_size=10)
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


def test_get_document_by_id_with_relationships(setup_documents: Session):
    doc1 = get_document_by_id(setup_documents, "doc1")

    assert doc1 is not None
    assert doc1.id == "doc1"
    assert doc1.labels[0].value.id == "Main"
    assert doc1.items[0].url == "https://example.com/1"
    assert doc1.items[0].type == "cdn"
    assert doc1.items[0].content_type == "html"
    assert doc1.documents[0].type == "member_of"
    assert doc1.documents[0].value.id == "doc2"


def test_get_document_by_id_not_found(setup_documents: Session):
    doc = get_document_by_id(setup_documents, "nonexistent")
    assert doc is None


def test_get_all_documents_filter_by_label_existing(setup_documents: Session):

    documents = get_all_documents(
        setup_documents, page=1, page_size=10, label_id="Main"
    )

    assert len(documents) == 1
    doc = documents[0]
    assert doc.id == "doc1"
    assert any(lbl.value.id == "Main" for lbl in doc.labels)


def test_get_all_documents_filter_by_status(setup_documents: Session):

    documents = get_all_documents(
        setup_documents, page=1, page_size=10, status="PUBLISHED"
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


def test_get_all_documents_filter_by_label_nonexistent(setup_documents: Session):

    documents = get_all_documents(
        setup_documents, page=1, page_size=10, label_id="NonExistentLabel"
    )

    assert len(documents) == 0


def test_label_with_parent_returns_nested_label(session: Session):
    """A label with a parent should return the parent nested in labels."""
    create_label(
        session,
        "Federal Statutory Claims (US)",
        "Federal Statutory Claims (US)",
        type_="category",
    )
    create_label(
        session,
        "Endangered Species Act (US)",
        "Endangered Species Act (US)",
        type_="category",
    )
    link_label_to_parent(
        session, "Endangered Species Act (US)", "Federal Statutory Claims (US)"
    )

    create_document(session, "doc1", "Document 1")
    link_document_label(session, "doc1", "Endangered Species Act (US)", type="concept")

    doc = get_document_by_id(session, "doc1")

    assert doc is not None
    assert len(doc.labels) == 1

    label_rel = doc.labels[0]
    assert label_rel.value.id == "Endangered Species Act (US)"
    assert label_rel.value.type == "category"
    assert len(label_rel.value.labels) == 1

    parent = label_rel.value.labels[0]
    assert parent.type == "subconcept_of"
    assert parent.value.id == "Federal Statutory Claims (US)"
    assert parent.value.value == "Federal Statutory Claims (US)"
    assert parent.value.type == "category"
    assert parent.value.labels == []


def test_label_without_parent_has_empty_nested_labels(session: Session):
    """A label with no parent should return with an empty labels list."""
    create_label(session, "Principal", "Principal", type_="status")
    create_document(session, "doc1", "Document 1")
    link_document_label(session, "doc1", "Principal", type="status")

    doc = get_document_by_id(session, "doc1")

    assert doc is not None
    label_rel = doc.labels[0]
    assert label_rel.value.id == "Principal"
    assert label_rel.value.labels == []


def test_mixed_labels_with_and_without_parents(session: Session):
    """A document with some labels that have parents and some that don't."""
    create_label(
        session,
        "Federal Statutory Claims (US)",
        "Federal Statutory Claims (US)",
        type_="category",
    )
    create_label(
        session,
        "Endangered Species Act (US)",
        "Endangered Species Act (US)",
        type_="category",
    )
    create_label(session, "Principal", "Principal", type_="status")
    link_label_to_parent(
        session, "Endangered Species Act (US)", "Federal Statutory Claims (US)"
    )

    create_document(session, "doc1", "Document 1")
    link_document_label(session, "doc1", "Endangered Species Act (US)", type="concept")
    link_document_label(session, "doc1", "Principal", type="status")

    expected_total_document_labels = 2

    doc = get_document_by_id(session, "doc1")

    assert doc is not None
    assert len(doc.labels) == expected_total_document_labels

    by_id = {rel.value.id: rel for rel in doc.labels}

    assert len(by_id["Endangered Species Act (US)"].value.labels) == 1
    assert (
        by_id["Endangered Species Act (US)"].value.labels[0].value.id
        == "Federal Statutory Claims (US)"
    )
    assert len(by_id["Principal"].value.labels) == 0


def test_document_with_no_labels_returns_empty_labels(session: Session):
    """A document with no labels should return an empty labels list."""
    create_document(session, "doc1", "Document 1")

    doc = get_document_by_id(session, "doc1")

    assert doc is not None
    assert doc.labels == []


def test_related_document_labels_do_not_include_parents(session: Session):
    """Labels on a related document embedded in DocumentRelationship should also carry parent info."""
    create_label(
        session,
        "Federal Statutory Claims (US)",
        "Federal Statutory Claims (US)",
        type_="category",
    )
    create_label(
        session,
        "Endangered Species Act (US)",
        "Endangered Species Act (US)",
        type_="category",
    )
    link_label_to_parent(
        session, "Endangered Species Act (US)", "Federal Statutory Claims (US)"
    )

    create_document(session, "doc1", "Document 1")
    create_document(session, "doc2", "Document 2")
    link_documents(session, "doc1", "doc2")
    link_document_label(session, "doc2", "Endangered Species Act (US)", type="concept")

    doc = get_document_by_id(session, "doc1")

    assert doc is not None
    assert len(doc.documents) == 1

    related = doc.documents[0].value
    assert related.id == "doc2"
    assert len(related.labels) == 1
    assert related.labels[0].value.id == "Endangered Species Act (US)"
