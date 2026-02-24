from data_in_models.db_models import Document, Label
from sqlmodel import SQLModel


def test_can_create_tables(session):
    table_names = SQLModel.metadata.tables.keys()
    assert len(table_names) > 0
    assert "document" in table_names
    assert "label" in table_names
    assert "documentlabelrelationship" in table_names
    assert "documentdocumentrelationship" in table_names
    assert "labellabelrelationship" in table_names
    assert "item" in table_names


def test_can_insert_and_query(session):
    document = Document(id="test-doc-1", title="Document 1")
    session.add(document)
    session.commit()
    session.refresh(document)

    label = Label(id="label_1", value="Label 1", type="Type A")
    session.add(label)
    session.commit()

    queried_user = session.get(Document, document.id)
    assert queried_user is not None
    assert queried_user.title == "Document 1"
