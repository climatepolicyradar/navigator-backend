from sqlmodel import Session, SQLModel

from app.db_models import Document, Label


def test_can_create_tables(engine):
    table_names = SQLModel.metadata.tables.keys()
    assert "document" in table_names
    assert "label" in table_names
    assert "documentlabellink" in table_names
    assert "documentdocumentlink" in table_names
    assert "item" in table_names


def test_can_insert_and_query(engine):
    with Session(engine) as session:
        document = Document(id="test-doc-1", title="Document 1")
        session.add(document)
        session.commit()
        session.refresh(document)

        label = Label(id="label_1", title="Label 1", type="Type A")
        session.add(label)
        session.commit()

        queried_user = session.get(Document, document.id)
        assert queried_user is not None
        assert queried_user.title == "Document 1"
