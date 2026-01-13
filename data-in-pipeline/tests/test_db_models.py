import pytest
from sqlmodel import SQLModel, Field, Session, create_engine, Relationship
from testcontainers.postgres import PostgresContainer


class TestDocumentTestLabelLink(SQLModel, table=True):
    document_id: str | None = Field(
        default=None, foreign_key="testdocument.id", primary_key=True
    )
    label_id: str | None = Field(
        default=None, foreign_key="testlabel.id", primary_key=True
    )


class TestLabel(SQLModel, table=True):
    id: str = Field(primary_key=True)
    title: str
    documents: list["TestDocument"] = Relationship(
        back_populates="labels", link_model=TestDocumentTestLabelLink
    )


class TestDocument(SQLModel, table=True):
    id: str = Field(primary_key=True)
    title: str
    labels: list[TestLabel] = Relationship(
        back_populates="documents", link_model=TestDocumentTestLabelLink
    )


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:17") as postgres:
        yield postgres


@pytest.fixture
def engine(postgres_container):
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    yield engine
    SQLModel.metadata.drop_all(engine)


def test_can_create_tables(engine):
    table_names = SQLModel.metadata.tables.keys()
    assert "document" in table_names
    assert "label" in table_names


def test_can_insert_and_query(engine):
    with Session(engine) as session:
        document = TestDocument(id="doc_1", title="Document 1")
        session.add(document)
        session.commit()
        session.refresh(document)

        label = TestLabel(id="label_1", title="Label 1")
        session.add(label)
        session.commit()

        queried_user = session.get(TestDocument, document.id)
        assert queried_user is not None
        assert queried_user.title == "Document 1"
