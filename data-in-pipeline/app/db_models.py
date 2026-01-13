from datetime import datetime

from sqlmodel import Field, Relationship, SQLModel


class Document(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    title: str
    description: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    labels: list["Label"] = Relationship(
        back_populates="documents", link_model="DocumentLabelLink"
    )
    items: list["Item"] = Relationship(back_populates="document")


class Label(SQLModel, table=True):
    id: str = Field(default=None, primary_key=True)
    title: str
    type: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class DocumentLabelLink(SQLModel, table=True):
    document_id: int = Field(foreign_key="documents.id", primary_key=True)
    label_id: str = Field(foreign_key="labels.id", primary_key=True)
    relationship_type: str
    timestamp: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class DocumentDocumentLink(SQLModel, table=True):
    source_document_id: int = Field(foreign_key="documents.id", primary_key=True)
    related_document_id: int = Field(foreign_key="documents.id", primary_key=True)
    relationship_type: str
    timestamp: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Item(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    document_id: int = Field(foreign_key="documents.id")
    url: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    document: Document = Relationship(back_populates="items")
