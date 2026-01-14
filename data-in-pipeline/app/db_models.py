from datetime import UTC, datetime

from sqlmodel import Field, Relationship, SQLModel


class WithDbDatetimeFields(SQLModel):
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)},
    )


class Document(WithDbDatetimeFields, table=True):
    id: str = Field(primary_key=True)
    title: str
    description: str | None = None

    items: list["Item"] = Relationship(back_populates="document")
    labels: list["DocumentLabelLink"] = Relationship(back_populates="document")


class Label(WithDbDatetimeFields, table=True):
    id: str = Field(primary_key=True)
    title: str
    type: str

    documents: list["DocumentLabelLink"] = Relationship(back_populates="label")


class DocumentLabelLink(WithDbDatetimeFields, table=True):
    relationship_type: str
    timestamp: datetime | None = None
    document_id: str = Field(foreign_key="document.id", primary_key=True)
    label_id: str = Field(foreign_key="label.id", primary_key=True)

    label: Label = Relationship(back_populates="documents")
    document: Document = Relationship(back_populates="labels")


class DocumentDocumentLink(WithDbDatetimeFields, table=True):
    relationship_type: str
    timestamp: datetime | None = None

    source_document_id: str = Field(foreign_key="document.id", primary_key=True)
    related_document_id: str = Field(foreign_key="document.id", primary_key=True)


class Item(WithDbDatetimeFields, table=True):
    url: str | None = None
    id: str = Field(primary_key=True)
    document_id: str = Field(foreign_key="document.id")

    document: Document = Relationship(back_populates="items")
