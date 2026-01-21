from datetime import UTC, datetime

from sqlalchemy import Column, ForeignKey, String
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
    id: str = Field(sa_column=Column(String, primary_key=True))
    title: str = Field(sa_column=Column(String, nullable=False))
    description: str | None = Field(sa_column=Column(String, nullable=True))

    items: list["Item"] = Relationship(back_populates="document")
    labels: list["DocumentLabelLink"] = Relationship(back_populates="document")


class Label(WithDbDatetimeFields, table=True):
    id: str = Field(sa_column=Column(String, primary_key=True))
    title: str = Field(sa_column=Column(String, nullable=False))
    type: str = Field(sa_column=Column(String, nullable=False))

    documents: list["DocumentLabelLink"] = Relationship(back_populates="label")


class DocumentLabelLink(WithDbDatetimeFields, table=True):
    relationship_type: str = Field(sa_column=Column(String, nullable=False))
    timestamp: datetime | None = None
    document_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("document.id"),
            primary_key=True,
            nullable=False,
        )
    )
    label_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("label.id"),
            primary_key=True,
            nullable=False,
        )
    )

    label: Label = Relationship(back_populates="documents")
    document: Document = Relationship(back_populates="labels")


class DocumentDocumentLink(WithDbDatetimeFields, table=True):
    relationship_type: str = Field(sa_column=Column(String, nullable=False))
    timestamp: datetime | None = None

    source_document_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("document.id"),
            primary_key=True,
            nullable=False,
        )
    )
    related_document_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("document.id"),
            primary_key=True,
            nullable=False,
        )
    )


class Item(WithDbDatetimeFields, table=True):
    url: str | None = Field(sa_column=Column(String, nullable=True))
    id: str = Field(sa_column=Column(String, primary_key=True))
    document_id: str = Field(
        sa_column=Column(String, ForeignKey("document.id"), nullable=False)
    )

    document: Document = Relationship(back_populates="items")
