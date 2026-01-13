from datetime import datetime

from sqlalchemy import Column, DateTime, func
from sqlmodel import Field, Relationship, SQLModel


class Document(SQLModel, table=True):
    id: str = Field(primary_key=True)
    title: str
    description: str | None = None
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )
    items: list["Item"] = Relationship(back_populates="document")

    labels: list["DocumentLabelLink"] = Relationship(back_populates="document")


class Label(SQLModel, table=True):
    id: str = Field(primary_key=True)
    title: str
    type: str

    documents: list["DocumentLabelLink"] = Relationship(back_populates="label")

    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )


class DocumentLabelLink(SQLModel, table=True):
    document_id: str = Field(foreign_key="document.id", primary_key=True)
    label_id: str = Field(foreign_key="label.id", primary_key=True)
    relationship_type: str
    timestamp: datetime | None = None

    label: Label = Relationship(back_populates="documents")
    document: Document = Relationship(back_populates="labels")

    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )


class DocumentDocumentLink(SQLModel, table=True):
    source_document_id: str = Field(foreign_key="document.id", primary_key=True)
    related_document_id: str = Field(foreign_key="document.id", primary_key=True)
    relationship_type: str
    timestamp: datetime | None = None
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )


class Item(SQLModel, table=True):
    id: str = Field(primary_key=True)
    document_id: str = Field(foreign_key="document.id")
    url: str | None = None

    document: Document = Relationship(back_populates="items")

    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )
