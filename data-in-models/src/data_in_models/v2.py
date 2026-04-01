from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class LabelRelationship(BaseModel):
    type: str
    value: LabelWithoutLabelRelationships
    timestamp: datetime | None = None


class BaseLabel(BaseModel):
    value: str
    attributes: dict[str, str | float | bool] = {}


class Label(BaseLabel):
    labels: list[LabelRelationship]


class LabelWithoutLabelRelationships(BaseLabel):
    pass


class Item(BaseModel):
    url: str | None = None
    type: str
    content_type: str | None = None


class DocumentRelationship(BaseModel):
    type: str
    value: DocumentWithoutDocumentRelationships
    timestamp: datetime | None = None


class BaseDocument(BaseModel):
    id: str
    title: str
    description: str | None = None
    items: list[Item] = []
    attributes: dict[str, str | float | bool] = {}


class Document(BaseDocument):
    labels: list[LabelRelationship]
    documents: list[DocumentRelationship]


class DocumentWithoutDocumentRelationships(BaseDocument):
    labels: list[LabelRelationship] = []
