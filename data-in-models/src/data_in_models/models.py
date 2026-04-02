from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class WithRelationships(BaseModel):
    labels: list[LabelRelationship] = []
    documents: list[DocumentRelationship] = []


class WithAttributes(BaseModel):
    # the `key` of the `dict` will probably be managed via our knowledge managers
    # and not be a free for all
    attributes: dict[str, str | float | bool] = {}


class LabelBase(WithAttributes):
    id: str
    type: str
    value: str


class LabelWithoutDocumentRelationships(LabelBase):
    """A Label that can carry nested label relationships, but no document relationships.
    Used as the value type in LabelRelationship to prevent circular document nesting."""

    labels: list[LabelRelationship] = []


class Label(LabelWithoutDocumentRelationships):
    """Full Label — includes both label and document relationships."""

    documents: list[DocumentRelationship] = []


class LabelRelationship(BaseModel):
    type: str
    value: LabelWithoutDocumentRelationships | Label
    timestamp: datetime | None = None


class Item(BaseModel):
    url: str | None = None
    type: str
    content_type: str | None = None


class BaseDocument(WithAttributes):
    id: str
    title: str
    description: str | None = None

    """@see: https://en.wikipedia.org/wiki/Functional_Requirements_for_Bibliographic_Records"""
    items: list[Item] = []


class DocumentRelationship(BaseModel):
    type: str
    value: DocumentWithoutRelationships
    timestamp: datetime | None = None


class Document(BaseDocument, WithRelationships):
    pass


class DocumentWithoutRelationships(BaseDocument):
    labels: list[LabelRelationship] = []
