from datetime import datetime

from pydantic import BaseModel


class WithRelationships(BaseModel):
    labels: list["LabelRelationship"] = []
    documents: list["DocumentRelationship"] = []


class Label(WithRelationships):
    id: str
    type: str
    value: str


class LabelRelationship(BaseModel):
    type: str
    value: Label
    timestamp: datetime | None = None


class Item(BaseModel):
    url: str | None = None


class BaseDocument(BaseModel):
    id: str
    title: str
    description: str | None = None

    """@see: https://en.wikipedia.org/wiki/Functional_Requirements_for_Bibliographic_Records"""
    items: list[Item] = []


class DocumentRelationship(BaseModel):
    type: str
    value: "DocumentWithoutRelationships"
    timestamp: datetime | None = None


class Document(BaseDocument, WithRelationships):
    pass


class DocumentWithoutRelationships(BaseDocument):
    labels: list["LabelRelationship"] = []
