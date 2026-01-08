from datetime import datetime

from pydantic import BaseModel


class Label(BaseModel):
    id: str
    title: str
    type: str


class DocumentLabelRelationship(BaseModel):
    type: str
    label: Label
    timestamp: datetime | None = None


class BaseDocument(BaseModel):
    id: str
    title: str
    labels: list[DocumentLabelRelationship] = []


class DocumentDocumentRelationship(BaseModel):
    type: str
    document: "DocumentWithoutRelationships"
    timestamp: datetime | None = None


class Item(BaseModel):
    url: str | None = None


class Document(BaseDocument):
    description: str | None = None
    relationships: list[DocumentDocumentRelationship] = []

    """
    Based on the FRBR ontology
    @see: https://en.wikipedia.org/wiki/Functional_Requirements_for_Bibliographic_Records
    """
    items: list[Item] = []


class DocumentWithoutRelationships(BaseDocument):
    pass
