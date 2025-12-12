import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Generic, TypeVar

from pydantic import BaseModel, Field

ExtractedData = TypeVar("ExtractedData")


class ExtractedMetadata(BaseModel):
    endpoint: str
    http_status: HTTPStatus


class ExtractedEnvelope(BaseModel, Generic[ExtractedData]):
    """Envelope for extracted data from any source document."""

    data: ExtractedData
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_name: str
    source_record_id: str
    raw_payload: ExtractedData
    content_type: str = "application/json"
    connector_version: str
    extracted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: ExtractedMetadata
    task_run_id: str
    flow_run_id: str


class Identified(BaseModel, Generic[ExtractedData]):
    data: ExtractedData
    id: str
    source: str


class Label(BaseModel):
    id: str
    title: str
    type: str


class DocumentLabelRelationship(BaseModel):
    type: str
    label: Label
    timestamp: datetime | None = None


class Item(BaseModel):
    url: str | None = None


class BaseDocument(BaseModel):
    id: str
    title: str
    description: str | None = None
    labels: list[DocumentLabelRelationship] = []

    """
    This needs work, but is a decent placeholder while we work through the model.
    It is lightly based on the FRBR ontology.

    @see: https://en.wikipedia.org/wiki/Functional_Requirements_for_Bibliographic_Records
    """
    items: list[Item] = []


class DocumentDocumentRelationship(BaseModel):
    type: str
    document: "DocumentWithoutRelationships"
    timestamp: datetime | None = None


class Document(BaseDocument):
    relationships: list[DocumentDocumentRelationship] = []


class DocumentWithoutRelationships(BaseDocument):
    pass
