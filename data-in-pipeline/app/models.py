import uuid
from datetime import datetime
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
    raw_payload: str
    content_type: str = "application/json"
    connector_version: str
    run_id: str | None = None
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: ExtractedMetadata
    task_run_id: str | None
    flow_run_id: str | None


class Identified(BaseModel, Generic[ExtractedData]):
    data: ExtractedData
    id: str
    source: str


class Document(BaseModel):
    id: str
    title: str
