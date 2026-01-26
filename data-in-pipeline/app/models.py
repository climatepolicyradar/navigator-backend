import uuid
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TypeVar

from pydantic import BaseModel, Field

ExtractedData = TypeVar("ExtractedData")


class ExtractedMetadata(BaseModel):
    endpoint: str
    http_status: HTTPStatus


class ExtractedEnvelope[ExtractedData](BaseModel):
    """Envelope for extracted data from any source document."""

    data: ExtractedData
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_name: str
    source_record_id: str
    raw_payload: ExtractedData
    content_type: str = "application/json"
    connector_version: str
    extracted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: ExtractedMetadata
    task_run_id: str
    flow_run_id: str


class Identified[ExtractedData](BaseModel):
    data: ExtractedData
    id: str
    source: str
