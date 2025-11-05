import uuid
from datetime import datetime
from typing import Any, Dict, Generic, Optional, TypeVar

from pydantic import BaseModel, Field

ExtractedData = TypeVar("ExtractedData")


class ExtractedEnvelope(BaseModel, Generic[ExtractedData]):
    """Envelope for extracted data from any source document."""

    data: ExtractedData
    envelope_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_name: str
    source_record_id: str
    raw_payload: str
    content_type: str = "application/json"
    connector_version: str
    run_id: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # A simple source identifier (like "navigator")
    source: Optional[str] = None


class Identified(BaseModel, Generic[ExtractedData]):
    data: ExtractedData
    id: str
    source: str


class Document(BaseModel):
    id: str
