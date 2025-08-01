from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Union

from cpr_sdk.pipeline_general_models import BackendDocument
from pydantic import BaseModel, ConfigDict, field_validator

from app.models import CLIMATE_LAWS_MATCH

Json = dict[str, Any]


class Event(BaseModel):  # noqa: D101
    name: str
    description: str
    created_ts: datetime

    def to_json(self) -> Mapping[str, Any]:
        """Provide a serialisable version of the model"""
        return {
            "name": self.name,
            "description": self.description,
            "created_ts": self.created_ts.isoformat(),
        }


class LinkableFamily(BaseModel):
    """For the frontend the minimum information to link to a family"""

    title: str
    slug: str
    description: str


class CollectionOverviewResponse(BaseModel):
    """Response for a Collection - without families"""

    import_id: str
    title: str
    description: str
    families: list[LinkableFamily]
    slug: str | None = None


class FamilyEventsResponse(BaseModel):
    """Response for a FamilyEvent, part of documents endpoints"""

    title: str
    date: datetime
    event_type: str
    status: str


class FamilyDocumentResponse(BaseModel):
    """Response for a FamilyDocument, without any family information"""

    import_id: str
    variant: Optional[str] = None
    slug: str
    # What follows is off PhysicalDocument
    title: str
    md5_sum: Optional[str] = None
    cdn_object: Optional[str] = None
    source_url: Optional[str] = None
    content_type: Optional[str] = None
    # TODO: Remove after transition to multiple languages
    language: str
    languages: Sequence[str]
    document_type: Optional[str] = None
    document_role: str

    @field_validator("source_url")
    @classmethod
    def _filter_climate_laws_url_from_source(cls, v):
        """Make sure we do not return climate-laws.org source URLs to the frontend"""
        if v is None or CLIMATE_LAWS_MATCH.match(v) is not None:
            return None
        return v


class FamilyContext(BaseModel):
    """Used to give the family context when returning a FamilyDocument"""

    title: str
    import_id: str
    geographies: list[str]
    category: str
    slug: str
    published_date: Optional[datetime] = None
    last_updated_date: Optional[datetime] = None
    corpus_id: str


class FamilyDocumentWithContextResponse(BaseModel):
    """Response for a FamilyDocument with its family's context"""

    family: FamilyContext
    document: FamilyDocumentResponse


class FamilyAndDocumentsResponse(BaseModel):
    """Response for a Family and its Documents, part of documents endpoints"""

    organisation: str
    import_id: str
    title: str
    summary: str
    geographies: list[str]
    category: str
    status: str
    metadata: dict
    slug: str
    events: list[FamilyEventsResponse]
    published_date: Optional[datetime] = None
    last_updated_date: Optional[datetime] = None
    documents: list[FamilyDocumentResponse]
    collections: list[CollectionOverviewResponse]
    corpus_id: str


class DocumentParserInput(BackendDocument):
    """Details of a document to be processed by the pipeline."""

    model_config = ConfigDict(from_attributes=True, validate_assignment=True)


class DocumentUpdateRequest(BaseModel):
    """The current supported fields allowed for update."""

    md5_sum: Optional[str] = None
    content_type: Optional[str] = None
    cdn_object: Optional[str] = None
    languages: Optional[Sequence[str]] = None

    def as_json(self) -> dict[str, Union[str, Sequence[str], None]]:
        """Convert to json for logging"""
        return {
            "md5_sum": self.md5_sum,
            "content_type": self.content_type,
            "cdn_object": self.cdn_object,
            "languages": self.languages,
        }

    def physical_doc_keys_json(self) -> dict[str, Union[str, None]]:
        """Convert to json updating only the physical document keys"""
        return {
            "md5_sum": self.md5_sum,
            "content_type": self.content_type,
            "cdn_object": self.cdn_object,
        }


class BulkIngestDetail(BaseModel):
    """Additional detail for bulk ingest."""

    document_count: int
    document_added_count: int
    document_skipped_count: int
    document_skipped_ids: list[str]


class BulkIngestResult(BaseModel):
    """Response for bulk ingest request."""

    import_s3_prefix: str
    detail: Optional[BulkIngestDetail] = None
