from datetime import datetime
from typing import ClassVar, Optional

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass
from app.core.ingestion.ingest_row_base import BaseIngestRow

from app.db.models.law_policy import EventStatus, FamilyCategory


_REQUIRED_DOCUMENT_COLUMNS = [
    "ID",
    "Document ID",
    "Collection name",
    "Collection summary",
    "Document title",
    "Family name",
    "Family summary",
    "Document role",
    "Document variant",
    "Geography ISO",
    "Documents",
    "Category",
    "Sectors",
    "Instruments",
    "Frameworks",
    "Responses",
    "Natural Hazards",
    "Document Type",
    "Language",
    "Keywords",
    "Geography",
    "CPR Document ID",
    "CPR Family ID",
    "CPR Collection ID",
    "CPR Family Slug",
    "CPR Document Slug",
]
VALID_DOCUMENT_COLUMN_NAMES = set(_REQUIRED_DOCUMENT_COLUMNS)

_REQUIRED_EVENT_COLUMNS = [
    "Id",
    "Event type",
    "Title",
    "Date",
    "CPR Event ID",
    "CPR Family ID",
]
VALID_EVENT_COLUMN_NAMES = set(_REQUIRED_EVENT_COLUMNS)


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.forbid))
class CCLWDocumentIngestRow(BaseIngestRow):
    """Represents a single row of input from the documents-families-collections CSV."""

    id: str
    document_id: str
    collection_name: str
    collection_summary: str
    document_title: str
    family_name: str
    family_summary: str
    document_role: str
    document_variant: str
    geography_iso: str
    documents: str
    category: FamilyCategory
    sectors: list[str]  # METADATA
    instruments: list[str]  # METADATA
    frameworks: list[str]  # METADATA
    responses: list[str]  # METADATA - topics
    natural_hazards: list[str]  # METADATA - hazard
    keywords: list[str]
    document_type: str
    language: list[str]
    geography: str
    cpr_document_id: str
    cpr_family_id: str
    cpr_collection_id: str
    cpr_family_slug: str
    cpr_document_slug: str

    VALID_COLUMNS: ClassVar[set[str]] = VALID_DOCUMENT_COLUMN_NAMES

    @staticmethod
    def _key(key: str) -> str:
        return key.lower().replace(" ", "_")

    def get_first_url(self) -> Optional[str]:
        """
        Get the first URL from the 'documents' attribute.

        TODO: This could/should be written with more validation.
        """
        documents = self.documents.split(";")
        if len(documents) != 1:
            raise ValueError(f"Expected 1 document to be parsed from: {self.documents}")

        first_url = documents[0].split("|")[0]
        return first_url or None


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.ignore))
class EventIngestRow(BaseIngestRow):
    """Represents a single row of input from the events CSV."""

    id: str
    event_type: str
    title: str
    date: datetime
    cpr_event_id: str
    cpr_family_id: str
    event_status: EventStatus

    VALID_COLUMNS: ClassVar[set[str]] = VALID_EVENT_COLUMN_NAMES
