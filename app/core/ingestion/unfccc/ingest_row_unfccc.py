from datetime import datetime
from typing import ClassVar

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass
from app.core.ingestion.ingest_row_base import BaseIngestRow

_REQUIRED_DOCUMENT_COLUMNS = [
    "Category",
    "Submission type",
    "Family name",
    "Document title",
    "Documents",
    "Author",
    "Author type",
    "Geography",
    "Geography ISO",
    "Date",
    "Document role",
    "Document variant",
    "Language",
    "CPR Collection ID",
    "CPR Document ID",
    "CPR Family ID",
    "CPR Family Slug",
    "CPR Document Slug",
    "CPR Document Status",
    "Download URL",
]
VALID_DOCUMENT_COLUMN_NAMES = set(_REQUIRED_DOCUMENT_COLUMNS)

_REQUIRED_COLLECTION_COLUMNS = [
    "CPR Collection ID",
    "Collection name",
    "Collection summary",
]
VALID_COLLECTION_COLUMN_NAMES = set(_REQUIRED_COLLECTION_COLUMNS)


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.forbid))
class UNFCCCDocumentIngestRow(BaseIngestRow):
    """Represents a single row of input from the UNFCCC CSV."""

    category: str
    submission_type: str  # aka Document Type for UNFCCC
    family_name: str
    document_title: str
    documents: str
    author: str
    author_type: str  # METADATA
    geography: str
    geography_iso: str
    date: datetime
    document_role: str
    document_variant: str
    language: list[str]

    cpr_collection_id: str
    cpr_document_id: str
    cpr_family_id: str
    cpr_family_slug: str
    cpr_document_slug: str
    cpr_document_status: str
    download_url: str

    # FIXME: Where is the summary from?
    family_summary: str = "summary"

    VALID_COLUMNS: ClassVar[set[str]] = VALID_DOCUMENT_COLUMN_NAMES

    @staticmethod
    def _key(key: str) -> str:
        return key.lower().replace(" ", "_")


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.ignore))
class CollectonIngestRow(BaseIngestRow):
    """Represents a single row of input from the collection CSV."""

    cpr_collection_id: str
    collection_name: str
    collection_summary: str

    VALID_COLUMNS: ClassVar[set[str]] = VALID_COLLECTION_COLUMN_NAMES
