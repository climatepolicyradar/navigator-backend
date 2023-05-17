from datetime import datetime
from typing import ClassVar

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass
from app.core.ingestion.ingest_row_base import BaseIngestRow


_REQUIRED_DOCUMENT_COLUMNS = [
    "id",
    "md5sum",
    "Submission type",
    "Collection name",
    "Collection ID",
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
]
VALID_DOCUMENT_COLUMN_NAMES = set(_REQUIRED_DOCUMENT_COLUMNS)


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.forbid))
class UNFCCCDocumentIngestRow(BaseIngestRow):
    """Represents a single row of input from the documents-families-collections CSV."""

    id: str
    md5sum: str
    submission_type: str  # METADATA
    collection_name: str
    collection_id: str
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

    # FIXME: Modify these when the collections are ingested
    cpr_collection_id: str
    collection_summary: str
    # FIXME: Where is the family id / summary coming from?
    cpr_family_id: str
    family_summary: str
    # FIXME: Where is this coming from?
    cpr_document_id: str
    cpr_document_slug: str
    # FIXME: check this
    document_type = "UNFCCC"

    VALID_COLUMNS: ClassVar[set[str]] = VALID_DOCUMENT_COLUMN_NAMES

    @staticmethod
    def _key(key: str) -> str:
        return key.lower().replace(" ", "_")
