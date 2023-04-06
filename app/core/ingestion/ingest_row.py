import abc
from dataclasses import fields
from datetime import datetime
from typing import Any, ClassVar, Sequence

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass

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


def validate_csv_columns(
    column_names: Sequence[str],
    valid_column_names: set[str],
) -> list[str]:
    """Check that the given set of column names is valid."""
    missing = list(valid_column_names.difference(set(column_names)))
    missing.sort()
    return missing


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.forbid))
class BaseIngestRow(abc.ABC):
    """Represents a single row of input from a CSV."""

    row_number: int

    VALID_COLUMNS: ClassVar[set[str]] = set()

    @classmethod
    def from_row(cls, row_number: int, data: dict[str, str]):
        """Parse a row from a CSV."""
        field_info = cls.field_info()
        return cls(
            row_number=row_number,
            **{
                cls._key(k): cls._parse_str(cls._key(k), v, field_info)
                for (k, v) in data.items()
                if cls._key(k) in field_info.keys()
            },
        )

    @classmethod
    def field_info(cls) -> dict[str, type]:
        """Returns an information mapping from field name to expected type."""
        return {field.name: field.type for field in fields(cls)}

    @classmethod
    def _parse_str(cls, key: str, value: str, field_info: dict[str, type]) -> Any:
        if key not in field_info:
            # Let pydantic deal with unexpected fields
            return value

        if field_info[key] == datetime:
            return datetime.strptime(value, "%Y-%m-%d")

        if field_info[key] == list[str]:
            return [e.strip() for e in value.split(";") if e.strip()]

        if field_info[key] == int:
            return int(value) if value else 0

        if field_info[key] == str:
            if (na := str(value).lower()) == "n/a":
                return na
            else:
                return value

        # Let pydantic deal with other field types (e.g. str-Enums)
        return value

    @staticmethod
    def _key(key: str) -> str:
        return key.lower().replace(" ", "_")


@dataclass(config=ConfigDict(frozen=True, validate_assignment=True, extra=Extra.forbid))
class DocumentIngestRow(BaseIngestRow):
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

    def get_first_url(self) -> str:
        """
        Get the first URL from the 'documents' attribute.

        FIXME: This could/should be written with more validation.
        """
        documents = self.documents.split(";")
        if len(documents) != 1:
            raise ValueError(f"Expected 1 document to be parsed from: {self.documents}")

        first_url = documents[0].split("|")[0]
        return first_url


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
