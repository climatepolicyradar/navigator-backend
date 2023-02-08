"""Base definitions for data ingest"""
import json
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Collection, Generator, Sequence, Union, List, Literal
import datetime

from pydantic.main import BaseModel

from app.api.api_v1.schemas.document import DocumentCreateRequest


class ValidationError(Exception):
    """Base class for import validation errors."""

    def __init__(self, message: str, details: dict):
        self.message = message
        self.details = details


class ImportSchemaMismatchError(ValidationError):
    """Raised when a provided bulk import file fails CSV schema validation."""

    def __init__(self, message: str, details: dict):
        super().__init__(
            message=f"Bulk import file failed schema validation: {message}",
            details=details,
        )


class DocumentsFailedValidationError(ValidationError):
    """Raised when bulk import data rows fail validation."""

    def __init__(self, message: str, details: dict):
        super().__init__(
            message=f"Document data provided for import failed validation: {message}",
            details=details,
        )


@dataclass
class DocumentValidationResult:
    """Class describing the results of validating individual documents."""

    row: int
    create_request: DocumentCreateRequest
    errors: dict[str, Collection[str]]
    import_id: str


class DocumentGenerator(ABC):
    """Base class for all document sources."""

    @abstractmethod
    def process_source(self) -> Generator[Sequence[DocumentCreateRequest], None, None]:
        """Generate document groups for processing from the configured source."""

        raise NotImplementedError("process_source() not implemented")


@dataclass
class UpdateResult:
    """Class describing the results of comparing csv data against the db data to identify updates."""

    db_value: Union[str, datetime.datetime]
    csv_value: Union[str, datetime.datetime]
    updated: bool
    type: Literal["PhysicalDocument", "Family"]
    field: str


class InputData(BaseModel):
    """Expected input data containing both document updates and new documents for the ingest stage of the pipeline."""

    new_documents: List[dict]
    updated_documents: dict[str, dict[str, UpdateResult]]

    def to_json(self) -> dict:
        updated_documents_json = {}
        for document_id, update_result in self.updated_documents.items():
            updated_documents_json[document_id] = {
                field_result: json.loads(
                    json.dumps(update_result[field_result].__dict__)
                )
                for field_result in update_result
            }

        return {
            "new_documents": self.new_documents,
            "updated_documents": updated_documents_json,
        }
