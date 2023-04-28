"""Base definitions for data ingest"""


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
