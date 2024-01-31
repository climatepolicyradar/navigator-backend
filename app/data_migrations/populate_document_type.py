import json

from sqlalchemy.orm import Session

from db_client.models.law_policy import FamilyDocumentType
from .utils import load_list_idempotent


def populate_document_type(db: Session) -> None:
    """Populates the document_type table with pre-defined data."""

    # This is no longer fixed but additive,
    # meaning we will add anything here that is not present in the table

    with open(
        "app/data_migrations/data/law_policy/document_type_data.json"
    ) as submission_type_file:
        document_type_data = json.load(submission_type_file)
        load_list_idempotent(
            db, FamilyDocumentType, FamilyDocumentType.name, "name", document_type_data
        )

    with open(
        "app/data_migrations/data/unf3c/submission_type_data.json"
    ) as submission_type_file:
        submission_type_data = json.load(submission_type_file)
        document_type_data = [
            {"name": e["name"], "description": e["name"]} for e in submission_type_data
        ]
        load_list_idempotent(
            db, FamilyDocumentType, FamilyDocumentType.name, "name", document_type_data
        )
