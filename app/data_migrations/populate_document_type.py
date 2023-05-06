import json

from sqlalchemy.orm import Session

from app.db.models.law_policy import FamilyDocumentType
from .utils import has_rows, load_list


def populate_document_type(db: Session) -> None:
    """Populates the document_type table with pre-defined data."""

    if has_rows(db, FamilyDocumentType):
        return

    with open(
        "app/data_migrations/data/law_policy/document_type_data.json"
    ) as document_type_file:
        document_type_data = json.load(document_type_file)
        load_list(db, FamilyDocumentType, document_type_data)
