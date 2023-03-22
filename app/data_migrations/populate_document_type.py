import json

from sqlalchemy.orm import Session

from app.db.models.deprecated import DocumentType
from app.db.models.law_policy import FamilyDocumentType
from .utils import has_rows, load_list


def populate_document_type(db: Session) -> None:
    """Populates the document_type table with pre-defined data."""

    populate_old_schema = not has_rows(db, DocumentType)
    populate_new_schema = not has_rows(db, FamilyDocumentType)

    if not populate_old_schema and not populate_new_schema:
        return

    with open("app/data_migrations/data/document_type_data.json") as document_type_file:
        document_type_data = json.load(document_type_file)
        # TODO: Remove the following line when old schema is removed
        if populate_old_schema:
            load_list(db, DocumentType, document_type_data)
        if populate_new_schema:
            load_list(db, FamilyDocumentType, document_type_data)
