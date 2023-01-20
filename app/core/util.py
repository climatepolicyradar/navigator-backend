import logging
import os
import random
import string
from typing import Any, Optional


from sqlalchemy.orm import Session

from app.core.validation.types import DocumentValidationResult
from app.db.models import Document, Geography, DocumentType, Category
from app.db.session import Base

_LOGGER = logging.getLogger(__name__)


CDN_DOMAIN: str = os.getenv("CDN_DOMAIN", "cdn.climatepolicyradar.org")
# TODO: remove & replace with proper content-type handling through pipeline
CONTENT_TYPE_MAP = {
    ".pdf": "application/pdf",
    ".html": "text/html",
    ".htm": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


def to_cdn_url(s3_object_key: Optional[str]) -> Optional[str]:
    """Convert an s3 object key for a PDF in our s3 bucket to a URL to a PDF in our CDN.

    Args:
    :param str s3_url: object key for a PDF in our s3 bucket.

    Returns:
        str: URL to the PDF via our CDN domain.
    """
    if s3_object_key is None:
        return None
    return f"https://{CDN_DOMAIN}/{s3_object_key}"


def random_string(length=12):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def table_to_json(
    table: Base,
    db: Session,
) -> list[dict]:
    json_out = []

    for row in db.query(table).all():
        row_object = {col.name: getattr(row, col.name) for col in row.__table__.columns}
        json_out.append(row_object)

    return json_out


def tree_table_to_json(
    table: Base,
    db: Session,
) -> list[dict]:
    json_out = []
    child_list_map: dict[int, Any] = {}

    for row in db.query(table).order_by(table.id).all():
        row_object = {col.name: getattr(row, col.name) for col in row.__table__.columns}
        row_children: list[dict[str, Any]] = []
        child_list_map[row_object["id"]] = row_children

        # No parent indicates a top level element
        node_row_object = {"node": row_object, "children": row_children}
        node_id = row_object["parent_id"]
        if node_id is None:
            json_out.append(node_row_object)
        else:
            append_list = child_list_map.get(node_id)
            if append_list is None:
                raise RuntimeError(f"Could not locate parent node with id {node_id}")
            append_list.append(node_row_object)

    return json_out


def doc_has_updates(csv_document: DocumentValidationResult, db: Session) -> bool:
    """Compare the document provided in the csv against the document in the database to see if they are different."""
    # TODO do we need to handle a missing import_id?
    db_document = [
        doc
        for doc in db.query(Document).filter(
            Document.import_id == csv_document.import_id
        )
    ][0]

    results = {
        "name": {
            "db_value": db_document.name,
            "csv_value": csv_document.create_request.name,
        },
        "publication_ts": {
            "db_value": db_document.publication_ts,
            "csv_value": csv_document.create_request.publication_ts,
        },
        "description": {
            "db_value": db_document.description,
            "csv_value": csv_document.create_request.description,
        },
        "geography": {
            "db_value": [
                geo
                for geo in db.query(Geography).filter(
                    Geography.id == db_document.geography_id
                )
            ][0].value,
            "csv_value": csv_document.create_request.geography,
        },
        "type": {
            "db_value": [
                doc_type
                for doc_type in db.query(DocumentType).filter(
                    DocumentType.id == db_document.type_id
                )
            ][0].name,
            "csv_value": csv_document.create_request.type,
        },
        "category": {
            "db_value": [
                doc_cat
                for doc_cat in db.query(Category).filter(
                    Category.id == db_document.category_id
                )
            ][0].name,
            "csv_value": csv_document.create_request.category,
        },
    }
    for result in results:
        results[result]["updated"] = (
            results[result]["db_value"] != results[result]["csv_value"]
        )

    _LOGGER.info(f"{results} for {csv_document.import_id}")

    return any([results[result]["updated"] for result in results])
