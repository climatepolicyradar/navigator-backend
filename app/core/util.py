import logging
import os
import random
import string
from typing import Any, Optional, List
from dataclasses import fields
from cloudpathlib import S3Path
from sqlalchemy.orm import Session


from app.core.validation.types import (
    DocumentValidationResult,
    DocumentUpdateResults,
    UpdateResult,
)
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


def update_doc_in_db(
    updates: DocumentUpdateResults, import_id: str, db: Session
) -> None:
    """Update the document table in the database."""
    with db.begin_nested():
        for field in fields(DocumentUpdateResults):
            if getattr(updates, field.name).updated:
                db.query(Document).filter(Document.import_id == import_id).update(
                    {field.name: getattr(updates, field.name).csv_value},
                    synchronize_session="fetch",
                )
                _LOGGER.info(
                    f"Updated {import_id}:{field.name} from {getattr(updates, field.name).db_value} -> {getattr(updates, field.name).csv_value}"
                )


# TODO do we want to archive the old file?
def delete_doc_in_s3(
    import_id: str, bucket: str, prefixes: List[str], suffixes=None
) -> None:
    """Delete the document objects in the relevant s3 buckets."""
    if suffixes is None:
        suffixes = [".json", ".npy"]

    for prefix in prefixes:
        for suffix in suffixes:
            s3_path = S3Path(os.path.join("s3://", bucket, prefix, import_id + suffix))
            if s3_path.exists():
                s3_path.unlink()
                _LOGGER.info(f"Deleted {s3_path}.")
            else:
                _LOGGER.info(
                    f"Could not find {s3_path} and therefore did not delete document."
                )


def get_update_results(
    csv_document: DocumentValidationResult, db: Session
) -> DocumentUpdateResults:
    """
    Compare the document provided in the csv against the document in the database to see if they are different. Then
    update the database and S3 if they are different to represent the new data.
    """
    db_document = (
        db.query(Document).filter(Document.import_id == csv_document.import_id).scalar()
    )

    if db_document is None:
        # TODO how to handle this?
        raise RuntimeError(
            f"Could not find document with import_id {csv_document.import_id}"
        )

    geog_db_value = [
        geo
        for geo in db.query(Geography).filter(Geography.id == db_document.geography_id)
    ][0].value

    doc_type_db_value = [
        doc_type
        for doc_type in db.query(DocumentType).filter(
            DocumentType.id == db_document.type_id
        )
    ][0].name

    category_db_value = [
        doc_cat
        for doc_cat in db.query(Category).filter(Category.id == db_document.category_id)
    ][0].name

    update_results = DocumentUpdateResults(
        name=UpdateResult(
            csv_value=csv_document.create_request.name,
            db_value=db_document.name,
            updated=csv_document.create_request.name != db_document.name,
        ),
        publication_ts=UpdateResult(
            csv_value=csv_document.create_request.publication_ts,
            db_value=db_document.publication_ts,
            updated=csv_document.create_request.publication_ts
            != db_document.publication_ts,
        ),
        description=UpdateResult(
            csv_value=csv_document.create_request.description,
            db_value=db_document.description,
            updated=csv_document.create_request.description != db_document.description,
        ),
        geography=UpdateResult(
            csv_value=csv_document.create_request.geography,
            db_value=geog_db_value,
            updated=csv_document.create_request.geography != geog_db_value,
        ),
        type=UpdateResult(
            csv_value=csv_document.create_request.type,
            db_value=doc_type_db_value,
            updated=csv_document.create_request.type != doc_type_db_value,
        ),
        category=UpdateResult(
            csv_value=csv_document.create_request.category,
            db_value=category_db_value,
            updated=csv_document.create_request.category != category_db_value,
        ),
    )

    _LOGGER.info(f"{update_results} for {csv_document.import_id}")
    return update_results
