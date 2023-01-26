import logging
import os
import random
import string
from typing import Any, Optional, List

from botocore.exceptions import ClientError
from sqlalchemy.orm import Session

from app.core.validation.types import (
    DocumentValidationResult,
    UpdateResult,
)
from app.db.models.deprecated.document import Document, DocumentType, Category
from app.db.models.deprecated.geography import Geography
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
    updates: dict[str, UpdateResult], import_id: str, db: Session
) -> None:
    """Update the document table in the database."""
    with db.begin_nested():
        for field in updates:
            if updates[field].updated:
                docs_updated_no = (
                    db.query(Document)
                    .filter(Document.import_id == import_id)
                    .update(
                        {field: updates[field].csv_value},
                        synchronize_session="fetch",
                    )
                )
                if docs_updated_no != 1:
                    raise RuntimeError(
                        f"Expected to update 1 document but updated {docs_updated_no} during the udpate of: "
                        f"{import_id}:{field} from {updates[field].db_value} -> {updates[field].csv_value}"
                    )

                _LOGGER.info(
                    f"Updated {import_id}:{field} from {updates[field].db_value} -> {updates[field].csv_value}"
                )


def object_exists(client, bucket: str, key: str) -> bool:
    """
    Detect whether an S3 object exists in s3.

    params:
        bucket: str - the name of the bucket
        key: str - the key of the object to check
    returns:
        bool - True if the object exists, False otherwise
    """
    try:
        client.head_object(Bucket=bucket, Key=key)
        _LOGGER.info("Object '%s' found in bucket '%s'.", key, bucket)
        return True
    except ClientError as e:
        _LOGGER.info("Object '%s' not found in bucket '%s': '%s.", key, bucket, e)
        return False


# TODO do we want to archive the old file?
# TODO add type hint for s3_client
def delete_doc_in_s3(
    import_id: str, bucket: str, prefixes: List[str], s3_client: Any, suffixes=None
) -> None:
    """Delete the document objects in the relevant s3 buckets."""
    if suffixes is None:
        suffixes = [".json", ".npy"]

    for prefix in prefixes:
        for suffix in suffixes:
            s3_key = os.path.join(prefix, import_id + suffix)

            if object_exists(s3_client, bucket, s3_key):
                try:
                    s3_client.delete_object(Bucket=bucket, Key=s3_key)
                    _LOGGER.info(
                        "Deleted object '%s' from bucket '%s'.", s3_key, bucket
                    )
                except ClientError as e:
                    _LOGGER.error(
                        "Could not delete object '%s' from bucket '%s': '%s'.",
                        s3_key,
                        bucket,
                        e,
                    )


def get_update_results(
    csv_document: DocumentValidationResult, db: Session
) -> dict[str, UpdateResult]:
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

    update_results = {
        "name": UpdateResult(
            csv_value=csv_document.create_request.name,
            db_value=db_document.name,
            updated=csv_document.create_request.name != db_document.name,
        ),
        "publication_ts": UpdateResult(
            csv_value=csv_document.create_request.publication_ts,
            db_value=db_document.publication_ts,
            updated=csv_document.create_request.publication_ts
            != db_document.publication_ts,
        ),
        "description": UpdateResult(
            csv_value=csv_document.create_request.description,
            db_value=db_document.description,
            updated=csv_document.create_request.description != db_document.description,
        ),
        "geography": UpdateResult(
            csv_value=csv_document.create_request.geography,
            db_value=geog_db_value,
            updated=csv_document.create_request.geography != geog_db_value,
        ),
        "type": UpdateResult(
            csv_value=csv_document.create_request.type,
            db_value=doc_type_db_value,
            updated=csv_document.create_request.type != doc_type_db_value,
        ),
        "category": UpdateResult(
            csv_value=csv_document.create_request.category,
            db_value=category_db_value,
            updated=csv_document.create_request.category != category_db_value,
        ),
    }

    _LOGGER.info(f"{update_results} for {csv_document.import_id}")
    return update_results
