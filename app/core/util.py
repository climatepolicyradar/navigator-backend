import os
import random
import string
from typing import Any, Optional, List
import logging
from sqlalchemy.orm import Session

from app.core.import_row import CCLWImportRow
from app.core.validation.types import UpdateResult
from app.db.models.document import PhysicalDocument
from app.db.models.law_policy import FamilyDocument, Family
from app.db.session import Base

CDN_DOMAIN: str = os.getenv("CDN_DOMAIN", "cdn.climatepolicyradar.org")
# TODO: remove & replace with proper content-type handling through pipeline
CONTENT_TYPE_MAP = {
    ".pdf": "application/pdf",
    ".html": "text/html",
    ".htm": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}

_LOGGER = logging.getLogger(__name__)


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


def document_updates(row: CCLWImportRow, db: Session) -> List[UpdateResult]:
    """Identify any updates to the document by combining the results of various database queries."""

    return (
        physical_document_updates(db, row)
        + family_updates(db, row)
        + family_document_updates(db, row)
    )


def get_family_doc(db: Session, row: CCLWImportRow) -> FamilyDocument:
    """Get the family document from the database."""
    return (
        db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == row.cpr_document_id)
        .scalar()
    )


def family_updates(db: Session, row: CCLWImportRow) -> List[UpdateResult]:
    """Identify any updates to the family relating to the row by comparing against the relevant tables in
    the database.

    Args:
        row (CCLWImportRow): the row from the CSV.
        db (Session): the database session.

    Returns:
        dict[field_name: UpdateResult(field=value csv_value=value, db_value=value, update=bool, type=table_name)]
    """

    family = (
        db.query(Family).filter(Family.id == get_family_doc(db, row).family_id).scalar()
    )

    row_family_summary = row.family_summary.replace('"', "")

    return [
        result
        for result in [
            UpdateResult(
                db_value=family.title,
                csv_value=row.family_name,
                updated=family.title != row.family_name,
                type="Family",
                field="title",
            ),
            UpdateResult(
                db_value=family.description,
                csv_value=row_family_summary,
                updated=family.description != row_family_summary,
                type="Family",
                field="description",
            ),
        ]
        if result.updated
    ]


def physical_document_updates(db: Session, row: CCLWImportRow) -> List[UpdateResult]:
    """Identify any updates to the physical document by comparing against the relevant tables in the database.

    Args:
        row (CCLWImportRow): the row from the CSV.
        db (Session): the database session.

    Returns:
        List[UpdateResult(csv_value=value, db_value=value, update=bool, type=table_name, field=value)]"""

    physical_document = (
        db.query(PhysicalDocument)
        .filter(PhysicalDocument.id == get_family_doc(db, row).physical_document_id)
        .scalar()
    )

    db_source_url = (
        ""
        if physical_document.source_url == '{""}'
        else physical_document.source_url.replace("{", "")
        .replace("}", "")
        .split(",")[0]
    )
    csv_source_url = row.documents[0]

    return [
        result
        for result in [
            UpdateResult(
                db_value=physical_document.title,
                csv_value=row.document_title,
                updated=physical_document.title != row.document_title,
                type="PhysicalDocument",
                field="title",
            ),
            UpdateResult(
                db_value=db_source_url,
                csv_value=csv_source_url,
                updated=db_source_url != csv_source_url,
                type="PhysicalDocument",
                field="source_url",
            ),
        ]
        if result.updated
    ]


def family_document_updates(db: Session, row: CCLWImportRow) -> List[UpdateResult]:
    """Identify any updates to the family document by comparing against the relevant tables in the database.

    Args:
        row (CCLWImportRow): the row from the CSV.
        db (Session): the database session.

    Returns:
        List[UpdateResult(csv_value=value, db_value=value, update=bool, type=table_name, field=value)]"""

    family_document = get_family_doc(db, row)

    _LOGGER.info("family_document")
    _LOGGER.info(family_document)

    _LOGGER.info("row.cpr_document_status")
    _LOGGER.info(row.cpr_document_status)

    return [
        result
        for result in [
            UpdateResult(
                db_value=family_document.document_status.value,
                csv_value=row.cpr_document_status,
                updated=family_document.document_status.value
                != row.cpr_document_status,
                type="FamilyDocument",
                field="document_status",
            )
        ]
        if result.updated
    ]


def affects_pipline(update: UpdateResult) -> bool:
    """Determine if the update affects the pipeline and should thus trigger processing of the document."""
    if (
        (update.type == "PhysicalDocument" and update.field == "source_url")
        or (update.type == "Family" and update.field == "name")
        or (update.type == "Family" and update.field == "description")
        or (update.type == "FamilyDocument" and update.field == "document_status")
    ):
        return True
    return False
