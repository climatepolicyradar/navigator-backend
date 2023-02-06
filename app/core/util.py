import os
import random
import string
import sys
import datetime
from typing import Any, Optional, Union, List, Literal, Sequence

from pydantic.dataclasses import dataclass, _T
from sqlalchemy.orm import Session

from app.db.models.document import PhysicalDocument
from app.db.models.law_policy import FamilyDocument
from app.db.session import Base

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


# TODO move when we have a better idea of how we want to handle this
class DfcRow:
    row_number: int = 0
    id: str = ""
    document_id: str = ""
    cclw_description: str = ""
    part_of_collection: str = ""
    create_new_familyies: str = ""
    collection_id: str = ""
    collection_name: str = ""
    collection_summary: str = ""
    document_title: str = ""
    family_name: str = ""
    family_summary: str = ""
    family_id: str = ""
    document_role: str = ""
    applies_to_id: str = ""
    geography_iso: str = ""
    documents: str = ""
    category: str = ""
    events: list[str] = []
    sectors: list[str] = []
    instruments: list[str] = []
    frameworks: list[str] = []
    responses: list[str] = []
    natural_hazards: str = ""
    document_type: str = ""
    year: int = 0
    language: str = ""
    keywords: list[str] = []
    geography: str = ""
    parent_legislation: str = ""
    comment: str = ""
    cpr_document_id: str = ""
    cpr_family_id: str = ""
    cpr_collection_id: str = ""
    cpr_family_slug: str = ""
    cpr_document_slug: str = ""

    _semicolon_delimited_array_keys = ["sectors", "frameworks", "keywords", "responses"]
    _bar_delimited_array_keys = ["instruments", "events", "documents"]

    _int_keys = ["row_number", "year"]

    def __init__(self, row_number: int, row: dict = {}):
        """Creates a Row given a row in the CSV.

        This does the translation of column name to field name and also sets its value.

        Args:
            row_number (int): the row number of the CSV row.
            row (dict, optional): the dict of the row in the CSV. Defaults to None.
        """
        if row is not {}:
            setattr(self, "row_number", row_number)
            for key, value in row.items():
                # translate the column names to useful field names...
                k = key.lower().replace(" ", "_").replace("?", "").replace("/", "")
                # now set it
                self._set_value(k, value)

    def _set_value(self, key: str, value: str):
        """Sets the value given the type.

        This does the splitting of separated values into arrays.
        Any key that is not recognized causes the script to bail.
        """
        if not hasattr(self, key):
            print(f"Received an unknown column: {key}")
            sys.exit(10)

        if value.lower() == "n/a":
            setattr(self, key, value)
        elif key in self._semicolon_delimited_array_keys:
            setattr(self, key, value.split(";"))
        elif key in self._bar_delimited_array_keys:
            setattr(self, key, value.split("|"))
        elif key in self._int_keys:
            setattr(self, key, int(value) if value else 0)
        else:
            setattr(self, key, value)


@dataclass
class UpdateResult:
    """Class describing the results of comparing csv data against the db data to identify updates."""

    db_value: Union[str, datetime.datetime]
    csv_value: Union[str, datetime.datetime]
    updated: bool


def physical_document_updated(
    row: DfcRow, db: Session
) -> dict[str : dict[str, UpdateResult]]:
    """Identify any updates to the physical documents relating to the row by comparing against the relevant tables in
    the database.

    Args:
        row (DfcRow): the row from the CSV.
        db (Session): the database session.

    Returns:
        dict[physical_document_id: UpdateResult(field=value csv_value=value, db_value=value, update=bool)]
    """

    family_document = (
        db.query(FamilyDocument)
        .filter(FamilyDocument.family_id == row.family_id)
        .scalar()
    )

    physical_document = (
        db.query(PhysicalDocument)
        .filter(PhysicalDocument.id == family_document.physical_document_id)
        .scalar()
    )

    return {
        str(physical_document.id): {
            "title": UpdateResult(
                db_value=physical_document.title,
                csv_value=row.document_title,
                updated=physical_document.title != row.document_title,
            ),
            "source_url": UpdateResult(
                db_value=physical_document.source_url,
                csv_value=row.documents,
                updated=physical_document.source_url != row.documents,
            ),
            "date": UpdateResult(
                db_value=physical_document.date.year,
                csv_value=row.year,
                updated=physical_document.date.year != row.year,
            ),
            "format": UpdateResult(
                db_value=physical_document.format,
                csv_value=row.document_type,
                updated=physical_document.format != row.document_type,
            ),
        }
    }
