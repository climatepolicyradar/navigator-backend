import sys
from typing import Callable, cast

from sqlalchemy.orm import Session
from app.db.models.app.users import Organisation

from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from app.db.session import SessionLocal

from scripts.ingest_dfc.dfc.collection import collection_from_row
from scripts.ingest_dfc.dfc.family import family_from_row
from scripts.ingest_dfc.dfc.validator import validate_row
from scripts.ingest_dfc.utils import DfcRow, IngestContext


ProcessFunc = Callable[[IngestContext, DfcRow], None]


def ingest_row(db: Session, context: IngestContext, row: DfcRow) -> dict:
    """
    Create the constituent elements in the database that represent this row.

    :param [Session] db: the connection to the database.
    :param [DfcRow] row: the DfcRow object of the current CSV row
    :returns [dict[str, Any]]: a result dictionary describing what was created
    """
    result = {}
    import_id = row.cpr_document_id

    existing_document = (
        db.query(Document).filter(Document.import_id == import_id).one_or_none()
    )
    if existing_document is None:
        # If there does not already exist a document with the given import_id, do not
        # attempt to migrate
        print("skipping!")
        return result
    print("processing")

    print(f"- Creating FamilyDocument for import {import_id}")
    family = family_from_row(
        db,
        row,
        existing_document,
        context.org_id,
        result,
    )

    print(f"- Creating Collection if required for import {import_id}")

    collection_from_row(
        db,
        row,
        context.org_id,
        cast(str, family.import_id),
        result,
    )

    return result


def db_ready() -> bool:
    """Return True if we should process the row."""
    db = SessionLocal()
    num_new_documents = db.query(PhysicalDocument).count()
    num_old_documents = db.query(Document).count()
    print(
        f"Found {num_new_documents} new documents and {num_old_documents} old "
        "documents"
    )
    return True  # num_new_documents == 0 and num_old_documents > 0


def db_init() -> IngestContext:
    """
    Initialise the database

    :return [Organisation]: The organisation that will be used for the ingest.
    """
    db = SessionLocal()
    try:
        with db.begin():
            organisation = db.query(Organisation).filter_by(name="CCLW").one()
            return IngestContext(org_id=cast(int, organisation.id), results=[])
    finally:
        db.close()


def get_dfc_ingestor() -> ProcessFunc:
    """
    Get the ingestion function for ingesting a CSV.

    :return [ProcessFunc]: The function used to ingest the CSV file
    """

    def process(context: IngestContext, row: DfcRow) -> None:
        """Processes the row into the db."""
        sys.stdout.write(f"Ingesting row: {row.row_number}: ")

        db = SessionLocal()
        try:
            with db.begin():
                ingest_row(db, context, row=row)
        finally:
            db.close()
            sys.stdout.flush()

    return process


def get_dfc_validator() -> ProcessFunc:
    """
    Get the validation function for ingesting a CSV.

    :return [ProcessFunc]: The function used to validate the CSV file
    """

    def process(context: IngestContext, row: DfcRow) -> None:
        """Processes the row into the db."""
        db = SessionLocal()
        try:
            with db.begin():
                validate_row(db, context, row=row)
        finally:
            db.close()

    return process
