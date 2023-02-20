import sys
from typing import Callable, cast

from sqlalchemy.orm import Session
from app.core.ingestion.collection import collection_from_row
from app.core.ingestion.family import family_from_row
from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.utils import IngestContext
from app.core.ingestion.validator import validate_row
from app.db.models.app.users import Organisation

from app.db.models.deprecated import Document
from app.db.session import SessionLocal


ProcessFunc = Callable[[IngestContext, IngestRow], None]


def ingest_row(db: Session, context: IngestContext, row: IngestRow) -> dict:
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
        # FIXME: Need to be able to ingest a row that is brand new and
        # ready for the pipeline

        # If there does not already exist a document with the given import_id,
        # do not attempt to migrate
        return result

    family = family_from_row(
        db,
        row,
        existing_document,
        context.org_id,
        result,
    )

    collection_from_row(
        db,
        row,
        context.org_id,
        cast(str, family.import_id),
        result,
    )

    return result


def db_init(db: Session) -> IngestContext:
    """
    Initialise the database

    :return [Organisation]: The organisation that will be used for the ingest.
    """
    with db.begin():
        organisation = db.query(Organisation).filter_by(name="CCLW").one()
        return IngestContext(org_id=cast(int, organisation.id), results=[])


def get_dfc_ingestor() -> ProcessFunc:
    """
    Get the ingestion function for ingesting a CSV.

    :return [ProcessFunc]: The function used to ingest the CSV file
    """

    def process(context: IngestContext, row: IngestRow) -> None:
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

    def process(context: IngestContext, row: IngestRow) -> None:
        """Processes the row into the db."""
        db = SessionLocal()
        try:
            with db.begin():
                validate_row(db, context, row=row)
        finally:
            db.close()

    return process
