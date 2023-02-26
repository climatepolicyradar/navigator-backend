import sys
from typing import Callable, cast

from sqlalchemy.orm import Session
from app.core.ingestion.collection import collection_from_row
from app.core.ingestion.family import family_from_row
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.organisation import get_organisation_taxonomy
from app.core.ingestion.utils import IngestContext
from app.core.ingestion.validator import validate_document_row
from app.db.models.app.users import Organisation

from app.db.models.deprecated import Document
from app.db.session import SessionLocal


ProcessFunc = Callable[[IngestContext, DocumentIngestRow], None]


def ingest_document_row(
    db: Session, context: IngestContext, row: DocumentIngestRow
) -> dict:
    """
    Create the constituent elements in the database that represent this row.

    :param [Session] db: the connection to the database.
    :param [IngestRow] row: the IngestRow object of the current CSV row
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

        result["existing_document"] = False
        # If there does not already exist a document with the given import_id,
        # do not attempt to migrate
        return result

    result["existing_document"] = True

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

    def process(context: IngestContext, row: DocumentIngestRow) -> None:
        """Processes the row into the db."""
        sys.stdout.write(f"Ingesting row: {row.row_number}: ")

        db = SessionLocal()
        try:
            with db.begin():
                ingest_document_row(db, context, row=row)
        finally:
            db.close()
            sys.stdout.flush()

    return process


def get_dfc_validator(context: IngestContext) -> ProcessFunc:
    """
    Get the validation function for ingesting a CSV.

    :return [ProcessFunc]: The function used to validate the CSV file
    """
    db = SessionLocal()
    try:
        with db.begin():
            _, taxonomy = get_organisation_taxonomy(db, context.org_id)
    finally:
        db.close()

    def process(context: IngestContext, row: DocumentIngestRow) -> None:
        """Processes the row into the db."""
        validate_document_row(context=context, taxonomy=taxonomy, row=row)

    return process
