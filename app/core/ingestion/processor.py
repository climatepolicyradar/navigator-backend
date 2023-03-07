import logging
from typing import Any, Callable, TypeVar, cast

from sqlalchemy.orm import Session
from app.core.ingestion.collection import collection_from_row
from app.core.ingestion.event import family_event_from_row
from app.core.ingestion.family import family_from_row
from app.core.ingestion.ingest_row import (
    BaseIngestRow,
    DocumentIngestRow,
    EventIngestRow,
)
from app.core.ingestion.organisation import get_organisation_taxonomy
from app.core.ingestion.utils import IngestContext
from app.core.ingestion.validator import validate_document_row
from app.db.models.app.users import Organisation

from app.db.models.deprecated import Document

_LOGGER = logging.getLogger(__name__)


_RowType = TypeVar("_RowType", bound=BaseIngestRow)

ProcessFunc = Callable[[IngestContext, _RowType], None]


def ingest_document_row(
    db: Session, context: IngestContext, row: DocumentIngestRow
) -> dict[str, Any]:
    """
    Create the constituent elements in the database that represent this row.

    :param [Session] db: the connection to the database.
    :param [DocuemntIngestRow] row: the IngestRow object of the current CSV row
    :returns [dict[str, Any]]: a result dictionary describing what was created
    """
    result = {}
    import_id = row.cpr_document_id

    # TODO: Make existing document optional to allow creating new events, new documents
    #       created without an existing document should hit the pipeline as "new", any
    #       others should (maybe) generate updates(?)
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


def ingest_event_row(
    db: Session, context: IngestContext, row: EventIngestRow
) -> dict[str, Any]:
    """
    Create the constituent elements in the database that represent this row.

    :param [Session] db: the connection to the database.
    :param [EventIngestRow] row: the IngestRow object of the current CSV row
    :returns [dict[str, Any]]: a result dictionary describing what was created
    """
    result = {}
    family_event_from_row(db=db, row=row, result=result)
    return result


def initialise_context(db: Session) -> IngestContext:
    """
    Initialise the database

    :return [IngestContext]: The organisation that will be used for the ingest.
    """
    with db.begin():
        organisation = db.query(Organisation).filter_by(name="CCLW").one()
        return IngestContext(org_id=cast(int, organisation.id), results=[])


def get_event_ingestor(db: Session) -> ProcessFunc:
    """
    Get the ingestion function for ingesting an event CSV row.

    :return [ProcessFunc]: The function used to ingest the CSV row.
    """

    def process(context: IngestContext, row: EventIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Ingesting event row: {row.row_number}")

        with db.begin():
            ingest_event_row(db, context, row=row)

    return process


def get_dfc_ingestor(db: Session) -> ProcessFunc:
    """
    Get the ingestion function for ingesting a law & policy CSV row.

    :return [ProcessFunc]: The function used to ingest the CSV row.
    """

    def process(context: IngestContext, row: DocumentIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Ingesting document row: {row.row_number}")

        with db.begin():
            ingest_document_row(db, context, row=row)

    return process


def get_dfc_validator(db: Session, context: IngestContext) -> ProcessFunc:
    """
    Get the validation function for ingesting a law & policy CSV.

    :param [IngestContext] context: The context of the current ingest
    :return [ProcessFunc]: The function used to validate the CSV file
    """
    with db.begin():
        _, taxonomy = get_organisation_taxonomy(db, context.org_id)

    def process(context: IngestContext, row: DocumentIngestRow) -> None:
        """Processes the row into the db."""
        validate_document_row(context=context, taxonomy=taxonomy, row=row)

    return process
