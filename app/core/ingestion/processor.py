import logging
from typing import Any, Callable, TypeVar, cast

from sqlalchemy.orm import Session
from app.core.ingestion.collection import (
    create_collection,
    handle_cclw_collection_and_link,
    handle_link_collection_to_family,
)
from app.core.ingestion.cclw.event import family_event_from_row
from app.core.ingestion.family import handle_family_from_params
from app.core.ingestion.cclw.ingest_row_cclw import (
    CCLWDocumentIngestRow,
    EventIngestRow,
)
from app.core.ingestion.cclw.metadata import add_cclw_metadata
from app.core.ingestion.ingest_row_base import BaseIngestRow
from app.core.ingestion.metadata import Taxonomy
from app.core.ingestion.params import IngestParameters
from app.core.ingestion.unfccc.event import create_event_from_row
from app.core.ingestion.unfccc.ingest_row_unfccc import (
    CollectionIngestRow,
    UNFCCCDocumentIngestRow,
)
from app.core.ingestion.unfccc.metadata import add_unfccc_metadata
from app.core.organisation import get_organisation_taxonomy
from app.core.ingestion.utils import (
    CCLWIngestContext,
    IngestContext,
    Result,
    ResultType,
    UNFCCCIngestContext,
)
from app.core.ingestion.validator import (
    validate_cclw_document_row,
    validate_unfccc_document_row,
)
from app.db.models.app.users import Organisation
from app.db.models.law_policy.geography import GEO_INTERNATIONAL, GEO_NONE


_LOGGER = logging.getLogger(__name__)

_RowType = TypeVar("_RowType", bound=BaseIngestRow)

ProcessFunc = Callable[[IngestContext, _RowType], None]


def parse_csv_geography(csv_geo: str) -> str:
    if csv_geo == "":
        return GEO_NONE

    if csv_geo == "INT":
        return GEO_INTERNATIONAL  # Support old style

    return csv_geo


def build_params_from_cclw(row: CCLWDocumentIngestRow) -> IngestParameters:
    def add_metadata(db: Session, import_id: str, taxonomy: Taxonomy, taxonomy_id: int):
        add_cclw_metadata(db, import_id, taxonomy, taxonomy_id, row)

    return IngestParameters(
        create_collections=True,
        add_metadata=add_metadata,
        source_url=row.get_first_url(),
        document_id=row.document_id,
        collection_name=row.collection_name,
        collection_summary=row.collection_summary,
        document_title=row.document_title,
        family_name=row.family_name,
        family_summary=row.family_summary,
        document_role=row.document_role,
        document_variant=row.document_variant,
        geography_iso=parse_csv_geography(row.geography_iso),
        documents=row.documents,
        category=row.category,
        document_type=row.document_type,
        language=row.language,
        geography=row.geography,
        cpr_document_id=row.cpr_document_id,
        cpr_family_id=row.cpr_family_id,
        cpr_collection_ids=[row.cpr_collection_id],
        cpr_family_slug=row.cpr_family_slug,
        cpr_document_slug=row.cpr_document_slug,
        cpr_document_status=row.cpr_document_status,
    )


def build_params_from_unfccc(row: UNFCCCDocumentIngestRow) -> IngestParameters:
    def add_metadata(db: Session, import_id: str, taxonomy: Taxonomy, taxonomy_id: int):
        add_unfccc_metadata(db, import_id, taxonomy, taxonomy_id, row)

    def build_summary() -> str:
        start = f"{row.family_name}, {row.submission_type}"
        return f"{start} from {row.author} in {row.date.year}"

    return IngestParameters(
        create_collections=False,
        add_metadata=add_metadata,
        source_url=row.documents,
        document_id=row.cpr_document_id,
        collection_name="",
        collection_summary="",
        document_title=row.document_title,
        family_name=row.family_name,
        family_summary=build_summary(),
        document_role=row.document_role,
        document_variant=row.document_variant,
        geography_iso=parse_csv_geography(row.geography_iso),
        documents=row.documents,
        category=row.category,
        document_type=row.submission_type,
        language=row.language,
        geography=row.geography,
        cpr_document_id=row.cpr_document_id,
        cpr_family_id=row.cpr_family_id,
        cpr_collection_ids=row.cpr_collection_id,
        cpr_family_slug=row.cpr_family_slug,
        cpr_document_slug=row.cpr_document_slug,
        cpr_document_status="PUBLISHED",
    )


def ingest_cclw_document_row(
    db: Session, context: IngestContext, row: CCLWDocumentIngestRow
) -> dict[str, Any]:
    """
    Create the constituent elements in the database that represent this row.

    :param [Session] db: the connection to the database.
    :param [DocumentIngestRow] row: the IngestRow object of the current CSV row
    :returns [dict[str, Any]]: a result dictionary describing what was created
    """
    result = {}
    import_id = row.cpr_document_id

    _LOGGER.info(
        f"Ingest starting for row {row.row_number}.",
        extra={
            "props": {
                "row_number": row.row_number,
                "import_id": import_id,
            }
        },
    )
    params = build_params_from_cclw(row)
    family = handle_family_from_params(db, params, context.org_id, result)
    handle_cclw_collection_and_link(
        db, params, context.org_id, cast(str, family.import_id), result
    )

    _LOGGER.info(
        f"Ingest complete for row {row.row_number}",
        extra={"props": {"result": str(result)}},
    )

    return result


def ingest_unfccc_document_row(
    db: Session,
    context: IngestContext,
    row: UNFCCCDocumentIngestRow,
) -> dict[str, Any]:
    """
    Create the constituent elements in the database that represent this row.

    :param [Session] db: the connection to the database.
    :param [DocumentIngestRow] row: the IngestRow object of the current CSV row
    :returns [dict[str, Any]]: a result dictionary describing what was created
    """
    result = {}
    import_id = row.cpr_document_id

    _LOGGER.info(
        f"Ingest starting for row {row.row_number}.",
        extra={
            "props": {
                "row_number": row.row_number,
                "import_id": import_id,
            }
        },
    )

    params = build_params_from_unfccc(row)
    family = handle_family_from_params(db, params, context.org_id, result)
    handle_link_collection_to_family(
        db, params.cpr_collection_ids, cast(str, family.import_id), result
    )

    # Now create a FamilyEvent to store the date
    create_event_from_row(db, row)

    ctx = cast(UNFCCCIngestContext, context)
    ctx.download_urls[import_id] = row.download_url

    _LOGGER.info(
        f"Ingest complete for row {row.row_number}",
        extra={"props": {"result": str(result)}},
    )

    return result


def ingest_collection_row(
    db: Session, context: IngestContext, row: CollectionIngestRow
) -> dict[str, Any]:
    result = {}
    create_collection(db, row, context.org_id, result)
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


def initialise_context(db: Session, org_name: str) -> IngestContext:
    """
    Initialise the database

    :return [IngestContext]: The organisation that will be used for the ingest.
    """
    with db.begin():
        organisation = db.query(Organisation).filter_by(name=org_name).one()
        if org_name == "CCLW":
            return CCLWIngestContext(
                org_name=org_name, org_id=cast(int, organisation.id), results=[]
            )
        if org_name == "UNFCCC":
            return UNFCCCIngestContext(
                org_name=org_name, org_id=cast(int, organisation.id), results=[]
            )
        raise ValueError(f"Code not in sync with data - org {org_name} unknown to code")


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


def get_collection_ingestor(db: Session) -> ProcessFunc:
    """
    Get the ingestion function for ingesting a collection CSV row.

    :return [ProcessFunc]: The function used to ingest the CSV row.
    """

    def process(context: IngestContext, row: CollectionIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Ingesting collection row: {row.row_number}")

        with db.begin():
            ingest_collection_row(db, context, row=row)

    return process


def get_cclw_document_ingestor(db: Session, context: IngestContext) -> ProcessFunc:
    """
    Get the ingestion function for ingesting a law & policy CSV row.

    :return [ProcessFunc]: The function used to ingest the CSV row.
    """

    def cclw_process(context: IngestContext, row: CCLWDocumentIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Ingesting document row: {row.row_number}")

        with db.begin():
            try:
                ingest_cclw_document_row(db, context, row=row)
            except Exception as e:
                error = Result(
                    ResultType.ERROR, f"Row {row.row_number}: Error {str(e)}"
                )
                context.results.append(error)
                _LOGGER.error(
                    "Error on ingest",
                    extra={"props": {"row_number": row.row_number, "error": str(e)}},
                )

    return cclw_process


def get_unfccc_document_ingestor(db: Session, context: IngestContext) -> ProcessFunc:
    """
    Get the ingestion function for ingesting a law & policy CSV row.

    :return [ProcessFunc]: The function used to ingest the CSV row.
    """

    def unfccc_process(context: IngestContext, row: UNFCCCDocumentIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Ingesting document row: {row.row_number}")

        with db.begin():
            try:
                ingest_unfccc_document_row(db, context, row=row)
            except Exception as e:
                error = Result(
                    ResultType.ERROR, f"Row {row.row_number}: Error {str(e)}"
                )
                context.results.append(error)
                _LOGGER.error(
                    "Error on ingest",
                    extra={"props": {"row_number": row.row_number, "error": str(e)}},
                )

    return unfccc_process


def get_document_validator(db: Session, context: IngestContext) -> ProcessFunc:
    """
    Get the validation function for ingesting a law & policy CSV.

    :param [IngestContext] context: The context of the current ingest
    :return [ProcessFunc]: The function used to validate the CSV file
    """
    with db.begin():
        _, taxonomy = get_organisation_taxonomy(db, context.org_id)

    def cclw_process(context: IngestContext, row: CCLWDocumentIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Validating document row: {row.row_number}")
        with db.begin():
            validate_cclw_document_row(
                db=db,
                context=cast(CCLWIngestContext, context),
                taxonomy=taxonomy,
                row=row,
            )

    def unfccc_process(context: IngestContext, row: UNFCCCDocumentIngestRow) -> None:
        """Processes the row into the db."""
        _LOGGER.info(f"Validating document row: {row.row_number}")
        with db.begin():
            validate_unfccc_document_row(
                db=db,
                context=cast(UNFCCCIngestContext, context),
                taxonomy=taxonomy,
                row=row,
            )

    if context.org_name == "CCLW":
        return cclw_process
    elif context.org_name == "UNFCCC":
        return unfccc_process

    raise ValueError(f"Unknown org {context.org_name} for validation.")
