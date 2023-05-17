from sqlalchemy import Column
from sqlalchemy.orm import Session

from app.core.ingestion.ingest_row_cclw import CCLWDocumentIngestRow, EventIngestRow
from app.core.ingestion.ingest_row_unfccc import UNFCCCDocumentIngestRow
from app.core.ingestion.metadata import build_metadata, Taxonomy
from app.core.ingestion.utils import (
    IngestContext,
    Result,
    ResultType,
)
from app.db.models.law_policy.family import (
    FamilyDocumentRole,
    FamilyDocumentType,
    Variant,
    Geography,
)
from app.db.session import Base

DbTable = Base
CheckResult = Result


def _check_value_in_db(
    row_num: int,
    db: Session,
    value: str,
    model: DbTable,
    model_field: Column,
) -> CheckResult:
    if value != "":
        val = db.query(model).filter(model_field == value).one_or_none()
        if val is None:
            result = Result(
                ResultType.ERROR,
                f"Row {row_num}: Not found in db {model.__tablename__}={value}",
            )
            return result
    return Result()


def _check_geo_in_db(row_num: int, db: Session, geo_iso: str) -> CheckResult:
    if geo_iso == "":
        return Result(
            ResultType.ERROR,
            f"Row {row_num}: Geography is empty.",
        )
    val = db.query(Geography).filter(Geography.value == geo_iso).one_or_none()
    if val is None:
        result = Result(
            ResultType.ERROR,
            f"Row {row_num}: Geography {geo_iso} found in db",
        )
        return result
    return Result()


def validate_unfccc_document_row(
    db: Session,
    context: IngestContext,
    row: UNFCCCDocumentIngestRow,
    taxonomy: Taxonomy,
) -> None:
    """
    Validate the constituent elements that represent this law & policy document row.

    :param [IngestContext] context: The ingest context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    :param [Taxonomy] taxonomy: the Taxonomy against which metadata should be validated.
    """

    errors = []
    # n = row.row_number
    # result = _check_value_in_db(
    #     n, db, row.document_type, FamilyDocumentType, FamilyDocumentType.name
    # )
    # if result.type != ResultType.OK:
    #     errors.append(result)

    # result = _check_value_in_db(
    #     n, db, row.document_role, FamilyDocumentRole, FamilyDocumentRole.name
    # )
    # if result.type != ResultType.OK:
    #     errors.append(result)

    # result = _check_value_in_db(
    #     n, db, row.document_variant, Variant, Variant.variant_name
    # )
    # if result.type != ResultType.OK:
    #     errors.append(result)

    # result = _check_geo_in_db(n, db, row.geography_iso)
    # if result.type != ResultType.OK:
    #     errors.append(result)

    # # Check metadata
    # result, _ = build_metadata(taxonomy, row)
    # if result.type != ResultType.OK:
    #     errors.append(result)

    # # Check family
    # context.consistency_validator.check_family(
    #     row.row_number,
    #     row.cpr_family_id,
    #     row.family_name,
    #     row.family_summary,
    #     errors,
    # )

    # # Check collection
    # context.consistency_validator.check_collection(
    #     row.row_number,
    #     row.cpr_collection_id,
    #     row.collection_name,
    #     row.collection_summary,
    #     errors,
    # )

    if len(errors) > 0:
        context.results += errors
    else:
        context.results.append(Result())


def validate_cclw_document_row(
    db: Session,
    context: IngestContext,
    row: CCLWDocumentIngestRow,
    taxonomy: Taxonomy,
) -> None:
    """
    Validate the constituent elements that represent this law & policy document row.

    :param [IngestContext] context: The ingest context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    :param [Taxonomy] taxonomy: the Taxonomy against which metadata should be validated.
    """

    errors = []
    n = row.row_number
    result = _check_value_in_db(
        n, db, row.document_type, FamilyDocumentType, FamilyDocumentType.name
    )
    if result.type != ResultType.OK:
        errors.append(result)

    result = _check_value_in_db(
        n, db, row.document_role, FamilyDocumentRole, FamilyDocumentRole.name
    )
    if result.type != ResultType.OK:
        errors.append(result)

    result = _check_value_in_db(
        n, db, row.document_variant, Variant, Variant.variant_name
    )
    if result.type != ResultType.OK:
        errors.append(result)

    result = _check_geo_in_db(n, db, row.geography_iso)
    if result.type != ResultType.OK:
        errors.append(result)

    # Check metadata
    result, _ = build_metadata(taxonomy, row)
    if result.type != ResultType.OK:
        errors.append(result)

    # Check family
    context.consistency_validator.check_family(
        row.row_number,
        row.cpr_family_id,
        row.family_name,
        row.family_summary,
        errors,
    )

    # Check collection
    context.consistency_validator.check_collection(
        row.row_number,
        row.cpr_collection_id,
        row.collection_name,
        row.collection_summary,
        errors,
    )

    if len(errors) > 0:
        context.results += errors
    else:
        context.results.append(Result())


def validate_event_row(context: IngestContext, row: EventIngestRow) -> None:
    """
    Validate the constituent elements that represent this event row.

    :param [IngestContext] context: The ingest context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    """
    result = Result(ResultType.OK, f"Event: {row.cpr_event_id}, org {context.org_id}")
    context.results.append(result)
