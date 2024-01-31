from sqlalchemy import Column
from sqlalchemy.orm import Session

from app.core.ingestion.cclw.ingest_row_cclw import (
    CCLWDocumentIngestRow,
    EventIngestRow,
)
from app.core.ingestion.metadata import Taxonomy
from app.core.ingestion.unfccc.ingest_row_unfccc import UNFCCCDocumentIngestRow
from app.core.ingestion.cclw.metadata import build_cclw_metadata
from app.core.ingestion.utils import (
    CCLWIngestContext,
    IngestContext,
    Result,
    ResultType,
    UNFCCCIngestContext,
)
from app.core.ingestion.unfccc.metadata import build_unfccc_metadata
from app.core.validation import IMPORT_ID_MATCHER
from db_client.models.law_policy.family import (
    FamilyDocumentRole,
    FamilyDocumentType,
    Variant,
    Geography,
)
from db_client.models.law_policy.geography import GEO_INTERNATIONAL, GEO_NONE
from db_client.models import Base

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
    if geo_iso == "INT":
        geo_iso = GEO_INTERNATIONAL

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
    context: UNFCCCIngestContext,
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
    n = row.row_number

    # don't validate: collection_name: str
    # don't validate: family_name: str
    # don't validate: document_title: str
    # don't validate: documents: str
    # don't validate: author: str
    # don't validate: geography: str
    # don't validate: date: datetime

    # Validate family id
    if IMPORT_ID_MATCHER.match(row.cpr_family_id) is None:
        errors.append(
            Result(ResultType.ERROR, f"Family ID format error {row.cpr_family_id}")
        )

    # Validate collection id (optional)
    if row.cpr_collection_id:
        for collection_id in row.cpr_collection_id:
            if IMPORT_ID_MATCHER.match(collection_id) is None:
                errors.append(
                    Result(
                        ResultType.ERROR, f"Collection ID format error {collection_id}"
                    )
                )

    # Validate document id
    if IMPORT_ID_MATCHER.match(row.cpr_document_id) is None:
        errors.append(
            Result(ResultType.ERROR, f"Document ID format error {row.cpr_document_id}")
        )

    # validate: document_role: str
    result = _check_value_in_db(
        n, db, row.document_role, FamilyDocumentRole, FamilyDocumentRole.name
    )
    if result.type != ResultType.OK:
        errors.append(result)

    # validate: document_variant: str
    result = _check_value_in_db(
        n, db, row.document_variant, Variant, Variant.variant_name
    )
    if result.type != ResultType.OK:
        errors.append(result)

    # validate: geography_iso: str
    if row.geography_iso != "":
        result = _check_geo_in_db(n, db, row.geography_iso)
        if result.type != ResultType.OK:
            errors.append(result)
    else:
        row.geography_iso = GEO_NONE

    # validate: Submission type as document type
    result = _check_value_in_db(
        n, db, row.submission_type, FamilyDocumentType, FamilyDocumentType.name
    )
    if result.type != ResultType.OK:
        errors.append(result)

    # validate: language: list[str]

    # Check metadata
    # validate: author_type: str  # METADATA
    result, _ = build_unfccc_metadata(taxonomy, row)
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

    # Add to the collections that are referenced so we can validate later
    context.collection_ids_referenced.extend(row.cpr_collection_id)

    if len(errors) > 0:
        context.results += errors
    else:
        context.results.append(Result())


def validate_cclw_document_row(
    db: Session,
    context: CCLWIngestContext,
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
    result, _ = build_cclw_metadata(taxonomy, row)
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
