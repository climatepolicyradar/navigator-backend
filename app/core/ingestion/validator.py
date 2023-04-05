from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import DocumentIngestRow, EventIngestRow
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
)
from app.db.session import Base

DbTable = Base
CheckResult = Result


def _check_value_in_db(
    row_num: int, db: Session, value: str, model: DbTable
) -> CheckResult:
    if value != "":
        val = db.query(model).get(value)
        if val is None:
            result = Result(
                ResultType.ERROR,
                f"Row {row_num}: Not found in db {model.__tablename__}={value}",
            )
            return result
    return Result()


def validate_document_row(
    db: Session,
    context: IngestContext,
    row: DocumentIngestRow,
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
    result = _check_value_in_db(n, db, row.document_type, FamilyDocumentType)
    if result.type != ResultType.OK:
        errors.append(result)

    result = _check_value_in_db(n, db, row.document_role, FamilyDocumentRole)
    if result.type != ResultType.OK:
        errors.append(result)

    result = _check_value_in_db(n, db, row.document_variant, Variant)
    if result.type != ResultType.OK:
        errors.append(result)

    # Check metadata
    result, _ = build_metadata(taxonomy, row)
    if result.type != ResultType.OK:
        errors.append(result)

    on_row = f"on row {row.row_number}"
    # Check family
    family_id = row.cpr_family_id

    if family_id in context.mde.families.keys():
        name, summary = context.mde.families[family_id]
        if name != row.family_name:
            errors.append(
                Result(
                    ResultType.ERROR,
                    f"Family {family_id} has differing name {on_row}",
                )
            )
        if summary != row.family_summary:
            errors.append(
                Result(
                    ResultType.ERROR,
                    f"Family {family_id} has differing summary {on_row}",
                )
            )
    else:
        context.mde.families[family_id] = (row.family_name, row.family_summary)

    # Check collection
    collection_id = row.cpr_collection_id

    if collection_id in context.mde.collections.keys():
        name, summary = context.mde.collections[collection_id]
        if name != row.collection_name:
            errors.append(
                Result(
                    ResultType.ERROR,
                    f"Collection {collection_id} has differing name {on_row}",
                )
            )
        if summary != row.collection_summary:
            errors.append(
                Result(
                    ResultType.ERROR,
                    f"Collection {collection_id} has differing summary {on_row}",
                )
            )
    else:
        context.mde.collections[collection_id] = (
            row.collection_name,
            row.collection_summary,
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
