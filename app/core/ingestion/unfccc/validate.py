from typing import cast
from sqlalchemy.orm import Session
from app.core.ingestion.processor import get_document_validator
from app.core.ingestion.unfccc.ingest_row_unfccc import (
    CollectionIngestRow,
    UNFCCCDocumentIngestRow,
)
from app.core.ingestion.utils import (
    IngestContext,
    Result,
    ResultType,
    UNFCCCIngestContext,
    get_result_counts,
)
from app.core.ingestion.reader import read


def validate_unfccc_csv(
    documents_file_contents: str,
    collection_file_contents: str,
    db: Session,
    context: UNFCCCIngestContext,
    all_results: list[Result],
) -> str:
    """
    Validates the csv file

    :param UploadFile law_policy_csv: incoming file to validate
    :param Session db: connection to the database
    :param IngestContext context: the ingest context
    :param list[Result] all_results: the results
    :return tuple[str, str]: the file contents of the csv and the summary message
    """

    # First read all the ids in the collection_csv
    def collate_ids(context: IngestContext, row: CollectionIngestRow) -> None:
        ctx = cast(UNFCCCIngestContext, context)
        ctx.collection_ids_defined.append(row.cpr_collection_id)

    read(collection_file_contents, context, CollectionIngestRow, collate_ids)

    # Now do the validation of the documents
    validator = get_document_validator(db, context)
    read(documents_file_contents, context, UNFCCCDocumentIngestRow, validator)
    # Get the rows here as this is the length of results
    rows = len(context.results)

    # Check the set of defined collections against those referenced
    defined = set(context.collection_ids_defined)
    referenced = set(context.collection_ids_referenced)

    defined_not_referenced = defined.difference(referenced)

    if len(defined_not_referenced) > 0:
        # Empty collections are allowed, but need reporting
        context.results.append(
            Result(
                ResultType.OK,
                "The following Collection IDs were "
                + f"defined and not referenced: {list(defined_not_referenced)}",
            )
        )

    referenced_not_defined = referenced.difference(defined)
    if len(referenced_not_defined) > 0:
        context.results.append(
            Result(
                ResultType.ERROR,
                "The following Collection IDs were "
                f"referenced and not defined: {list(referenced_not_defined)}",
            )
        )

    _, fails, resolved = get_result_counts(context.results)
    all_results.extend(context.results)

    context.results = []
    message = (
        f"UNFCCC validation result: {rows} Rows, {fails} Failures, "
        f"{resolved} Resolved"
    )

    return message
