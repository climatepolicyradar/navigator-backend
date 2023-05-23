import logging
from typing import Union, cast
from sqlalchemy.orm import Session
from app.core.aws import S3Client

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from app.core.aws import S3Document

from app.api.api_v1.schemas.document import (
    BulkIngestResult,
)
from app.core.auth import get_superuser_details
from app.core.aws import get_s3_client
from app.core.ingestion.unfccc.ingest_row_unfccc import (
    CollectionIngestRow,
    UNFCCCDocumentIngestRow,
)

from app.core.ingestion.pipeline import generate_pipeline_ingest_input
from app.core.ingestion.processor import (
    get_collection_ingestor,
    initialise_context,
    get_unfccc_document_ingestor,
    get_document_validator,
)
from app.core.ingestion.reader import get_file_contents, read
from app.core.ingestion.utils import (
    IngestContext,
    Result,
    ResultType,
    UNFCCCIngestContext,
)
from app.core.ingestion.utils import (
    ValidationResult,
    get_result_counts,
)
from app.core.validation.types import ImportSchemaMismatchError
from app.core.validation.util import (
    get_new_s3_prefix,
    write_csv_to_s3,
    write_documents_to_s3,
    write_ingest_results_to_s3,
)
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

unfccc_ingest_router = r = APIRouter()


def start_unfccc_ingest(
    db: Session,
    s3_client: S3Client,
    s3_prefix: str,
    documents_file_contents: str,
    collection_file_contents: str,
):
    context = None
    # TODO: add a way for a user to monitor progress of the ingest
    try:
        context = initialise_context(db, "UNFCCC")
        # First the collections....
        collection_ingestor = get_collection_ingestor(db)
        read(
            collection_file_contents, context, CollectionIngestRow, collection_ingestor
        )

        document_ingestor = get_unfccc_document_ingestor(db, context)
        read(
            documents_file_contents, context, UNFCCCDocumentIngestRow, document_ingestor
        )
    except Exception as e:
        # This is a background task, so do not raise
        _LOGGER.exception(
            "Unexpected error on ingest", extra={"props": {"errors": str(e)}}
        )

    try:
        if context is not None:
            write_ingest_results_to_s3(
                s3_client=s3_client,
                s3_prefix=s3_prefix,
                results=context.results,
            )
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error writing ingest results to s3",
            extra={"props": {"errors": str(e)}},
        )

    try:
        pipeline_ingest_input = generate_pipeline_ingest_input(db)
        ctx = cast(UNFCCCIngestContext, context)
        # We now have to populate the download_url values...
        for doc in pipeline_ingest_input:
            doc.download_url = ctx.download_urls[doc.import_id]
        write_documents_to_s3(
            s3_client=s3_client,
            s3_prefix=s3_prefix,
            documents=pipeline_ingest_input,
        )
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error writing pipeline input document to s3",
            extra={"props": {"errors": str(e)}},
        )


@r.post(
    "/bulk-ingest/validate/unfccc",
    response_model=ValidationResult,
    status_code=status.HTTP_200_OK,
)
def validate_unfccc_law_policy(
    request: Request,
    unfccc_data_csv: UploadFile,
    collection_csv: UploadFile,
    db=Depends(get_db),
    current_user=Depends(get_superuser_details),
):
    """
    Validates the provided CSV into the document / family / collection schema.

    :param [Request] request: Incoming request (UNUSED).
    :param [UploadFile] law_policy_csv: CSV file to ingest.
    :param [Session] db: Database connection.
        Defaults to Depends(get_db).
    :param [JWTUser] current_user: Current user.
        Defaults to Depends(get_current_active_superuser).
    :return [str]: A path to an s3 object containing document updates to be processed
        by the ingest pipeline.
    :raises HTTPException: The following HTTPExceptions are raised on errors:
        400 If the provided CSV file fails schema validation
        422 On failed validation on the input CSV (results included)
        500 On an unexpected error
    """

    _LOGGER.info(
        f"Superuser '{current_user.email}' triggered Bulk Document Validation for "
        "UNFCCC Law & Policy data"
    )

    try:
        context = initialise_context(db, "UNFCCC")
    except Exception as e:
        _LOGGER.exception(
            "Failed to create ingest context", extra={"props": {"errors": str(e)}}
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    all_results = []

    try:
        _, _, message = _validate_unfccc_csv(
            unfccc_data_csv,
            collection_csv,
            db,
            cast(UNFCCCIngestContext, context),
            all_results,
        )
    except ImportSchemaMismatchError as e:
        _LOGGER.exception(
            "Provided CSV failed law & policy schema validation",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST) from e
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, validating law & policy CSV on ingest",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    # Intended output for this is the console - so for now just format it up for that.
    errors = [r for r in all_results if r.type == ResultType.ERROR]
    return ValidationResult(message=message, errors=errors)


@r.post(
    "/bulk-ingest/unfccc",
    response_model=BulkIngestResult,
    status_code=status.HTTP_202_ACCEPTED,
)
def ingest_unfccc_law_policy(
    request: Request,
    unfccc_data_csv: UploadFile,
    collection_csv: UploadFile,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    current_user=Depends(get_superuser_details),
    s3_client=Depends(get_s3_client),
):
    """
    Ingest the provided CSV into the document / family / collection schema.

    :param [Request] request: Incoming request (UNUSED).
    :param [UploadFile] unfccc_data_csv: CSV file containing documents to ingest.
    :param [UploadFile] collection_csv: CSV file containing collection to ingest.
    :param [BackgroundTasks] background_tasks: Tasks API to start ingest task.
    :param [Session] db: Database connection.
        Defaults to Depends(get_db).
    :param [JWTUser] current_user: Current user.
        Defaults to Depends(get_current_active_superuser).
    :param [S3Client] s3_client: S3 connection.
        Defaults to Depends(get_s3_client).
    :return [str]: A path to an s3 object containing document updates to be processed
        by the ingest pipeline.
    :raises HTTPException: The following HTTPExceptions are raised on errors:
        400 If the provided CSV file fails schema validation
        422 On failed validation on the input CSV (results included)
        500 On an unexpected error
    """
    # TODO: Combine with event import? refactor out shared structure?

    _LOGGER.info(
        f"Superuser '{current_user.email}' triggered Bulk Document Ingest for "
        "UNFCCC Law & Policy data"
    )

    try:
        context = initialise_context(db, "UNFCCC")
    except Exception as e:
        _LOGGER.exception(
            "Failed to create ingest context", extra={"props": {"errors": str(e)}}
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    all_results = []

    # PHASE 1 - Validate
    try:
        documents_file_contents, collection_file_contents, _ = _validate_unfccc_csv(
            unfccc_data_csv,
            collection_csv,
            db,
            cast(UNFCCCIngestContext, context),
            all_results,
        )
    except ImportSchemaMismatchError as e:
        _LOGGER.exception(
            "Provided CSV failed law & policy schema validation",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST) from e
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, validating law & policy CSV on ingest",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    # If we have any validation errors then raise
    validation_errors = [r for r in context.results if r.type == ResultType.ERROR]
    if validation_errors:
        _LOGGER.error(
            "Ingest failed validation (results attached)",
            extra={"errors": validation_errors},
        )
        error_details = [e.details for e in validation_errors]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=error_details
        )

    # PHASE 2 - Validation completed without errors, so store the ingest files. This
    #           will let us investigate errors later
    s3_prefix = get_new_s3_prefix()
    try:
        result_documents: Union[S3Document, bool] = write_csv_to_s3(
            s3_client=s3_client,
            s3_prefix=s3_prefix,
            s3_content_label="documents",
            file_contents=documents_file_contents,
        )
        result_collections: Union[S3Document, bool] = write_csv_to_s3(
            s3_client=s3_client,
            s3_prefix=s3_prefix,
            s3_content_label="collections",
            file_contents=collection_file_contents,
        )

        if (
            type(result_documents) is bool
        ):  # S3Client returns False if the object was not created
            _LOGGER.error(
                "Write Bulk Document Ingest CSV to S3 Failed.",
                extra={
                    "props": {
                        "superuser_email": current_user.email,
                    }
                },
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unexpected error, fail to write Bulk Document Ingest CSV to S3",
            )
        if (
            type(result_collections) is bool
        ):  # S3Client returns False if the object was not created
            _LOGGER.error(
                "Write Bulk Collections Ingest CSV to S3 Failed.",
                extra={
                    "props": {
                        "superuser_email": current_user.email,
                    }
                },
            )
            msg = "Unexpected error, fail to write Bulk Collections Ingest CSV to S3"
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=msg,
            )
        else:
            documents_csv_s3_location = str(result_documents.url)
            collections_csv_s3_location = str(result_collections.url)
            _LOGGER.info(
                "Write Event UNFCCC Ingest CSV complete.",
                extra={
                    "props": {
                        "superuser_email": current_user.email,
                        "documents_csv_s3_location": documents_csv_s3_location,
                        "collections_csv_s3_location": collections_csv_s3_location,
                    }
                },
            )
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, writing Bulk Document Ingest CSV content to S3",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    # PHASE 3 - Start the ingest (kick off background task to do the actual ingest)
    background_tasks.add_task(
        start_unfccc_ingest,
        db,
        s3_client,
        s3_prefix,
        documents_file_contents,
        collection_file_contents,
    )

    _LOGGER.info(
        "Background Bulk Document/Event Ingest Task added",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "documents_csv_s3_location": documents_csv_s3_location,
            }
        },
    )

    # TODO: Add some way the caller can monitor processing pipeline...
    return BulkIngestResult(
        import_s3_prefix=s3_prefix,
        detail=None,  # TODO: add detail?
    )


def _validate_unfccc_csv(
    unfccc_data_csv: UploadFile,
    collection_csv: UploadFile,
    db: Session,
    context: UNFCCCIngestContext,
    all_results: list[Result],
) -> tuple[str, str, str]:
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

    collection_file_contents = get_file_contents(collection_csv)
    read(collection_file_contents, context, CollectionIngestRow, collate_ids)

    # Now do the validation of the documents
    documents_file_contents = get_file_contents(unfccc_data_csv)
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

    _LOGGER.info(message)

    return documents_file_contents, collection_file_contents, message
