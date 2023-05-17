import logging
from typing import Union
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
from app.core.unfccc_ingestion.ingest_row_unfccc import UNFCCCDocumentIngestRow

from app.core.unfccc_ingestion.pipeline import generate_pipeline_ingest_input
from app.core.ingestion.processor import (
    initialise_context,
    get_dfc_ingestor,
    get_dfc_validator,
)
from app.core.unfccc_ingestion.reader import get_file_contents, read
from app.core.ingestion.utils import (
    IngestContext,
    Result,
    ResultType,
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


def _start_ingest(
    db: Session,
    s3_client: S3Client,
    s3_prefix: str,
    documents_file_contents: str,
):
    context = None
    # TODO: add a way for a user to monitor progress of the ingest
    try:
        context = initialise_context(db, "UNFCCC")
        document_ingestor = get_dfc_ingestor(db)
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
    "/bulk-ingest/validate/unfccc/law-policy",
    response_model=ValidationResult,
    status_code=status.HTTP_200_OK,
)
def validate_unfccc_law_policy(
    request: Request,
    law_policy_csv: UploadFile,
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
        _, message = _validate_unfccc_csv(law_policy_csv, db, context, all_results)
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
    "/bulk-ingest/unfccc/law-policy",
    response_model=BulkIngestResult,
    status_code=status.HTTP_202_ACCEPTED,
)
def ingest_unfccc_law_policy(
    request: Request,
    law_policy_csv: UploadFile,
    events_csv: UploadFile,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    current_user=Depends(get_superuser_details),
    s3_client=Depends(get_s3_client),
):
    """
    Ingest the provided CSV into the document / family / collection schema.

    :param [Request] request: Incoming request (UNUSED).
    :param [UploadFile] law_policy_csv: CSV file containing documents to ingest.
    :param [UploadFile] events_csv: CSV file containing events to ingest.
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
        documents_file_contents, _ = _validate_unfccc_csv(
            law_policy_csv, db, context, all_results
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
        documents_csv_s3_location = str(result_documents.url)
        _LOGGER.info(
            "Write Event Ingest CSV complete.",
            extra={
                "props": {
                    "superuser_email": current_user.email,
                    "documents_csv_s3_location": documents_csv_s3_location,
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
        _start_ingest,
        db,
        s3_client,
        s3_prefix,
        documents_file_contents,
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
    law_policy_csv: UploadFile,
    db: Session,
    context: IngestContext,
    all_results: list[Result],
) -> tuple[str, str]:
    """
    Validates the csv file

    :param UploadFile law_policy_csv: incoming file to validate
    :param Session db: connection to the database
    :param IngestContext context: the ingest context
    :param list[Result] all_results: the results
    :return tuple[str, str]: the file contents of the csv and the summary message
    """
    documents_file_contents = get_file_contents(law_policy_csv)
    validator = get_dfc_validator(db, context)
    read(documents_file_contents, context, UNFCCCDocumentIngestRow, validator)
    rows, fails, resolved = get_result_counts(context.results)
    all_results.extend(context.results)
    context.results = []
    message = (
        f"Law & Policy validation result: {rows} Rows, {fails} Failures, "
        f"{resolved} Resolved"
    )

    _LOGGER.info(message)

    return documents_file_contents, message
