import logging
from io import StringIO
from typing import cast, Union

from sqlalchemy.orm import Session
from app.core.aws import S3Client

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy import update
from app.core.aws import S3Document

from app.api.api_v1.schemas.document import (
    BulkImportValidatedResult,
    DocumentCreateRequest,
    DocumentUpdateRequest,
)
from app.api.api_v1.schemas.user import User, UserCreateAdmin
from app.core.auth import get_current_active_superuser
from app.core.aws import get_s3_client
from app.core.email import (
    send_new_account_email,
    send_password_reset_email,
)
from app.core.ingestion.processor import (
    db_init,
    get_dfc_ingestor,
    get_dfc_validator,
)
from app.core.ingestion.reader import get_file_contents, read
from app.core.ingestion.utils import ResultType, get_result_counts
from app.core.ratelimit import limiter
from app.core.validation import IMPORT_ID_MATCHER
from app.core.validation.types import (
    ImportSchemaMismatchError,
    DocumentsFailedValidationError,
)
from app.core.validation.util import (
    get_valid_metadata,
    write_csv_to_s3,
)
from app.core.validation.cclw.law_policy.process_csv import (
    extract_documents,
    validated_input,
)
from app.db.crud.document import start_import
from app.db.crud.password_reset import (
    create_password_reset_token,
    invalidate_existing_password_reset_tokens,
)
from app.db.crud.user import (
    create_user,
    deactivate_user,
    edit_user,
    get_user,
    get_users,
)
from app.db.models.deprecated.document import Document
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

admin_users_router = r = APIRouter()

# TODO: revisit activation timeout
ACCOUNT_ACTIVATION_EXPIRE_MINUTES = 4 * 7 * 24 * 60  # 4 weeks


@r.get(
    "/users",
    response_model=list[User],
    response_model_exclude_none=True,
)
# TODO paginate
async def users_list(
    response: Response,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    """Gets all users"""
    _LOGGER.info(
        f"Superuser '{current_user.email}' listing all users.",
        extra={"props": {"superuser_email": current_user.email}},
    )
    users = get_users(db)
    # This is necessary for react-admin to work
    response.headers["Content-Range"] = f"0-9/{len(users)}"
    response.headers["Cache-Control"] = "no-cache, no-store, private"
    _LOGGER.info(f"Successfully retrieved user list for '{current_user.email}'.")
    return users


@r.get(
    "/users/{user_id}",
    response_model=User,
    response_model_exclude_none=True,
)
async def user_details(
    request: Request,
    response: Response,
    user_id: int,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    """Gets any user details"""
    _LOGGER.info(
        f"Superuser '{current_user.email}' retrieving details "
        f"about user id '{user_id}'",
        extra={"props": {"superuser_email": current_user.email, "user_id": user_id}},
    )
    user = get_user(db, user_id)
    response.headers["Cache-Control"] = "no-cache, no-store, private"
    _LOGGER.info(
        f"Successfully retrieved details about user id '{user_id}' "
        f"for '{current_user.email}'",
        extra={"props": {"superuser_email": current_user.email, "user_id": user_id}},
    )
    return user


@r.post("/users", response_model=User, response_model_exclude_none=True)
async def user_create(
    request: Request,
    user: UserCreateAdmin,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    """Creates a new user"""
    _LOGGER.info(
        f"Superuser '{current_user.email}' creating a new user",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_details": user.dict(),
            }
        },
    )
    try:
        db_user = create_user(db, user)
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Email already registered: {user.email}",
        )

    activation_token = create_password_reset_token(
        db, cast(int, db_user.id), minutes=ACCOUNT_ACTIVATION_EXPIRE_MINUTES
    )
    send_new_account_email(db_user, activation_token)

    _LOGGER.info(
        f"Superuser '{current_user.email}' successfully created new user",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_details": user.dict(),
            }
        },
    )
    return db_user


@r.put("/users/{user_id}", response_model=User, response_model_exclude_none=True)
async def user_edit(
    request: Request,
    response: Response,
    user_id: int,
    user: UserCreateAdmin,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    """Updates existing user"""
    _LOGGER.info(
        f"Superuser '{current_user.email}' updating user with id '{user_id}'",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_details": user.dict(),
            }
        },
    )
    updated_user = edit_user(db, user_id, user)

    # TODO: User updated email
    # send_email(EmailType.account_changed, updated_user)

    response.headers["Cache-Control"] = "no-cache, no-store, private"

    _LOGGER.info(
        f"Superuser '{current_user.email}' successfully "
        f"updated user with id '{user_id}'",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_details": user.dict(),
            }
        },
    )
    return updated_user


@r.delete("/users/{user_id}", response_model=User, response_model_exclude_none=True)
async def user_delete(
    request: Request,
    user_id: int,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    """Deletes existing user"""
    _LOGGER.info(
        f"Superuser '{current_user.email}' deactivating user with id '{user_id}'",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_id": user_id,
            }
        },
    )
    return deactivate_user(db, user_id)


@r.post(
    "/password-reset/{user_id}", response_model=bool, response_model_exclude_none=True
)
@limiter.limit("6/minute")
async def request_password_reset(
    request: Request,
    user_id: int,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    """
    Delete a password for a user, and kick off password-reset flow.

    As this flow is initiated by admins, it always
    - cancels existing tokens
    - creates a new token
    - sends an email

    Also see the equivalent unauthenticated endpoint.
    """
    _LOGGER.info(
        f"Superuser '{current_user.email}' resetting password for "
        f"user with id '{user_id}'",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_id": user_id,
            }
        },
    )

    deactivated_user = deactivate_user(db, user_id)
    invalidate_existing_password_reset_tokens(db, user_id)
    reset_token = create_password_reset_token(db, user_id)
    send_password_reset_email(deactivated_user, reset_token)

    _LOGGER.info(
        f"Superuser '{current_user.email}' successfully reset password for "
        f"user with id '{user_id}'",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "user_id": user_id,
            }
        },
    )
    return True


def _start_ingest(db: Session, s3_client: S3Client, file_contents: str):
    try:
        ingestor = get_dfc_ingestor()
        context = db_init(db)
        read(file_contents, context, ingestor)
    except Exception as e:
        _LOGGER.exception("Unexpected error on ingest", extra={"errors": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e
    # FIXME: Write document create/update JSON to S3
    # write_documents_to_s3(s3_client=s3_client, documents=document_parser_inputs)


@r.post(
    "/bulk-ingest/cclw/law-policy",
    response_model=str,
    status_code=status.HTTP_202_ACCEPTED,
)
def ingest_law_policy(
    request: Request,
    law_policy_csv: UploadFile,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
    s3_client=Depends(get_s3_client),
):
    """
    Ingest the provided CSV into the document / family / collection schema.

    :param [Request] request: Incoming request (UNUSED).
    :param [UploadFile] law_policy_csv: CSV file to ingest.
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
        "CCLW Law & Policy data"
    )
    validator = get_dfc_validator()

    # PHASE 1 - Validate
    try:
        file_contents = get_file_contents(law_policy_csv)
        context = db_init(db)
        read(file_contents, context, validator)
        rows, fails, resolved = get_result_counts(context.results)
        _LOGGER.info(
            f"Validation result: {rows} Rows, {fails} Failures, {resolved} Resolved"
        )
    except ImportSchemaMismatchError as e:
        _LOGGER.exception(
            "Provided CSV failed schema validation", extra={"props": {"errors": str(e)}}
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST) from e
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, validating on ingest",
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

    # No Validation errors - so write to s3
    try:
        result: Union[S3Document, bool] = write_csv_to_s3(
            s3_client=s3_client, file_contents=file_contents
        )
        if type(result) is bool:  # S3Client returns False if the object was not created
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
        else:
            csv_s3_location = str(result.url)
            _LOGGER.info(
                "Write Event Ingest CSV complete.",
                extra={
                    "props": {
                        "superuser_email": current_user.email,
                        "csv_s3_location": csv_s3_location,
                    }
                },
            )
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, writing Bulk Document Ingest CSV content to S3",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    # PHASE 2 - Ingest
    # Start the background task to do the actual event ingest.
    background_tasks.add_task(_start_ingest, db, s3_client, file_contents)

    _LOGGER.info(
        "Background Bulk Document Ingest Task added",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "csv_s3_location": csv_s3_location,
            }
        },
    )

    # TODO: Add some way the caller can monitor processing pipeline...
    return csv_s3_location


@r.post(
    "/bulk-ingest/cclw/event",
    response_model=str,
    status_code=status.HTTP_202_ACCEPTED,
)
def ingest_events(
    request: Request,
    event_csv: UploadFile,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
    s3_client=Depends(get_s3_client),
):
    """
    Ingest provided CSV into the events related to previously imported families.

    :param [Request] request: Incoming request (UNUSED).
    :param [UploadFile] event_csv: CSV file to ingest.
    :param [BackgroundTasks] background_tasks: Tasks API to start ingest task.
    :param [Session] db: Database connection.
        Defaults to Depends(get_db).
    :param [JWTUser] current_user: Current user.
        Defaults to Depends(get_current_active_superuser).
    :param [S3Client] s3_client: S3 connection.
        Defaults to Depends(get_s3_client).
    :return [str]: A path to an s3 object.
    :raises [HTTPException]: The following HTTPExceptions are raised on errors:
        400 If the provided CSV file fails schema validation
        422 On failed validation on the input CSV (results included)
        500 On an unexpected error
    """
    # TODO: Combine with document import? refactor out shared structure?
    # FIXME: this is currently just another document importer

    _LOGGER.info(
        f"Superuser '{current_user.email}' triggered Bulk Event Ingest for "
        "CCLW Law & Policy data"
    )
    validator = get_dfc_validator()

    # PHASE 1 - Validate
    try:
        file_contents = get_file_contents(event_csv)
        context = db_init(db)
        read(file_contents, context, validator)
        rows, fails, resolved = get_result_counts(context.results)
        _LOGGER.info(
            f"Validation result: {rows} Rows, {fails} Failures, {resolved} Resolved"
        )
    except ImportSchemaMismatchError as e:
        _LOGGER.exception(
            "Provided CSV failed schema validation", extra={"props": {"errors": str(e)}}
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST) from e
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, validating on ingest", extra={"errors": str(e)}
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    # If we have any validation errors then raise
    validation_errors = [r for r in context.results if r.type == ResultType.ERROR]
    if validation_errors:
        _LOGGER.error(
            "Bulk Event Ingest failed validation (results attached)",
            extra={"errors": validation_errors},
        )
        error_details = [e.details for e in validation_errors]
        raise HTTPException(
            status_code=status.HTTP_422_BAD_REQUEST, detail=error_details
        )

    # No Validation errors - so write to s3
    try:
        result: Union[S3Document, bool] = write_csv_to_s3(
            s3_client=s3_client, file_contents=file_contents
        )
        if type(result) is bool:  # S3Client returns False if the object was not created
            _LOGGER.error(
                "Write Bulk Event Ingest CSV to S3 Failed.",
                extra={
                    "props": {
                        "superuser_email": current_user.email,
                    }
                },
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unexpected error, failed to write Bulk Event Ingest CSV to S3",
            )
        else:
            csv_s3_location = str(result.url)
            _LOGGER.info(
                "Write Bulk Event Ingest CSV complete.",
                extra={
                    "props": {
                        "superuser_email": current_user.email,
                        "csv_s3_location": csv_s3_location,
                    }
                },
            )
    except Exception as e:
        _LOGGER.exception(
            "Unexpected error, writing Bulk Event Ingest CSV content to S3",
            extra={"props": {"errors": str(e)}},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) from e

    # PHASE 2 - Ingest
    # Start the background task to do the actual ingest.
    # FIXME: event ingest, not bulk ingest
    background_tasks.add_task(_start_ingest, db, s3_client, file_contents)

    _LOGGER.info(
        "Background Bulk Event Ingest Task added",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "csv_s3_location": csv_s3_location,
            }
        },
    )

    # TODO: Add some way the caller can monitor processing pipeline...
    return csv_s3_location


# TODO: This is the old endpoint which will get removed,
#       but left here for reference.
@r.post(
    "/bulk-imports/cclw/law-policy",
    response_model=BulkImportValidatedResult,
    status_code=status.HTTP_202_ACCEPTED,
)
def import_law_policy(
    request: Request,
    law_policy_csv: UploadFile,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
    s3_client=Depends(get_s3_client),
):
    """Process a Law/Policy data import"""
    _LOGGER.info(
        f"Superuser '{current_user.email}' triggered bulk import request for "
        "CCLW Law & Policy data"
    )

    try:
        file_contents = law_policy_csv.file.read().decode("utf8")
        csv_reader = validated_input(StringIO(initial_value=file_contents))

        valid_metadata = get_valid_metadata(db)
        existing_import_ids = [id[0] for id in db.query(Document.import_id)]

        encountered_errors = {}
        document_create_objects: list[DocumentCreateRequest] = []
        import_ids_to_create = []
        total_document_count = 0

        # TODO: Check for document existence?
        for validation_result in extract_documents(
            csv_reader=csv_reader, valid_metadata=valid_metadata
        ):
            total_document_count += 1
            if validation_result.errors:
                encountered_errors[validation_result.row] = validation_result.errors
            else:
                import_ids_to_create.append(validation_result.import_id)
                if validation_result.import_id not in existing_import_ids:
                    document_create_objects.append(validation_result.create_request)

        if encountered_errors:
            raise DocumentsFailedValidationError(
                message="File failed detailed validation.", details=encountered_errors
            )

        documents_ids_already_exist = set(import_ids_to_create).intersection(
            set(existing_import_ids)
        )
        document_skipped_count = len(documents_ids_already_exist)
        _LOGGER.info(
            "Bulk Import Validation Complete.",
            extra={
                "props": {
                    "superuser_email": current_user.email,
                    "document_count": total_document_count,
                    "document_added_count": total_document_count
                    - document_skipped_count,
                    "document_skipped_count": document_skipped_count,
                    "document_skipped_ids": list(documents_ids_already_exist),
                }
            },
        )

        result: Union[S3Document, bool] = write_csv_to_s3(
            s3_client=s3_client, file_contents=file_contents
        )

        csv_s3_location = "write failed" if type(result) is bool else str(result.url)
        _LOGGER.info(
            "Write Bulk Import CSV complete.",
            extra={
                "props": {
                    "superuser_email": current_user.email,
                    "csv_s3_location": csv_s3_location,
                }
            },
        )

        background_tasks.add_task(start_import, db, s3_client, document_create_objects)

        _LOGGER.info(
            "Background Bulk Import Task added",
            extra={
                "props": {
                    "superuser_email": current_user.email,
                    "csv_s3_location": csv_s3_location,
                }
            },
        )

        # TODO: Add some way the caller can monitor processing pipeline...

        return BulkImportValidatedResult(
            document_count=total_document_count,
            document_added_count=total_document_count - document_skipped_count,
            document_skipped_count=document_skipped_count,
            document_skipped_ids=list(documents_ids_already_exist),
            csv_s3_location=csv_s3_location,
        )
    except ImportSchemaMismatchError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=e.details,
        ) from e
    except DocumentsFailedValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=e.details,
        ) from e
    except Exception as e:
        _LOGGER.exception("Unexpected error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        ) from e


@r.put("/documents/{import_id_or_slug}", status_code=status.HTTP_200_OK)
async def update_document(
    request: Request,
    import_id_or_slug: str,
    meta_data: DocumentUpdateRequest,
    db=Depends(get_db),
    current_user=Depends(get_current_active_superuser),
):
    # TODO: As this grows move it out into the crud later.

    _LOGGER.info(
        f"Superuser '{current_user.email}' called update_document",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "import_id_or_slug": import_id_or_slug,
                "meta_data": meta_data,
            }
        },
    )

    # Note this code relies on the fields being the same as the db column names
    doc_update = update(Document)
    doc_update = doc_update.values(meta_data.dict())

    import_id = None
    slug = None

    doc_query = db.query(Document)
    if IMPORT_ID_MATCHER.match(import_id_or_slug) is not None:
        import_id = import_id_or_slug
        doc_update = doc_update.where(Document.import_id == import_id)
        doc_query = doc_query.filter(Document.import_id == import_id)
        _LOGGER.info("update_document called with import_id")
    else:
        slug = import_id_or_slug
        doc_update = doc_update.where(Document.slug == slug)
        doc_query = doc_query.filter(Document.slug == slug)
        _LOGGER.info("update_document called with slug")

    existing_doc = doc_query.first()

    if existing_doc is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    # TODO: Enforce uniqueness on import_id and slug on "Document"
    num_changed = db.execute(doc_update).rowcount

    if num_changed == 0:
        _LOGGER.info("update_document complete - nothing changed")
        return existing_doc  # Nothing to do - as should be idempotent

    if num_changed > 1:
        # This should never happen due to table uniqueness constraints
        # TODO Rollback
        raise HTTPException(
            detail=(
                f"There was more than one document identified by {import_id_or_slug}. "
                "This should not happen!!!"
            ),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    db.commit()
    db.refresh(existing_doc)
    _LOGGER.info(
        "Call to update_document complete",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "num_changed": num_changed,
                "import_id": existing_doc.import_id,
                "md5_sum": existing_doc.md5_sum,
                "content_type": existing_doc.content_type,
                "cdn_object": existing_doc.cdn_object,
            }
        },
    )
    return existing_doc
