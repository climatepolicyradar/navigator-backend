import logging

from fastapi import APIRouter, BackgroundTasks, Depends, Request, status
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import BulkIngestResult
from app.core.auth import get_superuser_details
from app.core.aws import S3Client, get_s3_client
from app.core.ingestion.pipeline import generate_pipeline_ingest_input
from app.core.validation.util import get_new_s3_prefix, write_documents_to_s3
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

pipeline_trigger_router = r = APIRouter()


def _start_ingest(
    db: Session,
    s3_client: S3Client,
    s3_prefix: str,
):
    """Writes a db state file to s3 which will trigger an ingest."""
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
    "/start-ingest",
    response_model=BulkIngestResult,
    status_code=status.HTTP_202_ACCEPTED,
)
def ingest_law_policy(
    request: Request,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    current_user=Depends(get_superuser_details),
    s3_client=Depends(get_s3_client),
):
    """
    Ingest the provided CSV into the document / family / collection schema.

    :param [Request] request: Incoming request (UNUSED).
    :param [BackgroundTasks] background_tasks: Tasks API to start ingest task.
    :param [Session] db: Database connection.
        Defaults to Depends(get_db).
    :param [JWTUser] current_user: Current user.
        Defaults to Depends(get_current_active_superuser).
    :param [S3Client] s3_client: S3 connection.
        Defaults to Depends(get_s3_client).
    :return [str]: A path to an s3 object containing document updates to be processed
        by the ingest pipeline.
    :raises HTTPException: The following HTTPExceptions are
        500 On an unexpected error
    """
    _LOGGER.info(
        f"Superuser '{current_user.email}' triggered Bulk Document Ingest for "
        "CCLW Law & Policy data"
    )

    s3_prefix = get_new_s3_prefix()

    background_tasks.add_task(
        _start_ingest,
        db,
        s3_client,
        s3_prefix,
    )

    _LOGGER.info(
        "Background Bulk Document/Event Ingest Task added",
        extra={
            "props": {
                "superuser_email": current_user.email,
            }
        },
    )

    return BulkIngestResult(
        import_s3_prefix=s3_prefix,
        detail=None,
    )
