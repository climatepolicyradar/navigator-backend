import logging

from data_in_models.models import Document as DocumentSchema
from fastapi import APIRouter, Depends, HTTPException, status

from app.alembic.run_migrations import run_migrations
from app.repository import check_db_health, create_documents, create_or_update_documents
from app.session import get_db, get_engine
from app.settings import settings

# Create router with /load prefix
router = APIRouter(prefix="/load")

_LOGGER = logging.getLogger(__name__)


@router.get("/health")
def health_check(db=Depends(get_db)):
    """Health check endpoint using session module's health check."""
    try:
        is_healthy = check_db_health(db)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    if not is_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection unhealthy",
        )
    return {"status": "ok", "version": settings.github_sha}


@router.post(
    "/documents", response_model=list[str], status_code=status.HTTP_201_CREATED
)
def create_document(documents: list[DocumentSchema], db=Depends(get_db)):
    if not documents:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No documents provided in request body",
        )

    try:
        return create_documents(db, documents)

    except Exception as e:
        _LOGGER.exception(f"Failed to create documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create documents",
        )


@router.put("/documents", response_model=str, status_code=status.HTTP_200_OK)
def update_documents(documents: list[DocumentSchema], db=Depends(get_db)):
    if not documents:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No documents provided in request body",
        )

    try:
        processed_documents = create_or_update_documents(db, documents)
        return f"Received {len(documents)} documents; Updated {len(processed_documents)} documents"

    except Exception as e:
        _LOGGER.exception(f"Failed to update documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update documents",
        )


@router.post("/run-migrations")
def run_schema_migrations(engine=Depends(get_engine)):
    try:
        run_migrations(engine)

    except Exception as e:
        _LOGGER.exception(f"Migration failed to run : {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run migrations",
        )

    return {"status": "ok", "detail": "Migrations ran successfully"}
