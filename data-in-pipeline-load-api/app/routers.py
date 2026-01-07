import logging

from fastapi import APIRouter, Depends, HTTPException, status
from repository import check_db_health
from session import get_db, test_db_connection
from settings import settings

_LOGGER = logging.getLogger(__name__)


# Create router with /load prefix
router = APIRouter(prefix="/load")


@router.get("/health")
def health_check():
    """Health check endpoint using session module's health check."""
    # try:
    # is_healthy = check_db_health(db)

    # except Exception as e:
    #     raise HTTPException(
    #         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
    #     )

    # if not is_healthy:
    #     raise HTTPException(
    #         status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    #         detail="Database connection unhealthy",
    #     )
    return {"status": "ok", "version": settings.github_sha}


@router.post("/")
def create_document():
    _LOGGER.error("Received POST request to the /load endpoint")
    test_db_connection()
    return "Received POST request to the /load endpoint"
