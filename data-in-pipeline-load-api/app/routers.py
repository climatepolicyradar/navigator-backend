from fastapi import APIRouter, Depends, HTTPException, status
from models import Document
from repository import check_db_health
from session import get_db
from settings import settings

# Create router with /load prefix
router = APIRouter(prefix="/load")


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


@router.post("/", response_model=list[str], status_code=status.HTTP_201_CREATED)
def create_document(documents: list[Document]):
    return [doc.id for doc in documents]
