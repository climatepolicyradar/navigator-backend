from fastapi import APIRouter, Depends
from session import check_db_health, get_db
from settings import settings

# Create router with /load prefix
router = APIRouter(prefix="/load")


@router.get("/health")
def health_check(db=Depends(get_db)):
    """Health check endpoint using session module's health check."""
    try:
        is_healthy = check_db_health()
        return {
            "status": "ok" if is_healthy else "error",
            "version": settings.github_sha,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/")
def create_document():
    return "Received POST request to the /load endpoint"
