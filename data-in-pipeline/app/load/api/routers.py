from fastapi import APIRouter, Depends
from session import get_db, test_db_connection
from settings import settings

# Create router with /load prefix
router = APIRouter(prefix="/load")


@router.get("/health")
def health_check(db=Depends(get_db)):
    try:
        with db as session:
            is_online = session.execute("SELECT 1") is not None
            return {
                "status": "ok" if is_online else "error",
                "version": settings.github_sha,
            }

    except Exception as e:
        return {"status": "error", "error": e}


@router.post("/")
def create_document():
    test_db_connection()
    return "Received POST request to the /load endpoint"
