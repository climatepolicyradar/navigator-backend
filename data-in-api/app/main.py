from fastapi import Depends, FastAPI, HTTPException, status

from app.repository import check_db_health
from app.session import get_db
from app.settings import settings
from fastapi import FastAPI

app = FastAPI(title="DATA IN API")


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-health-check")
def db_health_check(db=Depends(get_db)):
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
