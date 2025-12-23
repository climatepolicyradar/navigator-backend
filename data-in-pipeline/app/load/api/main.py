from pathlib import Path

from fastapi import Depends, FastAPI
from session import get_db
from settings import settings

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

# Create the FastAPI app
app = FastAPI(
    docs_url="/load/docs",
    redoc_url="/load/redoc",
    openapi_url="/load/openapi.json",
)


@app.get("/load/health")
def health_check(db=Depends(get_db)):
    try:
        with db as session:
            session.execute("SELECT 1")

    except Exception as e:
        return {"status": "error", "error": e}

    return {
        "status": "ok",
        "version": settings.github_sha,
    }


@app.post("/load")
def create_document():
    return "Received POST request to the /load endpoint"
