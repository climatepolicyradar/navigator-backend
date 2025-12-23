from pathlib import Path

from fastapi import FastAPI
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
def health_check():
    return {
        "status": "ok",
        "version": settings.github_sha,
    }


@app.post("/load")
def create_document():
    return "Received POST request to the /load endpoint"
