import logging
from pathlib import Path

from api.settings import settings
from fastapi import APIRouter, FastAPI

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel("INFO")

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

# Create the FastAPI app
app = FastAPI(
    docs_url="/documents/docs",
    redoc_url="/documents/redoc",
    openapi_url="/documents/openapi.json",
)

router = APIRouter(prefix="/documents")
app.include_router(router)


@app.get("/health")
@router.get("/health")
def health_check():
    return {
        "status": "ok",
        "version": settings.github_sha,
    }


@router.post("/")
def create_document():
    _LOGGER.info("Received POST request to the /documents/ endpoint")
