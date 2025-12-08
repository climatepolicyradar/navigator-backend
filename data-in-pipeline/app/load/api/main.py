import logging
from pathlib import Path

from api.router import router as documents_router
from api.settings import settings
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

# Create the FastAPI app
app = FastAPI(
    docs_url="/documents/docs",
    redoc_url="/documents/redoc",
    openapi_url="/documents/openapi.json",
)

# Include custom routers in our app
app.include_router(documents_router)


# Add CORS middleware to allow cross origin requests from any port
_ALLOW_ORIGIN_REGEX = (
    r"http://localhost:3000|"
    r"http://bs-local.com:3000|"
    r"https://.+\.climatepolicyradar\.org|"
    r"https://.+\.staging.climatepolicyradar\.org|"
    r"https://.+\.production.climatepolicyradar\.org|"
    r"https://.+\.sandbox\.climatepolicyradar\.org|"
    r"https://climate-laws\.org|"
    r"https://.+\.climate-laws\.org|"
    r"https://climateprojectexplorer\.org|"
    r"https://.+\.climateprojectexplorer\.org|"
    r"https://.+\.climatecasechart\.com"
)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=_ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# We use both routers to make sure we can have /families/health available publicly
# and /health available to the internal network & AppRunner health check.
@app.get("/health")
@documents_router.get("/health")
def health_check():
    return {
        "status": "ok",
        "version": settings.github_sha,  # @related: GITHUB_SHA_ENV_VAR
    }
