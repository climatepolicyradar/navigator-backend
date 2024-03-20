import logging
import logging.config
import os
from contextlib import asynccontextmanager

import json_logging
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from db_client import run_migrations
from fastapi import APIRouter, Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_health import health
from fastapi_pagination import add_pagination
from starlette.requests import Request

from app.api.api_v1.routers.admin import admin_document_router
from app.api.api_v1.routers.auth import auth_router
from app.api.api_v1.routers.documents import documents_router
from app.api.api_v1.routers.geographies import geographies_router
from app.api.api_v1.routers.lookups import lookups_router
from app.api.api_v1.routers.pipeline_trigger import (
    pipeline_trigger_router,
    start_scheduled_ingest,
)
from app.api.api_v1.routers.search import search_router
from app.api.api_v1.routers.summaries import summary_router
from app.core import config
from app.core.auth import get_superuser_details
from app.core.health import is_database_online
from app.db.session import SessionLocal, engine

os.environ["SKIP_ALEMBIC_LOGGING"] = "1"

# Clear existing log handlers so we always log in structured JSON
root_logger = logging.getLogger()
if root_logger.handlers:
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

for _, logger in logging.root.manager.loggerDict.items():
    if isinstance(logger, logging.Logger):
        logger.propagate = True
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {},
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
    },
}
logger = logging.getLogger(__name__)
logging.config.dictConfig(DEFAULT_LOGGING)

ENABLE_API_DOCS = os.getenv("ENABLE_API_DOCS", "False").lower() == "true"
_docs_url = "/api/docs" if ENABLE_API_DOCS else None
_openapi_url = "/api" if ENABLE_API_DOCS else None


@asynccontextmanager
async def lifespan(app_: FastAPI):
    """Run startup and shutdown events."""
    logger.info("Starting up...")
    run_migrations(engine)
    yield
    logger.info("Shutting down...")


app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url=_docs_url,
    openapi_url=_openapi_url,
    lifespan=lifespan,
)
json_logging.init_fastapi(enable_json=True)
json_logging.init_request_instrument(app)
json_logging.config_root_logger()

_ALLOW_ORIGIN_REGEX = (
    r"http://localhost:3000|"
    r"https://.+\.climatepolicyradar\.org|"
    r"https://.+\.dev.climatepolicyradar\.org|"
    r"https://.+\.sandbox\.climatepolicyradar\.org|"
    r"https://climate-laws\.org|"
    r"https://.+\.climate-laws\.org"
)

# Add CORS middleware to allow cross origin requests from any port
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=_ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# add health endpoint
app.add_api_route("/health", health([is_database_online]))


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.db = SessionLocal()
    response = await call_next(request)
    request.state.db.close()
    return response


@app.get("/api/v1", include_in_schema=False)
async def root():
    return {"message": "CPR API v1"}


# Create an admin router that is a combination of:
admin_router = APIRouter()
admin_router.include_router(pipeline_trigger_router)
admin_router.include_router(admin_document_router)

# App Routers
app.include_router(
    admin_router,
    prefix="/api/v1/admin",
    tags=["Admin"],
    dependencies=[Depends(get_superuser_details)],
)
app.include_router(auth_router, prefix="/api", tags=["Authentication"])
app.include_router(documents_router, prefix="/api/v1", tags=["Documents"])
app.include_router(lookups_router, prefix="/api/v1", tags=["Lookups"])
app.include_router(search_router, prefix="/api/v1", tags=["Searches"])
app.include_router(summary_router, prefix="/api/v1", tags=["Summaries"])
app.include_router(geographies_router, prefix="/api/v1", tags=["Geographies"])

# add pagination support to all routes that ask for it
add_pagination(app)

scheduler = BackgroundScheduler()
scheduler.add_job(
    start_scheduled_ingest,
    "cron",
    day_of_week="mon",
    hour=9,
    minute=15,
)
scheduler.start()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8888,
        log_config=DEFAULT_LOGGING,
    )  # type: ignore
