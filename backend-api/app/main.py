import logging
import logging.config
import os
from contextlib import asynccontextmanager

import json_logging
import uvicorn
from fastapi import APIRouter, Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_health import health
from fastapi_pagination import add_pagination
from starlette.requests import Request

from app import config
from app.api.api_v1.routers.admin import admin_document_router
from app.api.api_v1.routers.auth import auth_router
from app.api.api_v1.routers.collections import collections_router
from app.api.api_v1.routers.documents import documents_router
from app.api.api_v1.routers.lookups import lookups_router
from app.api.api_v1.routers.pipeline_trigger import pipeline_trigger_router
from app.api.api_v1.routers.search import search_router
from app.api.api_v1.routers.summaries import summary_router
from app.api.api_v1.routers.world_map import world_map_router
from app.clients.db.session import SessionLocal
from app.service.auth import get_superuser_details
from app.service.health import is_database_online
from app.service.vespa import make_vespa_search_adapter
from app.telemetry import Telemetry
from app.telemetry_config import ServiceManifest, TelemetryConfig
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

os.environ["SKIP_ALEMBIC_LOGGING"] = "1"


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
_LOGGER = logging.getLogger(__name__)
logging.config.dictConfig(DEFAULT_LOGGING)

try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file("service-manifest.json"), config.ENV, "0.1.0"
    )
except Exception as _:
    _LOGGER.error("Failed to load service manifest, using defaults")
    otel_config = TelemetryConfig(
        service_name="navigator-backend",
        namespace_name="navigator",
        service_version="0.0.0",
        environment=config.ENV,
    )

telemetry = Telemetry(otel_config)
tracer = telemetry.get_tracer()

_docs_description = """
This documentation is intended to explain the use of our search API for external 
developers and integrators. The API is a typical REST API where the requests and
responses are encoded as `application/json`.

We ask that users be respectful of its use and remind users that data is available to
download on request.

Please be aware that this documentation is still under development.
"""
ENABLE_API_DOCS = os.getenv("ENABLE_API_DOCS", "False").lower() == "true"
_docs_url = "/api/docs" if ENABLE_API_DOCS else None
_openapi_url = "/api" if ENABLE_API_DOCS else None


@asynccontextmanager
async def lifespan(app_: FastAPI):
    _LOGGER.info("Starting up...")
    app.state.vespa_search_adapter = make_vespa_search_adapter()
    yield
    _LOGGER.info("Shutting down...")


app = FastAPI(
    title=config.PROJECT_NAME,
    description=_docs_description,
    docs_url=_docs_url,
    openapi_url=_openapi_url,
    lifespan=lifespan,
)
json_logging.init_fastapi(enable_json=True)
json_logging.init_request_instrument(app)
json_logging.config_root_logger()

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
    r"https://.+\.climateprojectexplorer\.org"
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
app.add_api_route("/health", health([is_database_online]), include_in_schema=False)


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
admin_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)
admin_router.include_router(pipeline_trigger_router)
admin_router.include_router(admin_document_router)

# App Routers
app.include_router(
    admin_router,
    prefix="/api/v1/admin",
    tags=["Admin"],
    dependencies=[Depends(get_superuser_details)],
    include_in_schema=False,
)
app.include_router(
    auth_router, prefix="/api", tags=["Authentication"], include_in_schema=False
)
app.include_router(
    documents_router, prefix="/api/v1", tags=["Documents"], include_in_schema=False
)
app.include_router(
    lookups_router, prefix="/api/v1", tags=["Lookups"], include_in_schema=False
)
app.include_router(search_router, prefix="/api/v1", tags=["Searches"])
app.include_router(
    summary_router, prefix="/api/v1", tags=["Summaries"], include_in_schema=False
)
app.include_router(
    world_map_router, prefix="/api/v1", tags=["Geographies"], include_in_schema=False
)
app.include_router(
    collections_router, prefix="/api/v1", tags=["Collections"], include_in_schema=False
)

# add pagination support to all routes that ask for it
add_pagination(app)


if __name__ == "__main__":
    uvicorn.run(
        app,
        # trunk-ignore(bandit/B104)
        host="0.0.0.0",
        port=8888,
        log_config=DEFAULT_LOGGING,
    )  # type: ignore


telemetry.instrument_fastapi(app)
telemetry.setup_exception_hook()
