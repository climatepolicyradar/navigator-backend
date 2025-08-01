import logging
import os
from pathlib import Path
from typing import TypeVar

from api.telemetry import Telemetry
from api.telemetry_config import ServiceManifest, TelemetryConfig
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.model import Settings
from app.router import router as geographies_router

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent


# Open Telemetry initialisation
ENV = os.getenv("ENV", "development")
os.environ["OTEL_PYTHON_LOG_CORRELATION"] = "True"
try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file(f"{root_dir}/service-manifest.json"), ENV, "0.1.0"
    )
except Exception as _:
    _LOGGER.error("Failed to load service manifest, using defaults")
    otel_config = TelemetryConfig(
        service_name="navigator-backend",
        namespace_name="navigator",
        service_version="0.0.0",
        environment=ENV,
    )

telemetry = Telemetry(otel_config)
tracer = telemetry.get_tracer()

APIDataType = TypeVar("APIDataType")

settings = Settings()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
# TODO: Add OTel - https://linear.app/climate-policy-radar/issue/APP-572/add-otel-to-families-api

app = FastAPI(
    docs_url="/geographies/docs",
    redoc_url="/geographies/redoc",
    openapi_url="/geographies/openapi.json",
)

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


@app.get("/health")
@geographies_router.get("/health")
def health_check():
    return {
        "status": "ok",
        # @related: GITHUB_SHA_ENV_VAR
        "version": settings.github_sha,
    }


app.include_router(
    geographies_router,
    prefix="/geographies",
    tags=["Geographies"],
    include_in_schema=True,
)

# Open Telemetry instrumentation
telemetry.instrument_fastapi(app)
telemetry.setup_exception_hook()
