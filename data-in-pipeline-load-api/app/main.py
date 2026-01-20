import logging
import os
import sys
from pathlib import Path

from api import FastAPITelemetry, ServiceManifest, SQLAlchemyTelemetry, TelemetryConfig
from fastapi import FastAPI, Request

from app.routers import router
from app.session import get_engine

# Configure logging before anything else
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
_LOGGER = logging.getLogger(__name__)

# We always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

_LOGGER.debug("🚀 Starting FastAPI application")

# Configure Open Telemetry.
ENV = os.getenv("ENV", "development")
os.environ["OTEL_PYTHON_LOG_CORRELATION"] = "True"
try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file(f"{root_dir}/service-manifest.json"), ENV, "0.1.0"
    )
except Exception as _:
    _LOGGER.exception("Failed to load service manifest, using defaults")
    otel_config = TelemetryConfig(
        service_name="data-in-pipeline-load-api",
        namespace_name="data-fetching",
        service_version="0.0.0",
        environment=ENV,
    )

# Configure FastAPI and SQLAlchemy telemetry for the service.
telemetry = FastAPITelemetry(otel_config)
tracer = telemetry.get_tracer()
sqlalchemy_telemetry = SQLAlchemyTelemetry(tracer)
sqlalchemy_telemetry.instrument(get_engine())


# Create the FastAPI app
app = FastAPI(
    docs_url="/load/docs",
    redoc_url="/load/redoc",
    openapi_url="/load/openapi.json",
)


# Add debugging logs for routing
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # read the body
    body_bytes = await request.body()
    # decode for logging
    body_text = body_bytes.decode("utf-8") if body_bytes else "<empty>"
    # log request details
    print("=== Incoming Request ===")
    print(f"METHOD: {request.method}")
    print(f"PATH: {request.url.path}")
    print(f"HOST: {request.headers.get('host')}")
    print(f"BODY: {body_text}")
    print("========================")

    # set the body back so downstream handlers can read it
    async def receive():
        return {"type": "http.request", "body": body_bytes}

    request._receive = receive  # override the request's receive method
    # continue processing
    response = await call_next(request)
    return response


# Include router in app
app.include_router(router)
_LOGGER.debug("✅ FastAPI application initialised")
