import logging
import os
import sys
from pathlib import Path

from api import FastAPITelemetry, ServiceManifest, SQLAlchemyTelemetry, TelemetryConfig
from fastapi import FastAPI
from routers import router
from session import get_engine

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

# Configure Open Telemetry.
ENV = os.getenv("ENV", "development")
os.environ["OTEL_PYTHON_LOG_CORRELATION"] = "True"
try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file(f"{root_dir}/service-manifest.json"), ENV, "0.1.0"
    )
except Exception as _:
    _LOGGER.error("Failed to load service manifest, using defaults")
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

# Include router in app
app.include_router(router)
_LOGGER.info("✅ FastAPI application initialised")
