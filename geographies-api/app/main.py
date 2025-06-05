import os
from typing import Generic, TypeVar

from fastapi import APIRouter, FastAPI
from pydantic_settings import BaseSettings
from sqlmodel import SQLModel

from api.telemetry import Telemetry
from api.telemetry_config import ServiceManifest, TelemetryConfig
from api.telemetry_exceptions import ExceptionHandlingTelemetryRoute

APIDataType = TypeVar("APIDataType")


class APIResponse(SQLModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class Settings(BaseSettings):
    # @related: GITHUB_SHA_ENV_VAR
    github_sha: str = "unknown"


settings = Settings()

os.environ["SKIP_ALEMBIC_LOGGING"] = "1"
os.environ["OTEL_PYTHON_LOG_CORRELATION"] = "True"

try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file("service-manifest.json"), os.getenv("ENV", "development"), "0.1.0"
    )
except Exception as _:
    otel_config = TelemetryConfig(
        service_name="geographies-api",
        namespace_name="navigator",
        service_version="0.0.0",
        environment=os.getenv("ENV", "development"),
    )

telemetry = Telemetry(otel_config)
tracer = telemetry.get_tracer()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
router = APIRouter(
    prefix="/geographies",
    route_class=ExceptionHandlingTelemetryRoute,
)
app = FastAPI(
    docs_url="/geographies/docs",
    redoc_url="/geographies/redoc",
    openapi_url="/geographies/openapi.json",
)


class Geography(SQLModel):
    id: int


@router.get("/", response_model=APIResponse[Geography])
def read_documents():

    return APIResponse(
        data=[Geography(id=1)],
        total=1,
        page=1,
        page_size=1,
    )


class GeographyDocumentCount(SQLModel):
    alpha3: str
    name: str
    count: int


# we use both to make sure we can have /geographies/health available publically
# and /health available to the internal network / AppRunner healthcheck
@app.get("/health")
@router.get("/health")
def health_check():
    return {
        "status": "ok",
        # @related: GITHUB_SHA_ENV_VAR
        "version": settings.github_sha,
    }


app.include_router(router)

telemetry.instrument_fastapi(app)
telemetry.setup_exception_hook()
