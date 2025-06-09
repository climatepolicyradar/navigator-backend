from typing import TypeVar

from fastapi import FastAPI

from app.model import Settings
from app.router import router as geographies_router

APIDataType = TypeVar("APIDataType")

settings = Settings()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
# TODO: Add OTel - https://linear.app/climate-policy-radar/issue/APP-572/add-otel-to-families-api

app = FastAPI(
    docs_url="/geographies/docs",
    redoc_url="/geographies/redoc",
    openapi_url="/geographies/openapi.json",
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
