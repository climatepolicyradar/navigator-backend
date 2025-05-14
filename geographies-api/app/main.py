from typing import Generic, TypeVar

from fastapi import APIRouter, FastAPI
from pydantic_settings import BaseSettings
from sqlmodel import SQLModel

APIDataType = TypeVar("APIDataType")


class APIResponse(SQLModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class Settings(BaseSettings):
    pass


settings = Settings()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
# TODO: Add OTel - https://linear.app/climate-policy-radar/issue/APP-572/add-otel-to-families-api
router = APIRouter(
    prefix="/geographies",
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
    }


app.include_router(router)
