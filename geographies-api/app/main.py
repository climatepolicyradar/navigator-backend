from typing import Generic, Optional, TypeVar

import pycountry
from fastapi import APIRouter, FastAPI, HTTPException, Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from sqlmodel import SQLModel

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
        # @related: GITHUB_SHA_ENV_VAR
        "version": settings.github_sha,
    }


class CountryResponse(BaseModel):
    alpha_2: str
    alpha_3: str
    name: str
    official_name: Optional[str] = None
    numeric: str
    flag: Optional[str] = None


def get_country_by_code(code: str) -> CountryResponse:
    """
    Retrieve country information using ISO alpha-3 code.

    NOTE: This utility function is used to fetch country metadata from
    the `pycountry` library using a standard alpha-3 code. It includes
    flag emoji generation and handles missing countries gracefully.

    :param str code: ISO alpha-3 country code (e.g., 'USA', 'GBR').
    :return CountryResponse: An object containing country details,
        including name, codes, and emoji flag.
    :raises HTTPException: If the provided code does not match any
        known country.
    """
    country = pycountry.countries.get(alpha_3=code.upper())

    if not country:
        raise HTTPException(
            status_code=404, detail=f"Country with alpha-3 code '{code}' not found"
        )

    # Get flag emoji (Unicode flag representation)
    flag_emoji = "".join(chr(ord(c) + 127397) for c in country.alpha_2)

    return CountryResponse(
        alpha_2=country.alpha_2,
        alpha_3=country.alpha_3,
        name=country.name,
        official_name=getattr(country, "official_name", None),
        numeric=country.numeric,
        flag=flag_emoji,
    )


@app.get("/countries/{code}", response_model=CountryResponse)
async def get_country(
    code: str = Path(
        ..., description="ISO alpha-3 country code", min_length=3, max_length=3
    ),
) -> CountryResponse:
    """
    Get country information by ISO alpha-3 code.

    NOTE: This endpoint retrieves metadata about a country by its
    alpha-3 code (e.g., 'USA'). It can be used to populate region-level
    UI components or to enrich geographic data.

    :param str code: ISO alpha-3 country code (e.g., 'USA', 'GBR', 'CAN').
    :return CountryResponse: An object representing the country,
        including name, codes, and flag emoji.
    """
    return get_country_by_code(code)


app.include_router(router)
