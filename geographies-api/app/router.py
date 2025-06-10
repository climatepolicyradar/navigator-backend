import logging
from typing import TypeVar

from fastapi import APIRouter, HTTPException, Path

from .model import (
    APIResponse,
    CountryResponse,
    Geography,
    Settings,
    SubdivisionResponse,
)
from .service import (
    get_country_by_code,
    get_subdivisions_by_country,
    populate_initial_countries_data,
)

APIDataType = TypeVar("APIDataType")


_LOGGER = logging.getLogger(__name__)

settings = Settings()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
# TODO: Add OTel - https://linear.app/climate-policy-radar/issue/APP-572/add-otel-to-families-api
router = APIRouter(
    prefix="/geographies",
)


@router.get("/", response_model=APIResponse[Geography])
def read_documents():
    """Test endpoint."""
    return APIResponse(
        data=[Geography(id=1)],
        total=1,
        page=1,
        page_size=1,
    )


@router.get("/countries/{code}", response_model=CountryResponse)
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


@router.get("/subdivisions/{country_code}", response_model=list[SubdivisionResponse])
async def get_country_subdivisions(
    country_code: str = Path(
        ..., description="ISO alpha-3 country code", min_length=3, max_length=3
    ),
) -> list[SubdivisionResponse]:
    """
    Get subdivisions for a country by ISO alpha-3 code.

    NOTE: This endpoint retrieves first-level administrative subdivisions
    (such as states, provinces, or regions) for a given country using
    its ISO alpha-3 code (e.g., 'USA', 'AUS', 'CAN'). This can be used to
    support region-based filtering, selection menus, or geographic analysis.

    :param str country_code: ISO alpha-3 country code (e.g., 'USA', 'AUS', 'CAN').
    :return list[SubdivisionResponse]: A list of subdivision objects representing
        the primary administrative divisions of the country.
    """
    return get_subdivisions_by_country(country_code)


@router.get("populate-s3-bucket")
def populate_s3_bucket():
    """
    Populate the S3 bucket with geographies data.

    NOTE: This endpoint is intended for internal use to populate the S3 bucket
    with geography-related data.
    """

    try:
        populate_initial_countries_data()
    except Exception as e:
        _LOGGER.error(f"Error populating S3 bucket: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": "S3 bucket populated with geographies data"}
