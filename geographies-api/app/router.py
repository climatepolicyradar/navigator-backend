import logging
from typing import TypeVar

from fastapi import APIRouter, HTTPException, Path

from .model import CountryResponse, RegionResponse, Settings, SubdivisionResponse
from .service import (
    get_all_countries,
    get_all_country_subdivisions,
    get_all_regions,
    get_countries_by_region,
    get_country_by_code,
    get_region_by_slug,
    get_subdivisions_by_country,
    populate_initial_countries_data,
)

APIDataType = TypeVar("APIDataType")


_LOGGER = logging.getLogger(__name__)

settings = Settings()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
# TODO: Add OTel - https://linear.app/climate-policy-radar/issue/APP-572/add-otel-to-families-api
router = APIRouter()


@router.get("/regions", response_model=list[RegionResponse])
async def list_all_regions() -> list[RegionResponse]:
    """
    List all regions with their metadata.

    :return list[RegionResponse]: A list of region objects containing name, type, and slug.
    """
    return get_all_regions()


@router.get("/regions/{slug}", response_model=RegionResponse)
async def get_region(
    slug: str = Path(..., description="region slug")
) -> RegionResponse:
    """
    Get region with metadata by its slug.

    :param str slug: A slug representation of a region name.
    :return RegionResponse: A region object containing name, type, and slug.
    """

    result = get_region_by_slug(slug)

    if not result:
        error_msg = f"Could not find a region for slug: {slug}"
        _LOGGER.error(error_msg)
        raise HTTPException(status_code=404, detail=str(error_msg))
    return result


@router.get("/regions/{slug}/countries", response_model=list[CountryResponse])
async def get_countries_for_region(
    slug: str = Path(..., description="region slug")
) -> list[CountryResponse]:
    """
    Get all countries for a requested region by its slug.

    :param str slug: A slug representation of a region name.
    :return list[CountryResponse]: A list of country objects containing
        alpha-2, alpha-3 codes, name, official name, numeric code, and flag emoji,
        belonging to the requested region.
    """

    try:
        result = get_countries_by_region(slug)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not result:
        error_msg = f"Could not find a region for slug: {slug}"
        _LOGGER.error(error_msg)
        raise HTTPException(status_code=404, detail=str(error_msg))
    return result


@router.get("/", response_model=list[CountryResponse])
async def list_all_countries() -> list[CountryResponse]:
    """
    List all countries with their metadata.

    NOTE: This endpoint retrieves a list of all countries, including
    their ISO codes, names, and flag emojis. It can be used to populate
    dropdowns or selection menus in user interfaces.

    :return list[CountryResponse]: A list of country objects containing
        alpha-2, alpha-3 codes, name, official name, numeric code, and flag emoji.
    """
    return get_all_countries()


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
    result = get_country_by_code(code)

    if not result:
        raise HTTPException(
            status_code=404, detail=f"Country with alpha-3 code '{code}' not found"
        )
    return result


@router.get("/subdivisions", response_model=list[SubdivisionResponse])
async def get_subdivisions(
    country_code: str = Path(
        ..., description="ISO alpha-3 country code", min_length=3, max_length=3
    ),
) -> list[SubdivisionResponse]:
    """
    Get subdivisions for all countries.

    NOTE: This endpoint retrieves first-level administrative subdivisions
    (such as states, provinces, or regions) for a given country using
    its ISO alpha-3 code (e.g., 'USA', 'AUS', 'CAN'). This can be used to
    support region-based filtering, selection menus, or geographic analysis.

    :return list[SubdivisionResponse]: A list of subdivision objects representing
        the primary administrative divisions of the country.
    """
    result = get_all_country_subdivisions()

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Country with alpha-3 code '{country_code}' not found",
        )
    return result


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
    result = get_subdivisions_by_country(country_code)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Country with alpha-3 code '{country_code}' not found",
        )
    return result


@router.get("/populate-s3-bucket")
def populate_s3_bucket() -> dict[str, str]:
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
