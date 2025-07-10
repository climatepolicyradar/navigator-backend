import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import pycountry
import requests

from .data.cpr_custom_geographies import countries
from .data.geography_statistics_by_countries import geography_statistics_by_countries
from .data.regions import regions
from .data.regions_to_countries_mapping import regions_to_countries
from .model import (
    CountryResponse,
    CountryStatistics,
    RegionResponse,
    SubdivisionResponse,
)
from .s3_client import get_s3_client

_LOGGER = logging.getLogger(__name__)

CDN_URL = os.environ.get("CDN_URL")
GEOGRAPHIES_DOCUMENT_PATH = "geographies/countries.json"

DOCUMENT_URL = f"{CDN_URL}/{GEOGRAPHIES_DOCUMENT_PATH}"


def get_all_regions() -> list[RegionResponse]:
    """
    Retrieve all regions with their metadata.

    :return list[RegionResponse]: A list of region objects with metadata.
    """
    return [
        RegionResponse(name=region["name"], type=region["type"], slug=region["slug"])
        for region in regions
    ]


def get_region_by_slug(slug: str) -> RegionResponse | None:
    """
    Retrieve region information using a slug.

    :param str slug: A slug representation of a region name.
    :return regionResponse: An object containing region details,
        including name, type, and slug or None if not found.
    """
    for region in get_all_regions():
        if region.slug == slug:
            return region

    return None


def get_countries_by_region(slug: str) -> list[CountryResponse] | None:
    """
    Get all countries for a requested region by its slug.

    :param str slug: A slug representation of a region name.
    :return list[CountryResponse]: A list of country objects containing
        alpha-2, alpha-3 codes, name, official name, numeric code, and flag emoji,
        belonging to the requested region or None if slug is invalid or does not exist.
    :raises ValueError: If no country is returned for a code linked to the requested region.
    """
    selected_region_country_codes = regions_to_countries.get(slug)
    selected_countries = []

    if selected_region_country_codes:
        for country_code in selected_region_country_codes:
            selected_country = get_country_by_code(country_code)

            if not selected_country:
                raise ValueError(
                    f"Invalid country code in the region countries list: {country_code}"
                )
            else:
                selected_countries.append(selected_country)

    return selected_countries or None


def get_all_geography_statistics_by_countries() -> dict[str, CountryStatistics]:
    """
    Retrieve all geography statistics by countries.

    :return Dict[str, CountryStatistics]: A list of dictionaries containing country
        names, legislative processes, federal status, and federal details.
    """

    return {
        alpha3: CountryStatistics(
            name=country["name"],
            legislative_process=country["legislative_process"],
            federal=country["federal"],
            federal_details=country["federal_details"],
            political_groups=country["political_groups"],
            global_emissions_percent=country["global_emissions_percent"],
            climate_risk_index=country["climate_risk_index"],
            worldbank_income_group=country["worldbank_income_group"],
            visibility_status=country["visibility_status"],
        )
        for alpha3, country in geography_statistics_by_countries.items()
    }


class CustomCountriesError(Exception):
    """Custom exception for countries data loading errors"""

    pass


def load_cpr_custom_geographies() -> Dict[str, Any]:
    """
    Load custom CPR geography extensions from static JSON file.

    NOTE: This utility function loads custom geography data that extends
    the standard ISO 3166 country codes with CPR-specific entries like
    'International' and 'No Geography'. The data is stored in a python file that returns a dict with information was migrated from the database for better performance
    and deployment simplicity.

    :return Dict[str, Any]: Dictionary of custom geography entries keyed
        by alpha-3 codes, containing metadata like names, codes, and flags.
    """

    return countries


def get_country_by_code(code: str) -> CountryResponse | None:
    """
    Retrieve country information using ISO alpha-3 code.

    NOTE: This utility function is used to fetch country metadata from
    the `pycountry` library using a standard alpha-3 code. It includes
    flag emoji generation and handles missing countries gracefully.

    :param str code: ISO alpha-3 country code (e.g., 'USA', 'GBR').
    :return CountryResponse: An object containing country details,
        including name, codes, and emoji flag or None if not found.
    """
    data = get_geographies_data()

    countries = data.get("countries", {})
    country = countries.get(code.upper())

    if not country:
        return None

    return CountryResponse(
        alpha_2=country["alpha_2"],
        alpha_3=country["alpha_3"],
        name=country["name"],
        official_name=(
            country["official_name"] if country["official_name"] else country["name"]
        ),
        numeric=country["numeric"],
        flag=country["flag"],
    )


def get_subdivisions_by_country(country_code: str) -> list[SubdivisionResponse] | None:
    """
    Retrieve all subdivisions for a given country using ISO alpha-3 code.

    NOTE: This utility function fetches all administrative subdivisions
    (states, provinces, territories, etc.) for a specified country from
    the `pycountry` library. It validates the country exists before
    retrieving subdivisions and returns an empty list if no subdivisions
    are found for the country.

    :param str country_code: ISO alpha-3 country code (e.g., 'AUS', 'USA').
    :return list[SubdivisionResponse]: A list of objects containing subdivision
        details including codes, names, types, and parent relationships.
        Returns empty list if no subdivisions exist for the country or None
        if country was not found for country_code.
    """

    data = get_geographies_data()

    countries = data.get("subdivisions_grouped_by_countries", {})
    country = countries.get(country_code.upper())
    if not country:
        return None

    subdivisions = []

    for subdivision in country:
        subdivisions.append(
            SubdivisionResponse(
                code=subdivision["code"],
                name=subdivision["name"],
                type=subdivision["type"],
                country_alpha_2=subdivision["country_alpha_2"],
                country_alpha_3=subdivision["country_alpha_3"],
            )
        )

    return subdivisions


def get_all_country_subdivisions() -> list[SubdivisionResponse]:
    """
    Retrieve all subdivisions grouped by country (using ISO alpha-3 codes).

    NOTE: This utility function collects all administrative subdivisions
    available in the `pycountry` library and organizes them by their
    parent country (alpha-3 code).

    :return dict[str, list[SubdivisionResponse]]: A dictionary mapping
        alpha-3 country codes to their respective list of subdivisions.
    """

    data = get_geographies_data()

    subdivisions = data.get("subdivisions", [])

    return subdivisions


def get_all_pycountry_subdivisions_grouped_by_country() -> dict[str, list[dict]]:
    """
    Retrieve all subdivisions grouped by country (using ISO alpha-3 codes).

    NOTE: This utility function collects all administrative subdivisions
    available in the `pycountry` library and organizes them by their
    parent country (alpha-3 code).

    :return dict[str, list[SubdivisionResponse]]: A dictionary mapping
        alpha-3 country codes to their respective list of subdivisions.
    """
    subdivisions_by_country: dict[str, list[dict]] = {}

    for py_subdivision in pycountry.subdivisions:
        country_alpha_2 = py_subdivision.country_code  # type: ignore[attr-defined]
        country = pycountry.countries.get(alpha_2=country_alpha_2)

        if not country:
            continue  # Skip unrecognized country codes

        alpha_3 = country.alpha_3

        subdivision = SubdivisionResponse(
            code=py_subdivision.code,  # type: ignore[arg-type]
            name=py_subdivision.name,  # type: ignore[arg-type]
            type=py_subdivision.type,  # type: ignore[arg-type]
            country_alpha_2=country.alpha_2,
            country_alpha_3=country.alpha_3,
        ).model_dump()

        if alpha_3 not in subdivisions_by_country:
            subdivisions_by_country[alpha_3] = []

        subdivisions_by_country[alpha_3].append(subdivision)

    return subdivisions_by_country


def list_all_pycountry_subdivisions() -> list[dict]:
    """
    Return a flat list of all subdivisions across all countries.

    Each subdivision is represented as a dictionary (from model_dump()).
    """
    subdivisions_by_country = get_all_pycountry_subdivisions_grouped_by_country()
    all_subdivisions: list[dict] = []

    for subdivisions in subdivisions_by_country.values():
        all_subdivisions.extend(subdivisions)

    return all_subdivisions


def return_a_list_of_all_pycountry_country_objects() -> dict[str, dict]:
    """
    List all countries with their metadata.

    NOTE: This utility function retrieves all countries from the `pycountry`
    library and returns a list of country objects containing their
    alpha-2, alpha-3 codes, names, official names, numeric codes, and flags.
    It can be used to populate dropdowns or selection lists in the UI.

    :return list[CountryResponse]: A list of country objects with metadata.
    """

    countries = {
        country.alpha_3: CountryResponse(  # type: ignore[arg-type]
            alpha_2=country.alpha_2,  # type: ignore[arg-type]
            alpha_3=country.alpha_3,  # type: ignore[arg-type]
            name=country.name,  # type: ignore[arg-type]
            official_name=getattr(country, "official_name", None),
            numeric=country.numeric,  # type: ignore[arg-type]
            flag="".join(chr(ord(c) + 127397) for c in country.alpha_2),  # type: ignore[arg-type]
        ).model_dump()
        for country in pycountry.countries
    }

    custom_countries = load_cpr_custom_geographies()
    for alpha_3, custom_data in custom_countries.items():
        countries[alpha_3] = custom_data

    return countries


def get_all_countries() -> list[CountryResponse]:
    """
    Retrieve all countries with their metadata.

    NOTE: This utility function fetches all countries from the `pycountry`
    library and returns a list of country objects containing their
    alpha-2, alpha-3 codes, names, official names, numeric codes, and flags.
    It can be used to populate dropdowns or selection lists in the UI.

    :return list[CountryResponse]: A list of country objects with metadata.
    """

    data = get_geographies_data()
    if "countries" not in data:
        raise ValueError("Invalid data format: 'countries' key not found")
    result = list(data["countries"].values())
    return result


def populate_initial_countries_data():
    """
    Populate and upload initial country and subdivision reference data to S3.

    NOTE: This utility function compiles structured country and subdivision
    metadata using the `pycountry` library. It retrieves all countries and their
    associated subdivisions (grouped by ISO alpha-3 code), adds versioning and
    a timestamp, and uploads the resulting JSON document to a configured S3 bucket.


    :raises ConnectionError: If the S3 client is not connected.
    :raises ValueError: If the required GEOGRAPHIES_BUCKET environment variable is not set.
    """

    s3_client = get_s3_client()
    if s3_client.is_connected():
        _LOGGER.info("S3 client is connected")
    else:
        _LOGGER.error("S3 client is not connected")
        raise ConnectionError("Failed to connect to S3")

    # Create the object
    all_countries = return_a_list_of_all_pycountry_country_objects()
    subdivisions_grouped_by_countries = (
        get_all_pycountry_subdivisions_grouped_by_country()
    )
    all_subdivisions = list_all_pycountry_subdivisions()
    geography_statistics = get_all_geography_statistics_by_countries()
    regions = get_all_regions()
    countries_data = {
        "countries": all_countries,
        "subdivisions_grouped_by_countries": subdivisions_grouped_by_countries,
        "subdivisions": all_subdivisions,
        "regions": regions,
        "geography_statistics": geography_statistics,
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if os.environ["GEOGRAPHIES_BUCKET"] is None:
        raise ValueError("GEOGRAPHIES_BUCKET is not set")

    bucket_name = os.environ["GEOGRAPHIES_BUCKET"]
    file_key = "geographies/countries.json"
    s3_client.upload_json(countries_data, bucket_name, file_key)


def get_geographies_data(url: str | None = None) -> Dict[str, Any]:
    """
    Retrieve all countries data from the Climate Policy Radar CDN.

    NOTE: This utility function fetches country geographic data from the
    Climate Policy Radar CDN endpoint. It validates the response and handles
    common HTTP and JSON parsing errors gracefully.

    :param str url: The CDN URL to fetch countries data from. Defaults to
        the Climate Policy Radar countries endpoint.
    :return List[Dict[str, Any]]: A list of dictionaries containing country
        information including codes, names, and geographic details.
    :raises requests.RequestException: If the HTTP request fails due to
        network issues, timeouts, or server errors.
    :raises ValueError: If the response is not valid JSON or not a list.
    """

    url = DOCUMENT_URL if url is None else url

    if not url:
        raise ValueError("No URL provided for fetching countries data")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        return data

    except requests.RequestException as e:
        raise requests.RequestException(f"Failed to fetch countries data: {e}")
    except ValueError as e:
        raise ValueError(f"Invalid response format: {e}")
