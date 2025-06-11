import logging
import os
from datetime import datetime, timezone

import pycountry
from fastapi import HTTPException

from .model import CountryResponse, SubdivisionResponse
from .s3_client import get_s3_client

_LOGGER = logging.getLogger(__name__)


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


def get_subdivisions_by_country(country_code: str) -> list[SubdivisionResponse]:
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
        Returns empty list if no subdivisions exist for the country.
    :raises HTTPException: If the provided country code does not match any
        known country.
    """

    country = pycountry.countries.get(alpha_3=country_code.upper())
    if not country:
        raise HTTPException(
            status_code=404,
            detail=f"Country with alpha-3 code '{country_code}' not found",
        )

    subdivisions = []
    for py_subdivision in pycountry.subdivisions:
        # pycountry does not have type hints nor play nicely with strict static analysis
        if py_subdivision.country_code == country.alpha_2:  # type: ignore[arg-type]
            subdivisions.append(
                SubdivisionResponse(
                    code=py_subdivision.code,  # type: ignore[arg-type]
                    name=py_subdivision.name,  # type: ignore[arg-type]
                    type=py_subdivision.type,  # type: ignore[arg-type]
                    country_alpha_2=country.alpha_2,
                    country_alpha_3=country.alpha_3,
                )
            )

    return subdivisions


def get_all_subdivisions_grouped_by_country() -> dict[str, list[dict]]:
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


def list_all_countries() -> dict[str, dict]:
    """
    List all countries with their metadata.

    NOTE: This utility function retrieves all countries from the `pycountry`
    library and returns a list of country objects containing their
    alpha-2, alpha-3 codes, names, official names, numeric codes, and flags.
    It can be used to populate dropdowns or selection lists in the UI.

    :return list[CountryResponse]: A list of country objects with metadata.
    """

    return {
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
    all_countries = list_all_countries()
    all_subdivisons = get_all_subdivisions_grouped_by_country()
    countries_data = {
        "countries": all_countries,
        "subdivisions": all_subdivisons,
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if os.environ["GEOGRAPHIES_BUCKET"] is None:
        raise ValueError("GEOGRAPHIES_BUCKET is not set")

    bucket_name = os.environ["GEOGRAPHIES_BUCKET"]
    file_key = "geographies/countries.json"
    s3_client.upload_json(countries_data, bucket_name, file_key)
