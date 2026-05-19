"""
This is a copy/paste from the geographies API but simplified.
@see: ../../geographies-api/app/model.py

The reason we have not shared code as the geographies-api inherits a lot
of tech debt from having to support some legacy requirements from the frontend
which we will sunset soon.
"""

import csv
from pathlib import Path
from typing import Literal, cast

import pycountry
from pycountry.db import Subdivision as PyCountrySubdivision
from pydantic import BaseModel

_RAW_DATA_CSV = Path(__file__).parent / "geographies" / "raw-data.csv"


class GeographyBase(BaseModel):
    id: str
    name: str


class Country(GeographyBase):
    type: Literal["country"] = "country"
    alpha_2: str
    alpha_3: str
    numeric: str


class Subdivision(GeographyBase):
    type: Literal["subdivision"] = "subdivision"
    country_code: str


Geography = Country | Subdivision


class Geographies(BaseModel):
    countries: list[Country]
    subdivisions: list[Subdivision]


custom_countries = [
    Country(
        id="XAB",
        name="International",
        alpha_2="XA",
        alpha_3="XAB",
        numeric="",
    ),
    Country(
        id="EUR",
        name="European Union",
        alpha_2="EU",
        alpha_3="EUR",
        numeric="",
    ),
]


def _load_countries_from_raw_data() -> list[Country]:
    """Build country records from the curated ISO raw-data CSV.

    :return: One :class:`Country` per row in ``raw-data.csv``.
    :rtype: list[Country]
    """
    countries: list[Country] = []
    with _RAW_DATA_CSV.open(encoding="utf-8", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            alpha_2 = row["alpha-2"].strip()
            alpha_3 = row["alpha-3"].strip()
            numeric_raw = row["country-code"].strip()
            numeric = numeric_raw.zfill(3) if numeric_raw else ""
            countries.append(
                Country(
                    id=alpha_3,
                    name=row["ISO short name"].strip(),
                    alpha_2=alpha_2,
                    alpha_3=alpha_3,
                    numeric=numeric,
                )
            )
    return countries


subdivisions = cast(list[PyCountrySubdivision], pycountry.subdivisions)

geographies = Geographies(
    countries=_load_countries_from_raw_data() + custom_countries,
    subdivisions=[
        Subdivision(
            id=subdivision.code,
            name=subdivision.name,
            country_code=subdivision.country_code,
        )
        for subdivision in subdivisions
    ],
)

# cached for convenience
geographies_lookup = {country.id: country for country in geographies.countries} | {
    subdivision.id: subdivision for subdivision in geographies.subdivisions
}

geographies_by_alpha_2 = {country.alpha_2: country for country in geographies.countries}
