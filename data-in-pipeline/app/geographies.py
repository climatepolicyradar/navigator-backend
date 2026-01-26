"""
This is a copy/paste from the geographies API but simplified.
@see: ../../geographies-api/app/model.py

The reason we have not shared code as the geographies-api inherits a lot
of tech debt from having to support some legacy requirements from the frontend
which we will sunset soon.
"""

from typing import Literal, cast

import pycountry
from pycountry.db import Country as PyCountryCountry
from pycountry.db import Subdivision as PyCountrySubdivision
from pydantic import BaseModel


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

countries = cast(list[PyCountryCountry], pycountry.countries)
subdivisions = cast(list[PyCountrySubdivision], pycountry.subdivisions)

geographies = Geographies(
    countries=[
        Country(
            id=country.alpha_3,
            name=country.name,
            alpha_2=country.alpha_2,
            alpha_3=country.alpha_3,
            numeric=country.numeric,
        )
        for country in countries
    ]
    + custom_countries,
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
