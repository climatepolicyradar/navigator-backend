from unittest.mock import patch

import pytest
from slugify import slugify

from app.model import CountryResponse, RegionResponse, RegionType
from app.service import (
    get_all_regions,
    get_countries_by_region,
    get_geographies,
    get_region_by_slug,
)


def test_get_all_regions_successfully_returns_all_regions(expected_regions):
    expected_regions_response = [
        RegionResponse(
            name=region["name"], type=RegionType(region["type"]), slug=region["slug"]
        )
        for region in expected_regions
    ]

    actual_result = [RegionResponse(**region) for region in get_all_regions()]

    assert actual_result == expected_regions_response


def test_get_region_by_slug_returns_the_correct_region():
    expected_region = RegionResponse(
        name="South Asia", type=RegionType.WORLD_BANK_REGION, slug="south-asia"
    )

    result = get_region_by_slug("south-asia")

    assert result is not None

    actual_region = RegionResponse(
        name=result["name"], type=RegionType(result["type"]), slug=result["slug"]
    )

    assert actual_region == expected_region


def test_get_region_by_slug_returns_none_if_slug_invalid():
    assert not get_region_by_slug("invalid")


def test_get_countries_by_region_returns_all_countries_for_the_requested_region():
    expected_countries = [
        CountryResponse(
            alpha_2="CA",
            alpha_3="CAN",
            name="Canada",
            official_name="Canada",
            numeric="124",
            flag="🇨🇦",
        ),
        CountryResponse(
            alpha_2="US",
            alpha_3="USA",
            name="United States",
            official_name="United States of America",
            numeric="840",
            flag="🇺🇸",
        ),
    ]

    assert get_countries_by_region("north-america") == expected_countries


def test_get_countries_by_region_returns_none_if_region_slug_invalid():
    assert not get_countries_by_region("invalid")


def test_get_countries_by_region_raises_if_region_countries_list_contains_invalid_country_code():
    with patch("app.service.get_country_by_code", return_value=None):
        with pytest.raises(ValueError):
            get_countries_by_region("north-america")


@pytest.mark.parametrize(
    ("id", "name", "slug", "slugifies"),
    [
        # hacks
        ("MDA", "Moldova, Republic of", "moldova", False),
        ("IRN", "Iran, Islamic Republic of", "iran", False),
        ("SYR", "Syrian Arab Republic", "syria", False),
        # non hacks
        ("STP", "São Tomé and Príncipe", "sao-tome-and-principe", True),
        ("BIH", "Bosnia and Herzegovina", "bosnia-and-herzegovina", True),
    ],
)
def test_country_id_to_slug_hack(id: str, name: str, slug: str, slugifies: bool):
    geographies = get_geographies().countries

    country = next((c for c in geographies if c.id == id), None)
    assert country is not None

    if not slugifies:
        assert slugify(country.name) != slug
        assert country.slug == slug
    else:
        assert country.slug == slugify(country.name)
        assert country.slug == slug
