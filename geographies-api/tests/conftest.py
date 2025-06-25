from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

EXPECTED_REGIONS = [
    {
        "name": "North America",
        "type": "World Bank Region",
        "slug": "north-america",
    },
    {
        "name": "East Asia & Pacific",
        "type": "World Bank Region",
        "slug": "east-asia-pacific",
    },
    {
        "name": "Latin America & Caribbean",
        "type": "World Bank Region",
        "slug": "latin-america-caribbean",
    },
    {
        "name": "Sub-Saharan Africa",
        "type": "World Bank Region",
        "slug": "sub-saharan-africa",
    },
    {
        "name": "Middle East & North Africa",
        "type": "World Bank Region",
        "slug": "middle-east-north-africa",
    },
    {
        "name": "Europe & Central Asia",
        "type": "World Bank Region",
        "slug": "europe-central-asia",
    },
    {
        "name": "South Asia",
        "type": "World Bank Region",
        "slug": "south-asia",
    },
    {
        "name": "Other",
        "type": "Other",
        "slug": "other",
    },
]


@pytest.fixture
def test_client():
    """Get a TestClient instance."""
    yield TestClient(app)


@pytest.fixture
def mock_get_countries_data():
    all_countries = {
        "ABW": {
            "alpha_2": "AW",
            "alpha_3": "ABW",
            "name": "Aruba",
            "official_name": None,
            "numeric": "533",
            "flag": "🇦🇼",
        },
        "AFG": {
            "alpha_2": "AF",
            "alpha_3": "AFG",
            "name": "Afghanistan",
            "official_name": "Islamic Republic of Afghanistan",
            "numeric": "004",
            "flag": "🇦🇫",
        },
        "AGO": {
            "alpha_2": "AO",
            "alpha_3": "AGO",
            "name": "Angola",
            "official_name": "Republic of Angola",
            "numeric": "024",
            "flag": "🇦🇴",
        },
        "AIA": {
            "alpha_2": "AI",
            "alpha_3": "AIA",
            "name": "Anguilla",
            "official_name": None,
            "numeric": "660",
            "flag": "🇦🇮",
        },
        "CAN": {
            "alpha_2": "CA",
            "alpha_3": "CAN",
            "name": "Canada",
            "official_name": "Canada",
            "numeric": "124",
            "flag": "🇨🇦",
        },
        "USA": {
            "alpha_2": "US",
            "alpha_3": "USA",
            "name": "United States",
            "official_name": "United States of America",
            "numeric": "840",
            "flag": "🇺🇸",
        },
        "XAB": {
            "alpha_2": "XAB",
            "alpha_3": "XAB",
            "name": "International",
            "official_name": "International",
            "numeric": "",
            "flag": "",
        },
        "XAA": {
            "alpha_2": "XAA",
            "alpha_3": "XAA",
            "name": "No Geography",
            "official_name": "No Geography",
            "numeric": "",
            "flag": "",
        },
    }

    all_subdivisions = {
        "ABW": [
            {
                "code": "AW-01",
                "name": "Oranjestad",
                "type": "Region",
                "country_alpha_2": "AW",
                "country_alpha_3": "ABW",
            },
            {
                "code": "AW-02",
                "name": "San Nicolas",
                "type": "Region",
                "country_alpha_2": "AW",
                "country_alpha_3": "ABW",
            },
        ],
        "AFG": [
            {
                "code": "AF-BDS",
                "name": "Badakhshan",
                "type": "Province",
                "country_alpha_2": "AF",
                "country_alpha_3": "AFG",
            },
            {
                "code": "AF-BDG",
                "name": "Badghis",
                "type": "Province",
                "country_alpha_2": "AF",
                "country_alpha_3": "AFG",
            },
            {
                "code": "AF-BAL",
                "name": "Balkh",
                "type": "Province",
                "country_alpha_2": "AF",
                "country_alpha_3": "AFG",
            },
        ],
        "AGO": [
            {
                "code": "AO-BGO",
                "name": "Bengo",
                "type": "Province",
                "country_alpha_2": "AO",
                "country_alpha_3": "AGO",
            },
            {
                "code": "AO-BGU",
                "name": "Benguela",
                "type": "Province",
                "country_alpha_2": "AO",
                "country_alpha_3": "AGO",
            },
            {
                "code": "AO-BIE",
                "name": "Bié",
                "type": "Province",
                "country_alpha_2": "AO",
                "country_alpha_3": "AGO",
            },
        ],
        "AIA": [
            {
                "code": "AI-01",
                "name": "The Valley",
                "type": "District",
                "country_alpha_2": "AI",
                "country_alpha_3": "AIA",
            },
            {
                "code": "AI-02",
                "name": "North Side",
                "type": "District",
                "country_alpha_2": "AI",
                "country_alpha_3": "AIA",
            },
            {
                "code": "AI-03",
                "name": "West End",
                "type": "District",
                "country_alpha_2": "AI",
                "country_alpha_3": "AIA",
            },
        ],
    }

    countries_data = {
        "countries": all_countries,
        "subdivisions": all_subdivisions,
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    with patch(
        "app.service.get_countries_data", return_value=countries_data
    ) as mock_func:
        yield mock_func
