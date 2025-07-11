import os

import requests


def get_geographies_api_url() -> str:
    """
    Returns the correct geographies API URL based on the environment.
    """
    env = os.getenv("API_BASE_URL", "https://api.climatepolicyradar.org")
    return f"{env}/geographies/"


def fetch_all_countries():  # TODO: auto generate types @related AUTOGENERATED_GEO_TYPES
    """
    Fetches all countries from the geographies API.
    Returns a list of CountryResponse objects.
    """
    url = get_geographies_api_url()
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    countries = response.json()
    return countries
