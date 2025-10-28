import os


def get_api_url() -> str:
    """
    Returns the API URL based on the environment.
    """
    return os.getenv("API_BASE_URL", "https://api.climatepolicyradar.org")
