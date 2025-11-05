import requests
from pydantic import BaseModel

from app.models import Extracted
from app.util import get_api_url


class NavigatorFamily(BaseModel):
    import_id: str


API_BASE_URL = get_api_url()


def extract_navigator_family(import_id: str) -> Extracted[NavigatorFamily]:
    print(f"Fetching {import_id} from {API_BASE_URL}/families/{import_id}")
    data = _fetch(import_id)
    extacted = Extracted(data=data, source="navigator_family")
    return extacted


def _fetch(id: str) -> NavigatorFamily:
    """Fetch a family from the API.

    :param id: The id of the document.
    :type id: str

    :rtype: NavigatorFamily
    """
    url = f"{API_BASE_URL}/families/{id}"
    response = requests.get(url, timeout=1)
    response.raise_for_status()

    response_json = response.json()
    response_data = response_json["data"]

    data = NavigatorFamily.model_validate(response_data)

    return data
