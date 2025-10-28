from typing import Any

import requests

# from families_api.app.models import FamilyDocumentPublic
from pydantic import BaseModel

from app.models import SourceDocument
from app.util import get_api_url


class NavigatorDocument(BaseModel):
    # TODO: FLESH OUT, FamilyDocumentPublic
    import_id: str


API_BASE_URL = get_api_url()


def extract_navigator_document(import_id: str) -> SourceDocument[NavigatorDocument]:
    print(
        f"Fetching navigator document {import_id} from {API_BASE_URL}/families/documents/{import_id}"
    )
    json = fetch_document(import_id)

    print(f"Validating navigator document {import_id}...")
    source_data = NavigatorDocument.model_validate(json)

    print(f"Creating source document {import_id}...")
    source_document = SourceDocument[NavigatorDocument](
        source_data=source_data, source="navigator"
    )

    print("Done")
    return source_document


def fetch_document(id: str) -> dict[str, Any]:
    """Fetch a document from the API.

    :param id: The id of the document.
    :type id: str
    :return: The data from the JSON response from the API.
    :rtype: dict[str, Any]
    """
    url = f"{API_BASE_URL}/families/documents/{id}"
    response = requests.get(url, timeout=1)
    response.raise_for_status()

    response_json = response.json()
    return response_json["data"]
