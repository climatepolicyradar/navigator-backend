import os

import requests
from data_in_models.models import Document
from pydantic import TypeAdapter


def load_to_db(documents: list[Document]) -> list[str] | Exception:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :returns list[str]: List of ids of the saved documents.
    """

    try:
        load_api_base_url = os.getenv("DATA_IN_PIPELINE_LOAD_API_URL", "")
        # Ensure URL has a scheme - App Runner URLs may not include it
        if not load_api_base_url.startswith(("http://", "https://")):
            load_api_base_url = f"http://{load_api_base_url}"
        response = requests.post(
            url=f"{load_api_base_url}/load/",
            json=TypeAdapter(list[Document]).dump_json(documents),
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        return e

    return response.json()
