import os

import requests
from data_in_models.models import Document
from pydantic import TypeAdapter

from app.bootstrap_telemetry import get_logger


def load_to_db(documents: list[Document]) -> list[str] | Exception:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :returns list[str]: List of ids of the saved documents.
    """
    _LOGGER = get_logger()
    _LOGGER.info("Load Started for %d documents.", len(documents))

    try:
        load_api_base_url = os.getenv("DATA_IN_PIPELINE_LOAD_API_URL", "")

        # Ensure URL has a scheme - App Runner URLs may not include it
        if not load_api_base_url.startswith(("http://", "https://")):
            load_api_base_url = f"https://{load_api_base_url}"

        response = requests.post(
            url=f"{load_api_base_url}/load/documents",
            json=TypeAdapter(list[Document]).dump_python(documents, mode="json"),
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        _LOGGER.exception("Error loading documents to DB: %s", e)
        return e

    _LOGGER.info("Loaded %d documents to the load DB.", len(documents))
    return response.json()
