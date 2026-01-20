import logging
import os

import requests
from data_in_models.models import Document
from pydantic import TypeAdapter

_LOGGER = logging.getLogger(__name__)


def load_to_db(documents: list[Document]) -> list[str] | Exception:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :returns list[str]: List of ids of the saved documents.
    """

    try:
        load_api_base_url = os.getenv("DATA_IN_PIPELINE_LOAD_API_URL", "")

        #  DEBUGGING: adding healthcheck before load
        _LOGGER.info("Checking load API health endpoint before sending data")
        health_response = requests.get(
            url=f"{load_api_base_url}/load/health",
            timeout=10,
        )
        _LOGGER.info(f"Load API health check response: {health_response.status_code}")

        # Ensure URL has a scheme - App Runner URLs may not include it
        if not load_api_base_url.startswith(("http://", "https://")):
            load_api_base_url = f"http://{load_api_base_url}"

        _LOGGER.info(
            f"Sending {len(documents)} documents to load API at {load_api_base_url}/load"
        )
        response = requests.post(
            url=f"{load_api_base_url}/load",
            json=TypeAdapter(list[Document]).dump_python(documents),
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        return e

    return response.json()
