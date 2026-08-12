import os
from datetime import datetime

import requests
from data_in_models.models import Document
from pydantic import TypeAdapter

from app.bootstrap_telemetry import get_logger


def _get_load_api_base_url() -> str:
    load_api_base_url = f"{os.getenv('DATA_IN_PIPELINE_LOAD_API_URL', '')}/load"

    # Ensure URL has a scheme - App Runner URLs may not include it
    if not load_api_base_url.startswith(("http://", "https://")):
        load_api_base_url = f"https://{load_api_base_url}"

    return load_api_base_url


def load_to_db(
    documents: list[Document], run_version: datetime | None = None
) -> str | Exception:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :param datetime | None run_version: The version to run the load with (default: None).
    :returns list[str]: List of ids of the saved documents.
    """
    _LOGGER = get_logger()
    _LOGGER.info("Load Started for %d documents.", len(documents))

    try:
        response = requests.put(
            url=f"{_get_load_api_base_url()}/documents",
            params={"run_version": run_version.isoformat() if run_version else None},
            json=TypeAdapter(list[Document]).dump_python(documents, mode="json"),
            timeout=30,  # seconds - adjusted for batch processing
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        _LOGGER.exception(
            "Error loading documents to DB: %s. Response: %s",
            e,
            e.response.text if e.response else "No response available",
        )
        return e

    _LOGGER.info("Successfully loaded batch of %d documents.", len(documents))
    return response.text


def advance_version(run_timestamp: datetime) -> datetime | Exception:
    """Advance the sync version watermark to run_timestamp, never backwards.

    Call this once per full run, after every batch in that run has loaded
    successfully - never per batch.

    :param run_timestamp: The version value to advance to.
    :returns: The watermark after this call, or the Exception if it failed.
    """
    _LOGGER = get_logger()

    try:
        response = requests.post(
            url=f"{_get_load_api_base_url()}/version",
            json=run_timestamp.isoformat(),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        _LOGGER.exception(
            "Error advancing sync version: %s. Response: %s",
            e,
            e.response.text if e.response else "No response available",
        )
        return e

    return response.json()
