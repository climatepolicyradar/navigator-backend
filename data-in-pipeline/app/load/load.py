import os

import requests
from pydantic import TypeAdapter

from app.models import Document


def load_to_db(documents: list[Document]) -> list[str] | Exception:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :returns list[str]: List of ids of the saved documents.
    """

    try:
        response = requests.post(
            url=os.getenv("LOAD_API_URL", ""),
            data=TypeAdapter(list[Document]).dump_json(documents),
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        return e

    return response.json()
