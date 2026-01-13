import os

import requests

from app.models import Document


def load_to_db(documents: list[Document]) -> list[str]:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :returns list[str]: List of ids of the saved documents.
    """
    response = requests.post(url: os.getenv("LOAD_API_URL"), data: documents)

    return response.json()
