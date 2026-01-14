import requests
from pydantic import TypeAdapter

from app.models import Document
from app.util import get_ssm_parameter


def load_to_db(documents: list[Document]) -> list[str] | Exception:
    """Sends documents to the load API to be saved in the DB.

    :param list[Document] documents: List of document objects to be saved.
    :returns list[str]: List of ids of the saved documents.
    """

    try:
        load_api_url = get_ssm_parameter("/data-in-pipeline-load-api/url")
        response = requests.post(
            url=load_api_url,
            data=TypeAdapter(list[Document]).dump_json(documents),
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        return e

    return response.json()
