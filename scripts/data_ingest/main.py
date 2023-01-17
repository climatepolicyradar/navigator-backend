import logging
import logging.config
import os
import sys
from http import HTTPStatus
from pathlib import Path
from pprint import pprint

import requests

ADMIN_EMAIL_ENV = "SUPERUSER_EMAIL"
ADMIN_PASSWORD_ENV = "SUPERUSER_PASSWORD"
ADMIN_TOKEN_ENV = "SUPERUSER_TOKEN"
BULK_IMPORT_ENDPOINT = "/api/v1/bulk-imports/cclw/law-policy"


DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "INFO",
        },
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

logging.config.dictConfig(DEFAULT_LOGGING)
logger = logging.getLogger(__file__)


def _log_response(response: requests.Response) -> None:
    if response.status_code >= 400:
        logger.error(
            f"There was an error during a request to {response.url}. "
            f"STATUS: {response.status_code}, BODY:{response.content!r}"
        )

    logger.debug(f"STATUS: {response.status_code}, BODY:{response.content!r}")


def get_admin_token() -> str:
    """Go through the login flow & create access token for requests."""
    admin_user = os.getenv(ADMIN_EMAIL_ENV)
    admin_password = os.getenv(ADMIN_PASSWORD_ENV)

    if admin_user is None or admin_password is None:
        raise RuntimeError("Admin username & password env vars must be set")

    response = requests.post(
        get_request_url("/api/tokens"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"username": admin_user, "password": admin_password},
    )
    _log_response(response=response)

    token: str = response.json()["access_token"]
    return token


def get_admin_auth_headers() -> dict[str, str]:
    """Create the required auth headers for requests."""
    if (admin_user_token := os.getenv(ADMIN_TOKEN_ENV)) is None:
        admin_user_token = get_admin_token()
        os.environ[ADMIN_TOKEN_ENV] = admin_user_token

    return {
        "Authorization": "Bearer {}".format(admin_user_token),
        "Accept": "application/json",
    }


def get_request_url(endpoint: str) -> str:
    api_host = os.getenv("API_HOST", "http://backend:8888").rstrip("/")
    return f"{api_host}/{endpoint}"


def post_data_ingest(ingest_csv_path: Path) -> requests.Response:
    response = requests.post(
        get_request_url(BULK_IMPORT_ENDPOINT),
        headers=get_admin_auth_headers(),
        files={"law_policy_csv": (None, ingest_csv_path.read_bytes(), "text/csv")}
    )
    return response


def main(ingest_csv_path: Path):
    """Initial loader for alpha users.

    Bulk import data into the backend API database from CSV.

    :return: None
    """
    try:
        data_ingest_response = post_data_ingest(ingest_csv_path=ingest_csv_path)
    except Exception:
        logger.exception("Calling the endpoint raised an unexpected exception.")
        sys.exit(1)

    if data_ingest_response.status_code == HTTPStatus.ACCEPTED:
        logger.info(
            "The selected CSV file was succesfully validated & will now be processed. "
            f"Import stats:\n {pprint(data_ingest_response.json())}"
        )
        sys.exit(0)

    if data_ingest_response.status_code == HTTPStatus.BAD_REQUEST:
        logger.error(
            "The selected CSV failed schema validation and cannot be processed. "
            f"Details:\n {pprint(data_ingest_response.json())}"
        )
        sys.exit(10)

    if data_ingest_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        logger.error(
            "The selected CSV contains rows that fail validation and cannot be "
            f"processed. Details:\n {pprint(data_ingest_response.json())}"
        )
        sys.exit(20)

    logger.error(
        "An unexpected response was received when submitting the selected CSV file "
        f"for processing:\n Response Status: {data_ingest_response.status_code}\n"
        f"Response Body: {data_ingest_response.text}"
    )


if __name__ == "__main__":
    ingest_csv_path = Path(sys.argv[1])

    main(ingest_csv_path)
