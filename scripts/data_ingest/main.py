import json
import logging
import logging.config
import os
import sys
from http import HTTPStatus
from pathlib import Path

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

ADMIN_EMAIL_ENV = "SUPERUSER_EMAIL"
ADMIN_PASSWORD_ENV = "SUPERUSER_PASSWORD"
ADMIN_TOKEN_ENV = "SUPERUSER_TOKEN"
BULK_IMPORT_ENDPOINT = "api/v1/admin/bulk-imports/cclw/law-policy"


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
_LOG = logging.getLogger(__file__)


def _log_response(response: requests.Response) -> None:
    if response.status_code >= 400:
        _LOG.error(
            f"There was an error during a request to {response.url}. "
            f"STATUS: {response.status_code}, BODY:{response.content!r}"
        )

    _LOG.info(f"STATUS: {response.status_code}, BODY:{response.content!r}")


def get_admin_token() -> str:
    """Go through the login flow & create access token for requests."""

    _LOG.info("Getting auth token")
    admin_user = os.getenv(ADMIN_EMAIL_ENV)
    admin_password = os.getenv(ADMIN_PASSWORD_ENV)

    if admin_user is None or admin_password is None:
        raise RuntimeError("Admin username & password env vars must be set")

    response = requests.post(
        get_request_url("api/tokens"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"username": admin_user, "password": admin_password},
    )
    _log_response(response=response)

    token: str = response.json()["access_token"]
    _LOG.info("Returning auth token")
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
    """Build a URL from the path & the defined API_HOST."""

    api_host = os.getenv("API_HOST", "http://backend:8888").rstrip("/")
    return f"{api_host}/{endpoint}"


def post_data_ingest(ingest_csv_path: Path) -> requests.Response:
    """Trigger the CCLW bulk import endpoint with the given CSV file."""

    _LOG.info("Making bulk import request")

    mp_encoder = MultipartEncoder(
        fields={
            "law_policy_csv": (
                ingest_csv_path.name,
                open(ingest_csv_path, "rb"),
                "text/csv",
            ),
        }
    )
    request_headers = {
        **{"Content-Type": mp_encoder.content_type},
        **get_admin_auth_headers(),
    }
    response = requests.post(
        get_request_url(BULK_IMPORT_ENDPOINT),
        headers=request_headers,
        data=mp_encoder.to_string(),
    )
    _LOG.info("Bulk import request complete")
    _log_response(response)
    return response


def main(ingest_csv_path: Path):
    """
    Initial loader for alpha users.

    Bulk import data into the backend API database from CSV.

    :return: None
    """
    try:
        data_ingest_response = post_data_ingest(ingest_csv_path=ingest_csv_path)
    except Exception:
        _LOG.exception("Calling the endpoint raised an unexpected exception.")
        sys.exit(1)

    try:
        response_detail = json.dumps(json.loads(data_ingest_response.content), indent=2)
    except json.JSONDecodeError:
        response_detail = "No details found"
    if data_ingest_response.status_code == HTTPStatus.ACCEPTED:
        _LOG.info(
            "The selected CSV file was succesfully validated & will now be processed. "
            f"Import stats:\n {response_detail}"
        )
        sys.exit(0)

    if data_ingest_response.status_code == HTTPStatus.BAD_REQUEST:
        _LOG.error(
            "The selected CSV failed schema validation and cannot be processed. "
            f"Details:\n {response_detail}"
        )
        sys.exit(10)

    if data_ingest_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        _LOG.error(
            "The selected CSV contains rows that fail validation and cannot be "
            f"processed. Details:\n {response_detail}"
        )
        sys.exit(20)

    _LOG.error(
        "An unexpected response was received when submitting the selected CSV file "
        f"for processing:\n Response Status: {data_ingest_response.status_code}\n"
        f"Response content: {data_ingest_response.content}"
    )


if __name__ == "__main__":
    ingest_csv_path = Path(sys.argv[1])

    main(ingest_csv_path)
