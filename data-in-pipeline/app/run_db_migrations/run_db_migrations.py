import os

import requests

from app.bootstrap_telemetry import get_logger


def run_db_migrations() -> None:
    """Apply alembic migrations.

    Call through subprocess as opposed to the alembic command function
    as the server startup never completed when using the alembic
    solution.

    TODO: APP-1578 -Completing this method according to the agreed upon
    design is dependant on 1578 being unblocked. For now this is a
    temporary solution that uses the load API to run migrations, which
    unblocks development work.
    """

    logger = get_logger()

    # Ensure URL has a scheme - App Runner URLs may not include it
    load_api_base_url = os.getenv("DATA_IN_PIPELINE_LOAD_API_URL", "")
    if not load_api_base_url.startswith(("http://", "https://")):
        load_api_base_url = f"https://{load_api_base_url}"

    # Get the current and head versions of alembic from the load API
    try:
        response = requests.get(
            url=f"{load_api_base_url}/load/schema-info",
            timeout=2,  # seconds
        )
        response.raise_for_status()

        schema_info = response.json()
        current_version = schema_info["current_version"]
        head_version = schema_info["head_version"]
    except Exception as e:
        logger.exception(f"Error getting schema info from load API: {e}")
        raise e

    if current_version == head_version:
        logger.debug("No migrations to run, alembic is up to date")
        return

    # Run the migrations if not idempotent
    try:
        logger.debug(
            f"Upgrading schema from {current_version} to {head_version} via load API..."
        )
        response = requests.post(
            url=f"{load_api_base_url}/load/run-migrations",
            timeout=10,  # seconds
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception(
            f"Error upgrading schema from {current_version} to {head_version}  via load API"
        )
        raise e
    logger.info(
        f"Finished upgrading schema from {current_version} to {head_version} via load API"
    )
