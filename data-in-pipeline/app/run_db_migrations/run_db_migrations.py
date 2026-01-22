import os

import requests

from app.bootstrap_telemetry import get_logger


def run_db_migrations() -> None:
    """Apply alembic migrations.

    Call through subprocess as opposed to the alembic command function
    as the server startup never completed when using the alembic
    solution.

    """

    logger = get_logger()

    # Ensure URL has a scheme - App Runner URLs may not include it
    load_api_base_url = os.getenv("DATA_IN_PIPELINE_LOAD_API_URL", "")
    if not load_api_base_url.startswith(("http://", "https://")):
        load_api_base_url = f"https://{load_api_base_url}"

    try:
        logger.debug("Triggering schema migrations via load API...")
        response = requests.post(
            url=f"{load_api_base_url}/load/run-migrations",
            timeout=10,
        )
        response.raise_for_status()
    except Exception:
        logger.exception("Error triggering schema migrations via load API")
        raise

    logger.info("Schema migrations completed successfully via load API")
