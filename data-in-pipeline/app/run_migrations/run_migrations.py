import os

import requests

from app.bootstrap_telemetry import get_logger


def run_migrations() -> None:
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

    try:
        # Ensure URL has a scheme - App Runner URLs may not include it
        load_api_base_url = os.getenv("DATA_IN_PIPELINE_LOAD_API_URL", "")
        if not load_api_base_url.startswith(("http://", "https://")):
            load_api_base_url = f"https://{load_api_base_url}"

        logger.info("Running migrations via load API...")
        response = requests.post(
            url=f"{load_api_base_url}/load/run-migrations",
            timeout=10,  # seconds
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception("Error running migrations via load API")
        raise e
    logger.info("Finished running Load DB schema migrations")
