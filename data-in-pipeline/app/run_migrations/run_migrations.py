from app.bootstrap_telemetry import get_logger


def run_migrations() -> None:
    """
    Apply alembic migrations.

    Call through subprocess as opposed to the alembic command function as the server
    startup never completed when using the alembic solution.

    TODO: https://linear.app/climate-policy-radar/issue/APP-1600/get-a-working-db-url-into-run-migrations
    Completing this method is dependant on the above.
    """

    logger = get_logger()
    logger.info("run_migrations")
