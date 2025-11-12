import logging
import os
import sys

import prefect.logging

LOG_LEVEL = os.getenv("OTEL_LOG_LEVEL", "INFO").upper()
NUMERIC_LOG_LEVEL = getattr(logging, LOG_LEVEL, logging.INFO)
DISABLED = os.getenv("DISABLE_OTEL_LOGGING", "false").lower() == "true"
_LOGGER = logging.getLogger(__name__)

LoggingAdapter = logging.LoggerAdapter[logging.Logger]


def configure_logging():
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=LOG_LEVEL, stream=sys.stdout)


def ensure_logging_active():
    configure_logging()
    # Do NOT instrument in code; CLI handles it.
    # Respect the flag for any future custom logic.
    if DISABLED:
        _LOGGER.debug("OTel logging disabled by env.")


def get_logger() -> logging.Logger | LoggingAdapter:
    """
    Get a logger via Prefect.

    You can overwrite the logging level[2]. If not running in a flow
    or task run context, a logger that doesn't send to the Prefect API
    is returned.

    > `get_run_logger()` can only be used in the context of a flow or task.
    > To use a normal Python logger anywhere with your same configuration, use `get_logger()` from `prefect.logging`.
    > The logger retrieved with `get_logger()` will not send log records to the Prefect API.

    [1]: https://docs.prefect.io/v3/how-to-guides/workflows/add-logging
    [2]: https://docs.prefect.io/v3/api-ref/settings-ref#logging-level
    """
    try:
        logger = prefect.logging.get_run_logger()  # in flow/task → goes to Prefect UI
    except prefect.exceptions.MissingContextError:
        logger = prefect.logging.get_logger()  # outside → stdlib only
    logger.setLevel(NUMERIC_LOG_LEVEL)  # apply env-controlled level
    return logger


ensure_logging_active()
