import logging
import os
import sys

LOG_LEVEL = os.getenv("OTEL_LOG_LEVEL", "INFO").upper()
DISABLED = os.getenv("DISABLE_OTEL_LOGGING", "true").lower() == "true"
_LOGGER = logging.getLogger(__name__)


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


ensure_logging_active()
