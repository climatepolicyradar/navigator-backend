import logging
import logging.config
import os

from opentelemetry.instrumentation.logging import LoggingInstrumentor

LOG_LEVEL = os.getenv("OTEL_PYTHON_LOG_LEVEL", "INFO").upper()
ENV = os.getenv("ENV", "production")

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {},
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
    },
}
_LOGGER = logging.getLogger(__name__)
logging.config.dictConfig(DEFAULT_LOGGING)

if os.getenv("DISABLE_OTEL_LOGGING", "true").lower() == "false":
    try:
        LoggingInstrumentor().instrument(set_logging_format=False)
        _LOGGER.debug("Enabled OpenTelemetry logging instrumentor")
    except Exception as exc:  # noqa: BLE001
        _LOGGER.exception("Failed to enable OpenTelemetry logging instrumentor")
        raise RuntimeError("OpenTelemetry logging instrumentor failed") from exc
else:
    _LOGGER.debug("OpenTelemetry logging instrumentor disabled")
