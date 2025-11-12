import logging
import logging.config
import os

from opentelemetry.instrumentation.logging import LoggingInstrumentor

LOG_LEVEL = os.getenv("OTEL_PYTHON_LOG_LEVEL", "INFO").upper()
ENV = os.getenv("ENV", "development")

_LOGGER = logging.getLogger(__name__)

_INSTRUMENTED = False


def configure_logging() -> None:
    """Configure root logging for the application.

    :return: The function does not return anything.
    :rtype: None
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        _LOGGER.debug("Root logging already configured. Skipping setup.")
        return

    logging.basicConfig(level=LOG_LEVEL)
    current_level = logging.getLevelName(root_logger.level)
    _LOGGER.debug("Configured root logging at %s level.", current_level)


def _is_logging_enabled() -> bool:
    """Determine whether OpenTelemetry logging should be enabled.

    :return: True when logging instrumentation is enabled.
    :rtype: bool
    """
    flag = os.getenv("DISABLE_OTEL_LOGGING", "true").lower()
    is_enabled = flag == "false"
    _LOGGER.debug("OpenTelemetry logging enabled flag evaluated to %s.", is_enabled)
    return is_enabled


def _enable_logging_instrumentor(force: bool = False) -> None:
    """Enable the OpenTelemetry logging instrumentor when configured.

    :return: The function does not return anything.
    :rtype: None
    :raises RuntimeError: When instrumentation initialisation fails.
    """
    if not _is_logging_enabled():
        _LOGGER.debug("OpenTelemetry logging instrumentor disabled.")
        return

    global _INSTRUMENTED
    if _INSTRUMENTED and not force:
        _LOGGER.debug("OpenTelemetry logging instrumentor already enabled.")
        return

    try:
        LoggingInstrumentor().instrument(set_logging_format=False)
        _LOGGER.debug("Enabled OpenTelemetry logging instrumentor.")
        _INSTRUMENTED = True
    except Exception as exc:  # noqa: BLE001
        _LOGGER.exception(
            "Failed to enable OpenTelemetry logging instrumentor.",
        )
        raise RuntimeError("OpenTelemetry logging instrumentor failed.") from exc


def ensure_logging_active(force_instrumentation: bool = True) -> None:
    """Ensure logging configuration and instrumentation remain active.

    :param force_instrumentation: Whether to reinitialise instrumentation
        even when it has already been enabled, defaults to True
    :type force_instrumentation: bool, optional
    :return: The function does not return anything.
    :rtype: None
    """
    configure_logging()
    _enable_logging_instrumentor(force=force_instrumentation)


ensure_logging_active()
