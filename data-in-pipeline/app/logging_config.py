import logging
import os
import sys
from pathlib import Path

import prefect.logging

from api.telemetry_config import ServiceManifest, TelemetryConfig
from api.telemetry_prefect import PrefectTelemetry

LOG_LEVEL = os.getenv("OTEL_PYTHON_LOG_LEVEL", "INFO").upper()
ENV = os.getenv("ENV", "development")
NUMERIC_LOG_LEVEL = getattr(logging, LOG_LEVEL, logging.INFO)
DISABLED = os.getenv("DISABLE_OTEL_LOGGING", "false").lower() == "true"
_LOGGER = logging.getLogger(__name__)
_APP_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _APP_DIR.parent
_SERVICE_MANIFEST_PATH = _ROOT_DIR / "service-manifest.json"
_PREFECT_LOGGING_CONFIG_PATH = _APP_DIR / "prefect_logging.yaml"
_TELEMETRY: PrefectTelemetry | None = None

LoggingAdapter = logging.LoggerAdapter[logging.Logger]


def configure_logging() -> None:
    """Configure the root logger for stdout streaming.

    :return: The function does not return anything.
    :rtype: None
    """
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=LOG_LEVEL, stream=sys.stdout)
        _LOGGER.debug("🧵 Configured basic logging with level %s.", LOG_LEVEL)


def _is_logging_enabled() -> bool:
    """Determine whether OpenTelemetry logging should be enabled.

    :return: Whether OTEL logging instrumentation is enabled.
    :rtype: bool
    """
    flag = os.getenv("DISABLE_OTEL_LOGGING", "true").lower()
    is_enabled = flag == "false"
    _LOGGER.debug("🛰️ OTEL logging enabled flag is %s.", is_enabled)
    return is_enabled


def _set_prefect_logging_config_path() -> None:
    """Ensure Prefect uses the shared logging configuration.

    :return: The function does not return anything.
    :rtype: None
    """
    assert _PREFECT_LOGGING_CONFIG_PATH.exists(), (
        "Prefect logging configuration missing."
    )
    os.environ.setdefault(
        "PREFECT_LOGGING_CONF_PATH",
        str(_PREFECT_LOGGING_CONFIG_PATH),
    )
    _LOGGER.debug(
        "🪄 Prefect logging config path set to %s.",
        _PREFECT_LOGGING_CONFIG_PATH,
    )


def _load_telemetry_config() -> TelemetryConfig:
    """Load telemetry configuration from manifest or fall back to defaults.

    :return: Telemetry configuration for Prefect telemetry.
    :rtype: TelemetryConfig
    """
    service_version = os.getenv("SERVICE_VERSION", "0.0.0")
    try:
        manifest = ServiceManifest.from_file(_SERVICE_MANIFEST_PATH)
    except Exception as error:  # noqa: BLE001
        _LOGGER.warning(
            "🫧 Failed to load service manifest for telemetry: %s",
            error,
        )
        return TelemetryConfig(
            service_name="data-in-pipeline",
            namespace_name="data-fetching",
            service_version=service_version,
            environment=ENV,
        )
    return TelemetryConfig.from_service_manifest(
        manifest,
        ENV,
        service_version,
    )


def _ensure_prefect_telemetry() -> PrefectTelemetry | None:
    """Initialise Prefect telemetry if OTEL logging is enabled.

    :return: Prefect telemetry instance when configured.
    :rtype: PrefectTelemetry | None
    """
    global _TELEMETRY
    if not _is_logging_enabled():
        _LOGGER.debug("🪨 OTEL logging disabled by configuration.")
        return None
    if _TELEMETRY is not None:
        _LOGGER.debug("🧩 Prefect telemetry already initialised.")
        return _TELEMETRY

    telemetry_config = _load_telemetry_config()
    _TELEMETRY = PrefectTelemetry(config=telemetry_config)
    prefect_logger = _TELEMETRY.attach_to_prefect_logger()
    assert isinstance(prefect_logger, logging.Logger), (
        "Prefect logger initialisation failed."
    )
    _LOGGER.debug("🛰️ Prefect telemetry attached to %s.", prefect_logger.name)
    return _TELEMETRY


def ensure_logging_active(force_instrumentation: bool = True) -> PrefectTelemetry | None:
    """Ensure logging configuration and telemetry remain active.

    :param force_instrumentation: Retained for compatibility; ignored.
    :type force_instrumentation: bool, optional
    :return: Prefect telemetry instance when available.
    :rtype: PrefectTelemetry | None
    """
    _ = force_instrumentation
    configure_logging()
    _set_prefect_logging_config_path()
    return _ensure_prefect_telemetry()


def get_logger() -> logging.Logger | LoggingAdapter:
    """Return a Prefect-aware logger at the configured level.

    :return: Logger configured for Prefect flows or stdlib logging.
    :rtype: logging.Logger | LoggingAdapter
    """
    try:
        logger = prefect.logging.get_run_logger()
    except prefect.exceptions.MissingContextError:
        logger = prefect.logging.get_logger()
    logger.setLevel(NUMERIC_LOG_LEVEL)
    _LOGGER.debug("🪪 Provided logger %s at level %s.", logger.name, LOG_LEVEL)
    return logger


TELEMETRY = ensure_logging_active()

__all__ = ["ensure_logging_active", "get_logger", "TELEMETRY"]
