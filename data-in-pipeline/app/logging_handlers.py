import json
import logging
import os
from pathlib import Path
from threading import Lock

from opentelemetry._logs import get_logger_provider, set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs import LoggingHandler as OTelLoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

_LOGGER = logging.getLogger(__name__)
_RESOURCE_ATTRIBUTES: dict[str, str] = {}
_MANIFEST_ATTRIBUTES: dict[str, str] = {}
_LOCK = Lock()
_HANDLER: OTelLoggingHandler | None = None
_SERVICE_MANIFEST_PATH = Path(__file__).resolve().parents[1] / "service-manifest.json"


def _parse_resource_attributes(raw_attributes: str) -> dict[str, str]:
    """Parse OTEL resource attributes from an environment variable string.

    :param raw_attributes: The raw comma separated resource attributes.
    :type raw_attributes: str
    :return: Parsed key-value pairs representing resource attributes.
    :rtype: dict[str, str]
    """
    parsed_attributes: dict[str, str] = {}
    for fragment in raw_attributes.split(","):
        if "=" not in fragment:
            continue
        key, value = fragment.split("=", 1)
        parsed_attributes[key.strip()] = value.strip()
    return parsed_attributes


def _resource_config() -> dict[str, str]:
    """Compose the resource attributes used for OpenTelemetry logging.

    :return: The resource attributes mapping.
    :rtype: dict[str, str]
    """
    if _RESOURCE_ATTRIBUTES:
        return _RESOURCE_ATTRIBUTES

    attributes: dict[str, str] = {
        "service.name": os.getenv("OTEL_SERVICE_NAME", "data-in-pipeline"),
    }
    manifest_attributes = _load_manifest_attributes()
    attributes.update(manifest_attributes)

    raw_attributes = os.getenv("OTEL_RESOURCE_ATTRIBUTES")
    if raw_attributes:
        attributes.update(_parse_resource_attributes(raw_attributes))

    attributes["service.version"] = os.getenv("OTEL_SERVICE_VERSION", "unknown")
    _RESOURCE_ATTRIBUTES.update(attributes)

    return _RESOURCE_ATTRIBUTES


def _load_manifest_attributes() -> dict[str, str]:
    """Load service metadata from the service manifest file.

    :return: Resource attributes derived from the manifest.
    :rtype: dict[str, str]
    :raises RuntimeError: When the manifest cannot be parsed.
    """
    if _MANIFEST_ATTRIBUTES:
        return _MANIFEST_ATTRIBUTES

    if not _SERVICE_MANIFEST_PATH.exists():
        _LOGGER.debug(
            "Service manifest not found at %s.",
            _SERVICE_MANIFEST_PATH,
        )
        return {}

    try:
        manifest = json.loads(_SERVICE_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        _LOGGER.exception(
            "Failed to parse service manifest at %s.",
            _SERVICE_MANIFEST_PATH,
        )
        raise RuntimeError("Service manifest parsing failed.") from error

    mapped_attributes = {
        "service.name": manifest.get("service.name", "data-in-pipeline"),
        "service.namespace": manifest.get("service.namespace", "data-fetching"),
        "service.team": manifest.get("team", "unknown"),
    }
    _MANIFEST_ATTRIBUTES.update(mapped_attributes)
    return _MANIFEST_ATTRIBUTES


class OTLPLogHandler(logging.Handler):
    """Forward Python logging records to an OTLP collector.

    :param level: Logging level applied to the handler, defaults to
        logging.NOTSET
    :type level: int, optional
    :return: Initialised handler instance.
    :rtype: None
    """

    def __init__(self, level: int = logging.NOTSET) -> None:
        """Initialise the handler and ensure OTLP plumbing is configured.

        :param level: Logging level used by the handler, defaults to
            logging.NOTSET
        :type level: int, optional
        """
        super().__init__(level)
        self._otel_handler = self._initialise_handler()

    @staticmethod
    def _ensure_logger_provider() -> LoggerProvider:
        """Ensure a logger provider with OTLP export is registered.

        :return: Configured logger provider instance.
        :rtype: LoggerProvider
        """
        provider = get_logger_provider()
        if isinstance(provider, LoggerProvider):
            _LOGGER.debug("Reusing existing OTLP logger provider.")
            return provider

        resource = Resource.create(_resource_config())
        provider = LoggerProvider(resource=resource)
        exporter = OTLPLogExporter()
        provider.add_log_record_processor(
            BatchLogRecordProcessor(exporter),
        )
        set_logger_provider(provider)
        _LOGGER.debug(
            "Registered OTLP logger provider with resource %s.",
            resource.attributes,
        )
        return provider

    @classmethod
    def _initialise_handler(cls) -> OTelLoggingHandler:
        """Initialise or reuse the shared OTLP logging handler.

        :return: The shared OpenTelemetry logging handler.
        :rtype: OTelLoggingHandler
        """
        global _HANDLER
        if _HANDLER is not None:
            return _HANDLER

        with _LOCK:
            if _HANDLER is not None:
                return _HANDLER

            provider = cls._ensure_logger_provider()
            level = getattr(
                logging,
                os.getenv("OTEL_PYTHON_LOG_LEVEL", "INFO").upper(),
                logging.INFO,
            )
            handler = OTelLoggingHandler(
                level=level,
                logger_provider=provider,
            )
            endpoint = (
                os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
                or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
                or "default"
            )
            _LOGGER.debug(
                "Created OTLP logging handler forwarding to %s.",
                endpoint,
            )
            _HANDLER = handler
            return handler

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record via OpenTelemetry infrastructure.

        :param record: The logging record to forward.
        :type record: logging.LogRecord
        :return: The function does not return anything.
        :rtype: None
        """
        self._otel_handler.emit(record)

    def flush(self) -> None:
        """Flush the underlying OpenTelemetry handler.

        :return: The function does not return anything.
        :rtype: None
        """
        self._otel_handler.flush()

    def close(self) -> None:
        """Close the handler and release resources.

        :return: The function does not return anything.
        :rtype: None
        """
        try:
            self._otel_handler.close()
        finally:
            super().close()
