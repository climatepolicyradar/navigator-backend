"""
Telemetry bootstrap for the data-in-pipeline service.

This module provides a single initialization point for logging and OpenTelemetry.
Entry points should import this module before other app imports.
"""

import logging
import os
from pathlib import Path

from api import MetricsService, ServiceManifest, TelemetryConfig
from api.prefect_telemetry import PrefectTelemetry, get_logger

from app.pipeline_metrics import PipelineMetrics

# Suppress verbose HTTP client logs to prevent OTEL export feedback loop
# These libraries generate DEBUG logs for every HTTP request, which when
# captured by OTEL creates an infinite loop of log exports
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

_APP_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _APP_DIR.parent
_SERVICE_MANIFEST_PATH = _ROOT_DIR / "service-manifest.json"
_PREFECT_LOGGING_CONFIG_PATH = _APP_DIR / "prefect_logging.yaml"

# Set Prefect logging config path
if _PREFECT_LOGGING_CONFIG_PATH.exists():
    os.environ.setdefault(
        "PREFECT_LOGGING_CONF_PATH", str(_PREFECT_LOGGING_CONFIG_PATH)
    )

# Load configuration
_ENV = os.getenv("ENV", "development")
_SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.0.0")

_manifest = ServiceManifest.from_file(_SERVICE_MANIFEST_PATH)
_config = TelemetryConfig.from_service_manifest(_manifest, _ENV, _SERVICE_VERSION)

# Initialize telemetry
telemetry = PrefectTelemetry(config=_config)

# Initialize metrics
metrics_service = MetricsService(config=_config)

pipeline_metrics = PipelineMetrics(metrics_service=metrics_service)

if telemetry.logger:
    telemetry.logger.info("Telemetry bootstrap complete for %s", _config.service_name)

    telemetry.logger.info(
        "OTEL_SERVICE_NAME: " + os.getenv("OTEL_SERVICE_NAME", "not set")
    )
    telemetry.logger.info(
        "OTEL_TRACES_EXPORTER: " + os.getenv("OTEL_TRACES_EXPORTER", "not set")
    )
    telemetry.logger.info(
        "OTEL_METRICS_EXPORTER: " + os.getenv("OTEL_METRICS_EXPORTER", "not set")
    )
    telemetry.logger.info(
        "OTEL_LOGS_EXPORTER: " + os.getenv("OTEL_LOGS_EXPORTER", "not set")
    )
    telemetry.logger.info(
        "OTEL_EXPORTER_OTLP_ENDPOINT: "
        + os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "not set")
    )
    telemetry.logger.info(
        "OTEL_EXPORTER_OTLP_PROTOCOL: "
        + os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "not set")
    )
    telemetry.logger.info(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "
        + os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "not set")
    )
    telemetry.logger.info(
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "
        + os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "not set")
    )
    telemetry.logger.info(
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: "
        + os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "not set")
    )
    telemetry.logger.info(
        "OTEL_PYTHON_LOG_LEVEL: " + os.getenv("OTEL_PYTHON_LOG_LEVEL", "not set")
    )
    telemetry.logger.info(
        "OTEL_RESOURCE_ATTRIBUTES: " + os.getenv("OTEL_RESOURCE_ATTRIBUTES", "not set")
    )
    telemetry.logger.info(
        "PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY: "
        + os.getenv("PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY", "not set")
    )
    telemetry.logger.info(
        "PREFECT_LOGGING_LEVEL: " + os.getenv("PREFECT_LOGGING_LEVEL", "not set")
    )
    telemetry.logger.info(
        "PREFECT_LOGGING_EXTRA_LOGGERS: "
        + os.getenv("PREFECT_LOGGING_EXTRA_LOGGERS", "not set")
    )
    telemetry.logger.info(
        "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED: "
        + os.getenv("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED", "not set")
    )


__all__ = ["telemetry", "get_logger", "metrics_service", "pipeline_metrics"]
