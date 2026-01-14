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
from opentelemetry._logs import get_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

from app.log_context import (
    PipelineContextFilter,
    PipelineLogContextProcessor,
    log_context,
)
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

os.environ["OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"] = "True"

# Set short export interval for batch jobs (instead of default 60s)
# This ensures metrics are exported even if the process is terminated abruptly
os.environ.setdefault("METRICS_EXPORT_INTERVAL_MS", "5000")

# Set Prefect logging config path
if _PREFECT_LOGGING_CONFIG_PATH.exists():
    os.environ.setdefault(
        "PREFECT_LOGGING_SETTINGS_PATH", str(_PREFECT_LOGGING_CONFIG_PATH)
    )

# Load configuration
_ENV = os.getenv("ENV", "development")
_SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.0.0")

_manifest = ServiceManifest.from_file(_SERVICE_MANIFEST_PATH)
_config = TelemetryConfig.from_service_manifest(_manifest, _ENV, _SERVICE_VERSION)

# Initialise telemetry
telemetry = PrefectTelemetry(config=_config)

# Initialise metrics
metrics_service = MetricsService(config=_config)

pipeline_metrics = PipelineMetrics(metrics_service=metrics_service)

# Register pipeline log context processors
# This adds pipeline_stage, import_id, page_number to logs automatically
# Use root logger which has console handlers configured
_root_logger = logging.getLogger()

if not telemetry._disabled:
    # Add filter for console output (Python LogRecords)
    _root_logger.addFilter(PipelineContextFilter())
    _root_logger.info("PipelineContextFilter registered on root logger")

    # Add processor for OTEL log export
    try:
        lp = get_logger_provider()
        if hasattr(lp, "add_log_record_processor"):
            lp.add_log_record_processor(  # type: ignore (pyright will complain we can't access this member but we've just checked we can)
                PipelineLogContextProcessor(
                    OTLPLogExporter(endpoint=telemetry.log_endpoint)
                )
            )
            _root_logger.info(
                "PipelineLogContextProcessor registered | endpoint=%s",
                telemetry.log_endpoint,
            )
        else:
            _root_logger.warning(
                "LoggerProvider does not support add_log_record_processor"
            )
    except Exception as exc:
        _root_logger.warning("Failed to add pipeline log context processor: %s", exc)
else:
    _root_logger.info("Telemetry disabled - pipeline context processors not registered")

_logger = get_logger()
_logger.info(
    "Telemetry bootstrap complete | service=%s env=%s version=%s endpoint=%s",
    _config.service_name,
    _ENV,
    _SERVICE_VERSION,
    os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "not set"),
)


__all__ = [
    "telemetry",
    "get_logger",
    "metrics_service",
    "pipeline_metrics",
    "log_context",
]
