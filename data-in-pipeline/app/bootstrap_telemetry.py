"""
Telemetry bootstrap for the data-in-pipeline service.

This module provides a single initialization point for logging and OpenTelemetry.
Entry points should import this module before other app imports.
"""

import logging
import os
from pathlib import Path

from api import ServiceManifest, TelemetryConfig
from api.prefect_telemetry import PrefectTelemetry, get_logger

_APP_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _APP_DIR.parent
_SERVICE_MANIFEST_PATH = _ROOT_DIR / "service-manifest.json"
_PREFECT_LOGGING_CONFIG_PATH = _APP_DIR / "prefect_logging.yaml"

# Set Prefect logging config path
if _PREFECT_LOGGING_CONFIG_PATH.exists():
    os.environ.setdefault("PREFECT_LOGGING_CONF_PATH", str(_PREFECT_LOGGING_CONFIG_PATH))

# Load configuration
_ENV = os.getenv("ENV", "development")
_SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.0.0")

_manifest = ServiceManifest.from_file(_SERVICE_MANIFEST_PATH)
_config = TelemetryConfig.from_service_manifest(_manifest, _ENV, _SERVICE_VERSION)

# Initialize telemetry
telemetry = PrefectTelemetry(config=_config)

if telemetry.logger:
    telemetry.logger.info("Telemetry bootstrap complete for %s", _config.service_name)

__all__ = ["telemetry", "get_logger"]
