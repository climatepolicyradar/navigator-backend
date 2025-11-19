"""Application bootstrap configuring logging and telemetry."""

from app.bootstrap_telemetry import get_logger, telemetry

__all__ = ["telemetry", "get_logger"]
