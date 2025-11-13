"""Application bootstrap configuring logging and telemetry."""

from app.logging_config import TELEMETRY, ensure_logging_active

ensure_logging_active()

__all__ = ["TELEMETRY", "ensure_logging_active"]
