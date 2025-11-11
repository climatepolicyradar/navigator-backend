"""Application package initialisation including logging configuration."""

from app.logging_config import ensure_logging_active

ensure_logging_active(force_instrumentation=True)
