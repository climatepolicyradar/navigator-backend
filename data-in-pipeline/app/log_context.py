"""Simple pipeline log context using contextvars.

Provides automatic injection of pipeline-specific fields into log records
for both console output (via PipelineContextFilter) and OTEL export
(via PipelineLogContextProcessor).
"""

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from opentelemetry.sdk._logs._internal import LogData
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

_context: ContextVar[dict[str, Any]] = ContextVar("pipeline_context", default={})


def get_context() -> dict[str, Any]:
    """Get current pipeline context as a dict."""
    return _context.get().copy()


@contextmanager
def log_context(**kwargs):
    """Add fields to log context for duration of block.

    Composes with outer contexts - inner blocks inherit and can override.

    Example:
        with log_context(pipeline_stage="extract"):
            with log_context(page_number=1):
                logger.info("Fetching")  # Has both fields
    """
    previous = _context.get()
    _context.set({**previous, **kwargs})
    try:
        yield
    finally:
        _context.set(previous)


class PipelineContextFilter(logging.Filter):
    """Adds pipeline context to Python LogRecords (for console output)."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in get_context().items():
            setattr(record, key, value)
        return True


_LOGGER = logging.getLogger(__name__)


class PipelineLogContextProcessor(BatchLogRecordProcessor):
    """Adds pipeline context to OTEL log records before export."""

    def on_emit(self, log_data: LogData) -> None:
        try:
            ctx = get_context()
            if ctx and log_data.log_record.attributes is not None:
                for key, value in ctx.items():
                    if value is not None:
                        log_data.log_record.attributes[key] = value  # type: ignore[index]
        except Exception as exc:
            # Don't break logging on enrichment failure
            _LOGGER.debug("Failed to enrich log with pipeline context: %s", exc)
        finally:
            super().on_emit(log_data)
