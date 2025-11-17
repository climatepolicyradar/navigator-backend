"""
Prefect integration for Navigator telemetry.

PrefectTelemetry specialises BaseTelemetry for Prefect workers/runs.
It injects Prefect runtime context (flow_run_id, flow_name, task_run_id, task_name)
into:
- Python logging records via a logging.Filter (so Prefect's get_run_logger and stdlib logs carry fields),
- OTEL log records via an enriching processor (so your custom exporter/collector can promote to labels),
- OTEL spans via a SpanProcessor (optional, for TraceQL filtering).

This ensures Grafana can filter logs/spans by selected run_id or flow_name.
"""

import logging
from typing import Optional, TypedDict

import prefect
from api.base_telemetry import BaseTelemetry
from api.telemetry_config import TelemetryConfig
from opentelemetry.sdk._logs import LogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import Span, SpanProcessor


class FlowContext(TypedDict, total=False):
    flow_run_id: Optional[str]
    flow_run_name: Optional[str]


class TaskContext(TypedDict, total=False):
    task_run_id: Optional[str]
    task_run_name: Optional[str]


def get_flow_context() -> FlowContext:
    """Returns current flow context if available, else empty dict."""
    try:
        fr = prefect.runtime.flow_run
        flow_run_id = getattr(fr, "id", None)
        flow_run_name = getattr(fr, "name", None)

        return {
            "flow_run_id": flow_run_id,
            "flow_run_name": flow_run_name,
        }
    except Exception:
        return {}


def get_task_context() -> TaskContext:
    """Returns current task context if available, else empty dict."""
    try:
        tr = prefect.runtime.task_run
        return {
            "task_run_id": getattr(tr, "id", None),
            "task_run_name": getattr(tr, "name", None),
        }
    except Exception:
        return {}


class PrefectContextFilter(logging.Filter):
    """Adds Prefect flow/task context to LogRecord for formatters/handlers."""

    def filter(self, record: logging.LogRecord) -> bool:
        fc = get_flow_context()
        tc = get_task_context()
        # Attach as attributes so formatters/handlers can use them
        setattr(record, "flow_run_id", fc.get("flow_run_id"))
        setattr(record, "flow_run_name", fc.get("flow_run_name"))
        setattr(record, "task_run_id", tc.get("task_run_id"))
        setattr(record, "task_run_name", tc.get("task_run_name"))
        return True


class PrefectLogContextProcessor(BatchLogRecordProcessor):
    """
    Enrich OTEL log records with Prefect flow/task context before export.

    Downstream exporter/collector should promote these attributes to Loki labels:
      - flow_run_id
      - flow_run_name
      - task_run_id
      - task_run_name
    """

    def on_emit(self, record: LogRecord) -> None:
        try:
            fc = get_flow_context()
            tc = get_task_context()
            if fc.get("flow_run_id"):
                record.attributes["flow_run_id"] = fc["flow_run_id"]
            if fc.get("flow_run_name"):
                record.attributes["flow_run_name"] = fc["flow_run_name"]
            if tc.get("task_run_id"):
                record.attributes["task_run_id"] = tc["task_run_id"]
            if tc.get("task_run_name"):
                record.attributes["task_run_name"] = tc["task_run_name"]
        except Exception:
            # Avoid breaking logging on enrichment failure
            pass
        finally:
            super().on_emit(record)


class PrefectSpanContextProcessor(SpanProcessor):
    """Adds Prefect flow/task context attributes to every span on start."""

    def on_start(self, span: Span, parent_context) -> None:
        fc = get_flow_context()
        tc = get_task_context()
        if fc.get("flow_run_id"):
            span.set_attribute("flow_run_id", fc["flow_run_id"])
        if fc.get("flow_run_name"):
            span.set_attribute("flow_run_name", fc["flow_run_name"])
        if tc.get("task_run_id"):
            span.set_attribute("task_run_id", tc["task_run_id"])
        if tc.get("task_run_name"):
            span.set_attribute("task_run_name", tc["task_run_name"])

    def on_end(self, span: Span) -> None:
        pass


class PrefectTelemetry(BaseTelemetry):
    """Telemetry wiring specialised for Prefect workers."""

    def __init__(self, config: TelemetryConfig) -> None:
        # Initialise base wiring first
        super().__init__(config)
        # Add a filter so stdlib/Pefect loggers carry context
        root = logging.getLogger()
        root.addFilter(PrefectContextFilter())
        # Replace the default OTEL log processor with our enriching one
        # by re-registering processors after BaseTelemetry set up.
        # Note: BaseTelemetry adds a BatchLogRecordProcessor(OTLPLogExporter).
        # We add an additional processor that enriches then calls exporter.
        # If you need to replace instead of add, patch BaseTelemetry to expose logger_provider.
        # Here we add our processor in addition.
        try:
            # Access the logger provider via the logging handler
            # If not accessible, you can reconfigure logging in BaseTelemetry to store it.
            from opentelemetry._logs import get_logger_provider

            lp = get_logger_provider()
            # Find the OTLP exporter used by BaseTelemetry via existing processors (best effort).
            # If not introspectable, instantiate one more exporter pointing at the same endpoint.
            # We reuse config.otlp_endpoint to avoid guessing.
            from opentelemetry.exporter.otlp.proto.http._log_exporter import (
                OTLPLogExporter,
            )

            endpoint = (
                f"{self.config.otlp_endpoint}/v1/logs"
                if self.config.otlp_endpoint
                else None
            )
            lp.add_log_record_processor(
                PrefectLogContextProcessor(OTLPLogExporter(endpoint=endpoint))
            )
        except Exception:
            # As a fallback we rely on the logging.Filter to carry context on Python logs.
            # The OTEL log enrichment may be missing if logger provider is not accessible.
            pass

        # Enrich spans for Prefect runs
        try:
            from opentelemetry import trace

            provider = trace.get_tracer_provider()
            provider.add_span_processor(PrefectSpanContextProcessor())
        except Exception:
            pass

    def attach_to_prefect_logger(self, logger_name: str = "prefect") -> logging.Logger:
        """
        Attach OTLP logging to a Prefect logger and apply context filter.

        :param logger_name: Prefect logger name, defaults to "prefect".
        :return: Logger configured for Prefect instrumentation.
        """
        prefect_logger = logging.getLogger(logger_name)
        prefect_logger.setLevel(self.config.log_level)

        prefect_logger.addFilter(PrefectContextFilter())
        return prefect_logger
