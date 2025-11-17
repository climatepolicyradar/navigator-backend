"""
Prefect integration for Navigator telemetry.

PrefectTelemetry specialises BaseTelemetry for Prefect workers/runs.
It injects Prefect runtime context (flow_name, run_id, run_name, task_id, task_name)
into:
- Python logging records via a logging.Filter (so Prefect's get_run_logger and stdlib logs carry fields),
- OTEL log records via an enriching processor (so your custom exporter/collector can promote to labels),
- OTEL spans via a SpanProcessor (optional, for TraceQL filtering).

This ensures Grafana can filter logs/spans by selected flow_name, run_id or run_name.
"""

import logging
from typing import Optional

import prefect
from api.base_telemetry import BaseTelemetry
from api.telemetry_config import TelemetryConfig
from opentelemetry import trace
from opentelemetry._logs import get_logger_provider
from opentelemetry.context.context import Context
from opentelemetry.exporter.otlp.proto.http._log_exporter import (  # Find the OTLP exporter used by BaseTelemetry via existing processors (best effort).
    OTLPLogExporter,
)
from opentelemetry.sdk._logs._internal import LogData
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from pydantic import BaseModel

_LOGGER = logging.getLogger(__name__)


class FlowContext(BaseModel):
    flow_name: str | None


class FlowRunContext(BaseModel):
    run_id: str | None
    run_name: str | None


class TaskRunContext(BaseModel):
    task_id: str | None
    task_name: str | None


def get_flow_context() -> FlowContext:
    """Get flow context from flow run & deployment.

    We don't get the flow id because it's not available from the flow
    run object and I haven't been able to get it working using the
    deployment object.

    Returns current flow definition context if available, else empty dict."""
    try:
        flow_name = getattr(prefect.runtime.flow_run, "flow_name", None)
        if flow_name is None:
            return FlowContext(flow_name=None)

        return FlowContext(flow_name=str(flow_name))
    except Exception:
        return FlowContext(flow_name=None)


def get_run_context() -> FlowRunContext:
    """Get run context from flow run.

    Returns current flow run context if available, else empty dict."""
    try:
        run = prefect.runtime.flow_run
        return FlowRunContext(
            run_id=getattr(run, "id", None),
            run_name=getattr(run, "name", None),
        )
    except Exception:
        return FlowRunContext(run_id=None, run_name=None)


def get_task_context() -> TaskRunContext:
    """Get task context from task run.

    Returns current task run context if available, else empty dict."""
    try:
        task = prefect.runtime.task
        return TaskRunContext(
            task_id=getattr(task, "id", None),
            task_name=getattr(task, "name", None),
        )
    except Exception:
        return TaskRunContext(task_id=None, task_name=None)


class PrefectContextFilter(logging.Filter):
    """Adds Prefect flow/task run context to LogRecord for formatters/handlers."""

    def filter(self, record: logging.LogRecord) -> bool:
        flow_context = get_flow_context()
        setattr(record, "flow_name", flow_context.flow_name)

        run_context = get_run_context()
        setattr(record, "run_id", run_context.run_id)
        setattr(record, "run_name", run_context.run_name)

        task_context = get_task_context()
        setattr(record, "task_id", task_context.task_id)
        setattr(record, "task_name", task_context.task_name)

        return True


class PrefectLogContextProcessor(BatchLogRecordProcessor):
    """
    Enrich OTEL log records with Prefect flow/task run context before export.

    Downstream exporter/collector should promote these attributes to Loki labels:
      - flow_name
      - run_id
      - run_name
      - task_id
      - task_name
    """

    def on_emit(self, log_data: LogData) -> None:
        try:
            record = log_data.log_record

            flow_context = get_flow_context()
            flow_name = flow_context.flow_name
            if flow_name is not None and record.attributes is not None:
                record.attributes["flow_name"] = flow_name  # type: ignore[index]

            run_context = get_run_context()
            run_id = run_context.run_id
            if run_id is not None and record.attributes is not None:
                record.attributes["run_id"] = run_id  # type: ignore[index]

            run_name = run_context.run_name
            if run_name is not None and record.attributes is not None:
                record.attributes["run_name"] = run_name  # type: ignore[index]

            task_context = get_task_context()
            task_id = task_context.task_id
            if task_id is not None and record.attributes is not None:
                record.attributes["task_id"] = task_id  # type: ignore[index]

            task_name = task_context.task_name
            if task_name is not None and record.attributes is not None:
                record.attributes["task_name"] = task_name  # type: ignore[index]

        except Exception as exc:
            # Avoid breaking logging on enrichment failure
            _LOGGER.debug("Failed to enrich log record with Prefect context: %s", exc)
        finally:
            super().on_emit(log_data)


class PrefectSpanContextProcessor(SpanProcessor):
    """Adds Prefect flow/task run context attributes to every span on start."""

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        try:
            flow_context = get_flow_context()
            flow_name = flow_context.flow_name
            if flow_name is not None:
                span.set_attribute("flow_name", flow_name)

            run_context = get_run_context()
            run_id = run_context.run_id
            if run_id is not None:
                span.set_attribute("run_id", run_id)
            run_name = run_context.run_name
            if run_name is not None:
                span.set_attribute("run_name", run_name)

            task_context = get_task_context()
            task_id = task_context.task_id
            if task_id is not None:
                span.set_attribute("task_id", task_id)
            task_name = task_context.task_name
            if task_name is not None:
                span.set_attribute("task_name", task_name)
        except Exception as exc:
            _LOGGER.debug("Failed to enrich span with Prefect context: %s", exc)

    def on_end(self, span: ReadableSpan) -> None:
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
        # by reregistering processors after BaseTelemetry set up.
        #
        # NOTE: BaseTelemetry adds a BatchLogRecordProcessor(OTLPLogExporter).
        # We add an additional processor that enriches then calls the exporter.
        try:
            # Find the OTLP exporter used by BaseTelemetry via the existing logging
            # handler.
            lp = get_logger_provider()

            # If not introspectable, instantiate one more exporter pointing at the same
            # endpoint using config.otlp_endpoint to avoid guesswork.
            endpoint = (
                f"{self.config.otlp_endpoint}/v1/logs"
                if self.config.otlp_endpoint
                else None
            )
            if hasattr(lp, "add_log_record_processor"):
                lp.add_log_record_processor(  # type: ignore[attr-defined]
                    PrefectLogContextProcessor(OTLPLogExporter(endpoint=endpoint))
                )
        except Exception as exc:
            # As a fallback we rely on the logging.Filter to carry context on Python logs.
            # The OTEL log enrichment may be missing if logger provider is not accessible.
            _LOGGER.debug("Failed to add Prefect log context processor: %s", exc)

        # Enrich spans for Prefect runs
        try:
            provider = trace.get_tracer_provider()

            if hasattr(provider, "add_span_processor"):
                provider.add_span_processor(  # type: ignore[attr-defined]
                    PrefectSpanContextProcessor()
                )
        except Exception as exc:
            _LOGGER.debug("Failed to add Prefect span context processor: %s", exc)

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
