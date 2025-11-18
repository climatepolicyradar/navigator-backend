"""
Telemetry primitives shared across Navigator services.

This module defines `BaseTelemetry`, the abstraction that bootstraps
OpenTelemetry clients for downstream frameworks. It establishes tracing,
structured logging, and exception enrichment that concrete instrumenters
extend, ensuring each service inherits a consistent baseline configuration.
"""

import logging
import logging.config
import sys
import traceback
from types import TracebackType
from typing import Callable, Optional

from api.telemetry_config import TelemetryConfig

# Tracing imports - stable
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# These are beta still, so may change and break compatibility
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode, Tracer

_LOGGER = logging.getLogger(__name__)

ExceptionHook = Callable[
    [type[BaseException], BaseException, Optional[TracebackType]], None
]


class BaseTelemetry:
    """Shared OpenTelemetry bootstrap for Navigator services.

    :param config: Telemetry configuration values.
    :type config: TelemetryConfig
    """

    def __init__(self, config: TelemetryConfig) -> None:
        self.config = config
        self.resource = self.config.to_resource()
        self._disabled = config.disabled

        if self._disabled:
            _LOGGER.debug("Telemetry disabled by configuration.")
            self.tracer_provider = None
            self.logger_provider = None
            self.tracer = None
            self.logger = _LOGGER
            self.trace_endpoint = None
            self.log_endpoint = None
            return

        self._configure_tracing()
        self._configure_logging()

    def _configure_tracing(self) -> None:
        """Configure tracer provider and OTLP exporter.

        :return: The configured tracer provider.
        :rtype: TracerProvider
        """
        provider = TracerProvider(resource=self.resource)
        trace.set_tracer_provider(provider)

        trace_endpoint: Optional[str] = (
            f"{self.config.otlp_endpoint}/v1/traces"
            if self.config.otlp_endpoint
            else None
        )

        self.trace_endpoint = trace_endpoint

        span_exporter = OTLPSpanExporter(endpoint=trace_endpoint)
        provider.add_span_processor(BatchSpanProcessor(span_exporter))

        self.tracer_provider = provider
        self.tracer = trace.get_tracer(self.config.service_instance_id)

    def _configure_logging(self) -> None:
        """Configure logging providers and attach OTLP handlers.

        :return: Logger configured for the current service.
        :rtype: logging.Logger
        """

        self.logger_provider: Optional[LoggerProvider] = (
            None  # Set in _configure_logging
        )

        # First, we set up the logger provider
        otel_logger_provider = LoggerProvider(resource=self.resource)
        self.logger_provider = otel_logger_provider
        set_logger_provider(otel_logger_provider)

        # Then lets set up the things to process the logs in the provider,
        # ie batch them up and send them to the collector at the right endpoint
        log_endpoint: Optional[str] = (
            f"{self.config.otlp_endpoint}/v1/logs"
            if self.config.otlp_endpoint
            else None
        )
        self.log_endpoint = log_endpoint

        log_exporter = BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=log_endpoint),
        )

        otel_logger_provider.add_log_record_processor(log_exporter)

        # Now our logger provider is setup, we can start hooking into python's logging setup.
        #
        # Note: For Prefect flows, use prefect_logging.yaml to configure Python logging.
        # For FastAPI services, call logging.config.dictConfig(self.config.get_logging_config()).
        # These are separate because Prefect has its own logging handlers/formatters.

        log_level_value = self._resolve_log_level()

        log_handler = LoggingHandler(
            level=log_level_value,
            logger_provider=otel_logger_provider,
        )

        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)

        service_logger = logging.getLogger(self.config.service_name)
        service_logger.setLevel(log_level_value)

        self.logger = service_logger
        self.logger.info("🛰️ Telemetry initialised.")

    def _resolve_log_level(self) -> int:
        """Translate configured log level to ``logging`` constants.

        :return: Numeric level recognised by the ``logging`` module.
        :rtype: int
        :raises ValueError: If the configured level is invalid.
        """
        level = self.config.log_level

        if isinstance(level, int):
            return level

        if isinstance(level, str):
            normalised_level = level.strip().upper()
            resolved_level = logging.getLevelName(normalised_level)

            if isinstance(resolved_level, int):
                return resolved_level

        raise ValueError(f"Invalid log level: {level!r}")

    def setup_exception_hook(self) -> None:
        """Install exception hooks for the runtime.

        :return: The function does not return anything.
        :rtype: None
        """
        self.install_exception_hooks()

    def get_tracer(self) -> Tracer | None:
        """Return the configured tracer.

        :return: OpenTelemetry tracer instance.
        :rtype: Tracer
        """
        return self.tracer

    def get_logger(self) -> logging.Logger:
        """Return the configured service logger.

        :return: Service logger instrumented for OTLP.
        :rtype: logging.Logger
        """
        return self.logger

    def add_telemetry_for_exception(self, exc: BaseException) -> None:
        """Record details about ``exc`` on the current span.

        :param exc: The exception to handle.
        :type exc: BaseException
        """
        span = trace.get_current_span()
        span.record_exception(exc)

    def _enrich_with_exception(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: Optional[TracebackType],
    ) -> None:
        """Enrich the current span with detailed exception information.

        :param exc_type: The type of the captured exception.
        :type exc_type: Type[BaseException]
        :param exc_value: The exception instance encountered.
        :type exc_value: BaseException
        :param exc_traceback: The traceback linked to the exception.
        :type exc_traceback: TracebackType | None
        :return: The function does not return anything.
        :rtype: None
        """
        span: Span = trace.get_current_span()
        stacktrace = "".join(
            traceback.format_exception(exc_type, exc_value, exc_traceback)
        )

        if span and span.is_recording():
            span.set_status(Status(StatusCode.ERROR))
            span.add_event(
                name="exception",
                attributes={
                    "exception.type": exc_type.__name__,
                    "exception.message": str(exc_value),
                    "exception.stacktrace": stacktrace,
                },
            )
            return

        with trace.start_as_current_span("exception_without_span") as new_span:  # type: ignore
            new_span.set_status(Status(StatusCode.ERROR))
            new_span.add_event(
                name="exception",
                attributes={
                    "exception.type": exc_type.__name__,
                    "exception.message": str(exc_value),
                    "exception.stacktrace": stacktrace,
                },
            )

    def _make_exception_hook(
        self,
        previous_hook: Optional[ExceptionHook],
    ) -> ExceptionHook:
        """Create a synchronous exception hook that enriches spans.

        :param previous_hook: Previously registered exception hook to chain.
        :type previous_hook: ExceptionHook | None
        :return: Exception hook that records telemetry and chains hooks.
        :rtype: ExceptionHook
        """

        def catch_exception(
            exc_type: type[BaseException],
            exc_value: BaseException,
            exc_traceback: Optional[TracebackType],
        ) -> None:
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return

            self._enrich_with_exception(exc_type, exc_value, exc_traceback)

            if previous_hook:
                previous_hook(exc_type, exc_value, exc_traceback)
            return

        return catch_exception

    def install_exception_hooks(
        self,
        custom_excepthook: Optional[ExceptionHook] = None,
    ) -> None:
        """Install universal exception hooks for synchronous exceptions.

        :param custom_excepthook: Hook invoked for unhandled exceptions prior to the
            default chain being executed.
        :type custom_excepthook: ExceptionHook | None
        :return: The function does not return anything.
        :rtype: None
        """
        previous_hook = sys.excepthook
        sys.excepthook = custom_excepthook or self._make_exception_hook(previous_hook)

    def shutdown(self) -> None:
        """Shutdown telemetry providers to prevent export errors on exit.

        :return: The function does not return anything.
        :rtype: None
        """
        if self._disabled:
            return

        try:
            if self.logger_provider:
                self.logger_provider.shutdown()  # type: ignore[attr-defined]
        except Exception as exc:
            _LOGGER.debug("Failed to shutdown logger provider: %s", exc)

        try:
            if self.tracer_provider:
                self.tracer_provider.shutdown()
        except Exception as exc:
            _LOGGER.debug("Failed to shutdown tracer provider: %s", exc)

        # Also shutdown global providers in case Prefect or other services
        # initialised them. This is needed as otherwise tests will try to
        # export logs and traces to our OTLP endpoint, which will fail.
        try:
            from opentelemetry._logs import get_logger_provider

            global_logger_provider = get_logger_provider()
            if (
                global_logger_provider
                and global_logger_provider != self.logger_provider
            ):
                global_logger_provider.shutdown()  # type: ignore[attr-defined]
        except Exception as exc:
            _LOGGER.debug("Failed to shutdown global logger provider: %s", exc)

        try:
            global_tracer_provider = trace.get_tracer_provider()
            if (
                global_tracer_provider
                and global_tracer_provider != self.tracer_provider
                and hasattr(global_tracer_provider, "shutdown")
            ):
                global_tracer_provider.shutdown()  # type: ignore[attr-defined]
        except Exception as exc:
            _LOGGER.debug("Failed to shutdown global tracer provider: %s", exc)
