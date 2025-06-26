import functools
import logging
import logging.config
from contextlib import nullcontext
from typing import Callable

from app.telemetry_config import TelemetryConfig
from app.telemetry_exceptions import install_exception_hooks

# For fastapi auto-instrumentation
from fastapi import FastAPI

## Tracing imports - stable
from opentelemetry import trace

# GOTCHA: we trunk-ignore these imports as these libraries are still in RC - but we need them
# trunk-ignore(pyright-backend-api/reportPrivateImportUsage)
from opentelemetry._logs import set_logger_provider

# trunk-ignore(pyright-backend-api/reportPrivateImportUsage)
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# These are beta still, so may change and break compatibility
# trunk-ignore(pyright-backend-api/reportPrivateImportUsage)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

# trunk-ignore(pyright-backend-api/reportPrivateImportUsage)
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import NonRecordingSpan


def convert_to_loggable_string(obj):
    """OpenTelemetry enforces strict conventions on what can be logged as additional structured data,
    allowing only one of ['bool', 'str', 'bytes', 'int', 'float']. This function converts
    arbitrary objects to a string if possible.
    """
    if isinstance(obj, (bool, int, float, str, bytes)):
        return obj
    elif isinstance(obj, dict):
        return_str = ""
        for key, value in obj.items():
            return_str += f"{key}: {convert_to_loggable_string(value)} \n"
        return return_str
    elif isinstance(obj, list):
        return_str = ""
        for item in obj:
            return_str += f"{convert_to_loggable_string(item)} \n"
        return return_str
    else:
        return str(obj)


class Telemetry:
    """
    Primary telemetry class.

    This aims to provide a simple opinionated interface for capturing telemetry data.
    """

    def __init__(self, config: TelemetryConfig):
        """Initialise telemetry! ENGAGE"""
        self.config = config
        self.resource = self.config.to_resource()
        print(f"Telemetry config: {str(self.config)}")

        self.tracer_provider = TracerProvider(resource=self.resource)
        trace.set_tracer_provider(self.tracer_provider)

        otlp_exporter = OTLPSpanExporter(
            endpoint=f"{self.config.otlp_endpoint}/v1/traces"
        )

        self.tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

        self.tracer = trace.get_tracer(self.config.service_instance_id)

        self._configure_logging()
        self.get_logger().info("Telemetry initialized")

    def setup_exception_hook(self):
        """
        Setup the exception hook

        Last called wins so call this as the last thing in your main
        """
        install_exception_hooks()

    def get_tracer(self):
        """Returns the otel tracer"""
        return self.tracer

    def _configure_logging(self):
        """Configure logging integration"""
        logger_provider = LoggerProvider(resource=self.resource)
        set_logger_provider(logger_provider)

        log_exporter = BatchLogRecordProcessor(
            OTLPLogExporter(
                endpoint=(
                    f"{self.config.otlp_endpoint}/v1/logs"
                    if self.config.otlp_endpoint
                    else None
                )
            )
        )
        logger_provider.add_log_record_processor(log_exporter)
        self.logger = logging.getLogger(self.config.service_name)
        self.logger.setLevel(self.config.log_level)

        log_handler = LoggingHandler(
            level=self.config.log_level, logger_provider=logger_provider  # type: ignore
        )

        # We'll attach the OTLP handler to the root logger
        # Then we'll provide a custom namespaced logger for the service
        # This will help separating application-layer logs from other telemetry
        logging.config.dictConfig(self.config.get_logging_config())
        logging.getLogger().addHandler(log_handler)
        self.logger = logging.getLogger(self.config.service_name)
        self.logger.setLevel(self.config.log_level)

    def get_logger(self) -> logging.Logger:
        """Returns the telemetry-configured logger for the service"""
        return self.logger

    def instrument_fastapi(self, app: FastAPI):
        FastAPIInstrumentor.instrument_app(
            app, tracer_provider=self.tracer_provider, excluded_urls="/health"
        )
        app.state.telemetry = self


def observe(name: str) -> Callable:
    """Decorator to wrap a function in an OTel span."""

    def decorator(func: Callable):
        @functools.wraps(func)
        def wraps(*args, **kwargs):
            if isinstance(trace.get_current_span(), NonRecordingSpan):
                span = nullcontext()
            else:
                span = trace.get_tracer(func.__module__).start_as_current_span(name)

            with span:
                return func(*args, **kwargs)

        return wraps

    return decorator
