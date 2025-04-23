import asyncio
import hashlib
import logging
import logging.config
from functools import wraps
from time import perf_counter

# For fastapi auto-instrumentation
from fastapi import FastAPI

## Tracing imports - stable
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# These are beta still, so may change and break compatibility
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

from app.telemetry_config import TelemetryConfig
from app.telemetry_exceptions import (
    add_telemetry_for_exception,
    install_exception_hooks,
)


class Telemetry:
    """
    Primary telemetry class.

    This aims to provide a simple opinionated interface for capturing telemetry data.
    """

    def __init__(self, config: TelemetryConfig):
        """Initialise telemetry! ENGAGE"""
        self.config = config
        self.resource = self.config.to_resource()
        print(f"Telemetry config: {self.resource.__dict__}")

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

    def integrate(self, instrumentor: object):
        """
        Integrate with a specific library, framework, etc.

        :param instrumentor: An object that implements the integration with __call__ method.
        :param details: A dictionary of details for the integration.
        """
        instrumentor(self)

    def get_tracer(self):
        """Returns the otel tracer"""
        return self.tracer

    def _configure_logging(self):
        """Configure logging integration"""
        logger_provider = LoggerProvider(resource=self.resource)
        set_logger_provider(logger_provider)

        log_exporter = BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=f"{self.config.otlp_endpoint}/v1/logs")
        )
        logger_provider.add_log_record_processor(log_exporter)
        self.logger = logging.getLogger(self.config.service_name)
        self.logger.setLevel(self.config.log_level)

        log_handler = LoggingHandler(
            level=self.config.log_level, logger_provider=logger_provider
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
