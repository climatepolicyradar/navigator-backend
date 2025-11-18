"""
Metrics service for Navigator instrumentation.

This module provides `MetricsService`, a generic facade for creating and
managing OpenTelemetry metrics instruments. It handles MeterProvider setup
and provides factory methods for creating counters and histograms.
"""

import logging
from typing import Optional, Sequence

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

from api.telemetry_config import TelemetryConfig


class MetricsService:
    """Generic facade for instrumenting Navigator services with OpenTelemetry metrics.

    Metrics in OpenTelemetry work as follows:
    - A MeterProvider is the entry point, which comes from the OTel SDK
    - A MeterProvider has many Meters, which create Instruments (idempotently per meter scope)
    - An Instrument reports a Measurement

    The meter scope is set to the service name for consistent grouping.
    """

    def __init__(self, config: TelemetryConfig):
        """Initialize the MetricsService with telemetry configuration.

        :param config: Telemetry configuration values.
        :type config: TelemetryConfig
        """
        self.config = config
        self._disabled = config.disabled

        if self._disabled:
            logger = logging.getLogger(__name__)
            logger.debug("Metrics disabled by configuration.")
            self.meter_provider: Optional[MeterProvider] = None
            self.meter: Optional[Meter] = None
            self.metrics_endpoint: Optional[str] = None
            return

        self._configure_metrics()

    def _configure_metrics(self) -> None:
        """Configure meter provider and OTLP exporter.

        Sets up the MeterProvider with an OTLP exporter that sends metrics
        to the configured endpoint.
        """
        resource = self.config.to_resource()

        metrics_endpoint: Optional[str] = (
            f"{self.config.otlp_endpoint}/v1/metrics"
            if self.config.otlp_endpoint
            else None
        )
        self.metrics_endpoint = metrics_endpoint

        metric_exporter = OTLPMetricExporter(endpoint=metrics_endpoint)
        metric_reader = PeriodicExportingMetricReader(metric_exporter)

        provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader]
        )
        metrics.set_meter_provider(provider)

        self.meter_provider = provider
        self.meter = metrics.get_meter(self.config.service_name)

        logger = logging.getLogger(__name__)
        logger.info("📊 Metrics service initialised.")

    def full_metric_name(self, metric: str) -> str:
        """Return the properly namespaced full metric label.

        Uses cpr_{namespace}_{service}_{component}_{metric_name}

        :param metric: The base metric name.
        :type metric: str
        :return: The fully qualified metric name.
        :rtype: str
        """
        return f"cpr_{self.config.namespace_name}_{self.config.service_name}_{self.config.component_name}_{metric}"

    def create_counter(
        self,
        name: str,
        description: str = "",
        unit: str = "1",
    ) -> Optional[Counter]:
        """Create a counter instrument.

        Counters are used for values that only increase (e.g., request counts).

        :param name: The metric name.
        :type name: str
        :param description: Human-readable description of the metric.
        :type description: str
        :param unit: The unit of measurement (default "1" for counts).
        :type unit: str
        :return: The created counter, or None if metrics are disabled.
        :rtype: Counter | None
        """
        if self._disabled or self.meter is None:
            return None

        return self.meter.create_counter(
            name=self.full_metric_name(name),
            description=description,
            unit=unit,
        )

    def create_histogram(
        self,
        name: str,
        description: str = "",
        unit: str = "s",
        explicit_bucket_boundaries: Optional[Sequence[float]] = None,
    ) -> Optional[Histogram]:
        """Create a histogram instrument.

        Histograms are used for measuring distributions of values (e.g., latencies).

        :param name: The metric name.
        :type name: str
        :param description: Human-readable description of the metric.
        :type description: str
        :param unit: The unit of measurement (default "s" for seconds).
        :type unit: str
        :param explicit_bucket_boundaries: Custom bucket boundaries for the histogram.
        :type explicit_bucket_boundaries: Sequence[float] | None
        :return: The created histogram, or None if metrics are disabled.
        :rtype: Histogram | None
        """
        if self._disabled or self.meter is None:
            return None

        # Note: bucket boundaries are configured via Views in OpenTelemetry Python SDK
        # For now, we use the default boundaries
        return self.meter.create_histogram(
            name=self.full_metric_name(name),
            description=description,
            unit=unit,
        )

    def shutdown(self) -> None:
        """Shutdown the meter provider to flush pending metrics.

        Should be called before application exit to ensure all metrics are exported.
        """
        if self._disabled:
            return

        try:
            if self.meter_provider:
                self.meter_provider.shutdown()
        except Exception as exc:
            logger = logging.getLogger(__name__)
            logger.debug("Failed to shutdown meter provider: %s", exc)
