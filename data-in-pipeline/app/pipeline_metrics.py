"""
Pipeline-specific metrics for the data-in-pipeline service.

Defines labels for metrics, and PipelineMetrics which uses MetricsService
to define and manage pipeline-specific metrics like document processing counts,
error tracking, and operation durations.
"""

import functools
import os
import time
from enum import StrEnum
from typing import Callable, Optional, TypeVar

from api import MetricsService
from opentelemetry.metrics import Counter, Gauge, Histogram

F = TypeVar("F", bound=Callable)

# Default ECS Fargate allocation values (can be overridden via env vars)
DEFAULT_CPU_ALLOCATION_UNITS = 1024  # 1 vCPU
DEFAULT_MEMORY_ALLOCATION_MB = 2048  # 2 GB


class PipelineType(StrEnum):
    """Types of pipelines that process data."""

    DOCUMENT = "document"
    FAMILY = "family"


class Status(StrEnum):
    """Processing status outcomes."""

    SUCCESS = "success"
    FAILURE = "failure"


class Operation(StrEnum):
    """Pipeline operations/stages."""

    EXTRACT = "extract"
    IDENTIFY = "identify"
    TRANSFORM = "transform"
    LOAD = "load"
    PAGINATION = "pagination"


class ErrorType(StrEnum):
    """Categories of errors that can occur during pipeline operations."""

    NETWORK = "network"
    VALIDATION = "validation"
    TRANSFORM = "transform"
    STORAGE = "storage"
    UNKNOWN = "unknown"


class PipelineMetrics:
    """Pipeline-specific metrics using the MetricsService.

    Provides helper methods for recording:
    - Document processing counts by pipeline type and status
    - Error counts by operation and error type
    - Operation durations
    """

    def __init__(self, metrics_service: MetricsService):
        """Initialize pipeline metrics.

        :param metrics_service: The generic metrics service to use for creating instruments.
        :type metrics_service: MetricsService
        """
        self._metrics_service = metrics_service
        self._disabled = metrics_service._disabled

        # Base attributes included in all metrics
        self._base_attributes = {
            "environment": metrics_service.config.environment,
            "service": metrics_service.config.service_name,
        }

        # Initialize instruments
        self._documents_processed: Optional[Counter] = None
        self._document_errors: Optional[Counter] = None
        self._operation_duration: Optional[Histogram] = None

        # Cost component instruments
        self._cpu_allocation: Optional[Gauge] = None
        self._memory_allocation: Optional[Gauge] = None

        # Resource utilization instruments
        self._memory_usage: Optional[Histogram] = None
        self._memory_utilization: Optional[Histogram] = None
        self._cpu_utilization: Optional[Histogram] = None

        # Allocation values (from env vars or defaults)
        self._cpu_allocation_units = int(
            os.getenv("ECS_CPU_ALLOCATION", DEFAULT_CPU_ALLOCATION_UNITS)
        )
        self._memory_allocation_mb = int(
            os.getenv("ECS_MEMORY_ALLOCATION", DEFAULT_MEMORY_ALLOCATION_MB)
        )

        if not self._disabled:
            self._create_instruments()

    def _create_instruments(self) -> None:
        """Create the pipeline-specific metric instruments."""
        from app.bootstrap_telemetry import get_logger

        logger = get_logger()

        self._documents_processed = self._metrics_service.create_counter(
            name="documents_processed_total",
            description="Total number of documents processed by the pipeline",
            unit="1",
        )

        self._document_errors = self._metrics_service.create_counter(
            name="document_errors_total",
            description="Total number of errors encountered during document processing",
            unit="1",
        )

        self._operation_duration = self._metrics_service.create_histogram(
            name="pipeline_operation_duration_seconds",
            description="Duration of pipeline operations in seconds",
            unit="s",
        )

        # Cost component gauges
        self._cpu_allocation = (
            self._metrics_service.meter.create_gauge(
                name=self._metrics_service.full_metric_name(
                    "task_cpu_allocation_units"
                ),
                description="CPU units allocated to the task (1024 = 1 vCPU)",
                unit="1",
            )
            if self._metrics_service.meter
            else None
        )

        self._memory_allocation = (
            self._metrics_service.meter.create_gauge(
                name=self._metrics_service.full_metric_name(
                    "task_memory_allocation_mb"
                ),
                description="Memory allocated to the task in MB",
                unit="MiB",
            )
            if self._metrics_service.meter
            else None
        )

        # Resource utilization histograms
        self._memory_usage = self._metrics_service.create_histogram(
            name="task_memory_usage_bytes",
            description="Actual memory used by the task in bytes",
            unit="By",
        )

        self._memory_utilization = self._metrics_service.create_histogram(
            name="task_memory_utilization_ratio",
            description="Memory usage as ratio of allocated (0.0-1.0)",
            unit="1",
        )

        self._cpu_utilization = self._metrics_service.create_histogram(
            name="task_cpu_utilization_percent",
            description="CPU utilization percentage during task execution",
            unit="%",
        )

        # Record initial allocation values
        self._record_allocation()

        logger.info("Pipeline metrics instruments created")

    def record_processed(self, pipeline_type: PipelineType, status: Status) -> None:
        """Record a document processing event.

        :param pipeline_type: The type of pipeline (document or family).
        :type pipeline_type: PipelineType
        :param status: The processing status (success or failure).
        :type status: Status
        """
        if self._disabled or self._documents_processed is None:
            return

        self._documents_processed.add(
            1,
            attributes={
                **self._base_attributes,
                "pipeline_type": pipeline_type.value,
                "status": status.value,
            },
        )

    def record_error(self, operation: Operation, error_type: ErrorType) -> None:
        """Record an error event.

        :param operation: The pipeline operation where the error occurred.
        :type operation: Operation
        :param error_type: The category of error.
        :type error_type: ErrorType
        """
        if self._disabled or self._document_errors is None:
            return

        self._document_errors.add(
            1,
            attributes={
                **self._base_attributes,
                "operation": operation.value,
                "error_type": error_type.value,
            },
        )

    def record_duration(self, operation: Operation, duration_seconds: float) -> None:
        """Record the duration of a pipeline operation.

        :param operation: The pipeline operation that was timed.
        :type operation: Operation
        :param duration_seconds: The duration in seconds.
        :type duration_seconds: float
        """
        if self._disabled or self._operation_duration is None:
            return

        self._operation_duration.record(
            duration_seconds,
            attributes={
                **self._base_attributes,
                "operation": operation.value,
            },
        )

    def timed_operation(self, operation: Operation) -> Callable[[F], F]:
        """Decorator to time a function and record its duration.

        :param operation: The pipeline operation being timed.
        :type operation: Operation
        :return: A decorator that wraps the function with timing.
        :rtype: Callable[[F], F]
        """

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                try:
                    return func(*args, **kwargs)
                finally:
                    self.record_duration(operation, time.time() - start)

            return wrapper  # type: ignore

        return decorator

    def _record_allocation(self) -> None:
        """Record the current allocation values as gauges."""
        if self._disabled:
            return

        if self._cpu_allocation is not None:
            self._cpu_allocation.set(
                self._cpu_allocation_units,
                attributes=self._base_attributes,
            )

        if self._memory_allocation is not None:
            self._memory_allocation.set(
                self._memory_allocation_mb,
                attributes=self._base_attributes,
            )

    def record_resource_usage(
        self,
        operation: Operation,
        memory_usage_bytes: int,
        memory_utilization_ratio: float,
        cpu_percent: float,
    ) -> None:
        """Record resource utilization metrics for an operation.

        :param operation: The pipeline operation that was measured.
        :type operation: Operation
        :param memory_usage_bytes: Actual memory used in bytes.
        :type memory_usage_bytes: int
        :param memory_utilization_ratio: Memory used as ratio of allocated (0.0-1.0).
        :type memory_utilization_ratio: float
        :param cpu_percent: CPU utilization percentage.
        :type cpu_percent: float
        """
        if self._disabled:
            return

        attributes = {
            **self._base_attributes,
            "operation": operation.value,
        }

        if self._memory_usage is not None:
            self._memory_usage.record(memory_usage_bytes, attributes=attributes)

        if self._memory_utilization is not None:
            self._memory_utilization.record(
                memory_utilization_ratio, attributes=attributes
            )

        if self._cpu_utilization is not None:
            self._cpu_utilization.record(cpu_percent, attributes=attributes)

    def tracked_operation(self, operation: Operation) -> Callable[[F], F]:
        """Decorator to time a function and record resource usage.

        Combines timing and resource utilization tracking. Records:
        - Operation duration
        - Memory usage and utilization
        - CPU utilization

        :param operation: The pipeline operation being tracked.
        :type operation: Operation
        :return: A decorator that wraps the function with tracking.
        :rtype: Callable[[F], F]
        """
        from app.ecs_metadata import get_resource_usage

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()

                # Sample resource usage at start
                start_usage = get_resource_usage()

                try:
                    return func(*args, **kwargs)
                finally:
                    # Record duration
                    duration = time.time() - start
                    self.record_duration(operation, duration)

                    # Sample resource usage at end and record
                    end_usage = get_resource_usage()

                    # Use end usage if available, otherwise try start usage
                    usage = end_usage or start_usage
                    if usage is not None:
                        self.record_resource_usage(
                            operation=operation,
                            memory_usage_bytes=usage.memory_usage_bytes,
                            memory_utilization_ratio=usage.memory_utilization_ratio,
                            cpu_percent=usage.cpu_percent,
                        )

            return wrapper  # type: ignore

        return decorator
