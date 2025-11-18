"""
Pipeline-specific metrics for the data-in-pipeline service.

Defines labels for metrics, and PipelineMetrics which uses MetricsService
to define and manage pipeline-specific metrics like document processing counts,
error tracking, and operation durations.
"""

import functools
import time
from enum import StrEnum
from typing import Callable, Optional, TypeVar

from opentelemetry.metrics import Counter, Histogram

from api import MetricsService

F = TypeVar("F", bound=Callable)


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
