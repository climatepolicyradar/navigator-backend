"""
Pipeline-specific metrics for the data-in-pipeline service.

Defines labels for metrics, and PipelineMetrics which uses MetricsService
to define and manage pipeline-specific metrics like document processing counts,
error tracking, and operation durations.
"""

import datetime
import functools
import os
import resource
import time
from enum import StrEnum
from typing import Callable, Literal, Optional, TypeVar

import requests
from api import MetricsService
from opentelemetry.metrics import Counter, Histogram

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


def get_logs_url_for_run(run_id: str) -> str:
    """Get the logs URL for a run.

    It just builds up from a copied and pasted grafana URL so it's a little messy.
    """
    base_url = "https://climatepolicyradar.grafana.net/a/grafana-lokiexplore-app/explore/service/data-in-pipeline/logs"

    from_time = datetime.datetime.now() - datetime.timedelta(minutes=15)
    to_time = datetime.datetime.now() + datetime.timedelta(hours=6)

    # Convert to Unix timestamps in milliseconds (Grafana's expected format)
    from_ts = int(from_time.timestamp() * 1000)
    to_ts = int(to_time.timestamp() * 1000)

    time_string = f"&from={from_ts}&to={to_ts}"

    preset_query_vars = "&var-lineFormat=&var-ds=grafanacloud-logs&var-filters=service_name%7C%3D%7Cdata-in-pipeline&var-fields=&var-levels="

    query = f"patterns=%5B%5D{time_string}{preset_query_vars}&var-metadata=flow_run_name%7C%3D%7C{run_id}&var-jsonFields=&var-patterns=&var-lineFilterV2=&var-lineFilters=&timezone=browser&var-all-fields=flow_run_name%7C%3D%7C{run_id}&displayedFields=%5B%5D&urlColumns=%5B%5D&visualizationType=%22logs%22&prettifyLogMessage=false&sortOrder=%22Ascending%22&wrapLogMessage=true"
    return f"{base_url}?{query}"


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

        # These get included in all metrics. So be SUPER careful
        # because we don't want to explode cardinality of metrics
        # that we track.
        self._base_attributes = {"environment": metrics_service.config.environment}

        # Operational health metrics
        self._documents_processed: Optional[Counter] = None
        self._document_errors: Optional[Counter] = None
        self._operation_duration: Optional[Histogram] = None
        self._pipeline_run_duration: Optional[Histogram] = None
        self._memory_allocated: Optional[Counter] = None
        self._cpu_allocated: Optional[Counter] = None
        self._billable_cost_of_run: Optional[Counter] = None
        self._memory_actual: Optional[Counter] = None
        self._cpu_actual: Optional[Counter] = None
        self._cost_of_used_resources: Optional[Counter] = None

        if not self._disabled:
            self._create_instruments()

    def set_flow_run_name(self, flow_run_name: str) -> None:
        """Set the flow_run_name attribute for all metrics.

        Call at the start of each flow run to tag all metrics with the run identifier.
        This ensures counters and histograms are scoped to the flow run lifetime,
        enabling proper aggregation with max_over_time across runs.

        :param flow_run_name: The Prefect flow run name (e.g., "maroon-swan").
        :type flow_run_name: str
        """
        self._base_attributes["flow_run_name"] = flow_run_name

    def _create_instruments(self) -> None:
        """Create the data-in-pipeline-specific metric instruments."""
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

        self._pipeline_run_duration = self._metrics_service.create_histogram(
            name="pipeline_run_duration_seconds",
            description="End-to-end duration of pipeline runs",
            unit="s",
        )

        self._memory_allocated = self._metrics_service.create_counter(
            name="allocated_memory_megabytes",
            description="Allocated memory in megabytes",
            unit="mb",
        )

        self._cpu_allocated = self._metrics_service.create_counter(
            name="allocated_cpu_vcpus",
            description="Allocated CPU vCPUs",
            unit="vCPUs",
        )

        self._billable_cost_of_run = self._metrics_service.create_counter(
            name="billable_cost_of_run_dollars",
            description="Billable cost of run in dollars",
            unit="$",
        )

        self._memory_actual = self._metrics_service.create_counter(
            name="actual_memory_megabytes",
            description="Actual memory in megabytes",
            unit="mb",
        )

        self._cpu_actual = self._metrics_service.create_counter(
            name="actual_cpu_vcpus",
            description="Actual CPU vCPUs",
            unit="vCPUs",
        )

        self._cost_of_used_resources = self._metrics_service.create_counter(
            name="cost_of_used_resources_dollars",
            description="Cost of used resources in dollars",
            unit="$",
        )

        logger.info("Pipeline metrics instruments created")

    @functools.cache
    def _get_allocated_task_resources(self) -> tuple[float, float]:
        """Get the CPU and memory allocated by the pipeline task.

        :return: A dictionary with the CPU and memory allocated by the
            pipeline task.
        :raises RuntimeError: If the metadata env var is unavailable.
        """

        metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        if not metadata_uri:
            raise RuntimeError("Not running in ECS or metadata endpoint unavailable")

        # Get task level metadata
        timeout_secs = 2
        response = requests.get(f"{metadata_uri}/task", timeout=timeout_secs)
        response.raise_for_status()
        task_metadata = response.json()

        # Task-level limits
        cpu = float(
            task_metadata.get("Limits", {}).get("CPU")
        )  # vCPU units (1024 units = 1 vCPU)
        memory = float(task_metadata.get("Limits", {}).get("Memory"))  # Memory in MB
        return (cpu, memory)

    def get_actual_task_resources(self, start_wall: float) -> tuple[float, float]:
        """Get the actual CPU and memory used by the pipeline task.

        NB: We measure average CPU rather than peak CPU for now, because
        to get peak CPU would require regular sampling (or external
        measurement eg by container insights).
        To make this more accurate for right-sizing, average CPU is
        preferred; peak CPU should more be interpreted as the upper
        bound on CPU requirements.

        :param start_wall: The wall time at the start of the task.
        :type start_wall: float
        :return: A tuple with the average CPU and peak memory.
        :rtype: tuple[float, float]
        """
        # Get memory
        peak_memory = resource.getrusage(resource.RUSAGE_SELF)
        bytes_to_megabytes = 1024
        peak_memory_mb = peak_memory.ru_maxrss / bytes_to_megabytes

        # Get average vCPUs used (instead of peak vCPUs - see docstring)
        wall_time = time.time() - start_wall
        cpu_time = peak_memory.ru_utime + peak_memory.ru_stime
        avg_vcpus = cpu_time / wall_time  # average vCPUs/cores used

        return (avg_vcpus, peak_memory_mb)

    def measure_resource_allocations(self) -> tuple[float, float] | None:
        """Measure the resource allocations of the pipeline.

        :return: A tuple with the average CPU and peak memory.
        :rtype: tuple[float, float]
        """
        if (
            self._disabled
            or self._memory_allocated is None
            or self._cpu_allocated is None
        ):
            return

        cpu, memory = self._get_allocated_task_resources()

        # Get the CPU and memory allocated by the pipeline flow from the environment
        # variables AWS inject into ECS tasks.
        self._memory_allocated.add(
            memory,
            attributes={
                **self._base_attributes,
            },
        )

        self._cpu_allocated.add(
            cpu,
            attributes={
                **self._base_attributes,
            },
        )

        return (cpu, memory)

    def measure_resource_usage(self, start_wall: float) -> tuple[float, float] | None:
        """Measure the resource usage of the pipeline.

        :param start_wall: The wall time at the start of the task.
        :type start_wall: float
        :return: A tuple with the average CPU and peak memory.
        :rtype: tuple[float, float]
        """
        if self._disabled or self._memory_actual is None or self._cpu_actual is None:
            return

        cpu, memory = self.get_actual_task_resources(start_wall)

        self._memory_actual.add(
            memory,
            attributes={
                **self._base_attributes,
            },
        )

        self._cpu_actual.add(
            cpu,
            attributes={
                **self._base_attributes,
            },
        )

        return (cpu, memory)

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

    def record_operation_duration(
        self, operation: Operation, duration_seconds: float
    ) -> None:
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

    def record_run_duration(
        self,
        pipeline_type: PipelineType,
        duration_seconds: float,
        scope: str = "batch",
    ) -> None:
        """Record the end-to-end duration of a pipeline run.

        :param pipeline_type: The type of pipeline (document or family).
        :type pipeline_type: PipelineType
        :param duration_seconds: The duration in seconds.
        :type duration_seconds: float
        :param scope: The scope of measurement - "batch" for entire flow, "document" for single item.
        :type scope: str
        """
        if self._disabled or self._pipeline_run_duration is None:
            return

        self._pipeline_run_duration.record(
            duration_seconds,
            attributes={
                **self._base_attributes,
                "pipeline_type": pipeline_type.value,
                "scope": scope,
            },
        )

    def record_billable_cost(
        self,
        pipeline_type: PipelineType,
        duration_seconds: float,
        cpu: float,
        memory: float,
        fargate_cost_per_vcpu_hour: float = 0.04048,
        fargate_cost_per_mem_gb_hour: float = 0.004445,
    ) -> None:
        """Record the billable cost of a pipeline run based on allocated resources.

        In Fargate, the cost is calculated based on the allocated number
        of vCPUs and memory rather than what is actually used. This is
        why we have decided to try and separately track actual usage -
        so we can get insights and see whether we are rightsizing
        resources or not, and potentially predict costs in advance.
        To get an accurate cost, we should multiply each of measurements
        by the total duration (which includes the provisioning time), as
        this is what Fargate uses. However, we are unable to do this for
        now, so we are accepting the duration as the execution time
        (time taken to run the Prefect tasks excluding provisioning time)
        as the difference will be negligible, especially when running on
        large inputs in production. To measure inclusive of provisioning time
        use the spanmetrics processor on the otel collector to convert trace durations
        from flows into metrics.

        :param pipeline_type: The type of pipeline (document or family).
        :type pipeline_type: PipelineType
        :param duration_seconds: The duration in seconds.
        :type duration_seconds: float
        :param cpu: The allocated vCPUs.
        :type cpu: float
        :param memory: The allocated memory in megabytes.
        :type memory: float
        :param fargate_cost_per_vcpu_hour: The cost per vCPU hour in dollars.
        :type fargate_cost_per_vcpu_hour: float
        :param fargate_cost_per_mem_gb_hour: The cost per memory GB hour in dollars.
        :type fargate_cost_per_mem_gb_hour: float
        """
        if self._disabled or self._billable_cost_of_run is None:
            return

        megabytes_to_gigabytes = 1024
        memory_cost = (
            duration_seconds
            * (memory / megabytes_to_gigabytes)
            * fargate_cost_per_mem_gb_hour
        )
        cpu_cost = duration_seconds * cpu * fargate_cost_per_vcpu_hour
        total_cost = memory_cost + cpu_cost

        self._billable_cost_of_run.add(
            total_cost,
            attributes={
                **self._base_attributes,
                "pipeline_type": pipeline_type.value,
            },
        )

    def record_run_cost(
        self,
        pipeline_type: PipelineType,
        duration_seconds: float,
        cpu: float,
        memory: float,
        fargate_cost_per_vcpu_hour: float = 0.04048,
        fargate_cost_per_mem_gb_hour: float = 0.004445,
    ) -> None:
        """Record the hypothetical cost of used resources for a given run.

        In Fargate, the cost is calculated based on the allocated number
        of vCPUs and memory rather than what is actually used. This
        function attempts to separately track actual usage, so we can
        get insights and see whether we are rightsizing
        resources or not, and potentially predict costs in advance.

        :param pipeline_type: The type of pipeline (document or family).
        :type pipeline_type: PipelineType
        :param duration_seconds: The duration in seconds.
        :type duration_seconds: float
        :param cpu: The average vCPUs used by the run.
        :type cpu: float
        :param memory: The peak memory in megabytes used by the run.
        :type memory: float
        :param fargate_cost_per_vcpu_hour: The cost per vCPU hour in dollars.
        :type fargate_cost_per_vcpu_hour: float
        :param fargate_cost_per_mem_gb_hour: The cost per memory GB hour in dollars.
        :type fargate_cost_per_mem_gb_hour: float
        """
        if self._disabled or self._cost_of_used_resources is None:
            return

        megabytes_to_gigabytes = 1024
        memory_cost = (
            duration_seconds
            * (memory / megabytes_to_gigabytes)
            * fargate_cost_per_mem_gb_hour
        )
        cpu_cost = duration_seconds * cpu * fargate_cost_per_vcpu_hour
        total_cost = memory_cost + cpu_cost

        self._cost_of_used_resources.add(
            total_cost,
            attributes={
                **self._base_attributes,
                "pipeline_type": pipeline_type.value,
            },
        )

    def track(
        self,
        operation: Optional[Operation] = None,
        pipeline_type: Optional[PipelineType] = None,
        scope: Literal["operation", "document", "batch"] = "operation",
        flush_on_exit: bool = False,
    ) -> Callable[[F], F]:
        """Unified decorator for timing and metrics tracking.

        Records timing metrics and optionally sets log context for the decorated function.

        :param operation: If provided, records operation duration and sets
                         log_context(pipeline_stage=operation.value) for log enrichment.
        :type operation: Optional[Operation]
        :param pipeline_type: If provided, records run duration metric.
        :type pipeline_type: Optional[PipelineType]
        :param scope: The scope of measurement - "operation" for stage timing,
                     "document" or "batch" for e2e timing.
        :type scope: Literal["operation", "document", "batch"]
        :param flush_on_exit: If True, flushes metrics after completion (use for flows).
        :type flush_on_exit: bool
        :return: A decorator that wraps the function with timing.
        :rtype: Callable[[F], F]

        Usage examples:
            # Simple operation timing (replaces @timed_operation)
            @pipeline_metrics.track(operation=Operation.EXTRACT)
            def extract(): ...

            # Operation timing with log context (replaces @tracked_operation)
            @pipeline_metrics.track(operation=Operation.TRANSFORM)
            def transform(): ...

            # Per-document e2e timing (replaces @tracked_document)
            @pipeline_metrics.track(pipeline_type=PipelineType.DOCUMENT, scope="document")
            def etl_pipeline(doc_id): ...

            # Batch-level timing with flush (replaces @tracked_flow)
            @flow
            @pipeline_metrics.track(pipeline_type=PipelineType.FAMILY, scope="batch", flush_on_exit=True)
            def process_batch(): ...
        """
        from app.log_context import log_context

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Set up log context if operation is provided
                context_mgr = (
                    log_context(pipeline_stage=operation.value) if operation else None
                )

                start = time.time()
                try:
                    if context_mgr:
                        with context_mgr:
                            return func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                finally:
                    duration = time.time() - start

                    # Record operation duration if operation provided
                    if operation:
                        self.record_operation_duration(operation, duration)

                    # Record run duration and approximate cost if pipeline_type provided
                    if pipeline_type:
                        self.record_run_duration(pipeline_type, duration, scope)

                        allocated_resources = self.measure_resource_allocations()
                        if allocated_resources:
                            allocated_cpu, allocated_memory = allocated_resources
                            self.record_billable_cost(
                                pipeline_type, duration, allocated_cpu, allocated_memory
                            )

                        actual_resources = self.measure_resource_usage(start)
                        if actual_resources:
                            actual_cpu, actual_memory = actual_resources
                            self.record_run_cost(
                                pipeline_type, duration, actual_cpu, actual_memory
                            )

                    # Flush metrics if requested
                    if flush_on_exit:
                        self.save_metrics()

            return wrapper  # type: ignore

        return decorator

    def log_run_info(
        self, pipeline_type: PipelineType, item_count: int, run_id: str
    ) -> None:
        """Log structured run info for dashboard parsing.

        Emits a log with marker 'PIPELINE_RUN_INFO' and structured fields
        as OTEL attributes for reliable LogQL extraction.

        :param pipeline_type: The type of pipeline (document or family).
        :param item_count: Number of items to process.
        """
        from app.bootstrap_telemetry import get_logger
        from app.log_context import log_context

        with log_context(
            pipeline_type=pipeline_type.value,
            item_count=item_count,
            environment=self._base_attributes.get("environment", "unknown"),
            service_version=os.getenv("SERVICE_VERSION", "unknown"),
        ):
            get_logger().info(
                f"You can view logs for this run at {get_logs_url_for_run(run_id)}"
            )
            get_logger().info("PIPELINE_RUN_INFO")

    def save_metrics(self, timeout_ms: int = 10000) -> bool:
        """Force flush and ensure all metrics are exported.

        Call this before process exit to guarantee metrics delivery.

        :param timeout_ms: Maximum time to wait for flush in milliseconds.
        :type timeout_ms: int
        :return: True if successful, False otherwise.
        :rtype: bool
        """
        if self._disabled:
            return True

        try:
            success = self._metrics_service.force_flush(timeout_ms)
            return success
        except Exception as exc:
            from app.bootstrap_telemetry import get_logger

            get_logger().warning(f"Failed to flush metrics: {exc}")
            return False
