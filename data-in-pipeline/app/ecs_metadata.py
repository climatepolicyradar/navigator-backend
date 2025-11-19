"""
ECS Task Metadata client for retrieving container resource usage.

Fetches CPU and memory metrics from the ECS Task Metadata Endpoint v4.
Falls back to psutil for local development environments.
"""

import os
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class ResourceUsage:
    """Container resource usage snapshot."""

    memory_usage_bytes: int
    memory_limit_bytes: int
    cpu_percent: float

    @property
    def memory_utilization_ratio(self) -> float:
        """Memory usage as a ratio of the limit (0.0-1.0)."""
        if self.memory_limit_bytes == 0:
            return 0.0
        return self.memory_usage_bytes / self.memory_limit_bytes


class ECSMetadataClient:
    """Client for fetching resource metrics from ECS Task Metadata Endpoint v4.

    In ECS Fargate, the container metadata endpoint provides Docker stats
    including CPU and memory usage. For local development, falls back to psutil.
    """

    def __init__(self):
        """Initialize the metadata client."""
        self._metadata_uri = os.getenv("ECS_CONTAINER_METADATA_URI_V4")
        self._is_ecs = self._metadata_uri is not None

    @property
    def is_ecs_environment(self) -> bool:
        """Check if running in ECS environment."""
        return self._is_ecs

    def get_resource_usage(self) -> Optional[ResourceUsage]:
        """Get current container resource usage.

        :return: Resource usage snapshot, or None if unavailable.
        :rtype: ResourceUsage | None
        """
        if self._is_ecs:
            return self._get_ecs_stats()
        return self._get_local_stats()

    def _get_ecs_stats(self) -> Optional[ResourceUsage]:
        """Fetch stats from ECS Task Metadata Endpoint v4.

        The endpoint returns Docker stats format with cpu_stats and memory_stats.

        :return: Resource usage from ECS metadata, or None on error.
        :rtype: ResourceUsage | None
        """
        try:
            # Get container stats from the metadata endpoint
            stats_url = f"{self._metadata_uri}/stats"
            response = requests.get(stats_url, timeout=2)
            response.raise_for_status()
            stats = response.json()

            # Parse memory stats
            memory_stats = stats.get("memory_stats", {})
            memory_usage = memory_stats.get("usage", 0)
            memory_limit = memory_stats.get("limit", 0)

            # Parse CPU stats and calculate percentage
            cpu_percent = self._calculate_cpu_percent(stats)

            return ResourceUsage(
                memory_usage_bytes=memory_usage,
                memory_limit_bytes=memory_limit,
                cpu_percent=cpu_percent,
            )

        except (requests.RequestException, KeyError, ValueError):
            return None

    def _calculate_cpu_percent(self, stats: dict) -> float:
        """Calculate CPU percentage from Docker stats.

        Uses the same formula as `docker stats`:
        cpu_percent = (cpu_delta / system_delta) * num_cpus * 100

        :param stats: Docker stats dictionary.
        :type stats: dict
        :return: CPU usage percentage.
        :rtype: float
        """
        try:
            cpu_stats = stats.get("cpu_stats", {})
            precpu_stats = stats.get("precpu_stats", {})

            cpu_usage = cpu_stats.get("cpu_usage", {}).get("total_usage", 0)
            precpu_usage = precpu_stats.get("cpu_usage", {}).get("total_usage", 0)
            cpu_delta = cpu_usage - precpu_usage

            system_usage = cpu_stats.get("system_cpu_usage", 0)
            presystem_usage = precpu_stats.get("system_cpu_usage", 0)
            system_delta = system_usage - presystem_usage

            if system_delta > 0 and cpu_delta > 0:
                num_cpus = cpu_stats.get("online_cpus", 1)
                return (cpu_delta / system_delta) * num_cpus * 100.0

            return 0.0

        except (KeyError, TypeError, ZeroDivisionError):
            return 0.0

    def _get_local_stats(self) -> Optional[ResourceUsage]:
        """Get resource stats using psutil for local development.

        :return: Resource usage from psutil, or None if psutil unavailable.
        :rtype: ResourceUsage | None
        """
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            virtual_memory = psutil.virtual_memory()

            return ResourceUsage(
                memory_usage_bytes=memory_info.rss,
                memory_limit_bytes=virtual_memory.total,
                cpu_percent=process.cpu_percent(interval=0.1),
            )

        except ImportError:
            # psutil not available
            return None
        except Exception:
            return None


# Module-level singleton for convenience
_client: Optional[ECSMetadataClient] = None


def get_ecs_metadata_client() -> ECSMetadataClient:
    """Get the singleton ECS metadata client.

    :return: The ECS metadata client instance.
    :rtype: ECSMetadataClient
    """
    global _client
    if _client is None:
        _client = ECSMetadataClient()
    return _client


def get_resource_usage() -> Optional[ResourceUsage]:
    """Convenience function to get current resource usage.

    :return: Current resource usage, or None if unavailable.
    :rtype: ResourceUsage | None
    """
    return get_ecs_metadata_client().get_resource_usage()
