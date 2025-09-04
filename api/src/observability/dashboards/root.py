"""
Defines the root dashboard -- the single bookmark for engineers to have
"""

from grafana_foundation_sdk.builders.dashboard import Dashboard, Row, ThresholdsConfig
from grafana_foundation_sdk.builders.common import ReduceDataOptions
from grafana_foundation_sdk.models.common import TimeZoneUtc
from grafana_foundation_sdk.models.dashboard import Threshold

from api.telemetry_config import ServiceManifest
from observability.common import stat_panel, title_panel
from observability.slo_queries import availability_query, latency_query, error_rate_query, throughput_query
from observability.registry import ServiceRegistry

def service_row(service: ServiceManifest) -> Row:
    row = Row(f"[TEST] {service.service_name} ({service.service_namespace})") 

    row = row.with_panel(
        stat_panel(
            "Availability (%)",
            availability_query(service.service_name, "production")
        ).unit('percent').decimals(0) \
        .thresholds(
            ThresholdsConfig() \
            .steps([
                Threshold(value=None, color="red"),
                Threshold(value=99, color="orange"),
                Threshold(value=99.9, color="green")
            ])
        )
    )

    row = row.with_panel(
        stat_panel(
            "P99 Latency (s)",
            latency_query(service.service_name, "production")
        ).unit('s') \
        .thresholds(
            ThresholdsConfig() \
            .steps([
                Threshold(value=None, color="green"),
                Threshold(value=0.5, color="orange"),
                Threshold(value=1, color="red")
            ])
        )
    )

    row = row.with_panel(
        stat_panel(
            "Error Rate (rate/s)",
            error_rate_query(service.service_name, "production")
        ) \
        .thresholds(
            ThresholdsConfig() \
            .steps([
                Threshold(value=None, color="green"),
                Threshold(value=1, color="orange"),
                Threshold(value=3, color="red")
            ])
        )
    )

    row = row.with_panel(
        stat_panel(
            "Request rate (rate/s)",
            throughput_query(service.service_name, "production")
        ) \
        .reduce_options(ReduceDataOptions().calcs(["mean"])) \
        .thresholds(
            ThresholdsConfig() \
            .steps([
                Threshold(value=None, color="blue"),
                Threshold(value=0.01, color="green")
            ])
        )
    )

    return row


def root_dashboard(registry: ServiceRegistry) -> Dashboard:
    builder = (
        Dashboard("Root - Navigator Backend")
        .uid("root-navigator-backend")
        .tags([
            "service:navigator",
            "env:staging",
            "team:application",
            "level:high"
        ])
        .refresh("5m")
        .time("now-24h", "now")
        .timezone(TimeZoneUtc) # Per best practice recommendation
    )

    for service in registry.get_services():
        builder = builder.with_row(service_row(service))

    return builder