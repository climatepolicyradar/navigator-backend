"""
Defines a dashboard for API availability, assuming instrumented with otel 
"""
from grafana_foundation_sdk.builders.dashboard import Dashboard, Row
from grafana_foundation_sdk.models.common import TimeZoneUtc

from observability.common import key_urls, title_panel, stat_panel, trace_query_rate

def api_availability_dashboard(api_name: str) -> Dashboard:
    builder = (
        Dashboard(f"[TEST] {api_name} API Availability")
        .uid(f"test-{api_name}-api-availability")
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

    return builder

def backend_api_availability_dashboard() -> Dashboard:
    builder = api_availability_dashboard('backend-api') \
                    .with_panel(title_panel("Backend API Availability")) \
                    .with_row(Row("Backend API Availability")) \
                    .with_panel(
                        key_urls({
                            "Log stream": "https://climatepolicyradar.grafana.net/a/grafana-lokiexplore-app/explore/service/navigator-backend/logs?from=now-1h&to=now&var-ds=grafanacloud-logs&var-filters=service_name%7C%3D%7Cnavigator-backend&patterns=%5B%5D&var-lineFormat=&var-fields=environment%7C%3D%7C__CV%CE%A9__%7B%22parser%22:%22mixed%22__gfc__%22value%22:%22production%22%7D,production&var-levels=detected_level%7C%3D%7CERROR&var-levels=detected_level%7C%3D%7CWARN&var-levels=detected_level%7C%3D%7CINFO&var-metadata=&var-jsonFields=&var-patterns=&var-lineFilterV2=&var-lineFilters=&displayedFields=%5B%22msg%22%5D&urlColumns=%5B%22Time%22,%22Line%22,%22msg%22%5D&visualizationType=%22logs%22&var-fieldBy=$__all&var-labelBy=$__all&timezone=browser&var-all-fields=environment%7C%3D%7C__CV%CE%A9__%7B%22parser%22:%22mixed%22__gfc__%22value%22:%22production%22%7D,production&sortOrder=%22Descending%22&prettifyLogMessage=false&wrapLogMessage=false",
                            "High duration requests": "https://climatepolicyradar.grafana.net/a/grafana-exploretraces-app/explore?from=now-6h&to=now&timezone=browser&var-ds=grafanacloud-traces&var-primarySignal=nestedSetParent%3C0&var-filters=resource.service.name%7C%3D%7Cnavigator-backend&var-filters=resource.environment%7C%3D%7Cproduction&var-filters=duration%7C%3E%7C3&var-filters=span:name%7C%21%3D%7Cconnect&var-filters=span:name%7C%21~%7CSELECT%20navigator_2023_05_22&var-metric=rate&var-groupBy=resource.service.name&var-spanListColumns=&var-latencyThreshold=&var-partialLatencyThreshold=&actionView=traceList",
                            "Database queries": "https://climatepolicyradar.grafana.net/a/grafana-exploretraces-app/explore?from=now-6h&to=now&timezone=browser&var-ds=grafanacloud-traces&var-primarySignal=true&var-filters=resource.service.name%7C%3D%7Cnavigator-backend&var-filters=resource.environment%7C%3D%7Cproduction&var-filters=trace:rootName%7C%21%3D%7Cconnect&var-filters=span.db.statement%7C%21%3D%7CSELECT%201&var-metric=rate&var-groupBy=resource.service.name&var-spanListColumns=&var-latencyThreshold=&var-partialLatencyThreshold=&actionView=traceList",
                            "Github": "https://github.com/climatepolicyradar/navigator-backend",
                            "AppRunner service": "https://eu-west-1.console.aws.amazon.com/apprunner/home?region=eu-west-1#/services/dashboard?service_arn=arn%3Aaws%3Aapprunner%3Aeu-west-1%3A532586131621%3Aservice%2Fproduction-backend%2F0a90f1e9d66f4c3e93acb44da9d5d8ef&active_tab=logs",
                            "Deploys": "https://github.com/climatepolicyradar/navigator-infra/actions/workflows/deploy-backend.yml"
                        })
                    ) \
                    .with_panel(
                        stat_panel("Requests", trace_query_rate("navigator-backend", "production"))
                    )
    
    return builder

def geographies_availability_dashboard() -> Dashboard:
    builder = api_availability_dashboard('geographies-api') \
                    .with_panel(title_panel("Geographies API Availability")) \
                    .with_row(Row("Geographies API Availability")) \
                    .with_panel(
                        key_urls({
                            'OpenAPI': '...',
                            'Slow requests': '...',
                            'AppRunner service': '...',
                            'Github': 'https://github.com/climatepolicyradar/navigator-backend',
                            'etc': 'https://etc.com'
                        })
                    )
    
    return builder
