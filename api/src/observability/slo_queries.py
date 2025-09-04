"""
Defines consistent queries in TraceQL (ie using otel data) for SLOs
"""
from grafana_foundation_sdk.builders import prometheus, tempo
from observability.common import PROMETHEUS_DATASOURCE_REF, TRACES_DATASOURCE_REF,get_service_filter

def availability_query(service_name: str, environment: str) -> str:
    """
    Returns a query for all requests to a service
    """

    promql = f"""
    (sum
    (
        rate(
            traces_span_metrics_calls_total{{
                span_kind="SPAN_KIND_SERVER", 
                service_name="{service_name}", 
                http_status_code!~"5.."
            }}[5m]
        )
    ) 
    / 
    (
        sum 
        (
            rate(
                traces_span_metrics_calls_total{{
                    span_kind="SPAN_KIND_SERVER", 
                    service_name="{service_name}"
                }}[5m]
            )
        )
    )) * 100"""
     
    return prometheus.Dataquery() \
            .datasource(PROMETHEUS_DATASOURCE_REF) \
            .expr(promql)

def latency_query(service_name: str, environment: str) -> str:
    """
    Returns a query for the average latency of a service
    """

    ## TODO service / service_name is inconsistent here. 
    promql = f"""
        histogram_quantile(0.99, sum(rate(traces_spanmetrics_latency_bucket{{
            span_kind="SPAN_KIND_SERVER", 
            service="{service_name}",
        }}[5m])) by (le))
    """
     
    return prometheus.Dataquery() \
            .datasource(PROMETHEUS_DATASOURCE_REF) \
            .expr(promql)

def error_rate_query(service_name: str, environment: str) -> str:
    """
    Returns a query for the error rate of a service
    """

    traceql = f"""
        {get_service_filter(service_name, environment)}
        && {{span.http.status_code =~ "5.."}}
        | rate()
    """

    return tempo.TempoQuery() \
            .datasource(TRACES_DATASOURCE_REF) \
            .query(traceql)

def throughput_query(service_name: str, environment: str) -> str:
    """
    Returns a query for the throughput of a service
    """

    traceql = f"""
        {get_service_filter(service_name, environment)}
        | rate()
    """

    return tempo.TempoQuery() \
            .datasource(TRACES_DATASOURCE_REF) \
            .query(traceql)
