from grafana_foundation_sdk.builders import text, stat, dashboard
from grafana_foundation_sdk.builders import tempo
from grafana_foundation_sdk.models.dashboard import DataSourceRef
from grafana_foundation_sdk.models.common import BigValueColorMode

TRACES_DATASOURCE = "grafanacloud-climatepolicyradar-traces"
TRACES_DATASOURCE_REF = DataSourceRef(
    type_val="grafana",
    uid=TRACES_DATASOURCE
)

PROMETHEUS_DATASOURCE = "grafanacloud-climatepolicyradar-prom"
PROMETHEUS_DATASOURCE_REF = DataSourceRef(
    type_val="grafana",
    uid=PROMETHEUS_DATASOURCE
)

def key_urls(urls: dict[str, str]) -> text.Panel:
    """Returns a text panel with markdown formatted key URLs"""
    content = ""
    for name in urls.keys():
        content += f"- [{name}]({urls[name]})\n"

    return text.Panel() \
            .content(content) \
            .description("Key URLs for further investigation") \
            .display_name("Key links") \
            .title("Key links") \
            .transparent(True) \
            .span(3) \
            .mode("markdown")

def title_panel(title: str) -> text.Panel:
    """Returns a silly formatted title panel"""
    styles = "background: #121FCF;background: linear-gradient(to right, #121FCF 0%, #7FA6CF 88%);-webkit-background-clip: text;-webkit-text-fill-color: transparent;"
    content = f"<h1 style='{styles}'>{title}</h1>"

    return text.Panel() \
            .content(content) \
            .description("Title panel") \
            .display_name(title) \
            .title(title) \
            .transparent(True) \
            .span(24) \
            .height(3) \
            .mode("html")

def get_service_filter(service_name: str, environment: str) -> str:
    return f"{{resource.service.name=\"{service_name}\" && resource.environment=\"{environment}\"}}"
    
def stat_panel(title: str, query: tempo.TempoQuery) -> stat.Panel:
    return stat.Panel() \
            .title(title) \
            .with_target(query) \
            .span(3) \
            .height(3) \
            .color_mode(BigValueColorMode.BACKGROUND_SOLID) \
            .graph_mode('none')
