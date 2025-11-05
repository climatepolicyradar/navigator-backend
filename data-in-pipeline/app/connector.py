from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, HttpUrl

from app.util import get_api_url


class HttpEndpointConfig(BaseModel):
    path: str
    method: Literal["GET", "POST"] = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, str] = Field(default_factory=dict)
    body_template: Optional[str] = None


class PaginationConfig(BaseModel):
    style: Literal["page_number", "offset_limit", "cursor", "link_header"]
    page_param: str = "page"
    size_param: str = "size"
    initial_value: int = 1
    page_size: int = 100
    next_page_path: Optional[str] = None


class BaseConnectorConfig(BaseModel):
    connector_name: str
    source_id: str  # canonical identifier like "litigation/Sabin"

    # Auth  - I don't think this will be necessary at this point

    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: int = 5
    connection_pool_size: int = 10

    checkpoint_storage: Literal["prefect_state", "database", "s3"]
    checkpoint_key_prefix: str

    emit_metrics: bool = True
    log_level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = "INFO"


class NavigatorConnectorConfig(BaseConnectorConfig):
    connector_name = "navigator"

    # API specifics
    base_url: HttpUrl = HttpUrl(
        url=get_api_url(),
    )
    api_version: str = "v1"

    # Pagination (Navigator uses page number + offset)
    initial_page: int = 1
    page_size: int = 100
    max_pages: Optional[int] = None

    endpoints: List[str] = ["documents"]  # What to extract
    modified_since: Optional[datetime] = None  # Delta extraction
    include_deleted: bool = False


# Generic HTTP connector config
class HttpConnectorConfig(BaseConnectorConfig):
    connector_name = "http"

    # Generic HTTP settings
    base_url: HttpUrl
    endpoints: List[HttpEndpointConfig]

    default_headers: Dict[str, str] = Field(default_factory=dict)
    default_params: Dict[str, str] = Field(default_factory=dict)

    pagination: PaginationConfig

    response_parser: Literal["json", "xml", "csv", "raw"] = "json"
    data_path: Optional[str] = None
