from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from app.enums import CheckPointStorageType, PaginationStyle
from app.util import get_api_url


class HttpEndpointConfig(BaseModel):
    path: str
    method: Literal["GET", "POST"] = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, str] = Field(default_factory=dict)
    body_template: Optional[str] = None


class PaginationConfig(BaseModel):
    style: PaginationStyle
    page_param: str = "page"
    size_param: str = "size"
    initial_value: int = 1
    page_size: int = 100
    next_page_path: Optional[str] = None


class BaseConnectorConfig(BaseModel):
    connector_name: str
    source_id: str  # canonical identifier like "litigation/Sabin"

    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: int = 5
    connection_pool_size: int = 10

    # TODO : Implement convention for checkpoint keys and storage APP-1409
    checkpoint_storage: CheckPointStorageType
    checkpoint_key_prefix: str

    emit_metrics: bool = True
    log_level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = "INFO"


class NavigatorConnectorConfig(BaseConnectorConfig):
    connector_name: str = "navigator"

    base_url: str = get_api_url()
    api_version: str = "v1"

    initial_page: int = 1
    page_size: int = 100
    max_pages: Optional[int] = None

    modified_since: Optional[datetime] = None  # Delta extraction
    include_deleted: bool = False


# Generic HTTP connector config
class HttpConnectorConfig(BaseConnectorConfig):
    connector_name: str = "http"

    base_url: str
    endpoints: List[HttpEndpointConfig]

    default_headers: Dict[str, str] = Field(default_factory=dict)
    default_params: Dict[str, str] = Field(default_factory=dict)

    pagination: PaginationConfig

    response_parser: Literal["json", "xml", "csv", "raw"] = "json"
    data_path: Optional[str] = None
