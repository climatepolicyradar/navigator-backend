from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel

from app.enums import CheckPointStorageType, PaginationStyle
from app.util import get_api_url


class PaginationConfig(BaseModel):
    """
    Configuration for paginated API requests.

    Defines how pagination is handled when fetching data from an HTTP source.
    Supports multiple pagination styles (e.g. page-number, offset-limit, cursor, link-header),
    along with parameter names and defaults.
    """

    style: PaginationStyle = PaginationStyle.PAGE_NUMBER
    page_param: str = "page"
    size_param: str = "size"
    initial_value: int = 1
    page_size: int = 100
    next_page_path: Optional[str] = None


class HttpBaseConnectorConfig(BaseModel):
    """
    Base configuration shared by all HTTP-based connectors.

    Encapsulates connection, retry, and checkpointing behaviour for any connector
    that interacts with HTTP APIs. Used as the foundation for connector-specific configs.
    """

    base_url: str

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

    pagination: PaginationConfig = PaginationConfig()


class NavigatorConnectorConfig(HttpBaseConnectorConfig):
    """
    Configuration for the Navigator data connector.

    Extends the generic HTTP connector configuration with Navigator-specific (e.g interacting
    with the families and documents endpoints) settings, such as API versioning, pagination,
    and delta extraction options.

    """

    connector_name: str = "navigator"

    base_url: str = get_api_url()
    api_version: str = "v1"

    initial_page: int = 1
    page_size: int = 100
    max_pages: Optional[int] = None

    modified_since: Optional[datetime] = None  # Delta extraction
    include_deleted: bool = False
