from enum import Enum


class PaginationStyle(str, Enum):
    PAGE_NUMBER = "page_number"
    OFFSET_LIMIT = "offset_limit"
    CURSOR = "cursor"
    LINK_HEADER = "link_header"


class CheckPointStorageType(str, Enum):
    S3 = "s3"
    DATABASE = "database"
    PREFECT_STATE = "prefect_state"
