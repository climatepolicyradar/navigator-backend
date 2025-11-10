import os
from typing import Final, Optional

PROJECT_NAME = "navigator"

STATEMENT_TIMEOUT = os.getenv("STATEMENT_TIMEOUT", 10000)  # ms
SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "")
if not SQLALCHEMY_DATABASE_URI:
    raise RuntimeError("'{DATABASE_URL}' environment variable must be set")

PUBLIC_APP_URL = os.environ["PUBLIC_APP_URL"].rstrip("/")
API_V1_STR = "/api/v1"

# Vespa Config
VESPA_SECRETS_LOCATION: Optional[str] = os.getenv("VESPA_SECRETS_LOCATION", None)
VESPA_URL: str = os.environ["VESPA_URL"]

# Whole database dump
INGEST_CYCLE_START = os.getenv("INGEST_CYCLE_START")
DOCUMENT_CACHE_BUCKET = os.getenv("DOCUMENT_CACHE_BUCKET")
PIPELINE_BUCKET: str = os.getenv("PIPELINE_BUCKET", "not_set")
INGEST_TRIGGER_ROOT: Final = "input"
DEVELOPMENT_MODE: bool = os.getenv("DEVELOPMENT_MODE", "False").lower() == "true"
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
CDN_DOMAIN = os.getenv("CDN_DOMAIN")
ENV = os.getenv("ENV", "development")
VESPA_INSTANCE_URL = os.getenv("VESPA_INSTANCE_URL", "NOTSET")
VESPA_CLOUD_SECRET_TOKEN = os.getenv("VESPA_CLOUD_SECRET_TOKEN", "NOTSET")
