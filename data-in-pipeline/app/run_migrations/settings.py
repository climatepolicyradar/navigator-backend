import logging
import sys

from pydantic_settings import BaseSettings

# Configure logging before anything else - this module is imported early
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,  # Override any existing configuration
)
_LOGGER = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All database connection parameters are required and will be
    validated by pydantic on instantiation.
    """

    # DB connection parameters
    db_master_username: str
    managed_db_password: str
    aurora_writer_endpoint: str
    db_port: str
    db_name: str

    # Connection pool parameters
    statement_timeout: str = "10000"

    # SSL mode for database connections
    # Valid values: disable, allow, prefer, require, verify-ca, verify-full
    # 'prefer' tries SSL but falls back to non-SSL for local dev (validates
    # certs if SSL is used). For production RDS without cert validation,
    # set db_sslmode=require via environment variable.
    db_sslmode: str = "require"


# Pydantic settings are set from the env variables passed in via
# docker / ECS task definition. Missing required fields will raise a
# ValidationError on import.
# pyright: reportCallIssue=false
# Pyright doesn't recognize that BaseSettings loads from environment variables, so it
# flags required fields as missing constructor arguments. This is a false positive.
settings = Settings()
